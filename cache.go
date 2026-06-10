package httpcache

import (
	"bufio"
	"bytes"
	"container/list"
	"encoding/gob"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"net/http"
	"net/textproto"
	"os"
	pathutil "path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/soulteary/vfs-kit"
)

// hash64Pool reuses FNV-1a 64-bit hashers to reduce allocations in hashKey.
var hash64Pool = sync.Pool{
	New: func() interface{} { return fnv.New64a() },
}

const (
	headerPrefix = "header/"
	bodyPrefix   = "body/"
	formatPrefix = "v1/"

	lruEntrySnapshotsFile    = "_httpcache_kit_vfs_lru_entry_snapshots.gob"
	lruEntrySnapshotsVersion = "v1"
	lruEntrySnapshotsMagic   = "e9a22f25-dbb9-4c9f-b4ad-7d5d174c1789"
)

// Returned when a resource doesn't exist
var ErrNotFoundInCache = errors.New("not found in cache")

type Cache interface {
	Header(key string) (Header, error)
	Store(res *Resource, keys ...string) error
	Retrieve(key string) (*Resource, error)
	Invalidate(keys ...string)
	Freshen(res *Resource, keys ...string) error
}

// ExtendedCache extends Cache with management capabilities
type ExtendedCache interface {
	Cache
	// Stats returns current cache statistics
	Stats() CacheStats
	// Cleanup runs a manual cleanup cycle
	Cleanup() CleanupResult
	// Purge removes all cached items
	Purge() error
	// Close stops the cache and cleanup goroutines
	Close() error
}

// CacheStats holds cache statistics
type CacheStats struct {
	// TotalSize is the total size of cached items in bytes
	TotalSize int64
	// ItemCount is the number of cached items
	ItemCount int
	// StaleCount is the number of stale map entries
	StaleCount int
	// HitCount is the number of cache hits
	HitCount int64
	// MissCount is the number of cache misses
	MissCount int64
}

// CleanupResult holds the result of a cleanup operation
type CleanupResult struct {
	// RemovedItems is the number of items removed
	RemovedItems int
	// RemovedBytes is the number of bytes freed
	RemovedBytes int64
	// RemovedStaleEntries is the number of stale map entries removed
	RemovedStaleEntries int
	// Duration is how long the cleanup took
	Duration time.Duration
}

// cacheEntry tracks metadata for a cached item
type cacheEntry struct {
	key        string
	hashedKey  string
	size       int64
	storedAt   time.Time
	accessedAt time.Time
	element    *list.Element // for LRU tracking
}

// cache provides a storage mechanism for cached Resources
type cache struct {
	fs     vfs.VFS
	config *CacheConfig

	// stale map with mutex protection
	stale      map[string]time.Time
	staleMutex sync.RWMutex

	// LRU tracking
	lruList   *list.List             // front = most recently used
	lruIndex  map[string]*cacheEntry // hashedKey -> entry
	lruMutex  sync.RWMutex
	totalSize int64

	// Statistics
	hitCount  int64
	missCount int64
	statMutex sync.RWMutex

	// Cleanup control
	stopChan  chan struct{}
	stopped   bool
	closeOnce sync.Once
}

var _ Cache = (*cache)(nil)
var _ ExtendedCache = (*cache)(nil)

type Header struct {
	http.Header
	StatusCode int
}

// NewVFSCache returns a cache backend off the provided VFS
func NewVFSCache(fs vfs.VFS) Cache {
	return NewVFSCacheWithConfig(fs, nil)
}

// NewVFSCacheWithConfig returns a cache backend with custom configuration
func NewVFSCacheWithConfig(fs vfs.VFS, config *CacheConfig) ExtendedCache {
	if config == nil {
		config = DefaultCacheConfig()
	}
	config.Validate()

	c := &cache{
		fs:       fs,
		config:   config,
		stale:    make(map[string]time.Time),
		lruList:  list.New(),
		lruIndex: make(map[string]*cacheEntry),
		stopChan: make(chan struct{}),
	}

	// Start cleanup goroutine if interval is configured
	if config.CleanupInterval > 0 {
		go c.cleanupLoop()
	}

	return c
}

// NewMemoryCache returns an ephemeral cache in memory
func NewMemoryCache() Cache {
	return NewVFSCache(vfs.Memory())
}

// NewMemoryCacheWithConfig returns an ephemeral cache with custom configuration
func NewMemoryCacheWithConfig(config *CacheConfig) ExtendedCache {
	return NewVFSCacheWithConfig(vfs.Memory(), config)
}

// NewDiskCache returns a disk-backed cache
func NewDiskCache(dir string) (Cache, error) {
	return NewDiskCacheWithConfig(dir, nil)
}

// NewDiskCacheWithConfig returns a disk-backed cache with custom configuration
func NewDiskCacheWithConfig(dir string, config *CacheConfig) (ExtendedCache, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, err
	}
	fs, err := vfs.FS(dir)
	if err != nil {
		return nil, err
	}
	chfs, err := vfs.Chroot("/", fs)
	if err != nil {
		return nil, err
	}
	extCache := NewVFSCacheWithConfig(chfs, config)

	// Scan existing cache files to rebuild LRU index
	if c, ok := extCache.(*cache); ok {
		if err := c.scanExistingCache(); err != nil {
			debugf("warning: failed to scan existing cache: %v", err)
		}
	}

	return extCache, nil
}

// Entry snapshot
type entrySnapshot struct {
	Key        string
	HashedKey  string
	AccessedAt time.Time
}

func (s *entrySnapshot) updateToCacheEntry(e *cacheEntry) (updated bool) {
	if e != nil && s.Key == e.key && s.HashedKey == e.hashedKey {
		e.accessedAt = s.AccessedAt
		return true
	} else {
		return false
	}
}
func cacheEntryToSnapshot(e *cacheEntry) *entrySnapshot {
	if e != nil {
		return &entrySnapshot{
			Key:        e.key,
			HashedKey:  e.hashedKey,
			AccessedAt: e.accessedAt,
		}
	} else {
		return nil
	}

}

// serializeLRU serializes the current LRU index to a snapshot file on the VFS.
// It gob-encodes all entry snapshots, compresses with zstd, and writes to
// lruEntrySnapshotsFile with a magic header and version prefix.
func (c *cache) serializeLRU() error {
	c.lruMutex.RLock()
	snapshots := make([]*entrySnapshot, 0, len(c.lruIndex))
	for _, entry := range c.lruIndex {
		if s := cacheEntryToSnapshot(entry); s != nil {
			snapshots = append(snapshots, s)
		}
	}
	c.lruMutex.RUnlock()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(snapshots); err != nil {
		return fmt.Errorf("failed to gob encode LRU snapshots: %w", err)
	}

	compressed := &bytes.Buffer{}
	encZstd, err := zstd.NewWriter(compressed)
	if err != nil {
		return fmt.Errorf("failed to create zstd writer: %w", err)
	}
	if _, err := encZstd.Write(buf.Bytes()); err != nil {
		_ = encZstd.Close()
		return fmt.Errorf("failed to compress LRU snapshots: %w", err)
	}
	if err := encZstd.Close(); err != nil {
		return fmt.Errorf("failed to close zstd writer: %w", err)
	}

	header := &bytes.Buffer{}
	header.WriteString(lruEntrySnapshotsMagic)
	header.WriteString(lruEntrySnapshotsVersion)

	r := io.MultiReader(bytes.NewReader(header.Bytes()), bytes.NewReader(compressed.Bytes()))
	if _, err := c.vfsWrite(lruEntrySnapshotsFile, r); err != nil {
		return fmt.Errorf("failed to write LRU snapshot file: %w", err)
	}

	return nil
}

// deserializeLRU reads and restores the LRU snapshot from the VFS.
// It validates the magic header and version, then delegates integrity
// verification to zstd decompression. The snapshot file is removed
// regardless of success or failure.
func (c *cache) deserializeLRU() (map[string]*entrySnapshot, error) {
	f, err := c.fs.Open(lruEntrySnapshotsFile)
	if err != nil {
		if vfs.IsNotExist(err) {
			return nil, err
		}
		return nil, fmt.Errorf("failed to open LRU snapshot file: %w", err)
	}

	// Open succeeded: schedule deletion immediately to prevent stale files
	defer func() { _ = c.fs.Remove(lruEntrySnapshotsFile) }()

	data, err := io.ReadAll(f)
	_ = f.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to read LRU snapshot file: %w", err)
	}

	// Validate magic header
	magicLen := len(lruEntrySnapshotsMagic)
	if len(data) < magicLen+1 {
		return nil, fmt.Errorf("LRU snapshot file too short")
	}
	if string(data[:magicLen]) != lruEntrySnapshotsMagic {
		return nil, fmt.Errorf("LRU snapshot file magic mismatch")
	}

	// Parse version (fixed length from constant)
	versionLen := len(lruEntrySnapshotsVersion)
	offset := magicLen
	if len(data) < offset+versionLen {
		return nil, fmt.Errorf("LRU snapshot file too short for version")
	}
	version := string(data[offset : offset+versionLen])
	if version != lruEntrySnapshotsVersion {
		return nil, fmt.Errorf("LRU snapshot version mismatch: got %s, want %s", version, lruEntrySnapshotsVersion)
	}
	offset += versionLen

	// Remaining data is the zstd payload
	payload := data[offset:]

	// Decompress with zstd (integrity verification handled by zstd)
	decZstd, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd reader: %w", err)
	}
	defer decZstd.Close()

	decompressed, err := decZstd.DecodeAll(payload, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress LRU snapshot: %w", err)
	}

	// Gob decode
	var snapshots []*entrySnapshot
	dec := gob.NewDecoder(bytes.NewReader(decompressed))
	if err := dec.Decode(&snapshots); err != nil {
		return nil, fmt.Errorf("failed to gob decode LRU snapshots: %w", err)
	}

	// Convert to map
	result := make(map[string]*entrySnapshot, len(snapshots))
	for _, s := range snapshots {
		if s != nil {
			result[s.HashedKey] = s
		}
	}

	return result, nil
}

// scanExistingCache scans the cache directory for existing cached files
// and rebuilds the LRU index. This is called on startup for disk-backed caches.
func (c *cache) scanExistingCache() error {
	start := Clock()
	scannedFiles := 0
	recoveredSnapshots := 0
	var totalSize int64

	// Try to deserialize cache entry
	useSnapshots := false
	snapshots, err := c.deserializeLRU()
	if err == nil {
		useSnapshots = true
	} else {
		debugf("Failed to deserialize LRU snapshot due to: %s", err.Error())
	}

	// Scan body files (they contain the actual cached data)
	entryList := []*cacheEntry{}
	bodyDir := bodyPrefix + formatPrefix
	if err := c.scanDirectory(bodyDir, func(hashedKey string, info os.FileInfo) {
		// Create entry for this cached item
		entry := &cacheEntry{
			key:        hashedKey, // We don't have the original key, use hashed key
			hashedKey:  hashedKey,
			size:       info.Size(),
			storedAt:   info.ModTime(),
			accessedAt: info.ModTime(),
		}

		if useSnapshots {
			if s, ok := snapshots[entry.hashedKey]; ok {
				if s.updateToCacheEntry(entry) {
					recoveredSnapshots++
				}
			}
		}

		// Check if corresponding header file exists and add its size
		headerPath := headerPrefix + formatPrefix + hashedKey
		if headerInfo, err := c.fs.Stat(headerPath); err == nil {
			entry.size += headerInfo.Size()
		}

		// Add to temp list for sorting
		entryList = append(entryList, entry)

		scannedFiles++
		totalSize += entry.size
	}); err != nil {
		return err
	}

	// Sort entries from new to old
	sort.Slice(entryList, func(i, j int) bool {
		return entryList[i].accessedAt.After(entryList[j].accessedAt)
	})
	// Add to LRU tracking (older to last)
	c.lruMutex.Lock()
	for _, e := range entryList {
		e.element = c.lruList.PushBack(e)
		c.lruIndex[e.hashedKey] = e
		c.totalSize += e.size
	}
	c.lruMutex.Unlock()

	duration := Clock().Sub(start)
	if scannedFiles > 0 {
		debugf("scanned %d existing cache files (%d bytes) in %s, with %d recovered",
			scannedFiles, totalSize, duration, recoveredSnapshots)
	}

	return nil
}

// scanDirectory scans a directory for cached files and calls the callback for each
func (c *cache) scanDirectory(dir string, callback func(hashedKey string, info os.FileInfo)) error {
	files, err := c.fs.ReadDir(dir)
	if err != nil {
		if vfs.IsNotExist(err) {
			return nil // Directory doesn't exist yet, that's fine
		}
		return err
	}

	for _, info := range files {
		if info.IsDir() {
			continue
		}
		// The filename is the hashed key
		hashedKey := info.Name()
		callback(hashedKey, info)
	}

	return nil
}

func (c *cache) vfsWrite(path string, r io.Reader) (int64, error) {
	if err := vfs.MkdirAll(c.fs, pathutil.Dir(path), 0700); err != nil {
		return 0, fmt.Errorf("failed to create cache directory for %q: %w", path, err)
	}
	f, err := c.fs.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return 0, fmt.Errorf("failed to open cache file %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	n, err := io.Copy(f, r)
	if err != nil {
		return 0, fmt.Errorf("failed to write cache file %q: %w", path, err)
	}
	return n, nil
}

// Retrieve the Status and Headers for a given key path
func (c *cache) Header(key string) (Header, error) {
	path := headerPrefix + formatPrefix + hashKey(key)
	f, err := c.fs.Open(path)
	if err != nil {
		if vfs.IsNotExist(err) {
			return Header{}, ErrNotFoundInCache
		}
		return Header{}, fmt.Errorf("failed to open header file %q for key %q: %w", path, key, err)
	}
	defer func() { _ = f.Close() }()

	h, err := readHeaders(bufio.NewReader(f))
	if err != nil {
		return Header{}, fmt.Errorf("failed to read headers from %q for key %q: %w", path, key, err)
	}
	return h, nil
}

// Store a resource against a number of keys.
// When multiple keys are given (e.g. primary + Vary key), the same body is written
// for each key by using a fresh reader per key (buf is consumed on first read).
func (c *cache) Store(res *Resource, keys ...string) error {
	var buf = &bytes.Buffer{}

	if length, err := strconv.ParseInt(res.Header().Get("Content-Length"), 10, 64); err == nil {
		if _, err = io.CopyN(buf, res, length); err != nil {
			return err
		}
	} else if _, err = io.Copy(buf, res); err != nil {
		return err
	}

	bodyBytes := buf.Bytes()
	bodySize := int64(len(bodyBytes))

	for _, key := range keys {
		// Remove from stale map
		c.staleMutex.Lock()
		delete(c.stale, key)
		c.staleMutex.Unlock()

		hashedKey := hashKey(key)

		// Check if we need to evict items before storing
		c.evictIfNeeded(bodySize)

		// Use a fresh reader per key: io.Reader is consumed by storeBody, so later keys
		// would get empty body if we reused the same buffer.
		written, err := c.storeBody(bytes.NewReader(bodyBytes), key)
		if err != nil {
			return err
		}

		headerBytes, err := c.storeHeader(res.Status(), res.Header(), key)
		if err != nil {
			return err
		}

		// Update LRU tracking
		c.trackEntry(key, hashedKey, written+headerBytes)
	}

	return nil
}

func (c *cache) storeBody(r io.Reader, key string) (int64, error) {
	n, err := c.vfsWrite(bodyPrefix+formatPrefix+hashKey(key), r)
	if err != nil {
		return 0, fmt.Errorf("failed to store body for key %q: %w", key, err)
	}
	return n, nil
}

func (c *cache) storeHeader(code int, h http.Header, key string) (int64, error) {
	hb := &bytes.Buffer{}
	fmt.Fprintf(hb, "HTTP/1.1 %d %s\r\n", code, http.StatusText(code))
	if err := headersToWriter(h, hb); err != nil {
		return 0, fmt.Errorf("failed to serialize headers for key %q: %w", key, err)
	}
	n, err := c.vfsWrite(headerPrefix+formatPrefix+hashKey(key), bytes.NewReader(hb.Bytes()))
	if err != nil {
		return 0, fmt.Errorf("failed to store header for key %q: %w", key, err)
	}
	return n, nil
}

// Retrieve returns a cached Resource for the given key
func (c *cache) Retrieve(key string) (*Resource, error) {
	hashedKey := hashKey(key)
	bodyPath := bodyPrefix + formatPrefix + hashedKey
	f, err := c.fs.Open(bodyPath)
	if err != nil {
		if vfs.IsNotExist(err) {
			c.recordMiss()
			return nil, ErrNotFoundInCache
		}
		return nil, fmt.Errorf("failed to open body file %q for key %q: %w", bodyPath, key, err)
	}
	h, err := c.Header(key)
	if err != nil {
		_ = f.Close()
		if err == ErrNotFoundInCache {
			c.recordMiss()
			return nil, ErrNotFoundInCache
		}
		return nil, fmt.Errorf("failed to retrieve header for key %q: %w", key, err)
	}
	res := NewResource(h.StatusCode, f, h.Header)

	// Check stale map with proper locking
	c.staleMutex.RLock()
	staleTime, exists := c.stale[key]
	c.staleMutex.RUnlock()

	if exists {
		if !res.DateAfter(staleTime) {
			debugf("stale marker of %s found", staleTime)
			res.MarkStale()
		}
	}

	// Update LRU access time
	c.touchEntry(hashedKey)
	c.recordHit()

	return res, nil
}

func (c *cache) Invalidate(keys ...string) {
	debugf("invalidating %q", keys)
	c.staleMutex.Lock()
	defer c.staleMutex.Unlock()
	now := Clock()
	for _, key := range keys {
		c.stale[key] = now
	}
}

func (c *cache) Freshen(res *Resource, keys ...string) error {
	for _, key := range keys {
		if h, err := c.Header(key); err == nil {
			if h.StatusCode == res.Status() && headersEqual(h.Header, res.Header()) {
				debugf("freshening key %s", key)
				if _, err := c.storeHeader(h.StatusCode, res.Header(), key); err != nil {
					return fmt.Errorf("failed to freshen header for key %q: %w", key, err)
				}
			} else {
				debugf("freshen failed, invalidating %s", key)
				c.Invalidate(key)
			}
		}
	}
	return nil
}

func hashKey(key string) string {
	h := hash64Pool.Get().(hash.Hash64)
	defer func() {
		h.Reset()
		hash64Pool.Put(h)
	}()
	if _, err := h.Write([]byte(key)); err != nil {
		return "unable-to-calculate"
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func readHeaders(r *bufio.Reader) (Header, error) {
	tp := textproto.NewReader(r)
	line, err := tp.ReadLine()
	if err != nil {
		return Header{}, err
	}

	f := strings.SplitN(line, " ", 3)
	if len(f) < 2 {
		return Header{}, fmt.Errorf("malformed HTTP response: %s", line)
	}
	statusCode, err := strconv.Atoi(f[1])
	if err != nil {
		return Header{}, fmt.Errorf("malformed HTTP status code: %s", f[1])
	}

	mimeHeader, err := tp.ReadMIMEHeader()
	if err != nil {
		return Header{}, err
	}
	return Header{StatusCode: statusCode, Header: http.Header(mimeHeader)}, nil
}

func headersToWriter(h http.Header, w io.Writer) error {
	if err := h.Write(w); err != nil {
		return err
	}
	// ReadMIMEHeader expects a trailing newline
	_, err := w.Write([]byte("\r\n"))
	return err
}

// LRU tracking methods

// trackEntry adds or updates an entry in the LRU index
func (c *cache) trackEntry(key, hashedKey string, size int64) {
	c.lruMutex.Lock()
	defer c.lruMutex.Unlock()

	now := Clock()

	// Check if entry already exists
	if entry, exists := c.lruIndex[hashedKey]; exists {
		// Update existing entry
		c.totalSize -= entry.size
		entry.size = size
		entry.storedAt = now
		entry.accessedAt = now
		c.totalSize += size
		// Move to front of LRU list
		c.lruList.MoveToFront(entry.element)
	} else {
		// Create new entry
		entry := &cacheEntry{
			key:        key,
			hashedKey:  hashedKey,
			size:       size,
			storedAt:   now,
			accessedAt: now,
		}
		entry.element = c.lruList.PushFront(entry)
		c.lruIndex[hashedKey] = entry
		c.totalSize += size
	}
}

// touchEntry updates the access time and moves entry to front of LRU list
func (c *cache) touchEntry(hashedKey string) {
	c.lruMutex.Lock()
	defer c.lruMutex.Unlock()

	if entry, exists := c.lruIndex[hashedKey]; exists {
		entry.accessedAt = Clock()
		c.lruList.MoveToFront(entry.element)
	}
}

// removeEntry removes an entry from the cache and LRU tracking
func (c *cache) removeEntry(entry *cacheEntry) error {
	// Remove from filesystem
	bodyPath := bodyPrefix + formatPrefix + entry.hashedKey
	headerPath := headerPrefix + formatPrefix + entry.hashedKey

	if err := c.fs.Remove(bodyPath); err != nil && !vfs.IsNotExist(err) {
		debugf("failed to remove body file %s: %v", bodyPath, err)
	}
	if err := c.fs.Remove(headerPath); err != nil && !vfs.IsNotExist(err) {
		debugf("failed to remove header file %s: %v", headerPath, err)
	}

	// Remove from LRU tracking (assumes lruMutex is already held)
	c.lruList.Remove(entry.element)
	delete(c.lruIndex, entry.hashedKey)
	c.totalSize -= entry.size

	// Remove from stale map
	c.staleMutex.Lock()
	delete(c.stale, entry.key)
	c.staleMutex.Unlock()

	return nil
}

// evictIfNeeded evicts the least recently used items if cache size exceeds limit
func (c *cache) evictIfNeeded(additionalSize int64) {
	if c.config.MaxSize <= 0 {
		return
	}

	c.lruMutex.Lock()
	defer c.lruMutex.Unlock()

	targetSize := c.config.MaxSize - additionalSize
	if targetSize < 0 {
		targetSize = 0
	}

	// Evict from the back (least recently used)
	for c.totalSize > targetSize && c.lruList.Len() > 0 {
		elem := c.lruList.Back()
		if elem == nil {
			break
		}
		entry := elem.Value.(*cacheEntry)
		debugf("evicting LRU entry: %s (size: %d, accessed: %s)",
			entry.key, entry.size, entry.accessedAt.Format(time.RFC3339))
		_ = c.removeEntry(entry)

		// Record eviction metric
		if getDefaultMetrics() != nil {
			getDefaultMetrics().RecordCacheEviction("lru")
		}
	}
}

// Cleanup methods

// cleanupLoop runs periodic cleanup
func (c *cache) cleanupLoop() {
	ticker := time.NewTicker(c.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			result := c.Cleanup()
			if result.RemovedItems > 0 || result.RemovedStaleEntries > 0 {
				debugf("cleanup completed: removed %d items (%d bytes), %d stale entries in %s",
					result.RemovedItems, result.RemovedBytes, result.RemovedStaleEntries, result.Duration)
			}
			// entry serialize to vfs
			if err := c.serializeLRU(); err != nil {
				debugf("warning: failed to serialize LRU metadata: %s", err.Error())
			}
		case <-c.stopChan:
			return
		}
	}
}

// Cleanup runs a manual cleanup cycle
func (c *cache) Cleanup() CleanupResult {
	start := Clock()
	result := CleanupResult{}

	// Clean up stale map entries
	result.RemovedStaleEntries = c.cleanupStaleMap()

	// Clean up TTL-expired entries
	if c.config.TTL > 0 {
		removed, bytes := c.cleanupTTLExpired()
		result.RemovedItems += removed
		result.RemovedBytes += bytes
	}

	// Enforce size limit
	if c.config.MaxSize > 0 {
		removed, bytes := c.enforceMaxSize()
		result.RemovedItems += removed
		result.RemovedBytes += bytes
	}

	result.Duration = Clock().Sub(start)

	// Record cleanup duration metric
	if getDefaultMetrics() != nil {
		getDefaultMetrics().RecordCleanupDuration(result.Duration.Seconds())
		// Update cache stats metrics
		stats := c.Stats()
		getDefaultMetrics().UpdateCacheStats(stats)
	}

	return result
}

// cleanupStaleMap removes old stale map entries
func (c *cache) cleanupStaleMap() int {
	c.staleMutex.Lock()
	defer c.staleMutex.Unlock()

	cutoff := Clock().Add(-c.config.StaleMapTTL)
	removed := 0

	for key, staleTime := range c.stale {
		if staleTime.Before(cutoff) {
			delete(c.stale, key)
			removed++
		}
	}

	return removed
}

// cleanupTTLExpired removes items that have exceeded their TTL
func (c *cache) cleanupTTLExpired() (int, int64) {
	c.lruMutex.Lock()
	defer c.lruMutex.Unlock()

	cutoff := Clock().Add(-c.config.TTL)
	removed := 0
	var bytesRemoved int64

	// Iterate from back (oldest) to front
	for elem := c.lruList.Back(); elem != nil; {
		entry := elem.Value.(*cacheEntry)
		prev := elem.Prev()

		if entry.storedAt.Before(cutoff) {
			debugf("removing TTL-expired entry: %s (stored: %s)",
				entry.key, entry.storedAt.Format(time.RFC3339))
			bytesRemoved += entry.size
			_ = c.removeEntry(entry)
			removed++

			// Record eviction metric
			if getDefaultMetrics() != nil {
				getDefaultMetrics().RecordCacheEviction("ttl")
			}
		}

		elem = prev
	}

	return removed, bytesRemoved
}

// enforceMaxSize ensures cache doesn't exceed max size
func (c *cache) enforceMaxSize() (int, int64) {
	c.lruMutex.Lock()
	defer c.lruMutex.Unlock()

	removed := 0
	var bytesRemoved int64

	for c.totalSize > c.config.MaxSize && c.lruList.Len() > 0 {
		elem := c.lruList.Back()
		if elem == nil {
			break
		}
		entry := elem.Value.(*cacheEntry)
		debugf("enforcing max size, removing: %s (size: %d)",
			entry.key, entry.size)
		bytesRemoved += entry.size
		_ = c.removeEntry(entry)
		removed++

		// Record eviction metric
		if getDefaultMetrics() != nil {
			getDefaultMetrics().RecordCacheEviction("size_limit")
		}
	}

	return removed, bytesRemoved
}

// Statistics methods

func (c *cache) recordHit() {
	c.statMutex.Lock()
	c.hitCount++
	c.statMutex.Unlock()
}

func (c *cache) recordMiss() {
	c.statMutex.Lock()
	c.missCount++
	c.statMutex.Unlock()
}

// Stats returns current cache statistics
func (c *cache) Stats() CacheStats {
	c.lruMutex.RLock()
	totalSize := c.totalSize
	itemCount := c.lruList.Len()
	c.lruMutex.RUnlock()

	c.staleMutex.RLock()
	staleCount := len(c.stale)
	c.staleMutex.RUnlock()

	c.statMutex.RLock()
	hitCount := c.hitCount
	missCount := c.missCount
	c.statMutex.RUnlock()

	return CacheStats{
		TotalSize:  totalSize,
		ItemCount:  itemCount,
		StaleCount: staleCount,
		HitCount:   hitCount,
		MissCount:  missCount,
	}
}

// Purge removes all cached items
func (c *cache) Purge() error {
	c.lruMutex.Lock()
	defer c.lruMutex.Unlock()

	// Remove all entries
	for elem := c.lruList.Front(); elem != nil; {
		entry := elem.Value.(*cacheEntry)
		next := elem.Next()
		_ = c.removeEntry(entry)
		elem = next
	}

	// Clear stale map
	c.staleMutex.Lock()
	c.stale = make(map[string]time.Time)
	c.staleMutex.Unlock()

	return nil
}

// Close stops the cache and cleanup goroutines.
// Safe to call multiple times; only the first call performs the shutdown.
func (c *cache) Close() error {
	c.closeOnce.Do(func() {
		c.stopped = true
		close(c.stopChan)

		// entry serialize to vfs
		if err := c.serializeLRU(); err != nil {
			debugf("warning: failed to serialize LRU metadata: %s", err.Error())
		}
	})
	return nil
}
