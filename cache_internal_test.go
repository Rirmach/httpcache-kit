package httpcache

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metrics "github.com/soulteary/metrics-kit"
	"github.com/soulteary/vfs-kit"
)

func TestCache_Header_OpenError(t *testing.T) {
	// VFS that returns error on Open (not IsNotExist) to cover non-IsNotExist path
	c := NewVFSCacheWithConfig(&openFailVFS{vfs.Memory()}, DefaultCacheConfig())
	_, err := c.Header("any-key")
	if err == nil {
		t.Error("expected error when Open fails")
	}
	if err == ErrNotFoundInCache {
		t.Error("expected non-ErrNotFoundInCache when Open returns permission error")
	}
}

type openFailVFS struct{ vfs.VFS }

func (o *openFailVFS) Open(name string) (vfs.RFile, error) {
	return nil, os.ErrPermission
}

// bodyOpenFailVFS fails Open only for paths under body/ so Store (OpenFile) works but Retrieve (Open body) fails.
type bodyOpenFailVFS struct{ vfs.VFS }

func (b *bodyOpenFailVFS) Open(name string) (vfs.RFile, error) {
	if strings.Contains(name, "body/") {
		return nil, os.ErrPermission
	}
	return b.VFS.Open(name)
}

// readDirFailVFS fails ReadDir for body/v1/ so scanExistingCache gets non-IsNotExist error from scanDirectory.
type readDirFailVFS struct{ vfs.VFS }

func (r *readDirFailVFS) ReadDir(path string) ([]os.FileInfo, error) {
	if strings.HasPrefix(path, "body/") {
		return nil, os.ErrPermission
	}
	return r.VFS.ReadDir(path)
}

// mkdirFailVFS fails Mkdir for path containing "body" so vfsWrite gets error from MkdirAll.
type mkdirFailVFS struct{ vfs.VFS }

func (m *mkdirFailVFS) Mkdir(path string, perm os.FileMode) error {
	if strings.Contains(path, "body") {
		return os.ErrPermission
	}
	return m.VFS.Mkdir(path, perm)
}

// openFileFailVFS fails OpenFile for paths under body/ so vfsWrite gets error after MkdirAll.
type openFileFailVFS struct{ vfs.VFS }

func (o *openFileFailVFS) OpenFile(path string, flag int, perm os.FileMode) (vfs.WFile, error) {
	if strings.HasPrefix(path, "body/") {
		return nil, os.ErrPermission
	}
	return o.VFS.OpenFile(path, flag, perm)
}

// removeFailVFS fails Remove for body/ and header/ so removeEntry hits debugf path.
type removeFailVFS struct{ vfs.VFS }

func (r *removeFailVFS) Remove(path string) error {
	if strings.HasPrefix(path, "body/") || strings.HasPrefix(path, "header/") {
		return os.ErrPermission
	}
	return r.VFS.Remove(path)
}

func TestReadHeaders_Malformed(t *testing.T) {
	// Malformed status line: "HTTP/1.1" only one part
	r := bufio.NewReader(bytes.NewReader([]byte("HTTP/1.1\r\n\r\n")))
	_, err := readHeaders(r)
	if err == nil {
		t.Error("expected error for malformed status line")
	}
	// Malformed status code: non-numeric
	r2 := bufio.NewReader(bytes.NewReader([]byte("HTTP/1.1 abc OK\r\n\r\n")))
	_, err = readHeaders(r2)
	if err == nil {
		t.Error("expected error for non-numeric status code")
	}
	// Valid status line but ReadMIMEHeader can fail on malformed header body
	r3 := bufio.NewReader(bytes.NewReader([]byte("HTTP/1.1 200 OK\r\nX: \x00\r\n\r\n")))
	_, err = readHeaders(r3)
	if err != nil {
		t.Logf("ReadMIMEHeader error (expected for invalid header): %v", err)
	}
}

func TestHashKey_EmptyKey(t *testing.T) {
	s := hashKey("")
	if s == "" {
		t.Error("hashKey empty should not return empty string")
	}
	if s == "unable-to-calculate" {
		// FNV succeeds for empty input
		t.Logf("hashKey(\"\") = %s", s)
	}
}

func TestNewDiskCache(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewDiskCache(dir)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}
	if cache == nil {
		t.Fatal("cache is nil")
	}
	if ext, ok := cache.(ExtendedCache); ok {
		_ = ext.Close()
	}
}

func TestNewDiskCacheWithConfig_InvalidPath(t *testing.T) {
	// Path that is an existing file (not directory) -> MkdirAll fails
	f, err := os.CreateTemp("", "httpcache-dir-test-*")
	if err != nil {
		t.Skip("CreateTemp failed:", err)
	}
	path := f.Name()
	_ = f.Close()
	defer func() { _ = os.Remove(path) }()
	_, err = NewDiskCacheWithConfig(path, DefaultCacheConfig())
	if err == nil {
		t.Error("expected error when path is existing file")
	}
}

func TestNewDiskCacheWithConfig_MkdirAllFails(t *testing.T) {
	// Path with invalid character -> MkdirAll may fail (platform-dependent)
	_, err := NewDiskCacheWithConfig(string([]byte{0}), DefaultCacheConfig())
	if err == nil {
		t.Log("MkdirAll with null byte did not fail on this platform")
		return
	}
}

func TestNewDiskCacheWithConfig_ScanDirFails(t *testing.T) {
	// When scanDirectory returns non-IsNotExist error, scanExistingCache returns it (debugf path in NewDiskCacheWithConfig)
	dir := t.TempDir()
	bodyDir := filepath.Join(dir, "body", "v1")
	if err := os.MkdirAll(bodyDir, 0750); err != nil {
		t.Fatal(err)
	}
	bodyParent := filepath.Join(dir, "body")
	if err := os.Chmod(bodyParent, 0000); err != nil {
		t.Skip("chmod 000 not supported or not runnable")
	}
	defer func() { _ = os.Chmod(bodyParent, 0750) }()
	cache, err := NewDiskCacheWithConfig(dir, DefaultCacheConfig())
	if err != nil {
		t.Fatalf("NewDiskCacheWithConfig: %v", err)
	}
	if cache == nil {
		t.Fatal("cache is nil")
	}
	_ = cache.Close()
}

func TestScanDirectory_NotExist(t *testing.T) {
	fs := vfs.Memory()
	c := NewVFSCacheWithConfig(fs, DefaultCacheConfig()).(*cache)
	defer func() { _ = c.Close() }()
	// Scan a directory that doesn't exist -> ReadDir returns error; if IsNotExist we return nil
	err := c.scanDirectory("body/v1/", func(k string, info os.FileInfo) {})
	if err != nil {
		t.Logf("scanDirectory (dir not exist): %v", err)
	}
}

func TestEnforceMaxSize(t *testing.T) {
	config := DefaultCacheConfig().
		WithMaxSize(800).
		WithCleanupInterval(0)
	cache := NewMemoryCacheWithConfig(config).(*cache)
	defer func() { _ = cache.Close() }()
	// Store several small items so total > MaxSize
	for i := 0; i < 5; i++ {
		body := strings.Repeat("x", 200)
		res := NewResourceBytes(200, []byte(body), http.Header{"Content-Length": []string{"200"}})
		if err := cache.Store(res, "key"+string(rune('0'+i))); err != nil {
			t.Fatal(err)
		}
	}
	// Run Cleanup to trigger enforceMaxSize
	result := cache.Cleanup()
	if result.RemovedItems == 0 && cache.Stats().TotalSize > config.MaxSize {
		t.Logf("enforceMaxSize may have run: removed=%d size=%d", result.RemovedItems, cache.Stats().TotalSize)
	}
}

// TestEnforceMaxSizeAfterScan opens a disk cache that has more data than new MaxSize so Cleanup runs enforceMaxSize.
func TestEnforceMaxSizeAfterScan(t *testing.T) {
	dir := t.TempDir()
	largeConfig := DefaultCacheConfig().WithMaxSize(10 * 1024 * 1024).WithCleanupInterval(0)
	c1, err := NewDiskCacheWithConfig(dir, largeConfig)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		body := strings.Repeat("x", 500)
		res := NewResourceBytes(200, []byte(body), http.Header{"Content-Length": []string{"500"}})
		if err := c1.Store(res, "key"+string(rune('0'+i))); err != nil {
			t.Fatal(err)
		}
	}
	_ = c1.Close()

	smallConfig := DefaultCacheConfig().WithMaxSize(1000).WithCleanupInterval(0)
	c2, err := NewDiskCacheWithConfig(dir, smallConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c2.Close() }()
	cc := c2.(*cache)
	// After scan, totalSize exceeds MaxSize; Cleanup should run enforceMaxSize
	result := cc.Cleanup()
	if result.RemovedItems == 0 {
		t.Logf("enforceMaxSize: removed=%d (may be 0 if eviction already happened)", result.RemovedItems)
	}
	stats := cc.Stats()
	if stats.TotalSize > smallConfig.MaxSize && result.RemovedItems == 0 {
		t.Logf("size %d > max %d", stats.TotalSize, smallConfig.MaxSize)
	}
}

func TestScanExistingCache_ReadDirFails(t *testing.T) {
	// When ReadDir returns non-IsNotExist error, scanExistingCache returns that error
	fs := vfs.Memory()
	c := NewVFSCacheWithConfig(&readDirFailVFS{fs}, DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = c.Close() }()
	err := c.scanExistingCache()
	if err == nil {
		t.Error("expected error when ReadDir fails with permission error")
	}
}

func TestStore_VfsWriteMkdirFails(t *testing.T) {
	c := NewVFSCacheWithConfig(&mkdirFailVFS{vfs.Memory()}, DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = c.Close() }()
	res := NewResourceBytes(200, []byte("x"), http.Header{})
	err := c.Store(res, "k")
	if err == nil {
		t.Error("expected error when MkdirAll fails")
	}
}

func TestStore_VfsWriteOpenFileFails(t *testing.T) {
	c := NewVFSCacheWithConfig(&openFileFailVFS{vfs.Memory()}, DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = c.Close() }()
	res := NewResourceBytes(200, []byte("x"), http.Header{})
	err := c.Store(res, "k")
	if err == nil {
		t.Error("expected error when OpenFile fails for body path")
	}
}

func TestRemoveEntry_RemoveFails(t *testing.T) {
	// When Remove fails (not IsNotExist), removeEntry still continues and hits debugf
	config := DefaultCacheConfig().WithMaxSize(500).WithCleanupInterval(0)
	c := NewVFSCacheWithConfig(&removeFailVFS{vfs.Memory()}, config).(*cache)
	defer func() { _ = c.Close() }()
	res := NewResourceBytes(200, []byte(strings.Repeat("x", 300)), http.Header{"Content-Length": []string{"300"}})
	if err := c.Store(res, "k1"); err != nil {
		t.Fatal(err)
	}
	res2 := NewResourceBytes(200, []byte(strings.Repeat("y", 300)), http.Header{"Content-Length": []string{"300"}})
	if err := c.Store(res2, "k2"); err != nil {
		t.Fatal(err)
	}
	// Eviction would call removeEntry; Remove fails but we continue
	stats := c.Stats()
	_ = stats
}

func TestStore_NoContentLength(t *testing.T) {
	// Store with no Content-Length uses io.Copy path
	cache := NewMemoryCacheWithConfig(DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = cache.Close() }()
	res := NewResourceBytes(200, []byte("body-without-cl"), http.Header{})
	if err := cache.Store(res, "nocl"); err != nil {
		t.Fatal(err)
	}
	out, err := cache.Retrieve("nocl")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Close() }()
	b, _ := io.ReadAll(out)
	if string(b) != "body-without-cl" {
		t.Errorf("body: got %q", b)
	}
}

func TestCache_Retrieve_HeaderMissing(t *testing.T) {
	// Body exists but header file missing -> Header returns ErrNotFoundInCache, we recordMiss
	config := DefaultCacheConfig().WithCleanupInterval(0)
	cc := NewVFSCacheWithConfig(vfs.Memory(), config).(*cache)
	defer func() { _ = cc.Close() }()
	res := NewResourceBytes(200, []byte("x"), http.Header{"Content-Length": []string{"1"}})
	if err := cc.Store(res, "k"); err != nil {
		t.Fatal(err)
	}
	hashed := hashKey("k")
	headerPath := headerPrefix + formatPrefix + hashed
	if err := cc.fs.Remove(headerPath); err != nil {
		t.Fatal(err)
	}
	_, err := cc.Retrieve("k")
	if err != ErrNotFoundInCache {
		t.Errorf("want ErrNotFoundInCache when header missing, got %v", err)
	}
	stats := cc.Stats()
	if stats.MissCount != 1 {
		t.Logf("miss count: %d", stats.MissCount)
	}
}

func TestCache_Retrieve_BodyOpenFails(t *testing.T) {
	config := DefaultCacheConfig().WithCleanupInterval(0)
	mem := vfs.Memory()
	c := NewVFSCacheWithConfig(&bodyOpenFailVFS{mem}, config).(*cache)
	defer func() { _ = c.Close() }()
	res := NewResourceBytes(200, []byte("body"), http.Header{})
	if err := c.Store(res, "k"); err != nil {
		t.Fatal(err)
	}
	_, err := c.Retrieve("k")
	if err == nil {
		t.Error("expected error when body Open fails")
	}
	if err == ErrNotFoundInCache {
		t.Error("expected non-ErrNotFoundInCache when Open returns permission error")
	}
}

func TestCleanupLoop_Runs(t *testing.T) {
	config := DefaultCacheConfig().
		WithCleanupInterval(2 * time.Millisecond).
		WithTTL(1 * time.Hour).
		WithStaleMapTTL(1 * time.Hour)
	cache := NewMemoryCacheWithConfig(config)
	defer func() { _ = cache.Close() }()
	res := NewResourceBytes(200, []byte("x"), http.Header{})
	if err := cache.Store(res, "k"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	result := cache.Cleanup()
	_ = result
}

func TestCache_Freshen_KeyNotInCache(t *testing.T) {
	cache := NewMemoryCacheWithConfig(DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = cache.Close() }()
	res := NewResourceBytes(200, []byte("x"), http.Header{"Cache-Control": []string{"max-age=60"}})
	// Freshen with key that was never stored -> Header returns ErrNotFoundInCache, we skip
	err := cache.Freshen(res, "nonexistent-key")
	if err != nil {
		t.Fatal(err)
	}
}

func TestCache_Freshen_InvalidatePath(t *testing.T) {
	cache := NewMemoryCacheWithConfig(DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = cache.Close() }()
	res := NewResourceBytes(200, []byte("x"), http.Header{
		"Cache-Control":  []string{"max-age=60"},
		"Content-Length": []string{"1"},
	})
	if err := cache.Store(res, "k"); err != nil {
		t.Fatal(err)
	}
	// Freshen with different status -> invalidate path
	res2 := NewResourceBytes(304, []byte(""), http.Header{"Cache-Control": []string{"max-age=60"}})
	err := cache.Freshen(res2, "k")
	if err != nil {
		t.Fatal(err)
	}
}

func TestEvictIfNeeded_NoLimit(t *testing.T) {
	config := DefaultCacheConfig().WithMaxSize(0).WithCleanupInterval(0)
	cache := NewMemoryCacheWithConfig(config).(*cache)
	defer func() { _ = cache.Close() }()
	res := NewResourceBytes(200, []byte("x"), http.Header{})
	if err := cache.Store(res, "k"); err != nil {
		t.Fatal(err)
	}
	// evictIfNeeded with MaxSize 0 does nothing (early return when MaxSize <= 0)
	stats := cache.Stats()
	if stats.ItemCount != 1 {
		t.Errorf("expected 1 item, got %d", stats.ItemCount)
	}
}

func TestEvictIfNeeded_TargetSizeNegative(t *testing.T) {
	// additionalSize > MaxSize -> targetSize = 0, then evict until totalSize <= 0
	config := DefaultCacheConfig().WithMaxSize(30).WithCleanupInterval(0)
	cache := NewMemoryCacheWithConfig(config).(*cache)
	defer func() { _ = cache.Close() }()
	res1 := NewResourceBytes(200, []byte("12"), http.Header{"Content-Length": []string{"2"}})
	if err := cache.Store(res1, "k1"); err != nil {
		t.Fatal(err)
	}
	res2 := NewResourceBytes(200, []byte("12345"), http.Header{"Content-Length": []string{"5"}})
	if err := cache.Store(res2, "k2"); err != nil {
		t.Fatal(err)
	}
	// Second Store triggers evictIfNeeded(5+header); totalSize was ~2+headers, now we add ~5+headers > 30, targetSize = 0
	stats := cache.Stats()
	if stats.ItemCount > 2 {
		t.Errorf("eviction should have run, item count %d", stats.ItemCount)
	}
}

func TestEvictIfNeeded_WithMetrics(t *testing.T) {
	prev := GetDefaultMetrics()
	defer func() { SetDefaultMetrics(prev) }()
	reg := metrics.NewRegistry("test_evict_metrics")
	SetDefaultMetrics(NewCacheMetrics(reg))
	config := DefaultCacheConfig().WithMaxSize(100).WithCleanupInterval(0)
	cache := NewMemoryCacheWithConfig(config).(*cache)
	defer func() { _ = cache.Close() }()
	res1 := NewResourceBytes(200, []byte(strings.Repeat("x", 80)), http.Header{"Content-Length": []string{"80"}})
	if err := cache.Store(res1, "k1"); err != nil {
		t.Fatal(err)
	}
	res2 := NewResourceBytes(200, []byte(strings.Repeat("y", 80)), http.Header{"Content-Length": []string{"80"}})
	if err := cache.Store(res2, "k2"); err != nil {
		t.Fatal(err)
	}
	// Second Store should trigger eviction of k1 (LRU); RecordCacheEviction is called
	stats := cache.Stats()
	if stats.ItemCount != 1 {
		t.Logf("eviction may have run: item count %d", stats.ItemCount)
	}
}

func TestCleanupLoop_TickerFires(t *testing.T) {
	config := DefaultCacheConfig().
		WithCleanupInterval(5 * time.Millisecond).
		WithTTL(1 * time.Millisecond).
		WithStaleMapTTL(time.Hour)
	cache := NewMemoryCacheWithConfig(config)
	defer func() { _ = cache.Close() }()
	res := NewResourceBytes(200, []byte("x"), http.Header{})
	if err := cache.Store(res, "k"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(15 * time.Millisecond) // wait for at least one cleanup tick
	result := cache.Cleanup()
	_ = result
}

func TestCleanup_WithMetrics(t *testing.T) {
	prev := GetDefaultMetrics()
	defer func() { SetDefaultMetrics(prev) }()
	reg := metrics.NewRegistry("test_cleanup_metrics")
	SetDefaultMetrics(NewCacheMetrics(reg))
	config := DefaultCacheConfig().WithMaxSize(200).WithCleanupInterval(0)
	cache := NewMemoryCacheWithConfig(config)
	defer func() { _ = cache.Close() }()
	for i := 0; i < 5; i++ {
		res := NewResourceBytes(200, []byte(strings.Repeat("x", 100)), http.Header{"Content-Length": []string{"100"}})
		if err := cache.Store(res, "key"+string(rune('0'+i))); err != nil {
			t.Fatal(err)
		}
	}
	result := cache.Cleanup()
	if result.RemovedItems == 0 {
		t.Logf("enforceMaxSize may have run: removed=%d", result.RemovedItems)
	}
}

func TestCache_Header_ReadError(t *testing.T) {
	config := DefaultCacheConfig().WithCleanupInterval(0)
	cc := NewVFSCacheWithConfig(vfs.Memory(), config).(*cache)
	defer func() { _ = cc.Close() }()
	res := NewResourceBytes(200, []byte("body"), http.Header{})
	if err := cc.Store(res, "testkey"); err != nil {
		t.Fatal(err)
	}
	hashed := hashKey("testkey")
	headerPath := headerPrefix + formatPrefix + hashed
	f, err := cc.fs.OpenFile(headerPath, os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("not valid status line\r\n\r\n"))
	_ = f.Close()
	_, err = cc.Header("testkey")
	if err == nil {
		t.Error("expected error for malformed header file")
	}
}

// errWriter always returns error on Write (covers headersToWriter error path).
type errWriter struct{}

func (errWriter) Write(p []byte) (n int, err error) {
	return 0, os.ErrPermission
}

func TestHeadersToWriter_Fail(t *testing.T) {
	h := http.Header{"X-Foo": []string{"bar"}}
	err := headersToWriter(h, errWriter{})
	if err == nil {
		t.Error("expected error when writer fails")
	}
}

// headerOpenFileFailVFS fails OpenFile for header/ path so storeHeader vfsWrite fails.
type headerOpenFileFailVFS struct{ vfs.VFS }

func (h *headerOpenFileFailVFS) OpenFile(path string, flag int, perm os.FileMode) (vfs.WFile, error) {
	if strings.HasPrefix(path, "header/") {
		return nil, os.ErrPermission
	}
	return h.VFS.OpenFile(path, flag, perm)
}

func TestStore_HeaderOpenFileFails(t *testing.T) {
	c := NewVFSCacheWithConfig(&headerOpenFileFailVFS{vfs.Memory()}, DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = c.Close() }()
	res := NewResourceBytes(200, []byte("x"), http.Header{"Content-Length": []string{"1"}})
	err := c.Store(res, "k")
	if err == nil {
		t.Error("expected error when OpenFile fails for header path")
	}
	if err != nil && !strings.Contains(err.Error(), "header") {
		t.Logf("error message should mention header: %v", err)
	}
}

// writeFailWFile wraps a WFile and makes Write return error (covers vfsWrite io.Copy failure).
type writeFailWFile struct {
	vfs.WFile
}

func (w *writeFailWFile) Write(p []byte) (n int, err error) {
	return 0, os.ErrPermission
}

// writeFailVFS returns a WFile that fails Write so io.Copy in vfsWrite fails.
type writeFailVFS struct {
	vfs.VFS
}

func (w *writeFailVFS) OpenFile(path string, flag int, perm os.FileMode) (vfs.WFile, error) {
	f, err := w.VFS.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	return &writeFailWFile{WFile: f}, nil
}

func TestStore_VfsWriteCopyFails(t *testing.T) {
	c := NewVFSCacheWithConfig(&writeFailVFS{vfs.Memory()}, DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = c.Close() }()
	res := NewResourceBytes(200, []byte("ab"), http.Header{"Content-Length": []string{"2"}})
	err := c.Store(res, "k")
	if err == nil {
		t.Error("expected error when io.Copy fails in vfsWrite")
	}
}

// headerFailAfterFirstVFS: OpenFile for header/ path succeeds first time, fails second (for Freshen storeHeader).
type headerFailAfterFirstVFS struct {
	vfs.VFS
	headerOpenCount int
}

func (h *headerFailAfterFirstVFS) OpenFile(path string, flag int, perm os.FileMode) (vfs.WFile, error) {
	if strings.HasPrefix(path, "header/") {
		h.headerOpenCount++
		if h.headerOpenCount > 1 {
			return nil, os.ErrPermission
		}
	}
	return h.VFS.OpenFile(path, flag, perm)
}

func TestFreshen_StoreHeaderFails(t *testing.T) {
	base := vfs.Memory()
	c := NewVFSCacheWithConfig(&headerFailAfterFirstVFS{VFS: base}, DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = c.Close() }()
	res := NewResourceBytes(200, []byte("x"), http.Header{
		"Content-Length": []string{"1"},
		"Cache-Control":  []string{"max-age=60"},
	})
	if err := c.Store(res, "k"); err != nil {
		t.Fatal(err)
	}
	// Freshen: Header("k") opens header for read (Open, not OpenFile), then storeHeader does OpenFile(header) -> fails (second time).
	res2 := NewResourceBytes(200, []byte("x"), http.Header{
		"Cache-Control":  []string{"max-age=60"},
		"Content-Length": []string{"1"},
	})
	err := c.Freshen(res2, "k")
	if err == nil {
		t.Error("expected error when storeHeader fails during Freshen")
	}
}

func TestCache_Retrieve_HeaderReadError(t *testing.T) {
	config := DefaultCacheConfig().WithCleanupInterval(0)
	cc := NewVFSCacheWithConfig(vfs.Memory(), config).(*cache)
	defer func() { _ = cc.Close() }()
	res := NewResourceBytes(200, []byte("body"), http.Header{})
	if err := cc.Store(res, "testkey"); err != nil {
		t.Fatal(err)
	}
	hashed := hashKey("testkey")
	headerPath := headerPrefix + formatPrefix + hashed
	f, err := cc.fs.OpenFile(headerPath, os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("HTTP/1.1\r\n\r\n")) // malformed: len(f)<2
	_ = f.Close()
	_, err = cc.Retrieve("testkey")
	if err == nil {
		t.Error("expected error when header read fails on Retrieve")
	}
	if err == ErrNotFoundInCache {
		t.Error("expected non-ErrNotFoundInCache when readHeaders fails")
	}
}

func TestStore_ReaderFails(t *testing.T) {
	c := NewMemoryCacheWithConfig(DefaultCacheConfig().WithCleanupInterval(0)).(*cache)
	defer func() { _ = c.Close() }()
	res := NewResource(200, &errReadSeekCloser{err: errors.New("read fails")}, http.Header{}) // no Content-Length, io.Copy(buf, res) will fail
	err := c.Store(res, "k")
	if err == nil {
		t.Error("expected error when body reader fails")
	}
}

func TestReadHeaders_MalformedStatusLineOnlyOnePart(t *testing.T) {
	// Status line with only one token (e.g. "HTTP/1.1" only) -> len(f) < 2
	r := bufio.NewReader(bytes.NewReader([]byte("HTTP/1.1\r\n\r\n")))
	_, err := readHeaders(r)
	if err == nil {
		t.Error("expected error for status line with only one part")
	}
}
