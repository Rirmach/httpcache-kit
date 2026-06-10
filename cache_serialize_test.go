package httpcache_test

import (
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/soulteary/httpcache-kit"
)

func TestSerializeLRUOnClose(t *testing.T) {
	httpcache.SetDebugLogging(true)

	// Create a temporary directory for the cache
	tmpDir, err := os.MkdirTemp("", "cache-serialize-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	config := httpcache.DefaultCacheConfig().
		WithCleanupInterval(0)

	cache1, err := httpcache.NewDiskCacheWithConfig(tmpDir, config)
	if err != nil {
		t.Fatalf("failed to create first cache: %v", err)
	}

	// Store some items
	for i := 0; i < 3; i++ {
		body := strings.Repeat("x", 100)
		res := httpcache.NewResourceBytes(http.StatusOK, []byte(body), http.Header{})
		key := "key" + string(rune('0'+i))
		if err := cache1.Store(res, key); err != nil {
			t.Fatalf("failed to store item %d: %v", i, err)
		}
	}

	stats1 := cache1.Stats()
	if stats1.ItemCount != 3 {
		t.Fatalf("expected 3 items before close, got %d", stats1.ItemCount)
	}

	// Close should trigger serialization
	if err := cache1.Close(); err != nil {
		t.Fatalf("failed to close cache: %v", err)
	}

	// Verify snapshot file exists
	snapshotPath := tmpDir + "/_httpcache_kit_vfs_lru_entry_snapshots.gob"
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		t.Errorf("snapshot file should exist after close, but not found")
	}
}

func TestDeserializeLRUOnStartup(t *testing.T) {
	httpcache.SetDebugLogging(true)

	// Save original clock and restore after test
	originalClock := httpcache.Clock
	defer func() { httpcache.Clock = originalClock }()

	tmpDir, err := os.MkdirTemp("", "cache-deserialize-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	config := httpcache.DefaultCacheConfig().
		WithMaxSize(800). // Tuned to hold ~3 small items, evict 1 on large store
		WithCleanupInterval(0)

		// Create first cache and store items
	cache1, err := httpcache.NewDiskCacheWithConfig(tmpDir, config)
	if err != nil {
		t.Fatalf("failed to create first cache: %v", err)
	}

	// Store items with small delay to ensure different ModTime
	for i := 0; i < 3; i++ {
		body := strings.Repeat("x", 100)
		res := httpcache.NewResourceBytes(http.StatusOK, []byte(body), http.Header{})
		key := "key" + string(rune('0'+i))
		time.Sleep(2 * time.Second)
		if err := cache1.Store(res, key); err != nil {
			t.Fatalf("failed to store item %d: %v", i, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Change access order: key2 (newest), key0, key1 (oldest)
	// Sleep to ensure accessedAt has meaningful difference
	time.Sleep(10 * time.Millisecond)
	if _, err := cache1.Retrieve("key2"); err != nil {
		t.Fatalf("failed to retrieve key2: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if _, err := cache1.Retrieve("key0"); err != nil {
		t.Fatalf("failed to retrieve key0: %v", err)
	}

	if err := cache1.Close(); err != nil {
		t.Fatalf("failed to close first cache: %v", err)
	}

	// Create second cache - should deserialize snapshot
	cache2, err := httpcache.NewDiskCacheWithConfig(tmpDir, config)
	if err != nil {
		t.Fatalf("failed to create second cache: %v", err)
	}
	defer func() { _ = cache2.Close() }()

	// Verify all items exist
	stats2 := cache2.Stats()
	if stats2.ItemCount != 3 {
		t.Fatalf("expected 3 items after deserialize, got %d", stats2.ItemCount)
	}

	// Verify snapshot file is removed after deserialization
	snapshotPath := tmpDir + "/_httpcache_kit_vfs_lru_entry_snapshots.gob"
	if _, err := os.Stat(snapshotPath); !os.IsNotExist(err) {
		t.Errorf("snapshot file should be removed after deserialization")
	}

	// Store a larger item to trigger eviction.
	// With MaxSize=800, 3 items (~360 bytes) + new item (~520 bytes) = ~880 > 800.
	// The least recently used item (key1) should be evicted first.
	largeBody := strings.Repeat("x", 500)
	res := httpcache.NewResourceBytes(http.StatusOK, []byte(largeBody), http.Header{})
	if err := cache2.Store(res, "key_large"); err != nil {
		t.Fatalf("failed to store large item: %v", err)
	}

	// After eviction, item count should be 3 (2 old + 1 new), not 4.
	// If LRU order was NOT restored, key1 might not be the one evicted.
	stats3 := cache2.Stats()
	if stats3.ItemCount != 3 {
		t.Errorf("expected 3 items after eviction (2 old + 1 new), got %d", stats3.ItemCount)
	}

	// key1 should be evicted (least recently used in restored snapshot)
	if _, err := cache2.Retrieve("key1"); err != httpcache.ErrNotFoundInCache {
		t.Errorf("expected key1 to be evicted (LRU order restored), but got: %v", err)
	}

	// key2 and key0 should still exist (more recently used)
	if _, err := cache2.Retrieve("key2"); err != nil {
		t.Errorf("expected key2 to exist, but got: %v", err)
	}
	if _, err := cache2.Retrieve("key0"); err != nil {
		t.Errorf("expected key0 to exist, but got: %v", err)
	}
}

func TestSnapshotFileRemovedAfterDeserialize(t *testing.T) {
	httpcache.SetDebugLogging(true)

	tmpDir, err := os.MkdirTemp("", "cache-snapshot-remove-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	config := httpcache.DefaultCacheConfig().
		WithCleanupInterval(0)

	// Create first cache, store, close
	cache1, err := httpcache.NewDiskCacheWithConfig(tmpDir, config)
	if err != nil {
		t.Fatalf("failed to create first cache: %v", err)
	}

	body := strings.Repeat("x", 100)
	res := httpcache.NewResourceBytes(http.StatusOK, []byte(body), http.Header{})
	if err := cache1.Store(res, "testkey"); err != nil {
		t.Fatalf("failed to store item: %v", err)
	}
	if err := cache1.Close(); err != nil {
		t.Fatalf("failed to close cache: %v", err)
	}

	// Verify snapshot exists before second cache
	snapshotPath := tmpDir + "/_httpcache_kit_vfs_lru_entry_snapshots.gob"
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		t.Fatalf("snapshot file should exist before second cache creation")
	}

	// Create second cache - triggers deserialization
	cache2, err := httpcache.NewDiskCacheWithConfig(tmpDir, config)
	if err != nil {
		t.Fatalf("failed to create second cache: %v", err)
	}

	// Verify snapshot is removed
	if _, err := os.Stat(snapshotPath); !os.IsNotExist(err) {
		t.Errorf("snapshot file should be removed after deserialization")
	}

	if err := cache2.Close(); err != nil {
		t.Fatalf("failed to close second cache: %v", err)
	}
}

func TestSnapshotCorruptedMagic(t *testing.T) {
	httpcache.SetDebugLogging(true)

	tmpDir, err := os.MkdirTemp("", "cache-corrupt-magic-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Write a corrupted snapshot file with wrong magic
	snapshotPath := tmpDir + "/_httpcache_kit_vfs_lru_entry_snapshots.gob"
	corruptedData := []byte("INVALID_MAGIC_HEADER_v1_some_zstd_data")
	if err := os.WriteFile(snapshotPath, corruptedData, 0644); err != nil {
		t.Fatalf("failed to write corrupted snapshot: %v", err)
	}

	config := httpcache.DefaultCacheConfig().
		WithCleanupInterval(0)

	// Create cache - should handle corrupted snapshot gracefully
	cache, err := httpcache.NewDiskCacheWithConfig(tmpDir, config)
	if err != nil {
		t.Fatalf("failed to create cache with corrupted snapshot: %v", err)
	}
	defer func() { _ = cache.Close() }()

	// Verify snapshot file is removed even on failure
	if _, err := os.Stat(snapshotPath); !os.IsNotExist(err) {
		t.Errorf("corrupted snapshot file should be removed after failed deserialization")
	}

	// Cache should still be functional
	body := strings.Repeat("x", 100)
	res := httpcache.NewResourceBytes(http.StatusOK, []byte(body), http.Header{})
	if err := cache.Store(res, "testkey"); err != nil {
		t.Fatalf("cache should still work after failed snapshot load: %v", err)
	}
}

func TestSnapshotVersionMismatch(t *testing.T) {
	httpcache.SetDebugLogging(true)

	tmpDir, err := os.MkdirTemp("", "cache-version-mismatch-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Write snapshot with correct magic but wrong version
	snapshotPath := tmpDir + "/_httpcache_kit_vfs_lru_entry_snapshots.gob"
	magic := "e9a22f25-dbb9-4c9f-b4ad-7d5d174c1789"
	wrongVersion := "v999"
	data := []byte(magic + wrongVersion + "some_zstd_data")
	if err := os.WriteFile(snapshotPath, data, 0644); err != nil {
		t.Fatalf("failed to write version-mismatch snapshot: %v", err)
	}

	config := httpcache.DefaultCacheConfig().
		WithCleanupInterval(0)

	cache, err := httpcache.NewDiskCacheWithConfig(tmpDir, config)
	if err != nil {
		t.Fatalf("failed to create cache with version mismatch: %v", err)
	}
	defer func() { _ = cache.Close() }()

	// Verify snapshot file is removed
	if _, err := os.Stat(snapshotPath); !os.IsNotExist(err) {
		t.Errorf("version-mismatch snapshot file should be removed")
	}

	// Cache should still be functional
	body := strings.Repeat("x", 100)
	res := httpcache.NewResourceBytes(http.StatusOK, []byte(body), http.Header{})
	if err := cache.Store(res, "testkey"); err != nil {
		t.Fatalf("cache should still work after version mismatch: %v", err)
	}
}

func TestSnapshotZstdCorrupted(t *testing.T) {
	httpcache.SetDebugLogging(true)

	tmpDir, err := os.MkdirTemp("", "cache-zstd-corrupt-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Write snapshot with correct magic and version, but invalid zstd payload
	snapshotPath := tmpDir + "/_httpcache_kit_vfs_lru_entry_snapshots.gob"
	magic := "e9a22f25-dbb9-4c9f-b4ad-7d5d174c1789"
	version := "v1"
	data := []byte(magic + version + "not_valid_zstd_data")
	if err := os.WriteFile(snapshotPath, data, 0644); err != nil {
		t.Fatalf("failed to write zstd-corrupted snapshot: %v", err)
	}

	config := httpcache.DefaultCacheConfig().
		WithCleanupInterval(0)

	cache, err := httpcache.NewDiskCacheWithConfig(tmpDir, config)
	if err != nil {
		t.Fatalf("failed to create cache with zstd corruption: %v", err)
	}
	defer func() { _ = cache.Close() }()

	// Verify snapshot file is removed
	if _, err := os.Stat(snapshotPath); !os.IsNotExist(err) {
		t.Errorf("zstd-corrupted snapshot file should be removed")
	}

	// Cache should still be functional
	body := strings.Repeat("x", 100)
	res := httpcache.NewResourceBytes(http.StatusOK, []byte(body), http.Header{})
	if err := cache.Store(res, "testkey"); err != nil {
		t.Fatalf("cache should still work after zstd corruption: %v", err)
	}
}

func TestSerializeLRUOnCleanup(t *testing.T) {
	httpcache.SetDebugLogging(true)

	tmpDir, err := os.MkdirTemp("", "cache-cleanup-serialize-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	config := httpcache.DefaultCacheConfig().
		WithCleanupInterval(100 * time.Millisecond) // Short interval for testing

	cache, err := httpcache.NewDiskCacheWithConfig(tmpDir, config)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer func() { _ = cache.Close() }()

	// Verify snapshot file is removed
	snapshotPath := tmpDir + "/_httpcache_kit_vfs_lru_entry_snapshots.gob"
	if _, err := os.Stat(snapshotPath); !os.IsNotExist(err) {
		t.Errorf("corrupted snapshot file should be removed after failed deserialization")
	}

	// Store an item
	body := strings.Repeat("x", 100)
	res := httpcache.NewResourceBytes(http.StatusOK, []byte(body), http.Header{})
	if err := cache.Store(res, "testkey"); err != nil {
		t.Fatalf("failed to store item: %v", err)
	}

	// Wait for at least one cleanup cycle
	time.Sleep(350 * time.Millisecond)

	// Currently, snapshot should be created again
	// Verify snapshot file exists (created by cleanup loop)
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		t.Errorf("snapshot file should exist after cleanup cycle")
	}
}

func TestEmptyCacheSerialize(t *testing.T) {
	httpcache.SetDebugLogging(true)

	tmpDir, err := os.MkdirTemp("", "cache-empty-serialize-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	config := httpcache.DefaultCacheConfig().
		WithCleanupInterval(100 * time.Millisecond)

	// Create and immediately close empty cache
	cache1, err := httpcache.NewDiskCacheWithConfig(tmpDir, config)
	if err != nil {
		t.Fatalf("failed to create empty cache: %v", err)
	}
	if err := cache1.Close(); err != nil {
		t.Fatalf("failed to close empty cache: %v", err)
	}

	time.Sleep(350 * time.Millisecond)

	// Verify snapshot file exists (even if empty)
	snapshotPath := tmpDir + "/_httpcache_kit_vfs_lru_entry_snapshots.gob"
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		t.Errorf("snapshot file should exist even for empty cache")
	}

	// Create second cache - should deserialize empty snapshot without panic
	cache2, err := httpcache.NewDiskCacheWithConfig(tmpDir, config)
	if err != nil {
		t.Fatalf("failed to create second cache from empty snapshot: %v", err)
	}

	stats := cache2.Stats()
	if stats.ItemCount != 0 {
		t.Errorf("expected 0 items from empty snapshot, got %d", stats.ItemCount)
	}

	if err := cache2.Close(); err != nil {
		t.Fatalf("failed to close second cache: %v", err)
	}
}
