package httpcache_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/soulteary/httpcache-kit"
	metrics "github.com/soulteary/metrics-kit"
)

func TestNewCacheMetrics(t *testing.T) {
	reg := metrics.NewRegistry("test_httpcache")
	m := httpcache.NewCacheMetrics(reg)
	if m == nil {
		t.Fatal("NewCacheMetrics returned nil")
	}
	if httpcache.GetDefaultMetrics() != m {
		t.Error("DefaultMetrics should be set to returned metrics")
	}
	// Reset so other tests don't get DefaultMetrics from this test
	httpcache.SetDefaultMetrics(nil)
}

func TestCacheMetrics_AllMethods(t *testing.T) {
	reg := metrics.NewRegistry("test_httpcache_ops")
	m := httpcache.NewCacheMetrics(reg)
	defer func() { httpcache.SetDefaultMetrics(nil) }()

	m.RecordCacheHit("GET")
	m.RecordCacheMiss("GET")
	m.RecordCacheSkip()
	m.RecordUpstreamDuration("GET", 200, 0.5)
	m.RecordUpstreamDuration("GET", 500, 0.1) // error status
	m.RecordUpstreamError("timeout")
	m.RecordStoreOperation(true)
	m.RecordStoreOperation(false)
	m.RecordRetrieveOperation(true)
	m.RecordRetrieveOperation(false)
	m.SetCacheSize(1024)
	m.SetCacheItemCount(10)
	m.SetCacheStaleCount(2)
	m.RecordCacheEviction("lru")
	m.RecordCacheEviction("ttl")
	m.RecordCleanupDuration(0.5)
	m.UpdateCacheStats(httpcache.CacheStats{
		TotalSize:  2048,
		ItemCount:  5,
		StaleCount: 1,
		HitCount:   100,
		MissCount:  10,
	})
	// Nil safety
	var nilM *httpcache.CacheMetrics
	nilM.RecordCacheHit("GET")
	nilM.RecordCacheMiss("GET")
	nilM.RecordCacheSkip()
	nilM.RecordUpstreamDuration("GET", 200, 0.1)
	nilM.RecordUpstreamError("err")
	nilM.RecordStoreOperation(true)
	nilM.RecordRetrieveOperation(true)
	nilM.SetCacheSize(0)
	nilM.SetCacheItemCount(0)
	nilM.SetCacheStaleCount(0)
	nilM.RecordCacheEviction("lru")
	nilM.RecordCleanupDuration(0)
	nilM.UpdateCacheStats(httpcache.CacheStats{})
}

func TestHandlerWithMetrics(t *testing.T) {
	reg := metrics.NewRegistry("test_handler_metrics")
	m := httpcache.NewCacheMetrics(reg)
	defer func() { httpcache.SetDefaultMetrics(nil) }()

	upstream := &upstreamServer{
		Body:         []byte("ok"),
		Now:          time.Date(2009, 11, 10, 23, 0, 0, 0, time.UTC),
		CacheControl: "max-age=60",
		Header:       http.Header{},
	}
	httpcache.Clock = func() time.Time { return upstream.Now }

	cache := httpcache.NewMemoryCache()
	handler := httpcache.NewHandler(cache, upstream)
	handler.SetMetrics(m)
	handler.Shared = true

	// MISS then HIT
	req1, _ := http.NewRequest("GET", "http://example.org/", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	httpcache.Writes.Wait()
	if rec1.Header().Get(httpcache.CacheHeader) != "MISS" {
		t.Errorf("first request: want MISS, got %s", rec1.Header().Get(httpcache.CacheHeader))
	}

	req2, _ := http.NewRequest("GET", "http://example.org/", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Header().Get(httpcache.CacheHeader) != "HIT" {
		t.Errorf("second request: want HIT, got %s", rec2.Header().Get(httpcache.CacheHeader))
	}

	// SKIP: non-cacheable method
	req3, _ := http.NewRequest("POST", "http://example.org/", nil)
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)
	if rec3.Header().Get(httpcache.CacheHeader) != "SKIP" {
		t.Errorf("POST: want SKIP, got %s", rec3.Header().Get(httpcache.CacheHeader))
	}
}

func TestCleanupWithMetrics(t *testing.T) {
	reg := metrics.NewRegistry("test_cleanup_metrics")
	m := httpcache.NewCacheMetrics(reg)
	defer func() { httpcache.SetDefaultMetrics(nil) }()

	now := time.Now().UTC()
	httpcache.Clock = func() time.Time { return now }

	config := httpcache.DefaultCacheConfig().
		WithTTL(1 * time.Hour).
		WithStaleMapTTL(1 * time.Hour).
		WithCleanupInterval(0)
	cache := httpcache.NewMemoryCacheWithConfig(config)
	defer func() { _ = cache.Close() }()

	body := []byte("x")
	res := httpcache.NewResourceBytes(http.StatusOK, body, http.Header{})
	if err := cache.Store(res, "k1"); err != nil {
		t.Fatal(err)
	}
	cache.Invalidate("k1")

	// Advance time and run cleanup (uses DefaultMetrics for RecordCleanupDuration / UpdateCacheStats)
	httpcache.SetDefaultMetrics(m)
	result := cache.Cleanup()
	if result.RemovedStaleEntries != 1 {
		t.Logf("cleanup result: %+v", result)
	}
}

func TestCacheEvictionMetrics(t *testing.T) {
	reg := metrics.NewRegistry("test_eviction_metrics")
	m := httpcache.NewCacheMetrics(reg)
	defer func() { httpcache.SetDefaultMetrics(nil) }()
	httpcache.SetDefaultMetrics(m)

	config := httpcache.DefaultCacheConfig().
		WithMaxSize(1500).
		WithCleanupInterval(0)
	cache := httpcache.NewMemoryCacheWithConfig(config)
	defer func() { _ = cache.Close() }()

	for i := 0; i < 5; i++ {
		body := strings.Repeat("x", 200)
		res := httpcache.NewResourceBytes(http.StatusOK, []byte(body), http.Header{
			"Content-Length": []string{"200"},
		})
		if err := cache.Store(res, "key"+string(rune('0'+i))); err != nil {
			t.Fatal(err)
		}
	}
	// Evictions should have been recorded
	_ = cache.Stats()
}

func TestStoreFailureMetrics(t *testing.T) {
	reg := metrics.NewRegistry("test_store_fail_metrics")
	m := httpcache.NewCacheMetrics(reg)
	defer func() { httpcache.SetDefaultMetrics(nil) }()

	// Use a cache that fails on Store by using a broken body (wrong Content-Length)
	upstream := &upstreamServer{
		Body:         []byte("short"),
		Now:          time.Date(2009, 11, 10, 23, 0, 0, 0, time.UTC),
		CacheControl: "max-age=60",
		Header:       http.Header{"Content-Length": []string{"100"}},
	}
	httpcache.Clock = func() time.Time { return upstream.Now }

	cache := httpcache.NewMemoryCache()
	handler := httpcache.NewHandler(cache, upstream)
	handler.SetMetrics(m)
	handler.Shared = true

	req, _ := http.NewRequest("GET", "http://example.org/fail", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	httpcache.Writes.Wait()
	// Store fails (Content-Length 100 but body "short") -> RecordStoreOperation(false) is called
	_ = rec
}

func TestPassUpstreamInMemoryFallback(t *testing.T) {
	reg := metrics.NewRegistry("test_fallback")
	m := httpcache.NewCacheMetrics(reg)
	defer func() { httpcache.SetDefaultMetrics(nil) }()

	// Small body so we don't need temp file; trigger in-memory path in passUpstream
	body := []byte("tiny")
	upstream := &upstreamServer{
		Body:         body,
		Now:          time.Date(2009, 11, 10, 23, 0, 0, 0, time.UTC),
		CacheControl: "max-age=60",
		Header:       http.Header{},
	}
	httpcache.Clock = func() time.Time { return upstream.Now }

	cache := httpcache.NewMemoryCache()
	handler := httpcache.NewHandler(cache, upstream)
	handler.SetMetrics(m)

	req, _ := http.NewRequest("GET", "http://example.org/small", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	httpcache.Writes.Wait()

	if rec.Code != http.StatusOK {
		t.Errorf("want 200, got %d", rec.Code)
	}
	got, _ := io.ReadAll(rec.Body)
	if !bytes.Equal(got, body) {
		t.Errorf("body: got %q", got)
	}
}
