package downloader

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"oci-puller/pkg/coordinator"
	"oci-puller/pkg/interval"
	"oci-puller/pkg/logger"
)

func TestDownloader_Retry(t *testing.T) {
	// Initialize logger for test visibility
	logger.Init("debug")

	var (
		failCount int32
		maxFails  int32 = 3
	)

	// Mock Server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate failure for specific requests or randomly
		// Here we fail the first 3 requests for any chunk
		count := atomic.AddInt32(&failCount, 1)

		if count <= maxFails {
			t.Logf("Simulating failure #%d", count)
			// Choose between 500 or just closing connection (simulating EOF)
			// Closing connection is more like the user's issue
			conn, _, _ := w.(http.Hijacker).Hijack()
			conn.Close()
			return
		}

		// Success
		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			// HEAD or full get
			w.Header().Set("Content-Length", "1024")
			w.Header().Set("Accept-Ranges", "bytes")
			w.WriteHeader(http.StatusOK)
			return
		}

		// Handle range
		// Assuming range is well-formed bytes=start-end
		w.Header().Set("Content-Range", fmt.Sprintf("%s/1024", rangeHeader))
		w.WriteHeader(http.StatusPartialContent)
		// Write some dummy data
		w.Write(make([]byte, 10)) // Just write something, we don't check content strictly here, just success
	}))
	defer ts.Close()

	// Temp file
	tmpFile, err := os.CreateTemp("", "downloader_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Coordinator
	mgr := interval.NewManager()
	// Mock size 1024
	c := coordinator.NewCoordinator(tmpFile, 1024, mgr, func() {}, "")

	// Downloader
	dl := New(ts.URL, tmpFile, c, nil)
	dl.SetChunkSize(100) // Small chunks to trigger multiple requests
	dl.SetWorkers(2)     // concurrency

	// Run
	start := time.Now()
	err = dl.Start(context.Background())
	if err != nil {
		t.Fatalf("Downloader failed: %v", err)
	}

	duration := time.Since(start)
	t.Logf("Download finished in %v", duration)

	// Check if we actually retried
	// Since we can't easily inspect internal state without exporting, we rely on the fact that
	// if it didn't retry, it would have failed with EOF/Connection Reset on the first 3 requests.
	// And since we asserted err == nil, it must have retried.
	// We can also check atomic counter
	finalFailCount := atomic.LoadInt32(&failCount)
	if finalFailCount < maxFails {
		t.Errorf("Expected at least %d failures, got %d", maxFails, finalFailCount)
	}
}

// TestDownloader_MaxRetriesHonored 验证 SetMaxRetries 会限制单个分片的失败重试次数。
// 若 Start 仍使用硬编码 maxRetries=5，持续失败的分片会被尝试 6 次而不是 2 次。
func TestDownloader_MaxRetriesHonored(t *testing.T) {
	logger.Init("debug")

	var attempts int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		conn, _, _ := w.(http.Hijacker).Hijack()
		conn.Close()
	}))
	defer ts.Close()

	tmpFile, err := os.CreateTemp("", "downloader_max_retries_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	mgr := interval.NewManager()
	c := coordinator.NewCoordinator(tmpFile, 10, mgr, func() {}, "")

	dl := New(ts.URL, tmpFile, c, nil)
	dl.SetChunkSize(10)
	dl.SetWorkers(1)
	dl.SetMinSpeed(0)
	dl.SetStallTimeout(0)
	dl.SetMaxRetries(1)

	err = dl.Start(context.Background())
	if err == nil {
		t.Fatal("expected download to fail after exhausting retries")
	}

	got := atomic.LoadInt32(&attempts)
	if got != 2 {
		t.Errorf("expected 2 attempts (1 initial + 1 retry), got %d", got)
	}
}
