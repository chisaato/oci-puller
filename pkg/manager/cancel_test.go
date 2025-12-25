package manager

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"oci-puller/pkg/coordinator"
)

func TestGracefulCancellation(t *testing.T) {
	// 1. Setup Mock Server (Infinite stream or slow stream)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			w.Header().Set("Content-Length", "104857600")
			w.Header().Set("Accept-Ranges", "bytes")
			w.WriteHeader(http.StatusOK)
			return
		}

		w.Header().Set("Content-Length", "104857600") // 100MB
		w.Header().Set("Accept-Ranges", "bytes")
		w.WriteHeader(http.StatusOK)
		// Send some data then sleep to simulate slow download
		w.Write(make([]byte, 1024))
		time.Sleep(2 * time.Second)
		w.Write(make([]byte, 1024))
		// Keep connection open
		<-r.Context().Done()
	}))
	defer server.Close()

	// 2. Adjust Grace Period for testing
	originalGrace := coordinator.GracePeriod
	coordinator.GracePeriod = 500 * time.Millisecond
	defer func() { coordinator.GracePeriod = originalGrace }()

	// 3. Init Manager
	tmpDir, _ := os.MkdirTemp("", "oci-test")
	defer os.RemoveAll(tmpDir)
	mgr := NewManager(tmpDir)

	digest := "sha256:test"
	url := server.URL

	// 4. Client A connects
	t.Log("Client A connecting...")
	streamA, err := mgr.GetBlobStream(digest, url, nil)
	if err != nil {
		t.Fatalf("Failed to get stream A: %v", err)
	}

	// Verify active
	if _, ok := mgr.active.Load(digest); !ok {
		t.Fatal("Download should be active")
	}

	// 5. Client A disconnects
	t.Log("Client A disconnecting...")
	streamA.Close()

	// 6. Immediate check - should STILL be active (Grace period 500ms)
	time.Sleep(100 * time.Millisecond)
	if _, ok := mgr.active.Load(digest); !ok {
		t.Fatal("Download should still be active during grace period")
	}

	// 7. Client B connects within grace period
	t.Log("Client B connecting (Rescuing)...")
	streamB, err := mgr.GetBlobStream(digest, url, nil)
	if err != nil {
		t.Fatalf("Failed to get stream B: %v", err)
	}

	// Wait for grace period to pass -> Should NOT cancel
	time.Sleep(600 * time.Millisecond)
	if _, ok := mgr.active.Load(digest); !ok {
		t.Fatal("Download should still be active (rescued by Client B)")
	}

	// 8. Client B disconnects
	t.Log("Client B disconnecting...")
	streamB.Close()

	// 9. Wait for grace period to pass
	t.Log("Waiting for timeout...")
	time.Sleep(1000 * time.Millisecond)

	// 10. Check if gone
	if _, ok := mgr.active.Load(digest); ok {
		t.Fatal("Download should be canceled after grace period")
	}
	t.Log("Test passed!")
}
