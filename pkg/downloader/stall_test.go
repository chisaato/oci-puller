package downloader

import (
	"context"
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

// TestDownloader_StallDetection 验证：当某个分片的连接“没断，但吞吐量长期趋近于 0”
// （长尾场景，例如被中间设备限速卡住）时，Downloader 能够主动检测并中断重试，
// 而不是像此前那样一直卡住直到外层 1 小时的总超时。
func TestDownloader_StallDetection(t *testing.T) {
	logger.Init("debug")

	const totalSize = 300
	var attempts int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			w.Header().Set("Content-Length", "300")
			w.Header().Set("Accept-Ranges", "bytes")
			w.WriteHeader(http.StatusOK)
			return
		}

		n := atomic.AddInt32(&attempts, 1)
		w.Header().Set("Content-Range", rangeHeader+"/300")
		w.WriteHeader(http.StatusPartialContent)

		flusher, _ := w.(http.Flusher)

		// 第一次请求：先写入一点数据，然后彻底卡住（不关闭连接，也不再写数据），
		// 模拟“连接存活但长尾不动”的场景。之后请求正常返回，验证会被重试并最终成功。
		if n == 1 {
			w.Write([]byte{1, 2, 3})
			if flusher != nil {
				flusher.Flush()
			}
			<-r.Context().Done()
			return
		}

		w.Write(make([]byte, 100))
	}))
	defer ts.Close()

	tmpFile, err := os.CreateTemp("", "downloader_stall_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	mgr := interval.NewManager()
	c := coordinator.NewCoordinator(tmpFile, totalSize, mgr, func() {}, "")

	dl := New(ts.URL, tmpFile, c, nil)
	dl.SetChunkSize(100)
	dl.SetWorkers(1) // 单 worker，确保先撞上卡住的第一个分片再重试
	dl.SetStallTimeout(500 * time.Millisecond)
	dl.SetMinSpeed(0) // 只测试完全无进展检测，避免和低速判定的采样窗口互相干扰

	start := time.Now()
	err = dl.Start(context.Background())
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("Downloader failed: %v", err)
	}

	// 卡住的分片应当在 stallTimeout 附近被检测到并中断重试，
	// 而不是等到测试超时或外层长超时才结束。
	if duration > 5*time.Second {
		t.Errorf("expected stall detection to abort quickly, took %v", duration)
	}

	if atomic.LoadInt32(&attempts) < 2 {
		t.Errorf("expected at least 2 attempts (stalled + retry), got %d", attempts)
	}
}
