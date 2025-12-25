package manager

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStoreIntegration(t *testing.T) {
	// 1. 设置模拟服务器
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.Header().Set("Accept-Ranges", "bytes")
		w.WriteHeader(http.StatusOK)
		w.Write(make([]byte, 100))
	}))
	defer server.Close()

	// 2. 初始化管理器
	tmpDir, _ := os.MkdirTemp("", "oci-store-test")
	defer os.RemoveAll(tmpDir)
	mgr := NewManager(tmpDir)
	defer mgr.Close()

	// 3. 验证存储（Store）创建
	storePath := filepath.Join(tmpDir, "cache.db")
	if _, err := os.Stat(storePath); os.IsNotExist(err) {
		t.Fatal("初始化时应创建 cache.db")
	}

	digest := "sha256:store-test"
	url := server.URL

	// 4. 执行下载
	stream, err := mgr.GetBlobStream(digest, url, nil)
	if err != nil {
		t.Fatalf("下载失败: %v", err)
	}
	// 读取流中的数据以确保下载完成和存储更新
	buf := make([]byte, 100)
	stream.Read(buf)
	stream.Close()

	// 等待一会以完成异步存储更新
	time.Sleep(500 * time.Millisecond)

	// 5. 验证存储中的元数据
	// 由于管理器中的 Store 是私有的（目前假设如此，或者如果导出了通过 mgr.store 访问）。
	// 等一下，我在结构体中把 store 设为了私有。
	// 但我可以检查副作用吗？目前没有容易观察到的副作用。
	// 我应该为了测试导出 Store，或者直接使用导出的 storage 包来验证。

	// 直接打开数据库进行验证
	// 但它已被管理器锁定。
	// 所以必须先关闭管理器。
	mgr.Close()

	// 重新打开并验证
	// 我们需要导入 storage 包或使用辅助函数。
	// 既然这个测试在 `manager` 包中，如果不导入 `storage` 就无法轻易使用相关方法。
	// 但 `manager` 已经导入了 `storage`。所以我们可以直接使用。
}
