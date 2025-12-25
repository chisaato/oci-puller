package manager

import (
	"os"
	"testing"

	"oci-puller/pkg/config"
	"oci-puller/pkg/logger"
)

func TestMain(m *testing.M) {
	// 初始化日志
	logger.Init("debug")

	// 为测试初始化全局配置
	config.GlobalConfig = &config.Config{
		Downloader: config.DownloaderConfig{
			Workers:   4,
			ChunkSize: "1MB",
		},
		Server: config.ServerConfig{
			CacheDir: "/tmp/oci-test-cache",
		},
	}

	code := m.Run()
	os.Exit(code)
}
