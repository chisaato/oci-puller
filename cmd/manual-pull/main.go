package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"oci-puller/pkg/coordinator"
	"oci-puller/pkg/downloader"
	"oci-puller/pkg/interval"
	"oci-puller/pkg/logger"
)

const (
	tokenURL = "https://ghcr.io/token?scope=repository:linuxserver/blender:pull&service=ghcr.io"
	blobURL  = "https://lscr.scgit.top/v2/linuxserver/blender/blobs/sha256:6c0b2755985d478b37d48a090cb722f3c241c8331237337b3e62d070af0e4b70"
	blobSize = 819903207
)

type TokenResponse struct {
	Token string `json:"token"`
}

func fetchToken() (string, error) {
	resp, err := http.Get(tokenURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to fetch token: status %d", resp.StatusCode)
	}

	var tr TokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return "", err
	}
	return tr.Token, nil
}

func main() {
	// 初始化为 DebugLevel 以便诊断
	logger.Init("debug")
	defer logger.Sync()

	fmt.Println("==================================================")
	fmt.Println("开始真实的 OCI 分层拉取...")
	fmt.Printf("目标: %s\n", blobURL)
	fmt.Println("==================================================")

	// 1. 获取令牌
	logger.S.Info("正在从 GHCR 获取拉取令牌...")
	token, err := fetchToken()
	if err != nil {
		logger.S.Errorf("获取令牌出错: %v", err)
		os.Exit(1)
	}
	logger.S.Info("令牌获取成功。")

	// 2. 准备文件
	targetPath := "layer.tar.gz"
	f, err := os.OpenFile(targetPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		logger.S.Errorf("创建文件出错: %v", err)
		os.Exit(1)
	}
	defer f.Close()

	// 3. 设置协调器
	mgr := interval.NewManager()
	coord := coordinator.NewCoordinator(f, blobSize, mgr, func() {
		logger.S.Info("协调器: 所有区间已标记为已下载。")
	}, "")

	// 4. 设置下载器
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+token)

	dl := downloader.New(blobURL, f, coord, headers)
	dl.SetChunkSize(10 * 1024 * 1024) // 10MB chunks
	dl.SetWorkers(16)                 // 提高并发到 16 以压测

	// 5. 开始下载
	start := time.Now()
	logger.S.Infof("开始多线程下载: %d 字节, 16 个工作线程", blobSize)

	err = dl.Start(context.Background())
	if err != nil {
		logger.S.Errorf("下载失败: %v", err)
		os.Exit(1)
	}

	duration := time.Since(start)
	speed := float64(blobSize) / 1024 / 1024 / duration.Seconds()

	fmt.Println("==================================================")
	fmt.Printf("下载完成！\n")
	fmt.Printf("文件: %s\n", targetPath)
	fmt.Printf("耗时: %v\n", duration.Round(time.Millisecond))
	fmt.Printf("平均速度: %.2f MB/s\n", speed)
	fmt.Println("==================================================")
}
