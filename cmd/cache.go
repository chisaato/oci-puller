package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"oci-puller/pkg/config"
	"oci-puller/pkg/logger"
	"oci-puller/pkg/manager"

	"github.com/spf13/cobra"
)

// cacheCmd 代表 cache 命令
var cacheCmd = &cobra.Command{
	Use:   "cache",
	Short: "缓存管理命令",
	Long:  `管理 OCI Puller 的缓存，包括查看状态和手动清理`,
}

var cacheStatsCmd = &cobra.Command{
	Use:   "stats",
	Short: "查看缓存统计信息",
	Run:   runCacheStats,
}

var cacheCleanCmd = &cobra.Command{
	Use:   "clean [count]",
	Short: "清理指定数量的最旧缓存项目",
	Long: `清理指定数量的最旧缓存项目。
如果不指定数量，则清理足够的空间使缓存使用率低于最大限制的10%。`,
	Args: cobra.MaximumNArgs(1),
	Run:  runCacheClean,
}

func init() {
	rootCmd.AddCommand(cacheCmd)
	cacheCmd.AddCommand(cacheStatsCmd)
	cacheCmd.AddCommand(cacheCleanCmd)
}

func runCacheStats(cmd *cobra.Command, args []string) {
	// 初始化配置和日志
	if err := initConfigForCache(); err != nil {
		fmt.Printf("初始化配置失败: %v\n", err)
		os.Exit(1)
	}

	stats, cacheDir, err := getCacheStats()
	if err != nil {
		fmt.Printf("获取缓存统计信息失败: %v\n", err)
		os.Exit(1)
	}

	// 格式化输出
	fmt.Println("=== OCI Puller 缓存统计信息 ===")
	fmt.Printf("缓存目录: %s\n", cacheDir)
	fmt.Printf("总大小: %.2f GB\n", float64(stats.TotalSize)/(1024*1024*1024))
	fmt.Printf("最大大小: %.2f GB\n", float64(stats.MaxSize)/(1024*1024*1024))
	fmt.Printf("使用率: %.1f%%\n", stats.UsagePercent)
	fmt.Printf("项目数量: %d\n", stats.BlobCount)
	fmt.Printf("活跃下载: %d\n", stats.ActiveDownloads)

	if stats.UsagePercent > 90 {
		fmt.Println("⚠️  警告: 缓存使用率过高，建议清理")
	} else if stats.UsagePercent > 75 {
		fmt.Println("ℹ️  注意: 缓存使用率较高")
	}
}

func getCacheStats() (*manager.CacheStats, string, error) {
	stats, cacheDir, err := getCacheStatsFromServer()
	if err == nil {
		return stats, cacheDir, nil
	}

	logger.S.Debugw("通过运行中服务获取缓存统计失败，回退到本地读取", "error", err)
	return getCacheStatsLocal()
}

func getCacheStatsFromServer() (*manager.CacheStats, string, error) {
	serverURL, err := buildInternalServerURL(config.GlobalConfig.Server.Addr)
	if err != nil {
		return nil, "", err
	}

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(serverURL + internalCacheStatsPath)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("服务返回状态码 %d", resp.StatusCode)
	}

	var payload cacheStatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, "", fmt.Errorf("解析服务响应失败: %w", err)
	}
	if payload.Stats == nil {
		return nil, "", fmt.Errorf("服务响应缺少缓存统计信息")
	}

	cacheDir := payload.CacheDir
	if cacheDir == "" {
		cacheDir = config.GlobalConfig.Server.CacheDir
	}

	return payload.Stats, cacheDir, nil
}

func getCacheStatsLocal() (*manager.CacheStats, string, error) {
	mgr := manager.NewManager(config.GlobalConfig.Server.CacheDir)
	defer mgr.Close()

	stats, err := mgr.GetCacheStats()
	if err != nil {
		return nil, "", err
	}

	return stats, config.GlobalConfig.Server.CacheDir, nil
}

func buildInternalServerURL(addr string) (string, error) {
	trimmedAddr := strings.TrimSpace(addr)
	if trimmedAddr == "" {
		trimmedAddr = config.GlobalConfig.Server.Addr
	}

	if strings.HasPrefix(trimmedAddr, "http://") || strings.HasPrefix(trimmedAddr, "https://") {
		return strings.TrimRight(trimmedAddr, "/"), nil
	}

	host, port, err := net.SplitHostPort(trimmedAddr)
	if err != nil {
		if strings.HasPrefix(trimmedAddr, ":") {
			host = ""
			port = strings.TrimPrefix(trimmedAddr, ":")
		} else {
			return "", fmt.Errorf("解析服务地址失败: %w", err)
		}
	}

	if port == "" {
		return "", fmt.Errorf("服务地址缺少端口: %s", trimmedAddr)
	}
	if host == "" || host == "0.0.0.0" || host == "::" || host == "[::]" {
		host = "127.0.0.1"
	}

	return fmt.Sprintf("http://%s:%s", host, port), nil
}

func runCacheClean(cmd *cobra.Command, args []string) {
	// 初始化配置和日志
	if err := initConfigForCache(); err != nil {
		fmt.Printf("初始化配置失败: %v\n", err)
		os.Exit(1)
	}

	var count int
	var err error
	useAutoCount := len(args) == 0

	if len(args) > 0 {
		// 用户指定了清理数量
		count, err = strconv.Atoi(args[0])
		if err != nil || count <= 0 {
			fmt.Printf("无效的清理数量: %s (必须是正整数)\n", args[0])
			os.Exit(1)
		}
	}

	if err := runCacheCleanWithFallback(count, useAutoCount); err != nil {
		fmt.Printf("清理缓存失败: %v\n", err)
		os.Exit(1)
	}
}

func runCacheCleanWithFallback(count int, useAutoCount bool) error {
	if err := runCacheCleanFromServer(count, useAutoCount); err == nil {
		return nil
	} else {
		logger.S.Debugw("通过运行中服务清理缓存失败，回退到本地执行", "error", err)
	}

	return runCacheCleanLocal(count, useAutoCount)
}

func runCacheCleanFromServer(count int, useAutoCount bool) error {
	serverURL, err := buildInternalServerURL(config.GlobalConfig.Server.Addr)
	if err != nil {
		return err
	}

	var payload cacheCleanRequest
	if !useAutoCount {
		payload.Count = &count
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("序列化清理请求失败: %w", err)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(serverURL+internalCacheCleanPath, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("服务返回状态码 %d", resp.StatusCode)
	}

	var result cacheCleanResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("解析服务响应失败: %w", err)
	}

	if result.Message != "" {
		fmt.Println(result.Message)
		return nil
	}

	actualCount := result.Requested
	if result.UsedAuto {
		fmt.Printf("自动计算需要清理 %d 个项目\n", result.AutoCount)
		actualCount = result.AutoCount
	}

	fmt.Printf("正在清理 %d 个最旧的缓存项目...\n", actualCount)
	fmt.Println("缓存清理完成")
	if result.Stats != nil {
		fmt.Printf("清理后使用率: %.1f%%\n", result.Stats.UsagePercent)
	}

	return nil
}

func runCacheCleanLocal(count int, useAutoCount bool) error {
	mgr := manager.NewManager(config.GlobalConfig.Server.CacheDir)
	defer mgr.Close()

	actualCount := count
	if useAutoCount {
		stats, err := mgr.GetCacheStats()
		if err != nil {
			return fmt.Errorf("获取缓存统计信息失败: %w", err)
		}

		autoCount, needsCleanup := calculateAutoCleanCount(stats)
		if !needsCleanup {
			fmt.Println("缓存使用率正常，无需清理")
			return nil
		}

		actualCount = autoCount
		fmt.Printf("自动计算需要清理 %d 个项目\n", actualCount)
	}

	fmt.Printf("正在清理 %d 个最旧的缓存项目...\n", actualCount)
	if err := mgr.CleanupCache(actualCount); err != nil {
		return err
	}

	fmt.Println("缓存清理完成")

	stats, err := mgr.GetCacheStats()
	if err == nil {
		fmt.Printf("清理后使用率: %.1f%%\n", stats.UsagePercent)
	}

	return nil
}

func calculateAutoCleanCount(stats *manager.CacheStats) (int, bool) {
	if stats == nil || stats.UsagePercent < 90 {
		return 0, false
	}

	targetSize := int64(float64(stats.MaxSize) * 0.9)
	if stats.TotalSize <= targetSize {
		return 0, false
	}

	needToFree := stats.TotalSize - targetSize
	if stats.BlobCount <= 0 || stats.TotalSize <= 0 {
		return 10, true
	}

	avgSize := stats.TotalSize / int64(stats.BlobCount)
	if avgSize <= 0 {
		return 10, true
	}

	count := int(needToFree / avgSize)
	if needToFree%avgSize != 0 {
		count++
	}
	if count < 1 {
		count = 1
	}
	if count > 100 {
		count = 100
	}

	return count, true
}

func initConfigForCache() error {
	initConfig()

	// 初始化日志记录器
	logger.Init(config.GlobalConfig.Log.Level)

	return nil
}
