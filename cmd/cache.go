package cmd

import (
	"fmt"
	"os"
	"strconv"

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

	// 创建管理器
	mgr := manager.NewManager(config.GlobalConfig.Server.CacheDir)
	defer mgr.Close()

	// 获取统计信息
	stats, err := mgr.GetCacheStats()
	if err != nil {
		fmt.Printf("获取缓存统计信息失败: %v\n", err)
		os.Exit(1)
	}

	// 格式化输出
	fmt.Println("=== OCI Puller 缓存统计信息 ===")
	fmt.Printf("缓存目录: %s\n", config.GlobalConfig.Server.CacheDir)
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

func runCacheClean(cmd *cobra.Command, args []string) {
	// 初始化配置和日志
	if err := initConfigForCache(); err != nil {
		fmt.Printf("初始化配置失败: %v\n", err)
		os.Exit(1)
	}

	// 创建管理器
	mgr := manager.NewManager(config.GlobalConfig.Server.CacheDir)
	defer mgr.Close()

	var count int
	var err error

	if len(args) > 0 {
		// 用户指定了清理数量
		count, err = strconv.Atoi(args[0])
		if err != nil || count <= 0 {
			fmt.Printf("无效的清理数量: %s (必须是正整数)\n", args[0])
			os.Exit(1)
		}
	} else {
		// 自动计算需要清理的数量
		stats, err := mgr.GetCacheStats()
		if err != nil {
			fmt.Printf("获取缓存统计信息失败: %v\n", err)
			os.Exit(1)
		}

		if stats.UsagePercent < 90 {
			fmt.Println("缓存使用率正常，无需清理")
			return
		}

		// 计算需要清理的数量以使使用率降到90%以下
		targetSize := int64(float64(stats.MaxSize) * 0.9)
		if stats.TotalSize <= targetSize {
			fmt.Println("缓存使用率正常，无需清理")
			return
		}

		needToFree := stats.TotalSize - targetSize
		// 假设平均每个项目的大小
		avgSize := stats.TotalSize / int64(stats.BlobCount)
		if avgSize == 0 {
			count = 10 // 默认清理10个
		} else {
			count = int(needToFree / avgSize)
			if count < 1 {
				count = 1
			}
			if count > 100 {
				count = 100 // 最多清理100个
			}
		}

		fmt.Printf("自动计算需要清理 %d 个项目\n", count)
	}

	// 执行清理
	fmt.Printf("正在清理 %d 个最旧的缓存项目...\n", count)
	if err := mgr.CleanupCache(count); err != nil {
		fmt.Printf("清理缓存失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("缓存清理完成")

	// 显示清理后的统计信息
	stats, err := mgr.GetCacheStats()
	if err == nil {
		fmt.Printf("清理后使用率: %.1f%%\n", stats.UsagePercent)
	}
}

func initConfigForCache() error {
	initConfig()

	// 初始化日志记录器
	logger.Init(config.GlobalConfig.Log.Level)

	return nil
}
