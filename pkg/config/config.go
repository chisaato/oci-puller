package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config 全局配置结构
type Config struct {
	Server     ServerConfig     `mapstructure:"server"`
	Log        LogConfig        `mapstructure:"log"`
	Downloader DownloaderConfig `mapstructure:"downloader"`
	Cache      CacheConfig      `mapstructure:"cache"`
	Registries []RegistryConfig `mapstructure:"registries"`
}

type ServerConfig struct {
	Addr     string `mapstructure:"addr"`      // 监听地址
	CacheDir string `mapstructure:"cache_dir"` // 缓存目录
}

type LogConfig struct {
	Level string `mapstructure:"level"` // debug, info, warn, error
}

type DownloaderConfig struct {
	Workers   int    `mapstructure:"workers"`    // 并发数
	ChunkSize string `mapstructure:"chunk_size"` // 分块大小 (如 10MB)
}

type RegistryConfig struct {
	ID   string   `mapstructure:"id"`
	Host string   `mapstructure:"host"`
	URLs []string `mapstructure:"urls"`
}

type CacheConfig struct {
	MaxSize         string `mapstructure:"max_size"`         // 最大缓存大小 (如 "10GB", "500MB")
	CleanupInterval string `mapstructure:"cleanup_interval"` // 清理间隔 (如 "5m", "1h")
}

// ParseMaxSize 解析缓存最大大小配置
func (c *CacheConfig) ParseMaxSize() (int64, error) {
	s := strings.ToUpper(strings.TrimSpace(c.MaxSize))
	if s == "" {
		return 10 * 1024 * 1024 * 1024, nil // 默认 10GB
	}

	var multiplier int64 = 1
	if strings.HasSuffix(s, "GB") {
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "GB")
	} else if strings.HasSuffix(s, "MB") {
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "MB")
	} else if strings.HasSuffix(s, "KB") {
		multiplier = 1024
		s = strings.TrimSuffix(s, "KB")
	} else if strings.HasSuffix(s, "B") {
		s = strings.TrimSuffix(s, "B")
	}

	var val int64
	_, err := fmt.Sscanf(s, "%d", &val)
	if err != nil {
		return 0, fmt.Errorf("invalid cache max size format: %s", c.MaxSize)
	}

	return val * multiplier, nil
}

// ParseCleanupInterval 解析清理间隔配置
func (c *CacheConfig) ParseCleanupInterval() (time.Duration, error) {
	if c.CleanupInterval == "" {
		return 5 * time.Minute, nil // 默认 5分钟
	}
	return time.ParseDuration(c.CleanupInterval)
}

// ParseChunkSize 解析 "10MB", "512KB" 等字符串为字节数
func (c *DownloaderConfig) ParseChunkSize() (int64, error) {
	s := strings.ToUpper(strings.TrimSpace(c.ChunkSize))
	if s == "" {
		return 10 * 1024 * 1024, nil // 默认 10MB
	}

	var multiplier int64 = 1
	if strings.HasSuffix(s, "MB") {
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "MB")
	} else if strings.HasSuffix(s, "KB") {
		multiplier = 1024
		s = strings.TrimSuffix(s, "KB")
	} else if strings.HasSuffix(s, "GB") {
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "GB")
	} else if strings.HasSuffix(s, "B") {
		s = strings.TrimSuffix(s, "B")
	}

	var val int64
	_, err := fmt.Sscanf(s, "%d", &val)
	if err != nil {
		return 0, fmt.Errorf("invalid chunk size format: %s", c.ChunkSize)
	}

	return val * multiplier, nil
}

// GlobalConfig 全局配置实例
var GlobalConfig *Config

// LoadConfig 加载配置
// Viper 实例应该在 cmd 包中初始化并传入，或者直接使用全局 Viper
func Load() (*Config, error) {
	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	// 设置默认值
	if cfg.Server.Addr == "" {
		cfg.Server.Addr = ":7945"
	}
	if cfg.Server.CacheDir == "" {
		cfg.Server.CacheDir = "./data/cache"
	}
	if cfg.Downloader.Workers <= 0 {
		cfg.Downloader.Workers = 16
	}
	if cfg.Log.Level == "" {
		cfg.Log.Level = "info"
	}
	if cfg.Cache.MaxSize == "" {
		cfg.Cache.MaxSize = "10GB"
	}
	if cfg.Cache.CleanupInterval == "" {
		cfg.Cache.CleanupInterval = "5m"
	}

	GlobalConfig = &cfg
	return &cfg, nil
}
