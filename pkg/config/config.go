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
	Workers      int    `mapstructure:"workers"`       // 并发数
	ChunkSize    string `mapstructure:"chunk_size"`    // 分块大小 (如 10MB)
	MinSpeed     string `mapstructure:"min_speed"`     // 单个分片的最低瞬时速度 (如 "32KB")，持续低于此值判定为长尾/卡顿。"0" 表示禁用
	StallTimeout string `mapstructure:"stall_timeout"` // 单个分片允许的最长无进展时间 (如 "20s")，超时判定为连接已死
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

// parseByteSize 解析形如 "10MB", "512KB", "100B" 的字符串为字节数
func parseByteSize(s string) (int64, error) {
	s = strings.ToUpper(strings.TrimSpace(s))

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
	if _, err := fmt.Sscanf(s, "%d", &val); err != nil {
		return 0, fmt.Errorf("invalid byte size format: %s", s)
	}

	return val * multiplier, nil
}

// ParseMaxSize 解析缓存最大大小配置
func (c *CacheConfig) ParseMaxSize() (int64, error) {
	s := strings.TrimSpace(c.MaxSize)
	if s == "" {
		return 10 * 1024 * 1024 * 1024, nil // 默认 10GB
	}

	val, err := parseByteSize(s)
	if err != nil {
		return 0, fmt.Errorf("invalid cache max size format: %s", c.MaxSize)
	}
	return val, nil
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
	s := strings.TrimSpace(c.ChunkSize)
	if s == "" {
		return 10 * 1024 * 1024, nil // 默认 10MB
	}

	val, err := parseByteSize(s)
	if err != nil {
		return 0, fmt.Errorf("invalid chunk size format: %s", c.ChunkSize)
	}
	return val, nil
}

// ParseMinSpeed 解析单个分片的最低瞬时速度 (如 "32KB" 表示 32KB/s)。
// 空字符串使用默认值 32KB/s；"0" 表示禁用低速检测。
func (c *DownloaderConfig) ParseMinSpeed() (int64, error) {
	s := strings.TrimSpace(c.MinSpeed)
	if s == "" {
		return 32 * 1024, nil // 默认 32KB/s
	}
	if s == "0" {
		return 0, nil
	}

	val, err := parseByteSize(s)
	if err != nil {
		return 0, fmt.Errorf("invalid min_speed format: %s", c.MinSpeed)
	}
	return val, nil
}

// ParseStallTimeout 解析单个分片允许的最长无进展（完全无新字节）时间。
// 空字符串使用默认值 20s；"0" 表示禁用卡顿检测。
func (c *DownloaderConfig) ParseStallTimeout() (time.Duration, error) {
	s := strings.TrimSpace(c.StallTimeout)
	if s == "" {
		return 20 * time.Second, nil // 默认 20s
	}
	if s == "0" {
		return 0, nil
	}
	return time.ParseDuration(s)
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
		cfg.Server.Addr = ":9800"
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
