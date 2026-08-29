package config

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
)

func loadYAML(t *testing.T, yaml string) *Config {
	t.Helper()
	viper.Reset()
	viper.SetConfigType("yaml")
	if err := viper.ReadConfig(strings.NewReader(yaml)); err != nil {
		t.Fatalf("读取测试配置失败: %v", err)
	}
	cfg, err := Load()
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}
	return cfg
}

func TestLoad_MaxRetriesDefault(t *testing.T) {
	cfg := loadYAML(t, `
downloader:
  workers: 4
`)
	if cfg.Downloader.MaxRetries != 5 {
		t.Errorf("未配置 max_retries 时应默认为 5，得到 %d", cfg.Downloader.MaxRetries)
	}
}

func TestLoad_MaxRetriesFromYAML(t *testing.T) {
	cfg := loadYAML(t, `
downloader:
  max_retries: 2
`)
	if cfg.Downloader.MaxRetries != 2 {
		t.Errorf("配置 max_retries: 2 时应为 2，得到 %d", cfg.Downloader.MaxRetries)
	}
}
