package cmd

import (
	"fmt"
	"os"

	"oci-puller/pkg/config"
	"oci-puller/pkg/logger"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

// rootCmd 代表在没有子命令时调用的基础命令
var rootCmd = &cobra.Command{
	Use:   "oci-puller",
	Short: "Docker 拉取加速器",
	Long:  `OCI Puller 是一个高性能代理，旨在通过多线程下载和持久化缓存加速 Docker 镜像拉取。`,
	// 如果你的简单应用程序有关联的操作，请取消下面一行的注释：
	// Run: func(cmd *cobra.Command, args []string) { },
}

// Execute 将所有子命令添加到根命令并适当设置标志。
// 该函数由 main.main() 调用。对于 rootCmd，它只需要执行一次。
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// 在这里定义你的标志和配置设置。
	// Cobra 支持持久标志，如果在处定义，它们对于你的应用程序将是全局的。
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "配置文件 (默认为 ./config.yaml)")
}

// initConfig 读取配置文件和环境变量（如果已设置）。
func initConfig() {
	if cfgFile != "" {
		// 使用来自标志的配置文件。
		viper.SetConfigFile(cfgFile)
	} else {
		// 在当前目录中搜索名为 "config" 的配置（不带扩展名）。
		viper.AddConfigPath(".")
		viper.SetConfigType("yaml")
		viper.SetConfigName("config")
	}

	viper.SetEnvPrefix("OCI")
	viper.AutomaticEnv() // 读取匹配的环境变量

	// 如果找到了配置文件，则读取它。
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("使用配置文件:", viper.ConfigFileUsed())
	}

	// 加载配置到全局结构体
	if _, err := config.Load(); err != nil {
		fmt.Printf("加载配置出错: %v\n", err)
		os.Exit(1)
	}

	// 加载配置后立即初始化日志记录器
	logger.Init(config.GlobalConfig.Log.Level)
}
