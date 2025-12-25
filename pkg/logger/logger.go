package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var L *zap.Logger
var S *zap.SugaredLogger

func init() {
	// 默认初始化，防止 main 启动前调用崩溃
	Init("debug")
}

// Init 初始化全局日志
// levelStr: debug, info, warn, error
func Init(levelStr string) {
	config := zap.NewProductionConfig()
	// 使用自定义的编码配置，让输出更适合开发/调试阅读
	config.EncoderConfig.EncodeTime = zapcore.RFC3339TimeEncoder
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.EncoderConfig.EncodeCaller = zapcore.ShortCallerEncoder

	// 解析日志等级
	level, err := zapcore.ParseLevel(levelStr)
	if err != nil {
		level = zapcore.InfoLevel // 默认 Info
	}
	config.Level = zap.NewAtomicLevelAt(level)

	// 开发模式下通常更喜欢 Console 格式
	config.Encoding = "console"

	L, err = config.Build(zap.AddCaller(), zap.AddCallerSkip(0))
	if err != nil {
		panic(err)
	}
	S = L.Sugar()
}

// Sync 同步日志缓冲
func Sync() {
	_ = L.Sync()
}
