package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// 全局单例 logger
var globalLogger *zap.Logger

// InitLogger 初始化日志（在 main.go 最开头调用一次）
// isDev: true=开发模式(彩色、控制台、带调用栈)，false=生产模式(JSON)
func InitLogger(isDev bool) {
	var cfg zap.Config
	if isDev {
		// 开发环境：人类友好、彩色、打印行号
		cfg = zap.NewDevelopmentConfig()
		cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		// 生产环境：结构化 JSON、高性能、适合日志收集
		cfg = zap.NewProductionConfig()
	}

	// 统一日志字段格式
	cfg.EncoderConfig.TimeKey = "time"
	cfg.EncoderConfig.LevelKey = "level"
	cfg.EncoderConfig.CallerKey = "caller"
	cfg.EncoderConfig.MessageKey = "msg"
	cfg.EncoderConfig.StacktraceKey = "stack"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	var err error
	globalLogger, err = cfg.Build(zap.AddCallerSkip(1))
	if err != nil {
		panic("logger init failed: " + err.Error())
	}

	// 替换 zap 全局 logger，支持 zap.L() 直接使用
	zap.ReplaceGlobals(globalLogger)
}

// L 获取全局 Logger（所有包都用这个方法拿日志实例）
func L() *zap.Logger {
	if globalLogger == nil {
		panic("logger not initialized, please call logger.InitLogger() first")
	}
	return globalLogger
}
