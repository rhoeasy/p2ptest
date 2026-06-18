package logger

import (
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	globalLogger *zap.Logger
	once         sync.Once
)

func InitLogger(isDev bool) {
	once.Do(func() {
		var cfg zap.Config
		if isDev {
			cfg = zap.NewDevelopmentConfig()
			cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		} else {
			cfg = zap.NewProductionConfig()
		}

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

		zap.ReplaceGlobals(globalLogger)
	})
}

func RedirectToFile(path string) func() error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return func() error { return nil }
	}

	cfg := zap.NewProductionConfig()
	cfg.EncoderConfig.TimeKey = "time"
	cfg.EncoderConfig.LevelKey = "level"
	cfg.EncoderConfig.CallerKey = "caller"
	cfg.EncoderConfig.MessageKey = "msg"
	cfg.EncoderConfig.StacktraceKey = "stack"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	writer := zapcore.AddSync(file)
	fileCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(cfg.EncoderConfig),
		writer,
		zap.DebugLevel,
	)

	globalLogger = zap.New(fileCore, zap.AddCallerSkip(1))
	zap.ReplaceGlobals(globalLogger)

	return func() error {
		_ = globalLogger.Sync()
		return file.Close()
	}
}

func L() *zap.Logger {
	if globalLogger == nil {
		panic("logger not initialized, please call logger.InitLogger() first")
	}
	return globalLogger
}
