package main

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// appLogger adapts log/slog to the printf-style bridge.Logger interface.
type appLogger struct {
	level *slog.LevelVar
	l     *slog.Logger
}

func newAppLogger() *appLogger {
	level := new(slog.LevelVar)
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return &appLogger{
		level: level,
		l:     slog.New(handler),
	}
}

func (l *appLogger) Configure(rawLevel string) error {
	switch strings.ToLower(strings.TrimSpace(rawLevel)) {
	case "", "info":
		l.level.Set(slog.LevelInfo)
	case "debug":
		l.level.Set(slog.LevelDebug)
	case "warn", "warning":
		l.level.Set(slog.LevelWarn)
	case "error":
		l.level.Set(slog.LevelError)
	default:
		return fmt.Errorf("unknown log level %q (want debug, info, warn or error)", rawLevel)
	}
	return nil
}

func (l *appLogger) Debugf(format string, args ...any) {
	l.l.Debug(fmt.Sprintf(format, args...))
}

func (l *appLogger) Infof(format string, args ...any) {
	l.l.Info(fmt.Sprintf(format, args...))
}

func (l *appLogger) Warnf(format string, args ...any) {
	l.l.Warn(fmt.Sprintf(format, args...))
}

func (l *appLogger) Errorf(format string, args ...any) {
	l.l.Error(fmt.Sprintf(format, args...))
}
