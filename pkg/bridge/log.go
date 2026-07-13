package bridge

// Logger is the minimal printf-style logging interface the bridge writes to.
// Consumers plug in their own implementation (slog, zap, a plugin SDK logger).
type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// NopLogger discards all log output. It is the default when Options.Logger is nil.
type NopLogger struct{}

// Debugf discards the message.
func (NopLogger) Debugf(string, ...any) {}

// Infof discards the message.
func (NopLogger) Infof(string, ...any) {}

// Warnf discards the message.
func (NopLogger) Warnf(string, ...any) {}

// Errorf discards the message.
func (NopLogger) Errorf(string, ...any) {}
