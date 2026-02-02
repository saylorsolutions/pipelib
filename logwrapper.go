package pipe

import (
	"log/slog"
	"os"
)

const (
	DefaultLogLevel = slog.LevelWarn
)

var (
	leveler slog.LevelVar
)

func init() {
	leveler.Set(DefaultLogLevel)
}

// SetDefaultLoggerLevel will set the logging level used by the DefaultLogger.
func SetDefaultLoggerLevel(level slog.Level) {
	leveler.Set(level)
}

// Logger is an interface that should be satisfied to enable logging in a pipeline by passing it to NewContext.
// To use a slog.Logger as a Logger, use WrapSlogLogger.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	WithValue(args ...any) Logger
	WithGroupName(name string) Logger
}

type slogWrapper struct {
	*slog.Logger
}

// DefaultLogger will be called by NewContext when a nil Logger is given, and can be passed explicitly if desired.
// This defaults to a slog.Logger with a slog.TextHandler outputting to os.Stdout at the slog.LevelWarn level.
func DefaultLogger() Logger {
	return &slogWrapper{
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: &leveler,
		})),
	}
}

// WrapSlogLogger will wrap a slog.Logger as an implementation of the Logger interface.
func WrapSlogLogger(logger *slog.Logger) Logger {
	return &slogWrapper{Logger: logger}
}

func (w *slogWrapper) WithValue(args ...any) Logger {
	return &slogWrapper{
		Logger: w.Logger.With(args...),
	}
}

func (w *slogWrapper) WithGroupName(name string) Logger {
	return &slogWrapper{
		Logger: w.Logger.WithGroup(name),
	}
}
