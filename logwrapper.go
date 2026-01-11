package proctree

import (
	"log/slog"
	"os"
)

type logWrapper struct {
	*slog.Logger
}

func DefaultLogger() Logger {
	return &logWrapper{
		Logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
	}
}

func WrapSlogLogger(logger *slog.Logger) Logger {
	return &logWrapper{Logger: logger}
}

func (w *logWrapper) WithValue(args ...any) Logger {
	return &logWrapper{
		Logger: w.Logger.With(args...),
	}
}

func (w *logWrapper) WithGroupName(name string) Logger {
	return &logWrapper{
		Logger: w.Logger.WithGroup(name),
	}
}
