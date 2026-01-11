package proctree

import (
	"context"
	"sync"
	"time"
)

type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	WithValue(args ...any) Logger
	WithGroupName(name string) Logger
}

type Context struct {
	context.Context
	logger Logger

	mux  sync.RWMutex
	errs []error
}

func (c *Context) Info(message string, args ...any) {
	c.logger.Info(message, args...)
}

func (c *Context) Warn(message string, args ...any) {
	c.logger.Warn(message, args...)
}

func (c *Context) Error(message string, args ...any) {
	c.logger.Error(message, args...)
}

func (c *Context) GetAlerts() []error {
	c.mux.RLock()
	defer c.mux.RUnlock()
	return c.errs
}

// Alert is used to post errors that occurred in asynchronous processes.
// Alerts do not propagate to parent Contexts.
func (c *Context) Alert(err error) {
	if err != nil {
		if !isFiltered(err) && !isAllowedError(err) {
			c.mux.Lock()
			defer c.mux.Unlock()
			c.errs = append(c.errs, err)
		}
	}
}

func (c *Context) Debug(message string, args ...any) {
	c.logger.Debug(message, args...)
}

func (c *Context) With(args ...any) *Context {
	return &Context{Context: c.Context, logger: c.logger.WithValue(args...), errs: c.errs}
}

func (c *Context) WithGroup(name string) *Context {
	return &Context{Context: c.Context, logger: c.logger.WithGroupName(name), errs: c.errs}
}

func BaseContext(logger Logger) *Context {
	if logger == nil {
		logger = DefaultLogger()
	}
	return &Context{Context: context.Background(), logger: logger, errs: nil}
}

func WithCancel(base *Context) (*Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(base)
	return &Context{Context: ctx, logger: base.logger, errs: base.errs}, cancel
}

func WithTimeout(base *Context, timeout time.Duration) (*Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(base, timeout)
	return &Context{Context: ctx, logger: base.logger, errs: base.errs}, cancel
}

func WithDeadline(base *Context, deadline time.Time) (*Context, context.CancelFunc) {
	ctx, cancel := context.WithDeadline(base, deadline)
	return &Context{Context: ctx, logger: base.logger, errs: base.errs}, cancel
}
