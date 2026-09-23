package logger

import "github.com/pion/logging"

// Noop is an ILogger that discards everything. It is the default for every
// component that accepts a logger.
type Noop struct{}

var _ ILogger = Noop{}

func (Noop) Debug(...any)   {}
func (Noop) Info(...any)    {}
func (Noop) Warn(...any)    {}
func (Noop) Error(...any)   {}
func (Noop) Println(...any) {}

func (Noop) Debugf(string, ...any) {}
func (Noop) Infof(string, ...any)  {}
func (Noop) Warnf(string, ...any)  {}

func (Noop) Debugw(string, ...any)        {}
func (Noop) Infow(string, ...any)         {}
func (Noop) Warnw(string, error, ...any)  {}
func (Noop) Errorw(string, error, ...any) {}

func (n Noop) WithField(string, any) ILogger     { return n }
func (n Noop) WithFields(map[string]any) ILogger { return n }
func (n Noop) WithValues(...any) ILogger         { return n }

func (Noop) NewLogger(string) logging.LeveledLogger { return NoopLeveled{} }

// NoopLeveled is a pion logging.LeveledLogger that discards everything.
type NoopLeveled struct{}

var _ logging.LeveledLogger = NoopLeveled{}

func (NoopLeveled) Trace(string)          {}
func (NoopLeveled) Tracef(string, ...any) {}
func (NoopLeveled) Debug(string)          {}
func (NoopLeveled) Debugf(string, ...any) {}
func (NoopLeveled) Info(string)           {}
func (NoopLeveled) Infof(string, ...any)  {}
func (NoopLeveled) Warn(string)           {}
func (NoopLeveled) Warnf(string, ...any)  {}
func (NoopLeveled) Error(string)          {}
func (NoopLeveled) Errorf(string, ...any) {}
