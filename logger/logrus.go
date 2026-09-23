package logger

import (
	"github.com/pion/logging"
	"github.com/sirupsen/logrus"
)

// FromLogrus adapts any logrus logger or entry to ILogger.
func FromLogrus(l logrus.FieldLogger) ILogger {
	return logrusLogger{l: l}
}

type logrusLogger struct {
	l logrus.FieldLogger
}

var (
	_ ILogger      = logrusLogger{}
	_ DebugEnabler = logrusLogger{}
)

func (a logrusLogger) Debug(args ...any)   { a.l.Debug(args...) }
func (a logrusLogger) Info(args ...any)    { a.l.Info(args...) }
func (a logrusLogger) Warn(args ...any)    { a.l.Warn(args...) }
func (a logrusLogger) Error(args ...any)   { a.l.Error(args...) }
func (a logrusLogger) Println(args ...any) { a.l.Infoln(args...) }

func (a logrusLogger) Debugf(format string, args ...any) { a.l.Debugf(format, args...) }
func (a logrusLogger) Infof(format string, args ...any)  { a.l.Infof(format, args...) }
func (a logrusLogger) Warnf(format string, args ...any)  { a.l.Warnf(format, args...) }

func (a logrusLogger) Debugw(msg string, keysAndValues ...any) {
	a.entry(nil, keysAndValues...).Debug(msg)
}

func (a logrusLogger) Infow(msg string, keysAndValues ...any) {
	a.entry(nil, keysAndValues...).Info(msg)
}

func (a logrusLogger) Warnw(msg string, err error, keysAndValues ...any) {
	a.entry(err, keysAndValues...).Warn(msg)
}

func (a logrusLogger) Errorw(msg string, err error, keysAndValues ...any) {
	a.entry(err, keysAndValues...).Error(msg)
}

// entry builds the logrus entry for a structured call. The error goes under
// "err" rather than logrus' conventional "error" because the default logrus
// hooks replace the record message with the "error" field, which would lose
// the message the caller passed.
func (a logrusLogger) entry(err error, keysAndValues ...any) logrus.FieldLogger {
	l := a.l
	if fields := fieldsFromKeyValues(keysAndValues...); len(fields) > 0 {
		l = l.WithFields(logrus.Fields(fields))
	}
	if err != nil {
		l = l.WithField("err", err.Error())
	}
	return l
}

func (a logrusLogger) WithField(key string, value any) ILogger {
	return logrusLogger{l: a.l.WithField(key, value)}
}

func (a logrusLogger) WithFields(fields map[string]any) ILogger {
	return logrusLogger{l: a.l.WithFields(logrus.Fields(fields))}
}

func (a logrusLogger) WithValues(keysAndValues ...any) ILogger {
	fields := fieldsFromKeyValues(keysAndValues...)
	if len(fields) == 0 {
		return a
	}
	return logrusLogger{l: a.l.WithFields(logrus.Fields(fields))}
}

func (a logrusLogger) NewLogger(scope string) logging.LeveledLogger {
	return logrusLeveled{l: a.l.WithField("scope", scope)}
}

func (a logrusLogger) DebugEnabled() bool {
	switch l := a.l.(type) {
	case *logrus.Logger:
		return l.IsLevelEnabled(logrus.DebugLevel)
	case *logrus.Entry:
		return l.Logger.IsLevelEnabled(logrus.DebugLevel)
	default:
		return false
	}
}

// logrusLeveled adapts logrus to pion's logging.LeveledLogger so pion/webrtc
// internals log to the same sink.
type logrusLeveled struct {
	l logrus.FieldLogger
}

var _ logging.LeveledLogger = logrusLeveled{}

func (a logrusLeveled) Trace(msg string)                  { a.l.Debug(msg) }
func (a logrusLeveled) Tracef(format string, args ...any) { a.l.Debugf(format, args...) }
func (a logrusLeveled) Debug(msg string)                  { a.l.Debug(msg) }
func (a logrusLeveled) Debugf(format string, args ...any) { a.l.Debugf(format, args...) }
func (a logrusLeveled) Info(msg string)                   { a.l.Info(msg) }
func (a logrusLeveled) Infof(format string, args ...any)  { a.l.Infof(format, args...) }
func (a logrusLeveled) Warn(msg string)                   { a.l.Warn(msg) }
func (a logrusLeveled) Warnf(format string, args ...any)  { a.l.Warnf(format, args...) }
func (a logrusLeveled) Error(msg string)                  { a.l.Error(msg) }
func (a logrusLeveled) Errorf(format string, args ...any) { a.l.Errorf(format, args...) }
