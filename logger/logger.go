// Package logger defines the logging interface used throughout this module.
//
// There is deliberately no package-level default logger and no global state:
// every component takes an ILogger through its options and falls back to Noop
// when none is supplied. Applications that want output pass an adapter, e.g.
// logger.FromLogrus(logrus.StandardLogger()).
package logger

import "github.com/pion/logging"

// ILogger is the logging surface the SDK depends on. It is a superset of the
// two styles the code uses: printf/field-style calls (Info, Warnf, WithField)
// in the call and signalling layers, and structured key/value calls (Infow,
// Warnw) in the peer-connection layer.
type ILogger interface {
	Debug(args ...any)
	Info(args ...any)
	Warn(args ...any)
	Error(args ...any)
	Println(args ...any)

	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)

	Debugw(msg string, keysAndValues ...any)
	Infow(msg string, keysAndValues ...any)
	// Warnw and Errorw take the error explicitly so adapters can attach it
	// under a dedicated field; err may be nil.
	Warnw(msg string, err error, keysAndValues ...any)
	Errorw(msg string, err error, keysAndValues ...any)

	WithField(key string, value any) ILogger
	WithFields(fields map[string]any) ILogger
	WithValues(keysAndValues ...any) ILogger

	// NewLogger returns a pion-compatible logger for the given scope, so the
	// SDK can hand pion/webrtc a logger factory backed by the same sink.
	NewLogger(scope string) logging.LeveledLogger
}

// DebugEnabler is implemented by loggers that can report whether debug output
// would be emitted, letting callers skip expensive message construction.
type DebugEnabler interface {
	DebugEnabled() bool
}

// DebugEnabled reports whether l emits debug records. Loggers that do not
// implement DebugEnabler are assumed not to.
func DebugEnabled(l ILogger) bool {
	if d, ok := l.(DebugEnabler); ok {
		return d.DebugEnabled()
	}
	return false
}

// Field names used in structured log records. Using constants keeps the field
// names consistent across packages.
const (
	DtlsState     = "dtls_state"
	NegotiationId = "negotiation_id"
)

// fieldsFromKeyValues turns an alternating key/value slice into a map. Keys
// that are not strings, and a trailing key without a value, are ignored.
func fieldsFromKeyValues(keysAndValues ...any) map[string]any {
	if len(keysAndValues) == 0 {
		return nil
	}
	fields := make(map[string]any, len(keysAndValues)/2)
	for i := 0; i+1 < len(keysAndValues); i += 2 {
		key, ok := keysAndValues[i].(string)
		if !ok {
			continue
		}
		fields[key] = keysAndValues[i+1]
	}
	return fields
}
