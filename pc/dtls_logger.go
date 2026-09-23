package pc

import (
	"fmt"

	"github.com/pion/logging"

	"github.com/GetStream/getstream-go-webrtc/logger"
)

// pion logging scopes that carry DTLS handshake detail: pion/dtls logs under
// "dtls", pion/webrtc's DTLS transport logs under "DTLSTransport".
const (
	dtlsLogScope          = "dtls"
	dtlsTransportLogScope = "DTLSTransport"
)

// dtlsAwareLoggerFactory reroutes the pion DTLS logging scopes onto the logger
// bound to the peer connection, while every other scope keeps that logger as-is.
// The pion DTLS stack logs only at debug/trace. If the bound logger is already
// at debug or trace it is used directly (it will emit the stack logs as-is).
// Otherwise (info or higher) the stack would be silent, so we forward: the very
// chatty trace lines are dropped and debug lines (handshake failures such as
// "decrypt failed") are promoted to info so they stay visible.
type dtlsAwareLoggerFactory struct {
	base logger.ILogger
}

func newDTLSAwareLoggerFactory(base logger.ILogger) dtlsAwareLoggerFactory {
	return dtlsAwareLoggerFactory{base: base}
}

func (f dtlsAwareLoggerFactory) NewLogger(scope string) logging.LeveledLogger {
	switch scope {
	case dtlsLogScope, dtlsTransportLogScope:
		if logger.DebugEnabled(f.base) {
			return f.base.NewLogger(scope)
		}
		return dtlsScopeLogger{log: f.base.WithValues("scope", scope)}
	default:
		return f.base.NewLogger(scope)
	}
}

// dtlsScopeLogger reroutes the pion DTLS stack logs onto an info-or-higher
// logger: trace lines (very chatty per-message and per-flight dumps) are
// dropped, and debug lines (handshake failures such as "decrypt failed") are
// promoted to info so they stay visible. Info and above pass through unchanged.
type dtlsScopeLogger struct {
	log logger.ILogger
}

func (dtlsScopeLogger) Trace(string)          {}
func (dtlsScopeLogger) Tracef(string, ...any) {}

func (l dtlsScopeLogger) Debug(msg string)          { l.log.Info(msg) }
func (l dtlsScopeLogger) Debugf(f string, a ...any) { l.log.Info(fmt.Sprintf(f, a...)) }
func (l dtlsScopeLogger) Info(msg string)           { l.log.Info(msg) }
func (l dtlsScopeLogger) Infof(f string, a ...any)  { l.log.Info(fmt.Sprintf(f, a...)) }
func (l dtlsScopeLogger) Warn(msg string)           { l.log.Warn(msg) }
func (l dtlsScopeLogger) Warnf(f string, a ...any)  { l.log.Warn(fmt.Sprintf(f, a...)) }
func (l dtlsScopeLogger) Error(msg string)          { l.log.Error(msg) }
func (l dtlsScopeLogger) Errorf(f string, a ...any) { l.log.Error(fmt.Sprintf(f, a...)) }
