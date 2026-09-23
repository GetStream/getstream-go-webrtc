package pc

import (
	"bytes"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/logger"
)

// bufLogger builds an ILogger writing to buf.
func bufLogger(buf *bytes.Buffer, level logrus.Level) logger.ILogger {
	lr := logrus.New()
	lr.SetOutput(buf)
	lr.SetLevel(level)
	return logger.FromLogrus(lr)
}

// At info or higher the pion DTLS scope is forwarded: trace is dropped and debug
// is promoted to info so handshake failures stay visible.
func TestDTLSLoggerForwardsAtInfo(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	f := newDTLSAwareLoggerFactory(bufLogger(&buf, logrus.InfoLevel))
	dtls := f.NewLogger(dtlsLogScope)

	dtls.Trace("server: <- ClientHello")
	dtls.Tracef("[handshake:%s] flight0: %s", "server", "Preparing")
	require.NotContains(t, buf.String(), "ClientHello")
	require.NotContains(t, buf.String(), "Preparing")

	dtls.Debugf("%s: handshake parse failed: %s", "server", "boom")
	out := buf.String()
	require.Contains(t, out, "handshake parse failed")
	require.Contains(t, out, "level=info")
	require.Contains(t, out, "scope=dtls")

	// non-DTLS scopes keep the bound logger as-is: debug stays hidden at info.
	buf.Reset()
	f.NewLogger("ice").Debug("ice-noise")
	require.NotContains(t, buf.String(), "ice-noise")
}

// When the bound logger is already at debug, the DTLS scope uses it directly:
// debug stays at debug (no promotion) and trace surfaces too.
func TestDTLSLoggerUsesDebugLoggerDirectly(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	f := newDTLSAwareLoggerFactory(bufLogger(&buf, logrus.DebugLevel))
	dtls := f.NewLogger(dtlsLogScope)

	dtls.Debug("dtls-debug-line")
	out := buf.String()
	require.Contains(t, out, "dtls-debug-line")
	require.Contains(t, out, "level=debug")
}
