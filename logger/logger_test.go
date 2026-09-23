package logger_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/logger"
)

func newTestLogrus(t *testing.T, level logrus.Level) (*logrus.Logger, *bytes.Buffer) {
	t.Helper()

	buf := &bytes.Buffer{}
	l := logrus.New()
	l.Out = buf
	l.Level = level
	l.Formatter = &logrus.JSONFormatter{}
	return l, buf
}

func TestFromLogrusStructuredCalls(t *testing.T) {
	t.Parallel()

	l, buf := newTestLogrus(t, logrus.DebugLevel)
	log := logger.FromLogrus(l).WithField("peer_type", "publisher")

	log.Infow("negotiating", logger.NegotiationId, 7)
	log.Warnw("dtls trouble", errors.New("handshake failed"), logger.DtlsState, "failed")

	out := buf.String()
	require.Contains(t, out, `"peer_type":"publisher"`)
	require.Contains(t, out, `"negotiation_id":7`)
	require.Contains(t, out, `"dtls_state":"failed"`)
	require.Contains(t, out, `"err":"handshake failed"`)
	require.Contains(t, out, `"msg":"dtls trouble"`)
}

func TestFromLogrusDebugEnabled(t *testing.T) {
	t.Parallel()

	debugLogger, _ := newTestLogrus(t, logrus.DebugLevel)
	require.True(t, logger.DebugEnabled(logger.FromLogrus(debugLogger)))

	infoLogger, _ := newTestLogrus(t, logrus.InfoLevel)
	require.False(t, logger.DebugEnabled(logger.FromLogrus(infoLogger)))

	require.False(t, logger.DebugEnabled(logger.Noop{}))
}

func TestNoopIsSilent(t *testing.T) {
	t.Parallel()

	log := logger.Noop{}.WithField("k", "v").WithFields(map[string]any{"a": 1}).WithValues("b", 2)
	log.Info("nothing")
	log.Errorw("nothing", errors.New("boom"))
	log.NewLogger("scope").Error("nothing")
}
