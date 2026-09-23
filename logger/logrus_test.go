package logger_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/logger"
)

// Every call has to come out at the level the caller asked for: a warning that
// is recorded as info is invisible to whatever alerts on the logs.
func TestFromLogrusLevels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		log       func(logger.ILogger)
		wantLevel string
		wantMsg   string
	}{
		{
			name:      "debug",
			log:       func(l logger.ILogger) { l.Debug("hello") },
			wantLevel: "debug",
			wantMsg:   "hello",
		},
		{
			name:      "info",
			log:       func(l logger.ILogger) { l.Info("hello") },
			wantLevel: "info",
			wantMsg:   "hello",
		},
		{
			name:      "warn",
			log:       func(l logger.ILogger) { l.Warn("hello") },
			wantLevel: "warning",
			wantMsg:   "hello",
		},
		{
			name:      "error",
			log:       func(l logger.ILogger) { l.Error("hello") },
			wantLevel: "error",
			wantMsg:   "hello",
		},
		{
			name:      "println logs at info",
			log:       func(l logger.ILogger) { l.Println("hello") },
			wantLevel: "info",
			wantMsg:   "hello",
		},
		{
			name:      "debugf",
			log:       func(l logger.ILogger) { l.Debugf("hello %s", "world") },
			wantLevel: "debug",
			wantMsg:   "hello world",
		},
		{
			name:      "infof",
			log:       func(l logger.ILogger) { l.Infof("hello %s", "world") },
			wantLevel: "info",
			wantMsg:   "hello world",
		},
		{
			name:      "warnf",
			log:       func(l logger.ILogger) { l.Warnf("hello %s", "world") },
			wantLevel: "warning",
			wantMsg:   "hello world",
		},
		{
			name:      "debugw",
			log:       func(l logger.ILogger) { l.Debugw("hello") },
			wantLevel: "debug",
			wantMsg:   "hello",
		},
		{
			name:      "infow",
			log:       func(l logger.ILogger) { l.Infow("hello") },
			wantLevel: "info",
			wantMsg:   "hello",
		},
		{
			name:      "warnw",
			log:       func(l logger.ILogger) { l.Warnw("hello", nil) },
			wantLevel: "warning",
			wantMsg:   "hello",
		},
		{
			name:      "errorw",
			log:       func(l logger.ILogger) { l.Errorw("hello", nil) },
			wantLevel: "error",
			wantMsg:   "hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l, buf := newTestLogrus(t, logrus.DebugLevel)
			tt.log(logger.FromLogrus(l))

			record := map[string]any{}
			require.NoError(t, json.Unmarshal(buf.Bytes(), &record))
			require.Equal(t, tt.wantLevel, record["level"])
			require.Equal(t, tt.wantMsg, record["msg"])
			require.NotContains(t, record, "err", "no error was passed")
		})
	}
}

// Structured calls take alternating keys and values, and anything that does not
// fit that shape is dropped rather than logged under a made up key.
func TestFromLogrusKeyValuePairs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		keysAndValues []any
		wantFields    map[string]any
		unwantedKeys  []string
	}{
		{
			name:          "pairs become fields",
			keysAndValues: []any{"a", "one", "b", float64(2)},
			wantFields:    map[string]any{"a": "one", "b": float64(2)},
		},
		{
			name:          "a key with no value is dropped",
			keysAndValues: []any{"a", "one", "dangling"},
			wantFields:    map[string]any{"a": "one"},
			unwantedKeys:  []string{"dangling"},
		},
		{
			name:          "a non-string key is dropped with its value",
			keysAndValues: []any{7, "value", "a", "one"},
			wantFields:    map[string]any{"a": "one"},
			unwantedKeys:  []string{"7", "value"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l, buf := newTestLogrus(t, logrus.DebugLevel)
			logger.FromLogrus(l).Infow("structured", tt.keysAndValues...)

			record := map[string]any{}
			require.NoError(t, json.Unmarshal(buf.Bytes(), &record))
			for key, want := range tt.wantFields {
				require.Equal(t, want, record[key])
			}
			for _, key := range tt.unwantedKeys {
				require.NotContains(t, record, key)
			}
		})
	}
}

// The error goes under "err" and not "error": logrus' own hooks overwrite the
// record message from an "error" field, which would throw away the message the
// caller wrote.
func TestFromLogrusPutsTheErrorUnderErr(t *testing.T) {
	t.Parallel()

	l, buf := newTestLogrus(t, logrus.DebugLevel)
	logger.FromLogrus(l).Errorw("could not join", errors.New("boom"), "attempt", 2)

	record := map[string]any{}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &record))
	require.Equal(t, "could not join", record["msg"])
	require.Equal(t, "boom", record["err"])
	require.NotContains(t, record, "error")
	require.Equal(t, float64(2), record["attempt"])
}

func TestFromLogrusWithFieldsAndValuesAccumulate(t *testing.T) {
	t.Parallel()

	l, buf := newTestLogrus(t, logrus.DebugLevel)

	log := logger.FromLogrus(l).
		WithField("a", 1).
		WithFields(map[string]any{"b": 2}).
		WithValues("c", 3)
	log.Info("hello")

	record := map[string]any{}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &record))
	require.Equal(t, float64(1), record["a"])
	require.Equal(t, float64(2), record["b"])
	require.Equal(t, float64(3), record["c"])
}

// WithValues with nothing usable to add hands back the same logger, so callers
// can pass through optional context without building a new entry every time.
func TestWithValuesWithoutUsableFieldsReturnsTheSameLogger(t *testing.T) {
	t.Parallel()

	l, buf := newTestLogrus(t, logrus.DebugLevel)
	log := logger.FromLogrus(l)

	require.Equal(t, log, log.WithValues())
	require.Equal(t, log, log.WithValues("dangling"))

	log.WithValues(7, "value").Info("hello")

	record := map[string]any{}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &record))
	require.Len(t, record, 3, "only level, msg and time: %v", record)
}

// pion logs through its own leveled interface, and it has a trace level we do
// not, so trace records land at debug.
func TestNewLoggerForPion(t *testing.T) {
	t.Parallel()

	l, buf := newTestLogrus(t, logrus.DebugLevel)
	pion := logger.FromLogrus(l).NewLogger("ice")

	// The subtests share one buffer, so they cannot run in parallel.
	tests := []struct {
		name      string
		log       func()
		wantLevel string
		wantMsg   string
	}{
		{name: "trace is recorded at debug", log: func() { pion.Trace("hello") }, wantLevel: "debug", wantMsg: "hello"},
		{name: "tracef is recorded at debug", log: func() { pion.Tracef("hello %d", 1) }, wantLevel: "debug", wantMsg: "hello 1"},
		{name: "debug", log: func() { pion.Debug("hello") }, wantLevel: "debug", wantMsg: "hello"},
		{name: "debugf", log: func() { pion.Debugf("hello %d", 1) }, wantLevel: "debug", wantMsg: "hello 1"},
		{name: "info", log: func() { pion.Info("hello") }, wantLevel: "info", wantMsg: "hello"},
		{name: "infof", log: func() { pion.Infof("hello %d", 1) }, wantLevel: "info", wantMsg: "hello 1"},
		{name: "warn", log: func() { pion.Warn("hello") }, wantLevel: "warning", wantMsg: "hello"},
		{name: "warnf", log: func() { pion.Warnf("hello %d", 1) }, wantLevel: "warning", wantMsg: "hello 1"},
		{name: "error", log: func() { pion.Error("hello") }, wantLevel: "error", wantMsg: "hello"},
		{name: "errorf", log: func() { pion.Errorf("hello %d", 1) }, wantLevel: "error", wantMsg: "hello 1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()
			tt.log()

			record := map[string]any{}
			require.NoError(t, json.Unmarshal(buf.Bytes(), &record))
			require.Equal(t, tt.wantLevel, record["level"])
			require.Equal(t, tt.wantMsg, record["msg"])
			require.Equal(t, "ice", record["scope"], "pion records carry the scope they were created with")
		})
	}
}

// An entry is as good as a logger for reporting whether debug output would be
// emitted, since the SDK is usually handed an entry with context attached.
func TestDebugEnabledThroughAnEntry(t *testing.T) {
	t.Parallel()

	debugLogger, _ := newTestLogrus(t, logrus.DebugLevel)
	require.True(t, logger.DebugEnabled(logger.FromLogrus(debugLogger.WithField("a", 1))))

	infoLogger, _ := newTestLogrus(t, logrus.InfoLevel)
	require.False(t, logger.DebugEnabled(logger.FromLogrus(infoLogger.WithField("a", 1))))
}
