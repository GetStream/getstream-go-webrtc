package main

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func env(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

var localEnv = env(map[string]string{"STREAM_API_KEY": "key", "STREAM_API_SECRET": "secret"})

func TestParseConfigDefaultsToTheLocalStack(t *testing.T) {
	t.Parallel()

	c, err := parseConfig(nil, localEnv, io.Discard)
	require.NoError(t, err)
	require.Equal(t, envLocal, c.Env)
	require.Equal(t, flowLegacy, c.Flow)
	require.Equal(t, []string{modeCold, modeWarm}, c.Modes)
	require.Equal(t, []string{scenarioPubSub}, c.Scenarios)
	require.Equal(t, 100*time.Millisecond, c.RTT, "local runs inject the standard 100 ms")
	require.Equal(t, 10, c.Runs)
	require.InDelta(t, 3.5, c.Budget, 0)
	require.Equal(t, localBaseURL, c.BaseURL)
	require.Equal(t, localWSURL, c.WSURL)
	require.Equal(t, "auto", c.Location)
}

func TestParseConfigReadsTheFlags(t *testing.T) {
	t.Parallel()

	c, err := parseConfig([]string{
		"-mode", "warm", "-scenario", "pubsub, one-to-one", "-rtt", "0", "-runs", "3",
		"-sfu", "sfu-2", "-location", "AMS", "-budget", "4", "-out", "x.jsonl",
	}, env(map[string]string{
		"STREAM_BASE_URL": "http://127.0.0.1:4030", "STREAM_WS_URL": "ws://127.0.0.1:4800/api/v2/connect",
		"STREAM_API_KEY": "key", "STREAM_API_SECRET": "secret",
	}), io.Discard)
	require.NoError(t, err)
	require.Equal(t, []string{modeWarm}, c.Modes)
	require.Equal(t, []string{scenarioPubSub, scenarioOneToOne}, c.Scenarios)
	require.Zero(t, c.RTT, "an explicit 0 turns the delay off")
	require.Equal(t, 3, c.Runs)
	require.Equal(t, "AMS", c.Location)
	require.InDelta(t, 4, c.Budget, 0)
	require.Equal(t, "x.jsonl", c.Out)
	require.Equal(t, "http://127.0.0.1:4030", c.BaseURL)
	require.Equal(t, "ws://127.0.0.1:4800/api/v2/connect", c.WSURL)
	require.Equal(t, "sfu-2", c.joinQuery().Get("sfu_id"))
}

func TestParseConfigStaging(t *testing.T) {
	t.Parallel()

	staging := env(map[string]string{
		"STREAM_BASE_URL": "https://chat-edge.example.com", "STREAM_API_KEY": "key", "STREAM_API_SECRET": "secret",
		"STREAM_WS_URL": "ws://127.0.0.1:8800/api/v2/connect",
	})
	c, err := parseConfig([]string{"-env", "staging", "-pin-tag", "staging"}, staging, io.Discard)
	require.NoError(t, err)
	require.Zero(t, c.RTT, "staging is remote already")
	require.Equal(t, "wss://chat-edge.example.com/api/v2/connect", c.WSURL, "derived from the base URL, not a leftover local one")
	require.Equal(t, "staging", c.joinQuery().Get("pin_to_tag"))

	_, err = parseConfig([]string{"-env", "staging"}, staging, io.Discard)
	require.ErrorContains(t, err, "-sfu")
	_, err = parseConfig([]string{"-env", "staging", "-sfu", "x"}, localEnv, io.Discard)
	require.ErrorContains(t, err, "STREAM_BASE_URL")
}

func TestParseConfigRejects(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		args []string
		env  func(string) string
		want string
	}{
		"unknown flow":     {[]string{"-flow", "quick"}, localEnv, "want legacy or fast"},
		"unknown mode":     {[]string{"-mode", "cold,hot"}, localEnv, `"hot"`},
		"unknown scenario": {[]string{"-scenario", "mesh"}, localEnv, `"mesh"`},
		"no runs":          {[]string{"-runs", "0"}, localEnv, "-runs"},
		"no budget":        {[]string{"-budget", "0"}, localEnv, "-budget"},
		"two pins":         {[]string{"-sfu", "a", "-pin-tag", "b"}, localEnv, "pass one"},
		"unknown env":      {[]string{"-env", "prod"}, localEnv, "want local or staging"},
		"no credentials":   {nil, env(nil), "local-stack.sh env"},
		"stray argument":   {[]string{"extra"}, localEnv, "unexpected"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := parseConfig(tc.args, tc.env, io.Discard)
			require.ErrorContains(t, err, tc.want)
		})
	}
}
