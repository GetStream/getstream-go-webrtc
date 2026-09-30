package main

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fastJoinInterimBudget is the warm fast join's time to media against the T21 coordinator
// and T22 SFU, in round trips: 9.7 to 11.1 RTT median on the local stack on 2026-10-01.
// The SFU's candidates still come on the websocket, and ICE and DTLS take 6 RTT; T30-T33
// take it to 3.5.
const fastJoinInterimBudget = 11.5

// TestFastJoinWithinTheInterimBudget runs warm fast joins against the local stack (T03)
// with 100 ms injected and checks the median time to media both ways, within
// fastJoinInterimBudget round trips plus 30 ms. It needs the stack running with a
// fast_join coordinator and a FastJoin SFU:
//
//	eval "$(~/src/video-sfu/.factory/3rtt/tools/local-stack.sh env)"
//	JOINBENCH_LIVE=local go test -run WithinTheInterimBudget -v ./cmd/joinbench
func TestFastJoinWithinTheInterimBudget(t *testing.T) {
	if os.Getenv("JOINBENCH_LIVE") != envLocal {
		t.Skip("set JOINBENCH_LIVE=local, with the local stack's STREAM_* in the environment")
	}

	out := filepath.Join(t.TempDir(), "fast.jsonl")
	cfg, err := parseConfig([]string{
		"-env", envLocal, "-flow", flowFast, "-mode", modeWarm, "-scenario", "pubsub,one-to-one",
		"-runs", "5", "-out", out,
	}, os.Getenv, io.Discard)
	require.NoError(t, err)
	require.NoError(t, run(cfg, testWriter{t}))

	f, err := os.Open(out)
	require.NoError(t, err)
	defer f.Close()
	var publish, subscribe []float64
	lines := bufio.NewScanner(f)
	for lines.Scan() {
		var r runResult
		require.NoError(t, json.Unmarshal(lines.Bytes(), &r))
		require.Empty(t, r.Error, "run %d of %s", r.Run, r.Scenario)
		require.Equal(t, flowFast, r.Flow)
		require.NotNil(t, r.Subscribe)
		subscribe = append(subscribe, r.Subscribe.Ms)
		if r.Publish != nil {
			publish = append(publish, r.Publish.Ms)
		}
	}
	require.NoError(t, lines.Err())
	require.Len(t, subscribe, 10)
	require.Len(t, publish, 10)

	bound := fastJoinInterimBudget*float64(cfg.RTT/time.Millisecond) + 30
	for name, times := range map[string][]float64{"publish": publish, "subscribe": subscribe} {
		slices.Sort(times)
		median := (times[len(times)/2-1] + times[len(times)/2]) / 2
		t.Logf("%s to media: median %.0f ms, bound %.0f ms, runs %v", name, median, bound, times)
		require.LessOrEqual(t, median, bound, "%s to media", name)
	}
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}
