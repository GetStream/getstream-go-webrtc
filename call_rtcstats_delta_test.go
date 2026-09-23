package rtc

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	sfu_signal_rpc "github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"
	"github.com/stretchr/testify/require"
	"github.com/twitchtv/twirp"
)

// fakeSignalServer serves just enough of the SFU signal service to capture the
// SendStats calls the stats worker makes.
type fakeSignalServer struct {
	sfu_signal_rpc.SignalServer

	mu       sync.Mutex
	requests []*sfu_signal_rpc.SendStatsRequest
}

func (f *fakeSignalServer) SendStats(_ context.Context, req *sfu_signal_rpc.SendStatsRequest) (*sfu_signal_rpc.SendStatsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, req)
	return &sfu_signal_rpc.SendStatsResponse{}, nil
}

func (f *fakeSignalServer) snapshot() []*sfu_signal_rpc.SendStatsRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*sfu_signal_rpc.SendStatsRequest(nil), f.requests...)
}

// startFakeSFU serves the signal service over HTTP and returns the fake plus its
// base URL.
func startFakeSFU(t testing.TB) (*fakeSignalServer, string) {
	t.Helper()

	fake := &fakeSignalServer{}
	// The SDK's twirp client is built with an empty path prefix, so the server
	// must be too.
	handler := sfu_signal_rpc.NewSignalServerServer(fake, twirp.WithServerPathPrefix(""))
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return fake, srv.URL
}

// pointCallAtSFU repoints an existing dummy call's twirp client at url.
func pointCallAtSFU(t testing.TB, call *Call, url string) {
	t.Helper()

	cred := *call.cred.Load()
	cred.Server.URL = url
	call.SetCredentials(cred)
}

// getStatsPayloads extracts the getstats trace payloads from one RtcStats blob.
// Each trace line is [event, pcID, payload, timestamp].
func getStatsPayloads(t testing.TB, rtcStats string) []map[string]any {
	t.Helper()

	var lines []json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(rtcStats), &lines))

	var out []map[string]any
	for _, line := range lines {
		var entry []any
		require.NoError(t, json.Unmarshal(line, &entry))
		require.GreaterOrEqual(t, len(entry), 3)
		if event, _ := entry[0].(string); event != "getstats" {
			continue
		}
		payload, ok := entry[2].(map[string]any)
		require.True(t, ok, "getstats payload should be an object")
		out = append(out, payload)
	}
	return out
}

// records flattens the per-peer-connection getstats payloads into the individual
// stats records they carry, keyed by record id. The "timestamp" key each payload
// carries alongside its records is not a record and is skipped.
func records(payloads []map[string]any) map[string]map[string]any {
	out := make(map[string]map[string]any)
	for _, payload := range payloads {
		for id, val := range payload {
			if rec, ok := val.(map[string]any); ok {
				out[id] = rec
			}
		}
	}
	return out
}

// TestReportRtcStatsDeltaCompressionAcrossTicks pins the two-map contract in
// reportRtcStats: each tick emits a delta-compressed report while keeping the
// *uncompressed* stats as the baseline for the next tick. Compression mutates
// the map it is given, so emitting and retaining the same map would leave every
// later tick comparing against already-stripped records.
func TestReportRtcStatsDeltaCompressionAcrossTicks(t *testing.T) {
	t.Parallel()

	fake, url := startFakeSFU(t)
	call := GetDummyCall(t, "delta-user", true)
	pointCallAtSFU(t, call, url)

	const ticks = 3
	for range ticks {
		require.NoError(t, call.reportRtcStats(context.Background()))
	}

	requests := fake.snapshot()
	require.Len(t, requests, ticks, "every tick should reach the SFU")

	first := records(getStatsPayloads(t, requests[0].RtcStats))
	require.NotEmpty(t, first, "first tick should report stats")

	// Every later tick must strip the fields that did not change. The dummy call
	// has no media flowing, so from the second tick on the records are near-empty.
	for tick := 1; tick < ticks; tick++ {
		later := records(getStatsPayloads(t, requests[tick].RtcStats))
		require.NotEmpty(t, later, "tick %d should report stats", tick)

		compressed := 0
		for id, rec := range later {
			baseline, ok := first[id]
			if !ok {
				continue
			}
			require.NotContains(t, rec, "id",
				"tick %d record %q should have had its id stripped", tick, id)
			if len(rec) < len(baseline) {
				compressed++
			}
		}
		require.NotZero(t, compressed,
			"tick %d should have at least one delta-compressed record", tick)
	}

	// The retained baseline must be the uncompressed form. If the emitted
	// (compressed) map were retained instead, these records would have been
	// stripped down to almost nothing.
	require.NotEmpty(t, call.prevPublisherRtcStats)
	require.NotEmpty(t, call.prevSubscriberRtcStats)
	for _, baseline := range []map[string]any{call.prevPublisherRtcStats, call.prevSubscriberRtcStats} {
		for id, val := range baseline {
			rec, ok := val.(map[string]any)
			if !ok {
				continue
			}
			require.Contains(t, rec, "id",
				"retained baseline record %q must be uncompressed", id)
			require.Contains(t, rec, "type",
				"retained baseline record %q must be uncompressed", id)
		}
	}
}

// TestWebrtcStatsWorkerSurvivesRepeatedTicks asserts the worker keeps reporting
// tick after tick rather than reporting once and going quiet.
func TestWebrtcStatsWorkerSurvivesRepeatedTicks(t *testing.T) {
	t.Parallel()

	fake, url := startFakeSFU(t)
	call := GetDummyCall(t, "worker-user", true)
	pointCallAtSFU(t, call, url)

	// The worker skips ticks while the call is still connecting.
	call.connState.Store(CallConnectionStateConnected)
	call.cc.statsReportingInterval = 20 * time.Millisecond

	go call.webrtcStatsWorker()

	require.Eventually(t, func() bool {
		return len(fake.snapshot()) >= 3
	}, 5*time.Second, 20*time.Millisecond, "stats worker should keep reporting")

	metrics := call.GetStatsReportingMetrics()
	require.GreaterOrEqual(t, metrics.SuccessfulReports, uint64(3),
		"three ticks should have been reported successfully")
	require.Zero(t, metrics.FailedReports, "no tick should have failed")
}

// TestStatsTickRecoversFromPanic covers the recover scoping fix. Upstream the
// recover wrapped the whole worker, so one panicking tick silently ended stats
// reporting for the rest of the call; it now scopes to a single tick, which is
// counted as failed while the worker carries on.
func TestStatsTickRecoversFromPanic(t *testing.T) {
	t.Parallel()

	call := GetDummyCall(t, "panic-user", true)
	// Nil credentials make Client() panic inside the reporting path.
	call.cred.Store(nil)

	require.NotPanics(t, func() { call.statsTick() })
	require.NotZero(t, call.GetStatsReportingMetrics().FailedReports)

	// A later, healthy tick must still be able to report.
	fake, url := startFakeSFU(t)
	call = GetDummyCall(t, "panic-user-recovered", true)
	pointCallAtSFU(t, call, url)
	call.statsTick()
	require.NotEmpty(t, fake.snapshot())
}
