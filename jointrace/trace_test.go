package jointrace

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "rewrite the golden files")

var t0 = time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)

func at(ms float64) time.Time {
	return t0.Add(time.Duration(ms * float64(time.Millisecond)))
}

// todayJoin is a legacy join at 100 ms to every peer, laid out the way the SDK records
// it: two branches, publish and subscribe, run in parallel after sfu.join.
func todayJoin() Trace {
	rec := NewRecorder(t0)
	rec.SetRTT(PeerCoordinator, 100*time.Millisecond)
	rec.SetRTT(PeerSFU, 100*time.Millisecond)
	rec.SetRTT(PeerUDP, 100*time.Millisecond)
	add := func(name string, after []string, from, to float64, kind Kind, peer Peer) {
		rec.Add(Span{Name: name, After: after, Start: at(from), End: at(to), Kind: kind, Peer: peer})
	}
	add(CoordJoin, nil, 0, 400, KindNet, PeerCoordinator)
	rec.Add(Span{Name: CoordJoin + DetailTCP, Parent: CoordJoin, Start: at(0), End: at(100), Kind: KindNet, Peer: PeerCoordinator})
	rec.Add(Span{Name: CoordJoin + DetailTLS, Parent: CoordJoin, Start: at(100), End: at(300), Kind: KindNet, Peer: PeerCoordinator})
	rec.Add(Span{Name: CoordJoin + DetailFirstByte, Parent: CoordJoin, Start: at(300), End: at(400), Kind: KindNet, Peer: PeerCoordinator})
	rec.Add(Span{Name: CoordJoin + DetailServer, Parent: CoordJoin, Start: at(340), End: at(360), Kind: KindLocal, Peer: PeerCoordinator, Note: "server-timing"})
	add(PCsCreate, []string{CoordJoin}, 400, 402, KindLocal, PeerLocal)
	add(SFUWSDial, []string{PCsCreate}, 402, 702, KindNet, PeerSFU)
	add(SFUJoin, []string{SFUWSDial}, 702, 802, KindNet, PeerSFU)
	add(PubDebounce, []string{SFUJoin}, 802, 802.5, KindTimer, PeerLocal)
	add(PubOffer, []string{PubDebounce}, 802.5, 805, KindLocal, PeerLocal)
	add(PubSetPublisher, []string{PubOffer}, 805, 905, KindNet, PeerSFU)
	add(PubSFUCandidates, []string{PubSetPublisher}, 805, 860, KindNet, PeerSFU)
	add(PubICE, []string{PubSetPublisher, PubSFUCandidates}, 906, 1006, KindNet, PeerUDP)
	add(PubDTLS, []string{PubICE}, 1006, 1106, KindNet, PeerUDP)
	add(PubRTP, []string{PubDTLS}, 1106, 1110, KindLocal, PeerLocal)
	add(SubDebounce, []string{SFUJoin}, 802, 950, KindTimer, PeerSFU)
	add(SubOffer, []string{SubDebounce}, 950, 1000, KindNet, PeerSFU)
	add(SubSendAnswer, []string{SubOffer}, 1000, 1103, KindNet, PeerSFU)
	add(SubICE, []string{SubSendAnswer}, 1003, 1103, KindNet, PeerUDP)
	add(SubDTLS, []string{SubICE}, 1103, 1203, KindNet, PeerUDP)
	add(SubRTP, []string{SubDTLS}, 1203, 1225, KindLocal, PeerLocal)
	tr := rec.Trace()
	tr.JoinAt = t0
	return tr
}

func TestCriticalPathFollowsTheLatestDependency(t *testing.T) {
	path := todayJoin().CriticalPath()

	require.Equal(t, []string{
		CoordJoin, PCsCreate, SFUWSDial, SFUJoin,
		SubDebounce, SubOffer, SubSendAnswer, SubICE, SubDTLS, SubRTP,
	}, path.Names(), "sub.rtp ends last, and each step waits for the dependency that ended last")
	require.Equal(t, 1225*time.Millisecond, path.Total+path.Wait,
		"the path's cost and waits add up to the time to its end")
	require.Zero(t, path.Wait)
	require.Equal(t, 148*time.Millisecond, path.Timers)
	require.Equal(t, 24*time.Millisecond, path.Local)
	// sub.ice runs entirely inside sub.sendanswer, so it adds nothing.
	require.InDelta(t, 4+3+1+0.5+1.03+0+1, path.RTTs, 0.01)
	require.Zero(t, path.Steps[7].Cost)
}

func TestPathCountsGapsAsWait(t *testing.T) {
	rec := NewRecorder(t0)
	rec.Add(Span{Name: SFUWSDial, Start: at(0), End: at(100), Kind: KindNet, Peer: PeerSFU})
	rec.Add(Span{Name: SFUJoin, After: []string{SFUWSDial}, Start: at(130), End: at(230), Kind: KindNet, Peer: PeerSFU})
	path := rec.Trace().PathTo(SFUJoin)

	require.Equal(t, 200*time.Millisecond, path.Total)
	require.Equal(t, 30*time.Millisecond, path.Wait)
	require.Zero(t, path.RTTs, "no RTT measured, so no round trips")
}

func TestPathIgnoresMissingDependencies(t *testing.T) {
	rec := NewRecorder(t0)
	rec.Add(Span{Name: SubSendAnswer, After: []string{SubOffer}, Start: at(10), End: at(20), Kind: KindNet, Peer: PeerSFU})
	path := rec.Trace().CriticalPath()

	require.Equal(t, []string{SubSendAnswer}, path.Names())
	require.Equal(t, 10*time.Millisecond, path.Wait)
}

func TestRecorderKeepsTheFirstRecording(t *testing.T) {
	rec := NewRecorder(t0)
	rec.Add(Span{Name: SFUJoin, Start: at(0), End: at(10)})
	rec.Add(Span{Name: SFUJoin, Start: at(50), End: at(60)})
	rec.Add(Span{Name: SFUWSDial, Start: at(5), End: at(1)})
	rec.Add(Span{Name: PubDTLS, End: at(1)})
	rec.SetRTT(PeerSFU, 10*time.Millisecond)
	rec.SetRTT(PeerSFU, 90*time.Millisecond)
	rec.Seal()
	rec.Add(Span{Name: SubRTP, Start: at(0), End: at(1)})

	tr := rec.Trace()
	require.Len(t, tr.Spans, 2)
	join, _ := tr.Span(SFUJoin)
	require.Equal(t, at(10), join.End)
	dial, _ := tr.Span(SFUWSDial)
	require.Equal(t, dial.Start, dial.End, "an end before the start is clamped")
	require.Equal(t, 10*time.Millisecond, tr.RTT[PeerSFU])

	var nilRec *Recorder
	nilRec.Add(Span{Name: SFUJoin, Start: at(0), End: at(1)})
	require.Empty(t, nilRec.Trace().Spans)
}

// TestScratchSpansCountOnlyOnceMerged is a fast join's candidates: each records into its
// own scratch recorder, and only the one that took the client is merged.
func TestScratchSpansCountOnlyOnceMerged(t *testing.T) {
	rec := NewRecorder(t0)
	rec.Add(Span{Name: CoordFastJoin, Start: at(0), End: at(100)})
	failed, won := rec.Scratch(), rec.Scratch()
	failed.Add(Span{Name: SFUWSDial, Start: at(100), End: at(150)})
	failed.SetRTT(PeerSFU, 30*time.Millisecond)
	won.Add(Span{Name: SFUWSDial, Start: at(160), End: at(360)})
	won.Add(Span{Name: SFUWSDial + DetailTCP, Parent: SFUWSDial, Start: at(160), End: at(260)})
	won.SetRTT(PeerSFU, 100*time.Millisecond)
	won.Add(Span{Name: CoordFastJoin, Start: at(1), End: at(2)})
	rec.Merge(won)

	tr := rec.Trace()
	require.Len(t, tr.Spans, 3)
	dial, _ := tr.Span(SFUWSDial)
	require.Equal(t, at(160), dial.Start, "the candidate that took the client")
	coord, _ := tr.Span(CoordFastJoin)
	require.Equal(t, at(100), coord.End, "a merge keeps what was recorded first")
	require.Equal(t, 100*time.Millisecond, tr.RTT[PeerSFU])

	rec.Remove(SFUWSDial)
	require.Len(t, rec.Trace().Spans, 1, "with its detail spans")

	var nilRec *Recorder
	require.Nil(t, nilRec.Scratch())
	nilRec.Merge(won)
}

func TestReportRoundTripsAsJSON(t *testing.T) {
	tr := todayJoin()
	raw, err := json.Marshal(tr)
	require.NoError(t, err)

	var r Report
	require.NoError(t, json.Unmarshal(raw, &r))
	require.Len(t, r.Spans, len(tr.Spans))
	require.Equal(t, tr.CriticalPath().Names(), r.CriticalPath)
	require.InDelta(t, 1225, r.CriticalMs, 0.01)
	require.InDelta(t, 100, r.RTTMs[PeerSFU], 0.01)
	require.NotNil(t, r.PublishToMediaMs)
	require.InDelta(t, 1160, *r.PublishToMediaMs, 0.01, "first RTP sent plus RTT/2")
	require.NotNil(t, r.SubscribeToMediaMs)
	require.InDelta(t, 1225, *r.SubscribeToMediaMs, 0.01)

	for _, s := range r.Spans {
		if s.Name == SFUWSDial {
			require.True(t, s.Critical)
			require.InDelta(t, 3, s.RTTs, 0.01)
		}
		if s.Name == PubSetPublisher {
			require.False(t, s.Critical)
		}
	}
}

func TestRenderGolden(t *testing.T) {
	got := todayJoin().String()
	golden := filepath.Join("testdata", "today_join.golden")
	if *update {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(golden, []byte(got), 0o644))
	}
	want, err := os.ReadFile(golden)
	require.NoError(t, err, "run go test ./jointrace -update to create it")
	require.Equal(t, string(want), got)
}

func TestRenderEmpty(t *testing.T) {
	require.Equal(t, "join trace: no steps recorded\n", Trace{}.String())
}

// A join records a few dozen spans; collecting them must not be what makes it slow.
func TestRecordingIsCheap(t *testing.T) {
	const joins = 200
	start := time.Now()
	for range joins {
		tr := todayJoin()
		_ = tr.CriticalPath()
		_, _ = json.Marshal(tr)
	}
	perJoin := time.Since(start) / joins
	t.Logf("recording, critical path and JSON: %s per join", perJoin)
	require.Less(t, perJoin, time.Millisecond)
}

func BenchmarkRecordJoin(b *testing.B) {
	for b.Loop() {
		_ = todayJoin().CriticalPath()
	}
}
