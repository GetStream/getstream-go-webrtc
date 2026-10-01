package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/jointrace"
)

func TestMedian(t *testing.T) {
	t.Parallel()

	require.Zero(t, median(nil))
	require.InDelta(t, 2, median([]float64{3, 1, 2}), 0)
	require.InDelta(t, 2.5, median([]float64{4, 1, 3, 2}), 0)
	in := []float64{3, 1, 2}
	median(in)
	require.Equal(t, []float64{3, 1, 2}, in, "the input is left alone")
}

// legacyTrace is a warm join at a 100 ms round trip: the coordinator, the SFU websocket
// and join, a 150 ms debounce, SetPublisher, ICE, DTLS, and the first packets.
func legacyTrace(t *testing.T) jointrace.Trace {
	t.Helper()
	const rtt = 100 * time.Millisecond
	join := time.Unix(1000, 0)
	at := func(rtts float64) time.Time { return join.Add(time.Duration(rtts * float64(rtt))) }
	rec := jointrace.NewRecorder(join.Add(-3 * rtt))
	rec.SetRTT(jointrace.PeerCoordinator, rtt)
	rec.SetRTT(jointrace.PeerSFU, rtt)
	rec.SetRTT(jointrace.PeerUDP, rtt)
	net := func(name, after string, peer jointrace.Peer, from, to float64) {
		s := jointrace.Span{Name: name, Start: at(from), End: at(to), Kind: jointrace.KindNet, Peer: peer}
		if after != "" {
			s.After = []string{after}
		}
		rec.Add(s)
	}
	net(jointrace.CoordWSDial, "", jointrace.PeerCoordinator, -3, -1)
	net(jointrace.CoordWSAuth, jointrace.CoordWSDial, jointrace.PeerCoordinator, -1, -0.5)
	net(jointrace.CoordJoin, jointrace.CoordWSAuth, jointrace.PeerCoordinator, 0, 1)
	net(jointrace.SFUWSDial, jointrace.CoordJoin, jointrace.PeerSFU, 1, 3)
	net(jointrace.SFUJoin, jointrace.SFUWSDial, jointrace.PeerSFU, 3, 4)
	rec.Add(jointrace.Span{
		Name: jointrace.PubDebounce, After: []string{jointrace.SFUJoin}, Start: at(4), End: at(5.5),
		Kind: jointrace.KindTimer, Peer: jointrace.PeerLocal,
	})
	net(jointrace.PubSetPublisher, jointrace.PubDebounce, jointrace.PeerSFU, 5.5, 6.5)
	net(jointrace.PubICE, jointrace.PubSetPublisher, jointrace.PeerUDP, 6.5, 8.5)
	net(jointrace.PubDTLS, jointrace.PubICE, jointrace.PeerUDP, 8.5, 10.5)
	net(jointrace.PubRTP, jointrace.PubDTLS, jointrace.PeerUDP, 10.5, 10.5)
	net(jointrace.SubOffer, jointrace.SFUJoin, jointrace.PeerSFU, 4, 5)
	net(jointrace.SubICE, jointrace.SubOffer, jointrace.PeerUDP, 5, 7)
	net(jointrace.SubRTP, jointrace.SubICE, jointrace.PeerUDP, 7, 8)
	tr := rec.Trace()
	tr.JoinAt = join
	return tr
}

func TestTimeToMediaCountsFromJoin(t *testing.T) {
	t.Parallel()

	tr := legacyTrace(t)
	pub := timeToMedia(tr, true)
	require.NotNil(t, pub)
	require.InDelta(t, 1100, pub.Ms, 0.01, "first packet sent at 10.5 RTT, plus half an RTT in flight")
	require.InDelta(t, 11, pub.RTTs, 0.01)
	// The websocket before Join is not on the path from Join; the debounce is a timer.
	require.InDelta(t, 9+0.5, pub.NetRTTs, 0.01)
	require.InDelta(t, 150, pub.TimerMs, 0.01)
	require.Zero(t, pub.WaitMs, "the gap between the websocket and Join is not a wait of the join")
	require.Equal(t, []string{
		jointrace.CoordJoin, jointrace.SFUWSDial, jointrace.SFUJoin, jointrace.PubDebounce,
		jointrace.PubSetPublisher, jointrace.PubICE, jointrace.PubDTLS, jointrace.PubRTP,
	}, pub.Path)

	sub := timeToMedia(tr, false)
	require.NotNil(t, sub)
	require.InDelta(t, 800, sub.Ms, 0.01)
	require.InDelta(t, 8, sub.NetRTTs, 0.01)

	tr.Spans = tr.Spans[:3]
	require.Nil(t, timeToMedia(tr, true), "not reached")
}

func TestPeerTimeToMediaCountsFromTheJoinersJoin(t *testing.T) {
	t.Parallel()

	bob := legacyTrace(t)
	rec := jointrace.NewRecorder(bob.JoinAt.Add(-time.Minute))
	rec.Add(jointrace.Span{
		Name: jointrace.SubRTP, Start: bob.JoinAt.Add(time.Second), End: bob.JoinAt.Add(1200 * time.Millisecond),
		Kind: jointrace.KindNet, Peer: jointrace.PeerUDP,
	})
	alice := rec.Trace()

	peer := peerTimeToMedia(bob, alice)
	require.NotNil(t, peer)
	require.InDelta(t, 1200, peer.Ms, 0.01, "from bob's Join, not alice's")
	require.InDelta(t, 12, peer.RTTs, 0.01, "in bob's RTT_s")
	require.Empty(t, peer.Path)

	require.Nil(t, peerTimeToMedia(bob, jointrace.NewRecorder(bob.JoinAt).Trace()), "alice received nothing")
}

func TestRefRTTIsTheSFURoundTrip(t *testing.T) {
	t.Parallel()

	tr := jointrace.Trace{RTT: map[jointrace.Peer]time.Duration{
		jointrace.PeerCoordinator: 210 * time.Millisecond, jointrace.PeerSFU: 110 * time.Millisecond,
	}}
	require.Equal(t, 110*time.Millisecond, refRTT(tr))
	delete(tr.RTT, jointrace.PeerSFU)
	require.Equal(t, 210*time.Millisecond, refRTT(tr), "the coordinator's when the SFU's is unknown")
}

func runWith(mode string, run int, tr jointrace.Trace) runResult {
	r := runResult{Mode: mode, Scenario: scenarioOneToOne, Run: run, RTTcMs: 100, RTTsMs: 100, RTTudpMs: 100}
	r.addTrace(roleBoth, "bob", tr)
	r.Publish, r.Subscribe = timeToMedia(tr, true), timeToMedia(tr, false)
	return r
}

func TestSummarizeGroupsAndMedians(t *testing.T) {
	t.Parallel()

	tr := legacyTrace(t)
	results := []runResult{
		runWith(modeCold, 0, tr),
		runWith(modeWarm, 0, tr),
		{Mode: modeWarm, Scenario: scenarioOneToOne, Run: 1, Error: "bob join: refused"},
		runWith(modeWarm, 2, tr),
	}
	groups := summarize(results, 3.5)
	require.Len(t, groups, 2)
	cold, warm := groups[0], groups[1]
	require.Equal(t, modeCold, cold.Mode)
	require.Empty(t, cold.Verdict, "the budget is for warm starts")

	require.Equal(t, 2, warm.OK)
	require.Equal(t, 3, warm.Total)
	require.Equal(t, []string{"run 1: bob join: refused"}, warm.Errors)
	require.Equal(t, "FAIL", warm.Verdict)
	require.InDelta(t, 1100, warm.Publish.Ms, 0.01)
	require.InDelta(t, 11, warm.Publish.RTTs, 0.01)
	require.InDelta(t, 800, warm.Subscribe.Ms, 0.01)

	require.Len(t, warm.Roles, 1)
	role := warm.Roles[0]
	require.Equal(t, 2, role.PathRuns)
	require.Equal(t, tr.Report().CriticalPath, role.Path)
	require.Equal(t, jointrace.CoordWSDial, role.Nodes[0].Name, "ordered by start")
	require.InDelta(t, -300, role.Nodes[0].FromJoinMs, 0.01)
	for _, n := range role.Nodes {
		if n.Name == jointrace.SFUWSDial {
			require.InDelta(t, 2, n.RTTs, 0.01)
			require.Equal(t, 2, n.Critical)
		}
	}

	require.Equal(t, "PASS", summarize(results, 11)[1].Verdict, "both within the budget")

	var out bytes.Buffer
	printSummary(&out, config{Env: envLocal, Flow: flowLegacy, Budget: 3.5}, groups)
	require.Contains(t, out.String(), "warm budget 3.5 RTT: FAIL")
	require.Contains(t, out.String(), "critical path (2/2 runs)")
}
