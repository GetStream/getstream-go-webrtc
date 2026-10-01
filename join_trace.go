package rtc

import (
	"sync"
	"time"

	"github.com/GetStream/getstream-go-webrtc/jointrace"
	"github.com/GetStream/getstream-go-webrtc/pc"
)

// JoinTraceTimeout is how long after Join the OnJoinTrace handler is called at the latest,
// with whatever was recorded, when media has not started flowing both ways by then.
const JoinTraceTimeout = 10 * time.Second

// joinTracer records the spans of a call's first join. A reconnect or migration builds
// new peer connections but leaves the trace alone: it describes how the call came up,
// not how the last repair went.
type joinTracer struct {
	mu      sync.Mutex
	rec     *jointrace.Recorder
	joined  bool
	fired   bool
	timer   *time.Timer
	handler func(jointrace.Trace)

	// fast is set when the first join takes the fast path, whose steps depend on each
	// other differently.
	fast bool

	// Moments the spans are built from that are not spans of their own.
	pubSignalSent time.Time
	subOfferAt    time.Time

	// udpRTT samples the media path's RTT from the peer connections.
	udpRTT func() time.Duration
}

// begin starts the trace at the first Join and returns its recorder. It returns nil for
// a Join that re-enters to repair a call that had already joined.
func (j *joinTracer) begin(now time.Time, reconnect bool) *jointrace.Recorder {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.rec == nil {
		j.rec = jointrace.NewRecorder(now)
		j.timer = time.AfterFunc(JoinTraceTimeout, j.fire)
		return j.rec
	}
	if reconnect && j.joined {
		j.rec.Seal()
		return nil
	}
	return j.rec
}

// recorder is the trace's recorder, nil before the first Join. Recording into it is a
// no-op once it is sealed.
func (j *joinTracer) recorder() *jointrace.Recorder {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.rec
}

func (j *joinTracer) markJoined() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.joined = true
}

func (j *joinTracer) setFast(fast bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.fast = fast
}

func (j *joinTracer) isFast() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.fast
}

func (j *joinTracer) snapshot() jointrace.Trace {
	j.mu.Lock()
	rec, sample := j.rec, j.udpRTT
	j.mu.Unlock()
	if sample != nil {
		rec.SetRTT(jointrace.PeerUDP, sample())
	}
	return rec.Trace()
}

// done hands the trace to the handler once media flows both ways.
func (j *joinTracer) done() {
	rec := j.recorder()
	if rec.Has(jointrace.PubRTP) && rec.Has(jointrace.SubRTP) {
		j.fire()
	}
}

func (j *joinTracer) fire() {
	j.mu.Lock()
	if j.fired || j.rec == nil {
		j.mu.Unlock()
		return
	}
	j.fired = true
	if j.timer != nil {
		j.timer.Stop()
	}
	handler := j.handler
	j.mu.Unlock()
	if handler != nil {
		// Off the packet path: the snapshot asks ICE for the pair's RTT.
		go func() { handler(j.snapshot()) }()
	}
}

// JoinTrace returns what has been recorded of the call's first join so far.
func (c *Call) JoinTrace() jointrace.Trace {
	return c.trace.snapshot()
}

// OnJoinTrace sets a handler called once with the first join's trace: when media flows
// both ways (the first RTP packet sent and the first received), or JoinTraceTimeout
// after Join otherwise. Set it before Join. It runs on its own goroutine.
func (c *Call) OnJoinTrace(handler func(jointrace.Trace)) {
	c.trace.mu.Lock()
	defer c.trace.mu.Unlock()
	c.trace.handler = handler
}

// peerSpans records the spans a transport's timing completes.
func (c *Call) peerSpans(publisher bool, t pc.Timing) {
	rec := c.trace.recorder()
	if rec == nil {
		return
	}
	if c.trace.isFast() {
		fastPeerSpans(rec, publisher, t)
		return
	}
	if publisher {
		rec.Add(jointrace.Span{
			Name: jointrace.PubDebounce, After: []string{jointrace.SFUJoin},
			Start: t.NegotiationRequested, End: t.OfferStarted,
			Kind: jointrace.KindTimer, Peer: jointrace.PeerLocal,
		})
		c.trace.mu.Lock()
		sent := c.trace.pubSignalSent
		c.trace.mu.Unlock()
		rec.Add(jointrace.Span{
			Name: jointrace.PubSFUCandidates, After: []string{jointrace.PubSetPublisher},
			Start: sent, End: t.FirstRemoteCandidate,
			Kind: jointrace.KindNet, Peer: jointrace.PeerSFU,
			Note: "offer sent to first SFU candidate",
		})
		rec.Add(jointrace.Span{
			Name: jointrace.PubICE, After: []string{jointrace.PubSetPublisher, jointrace.PubSFUCandidates},
			Start: t.ICEChecking, End: t.ICEConnected,
			Kind: jointrace.KindNet, Peer: jointrace.PeerUDP,
		})
		rec.Add(jointrace.Span{
			Name: jointrace.PubDTLS, After: []string{jointrace.PubICE},
			Start: t.ICEConnected, End: t.DTLSConnected,
			Kind: jointrace.KindNet, Peer: jointrace.PeerUDP,
		})
		return
	}
	rec.Add(jointrace.Span{
		Name: jointrace.SubICE, After: []string{jointrace.SubSendAnswer},
		Start: t.ICEChecking, End: t.ICEConnected,
		Kind: jointrace.KindNet, Peer: jointrace.PeerUDP,
	})
	rec.Add(jointrace.Span{
		Name: jointrace.SubDTLS, After: []string{jointrace.SubICE},
		Start: t.ICEConnected, End: t.DTLSConnected,
		Kind: jointrace.KindNet, Peer: jointrace.PeerUDP,
	})
}

// fastPeerSpans is peerSpans for a fast join. The offer and answer went with the
// FastJoin. When they carried the SFU's candidates, ICE waits for nothing else; an SFU
// that only trickles them sends them on the websocket, so ICE also waits for its attach.
func fastPeerSpans(rec *jointrace.Recorder, publisher bool, t pc.Timing) {
	candidates, signalled := jointrace.SubSFUCandidates, jointrace.SubAnswer
	ice, dtls := jointrace.SubICE, jointrace.SubDTLS
	if publisher {
		candidates, signalled = jointrace.PubSFUCandidates, jointrace.SFUFastJoin
		ice, dtls = jointrace.PubICE, jointrace.PubDTLS
	}
	iceAfter := []string{signalled}
	if !t.RemoteCandidatesInDescription {
		iceAfter = append(iceAfter, candidates)
		if attached, ok := rec.Get(jointrace.SFUWS); ok {
			rec.Add(jointrace.Span{
				Name: candidates, After: []string{jointrace.SFUWS},
				Start: attached.End, End: t.FirstRemoteCandidate,
				Kind: jointrace.KindNet, Peer: jointrace.PeerSFU,
				Note: "trickled on the websocket once it attaches",
			})
		}
	}
	rec.Add(jointrace.Span{
		Name: ice, After: iceAfter,
		Start: t.ICEChecking, End: t.ICEConnected,
		Kind: jointrace.KindNet, Peer: jointrace.PeerUDP,
	})
	rec.Add(jointrace.Span{
		Name: dtls, After: []string{ice},
		Start: t.ICEConnected, End: t.DTLSConnected,
		Kind: jointrace.KindNet, Peer: jointrace.PeerUDP,
	})
}

// subscriberAnswered records the client's side of the fast join's subscriber offer:
// from the FastJoin response that carried it to the answer being ready to send.
func (c *Call) subscriberAnswered(at time.Time) {
	rec := c.trace.recorder()
	c.trace.mu.Lock()
	offerAt := c.trace.subOfferAt
	c.trace.mu.Unlock()
	rec.Add(jointrace.Span{
		Name: jointrace.SubAnswer, After: []string{jointrace.SFUFastJoin},
		Start: offerAt, End: at, Kind: jointrace.KindLocal, Peer: jointrace.PeerLocal,
		Note: "sent without waiting",
	})
}

// firstRTP records the first RTP packet sent by the publisher or received by the
// subscriber.
func (c *Call) firstRTP(publisher bool, dtlsConnected, at time.Time) {
	rec := c.trace.recorder()
	name, after := jointrace.SubRTP, jointrace.SubDTLS
	if publisher {
		name, after = jointrace.PubRTP, jointrace.PubDTLS
	}
	start := dtlsConnected
	if start.IsZero() || start.After(at) {
		start = at
	}
	rec.Add(jointrace.Span{
		Name: name, After: []string{after}, Start: start, End: at,
		Kind: jointrace.KindNet, Peer: jointrace.PeerUDP,
	})
	c.trace.done()
}

// subscriberOffer records the SFU's side of the subscriber offer: the wait between the
// SFU join and the offer arriving. The client sees only the arrival, so the split into
// the SFU's wait and the half round trip in flight is inferred from RTT_s.
func (c *Call) subscriberOffer(at time.Time) {
	rec := c.trace.recorder()
	c.trace.mu.Lock()
	if c.trace.subOfferAt.IsZero() {
		c.trace.subOfferAt = at
	}
	c.trace.mu.Unlock()
	joined, ok := rec.Get(jointrace.SFUJoin)
	if !ok || rec.Has(jointrace.SubOffer) {
		return
	}
	sent := at.Add(-rec.RTT(jointrace.PeerSFU) / 2)
	if sent.Before(joined.End) {
		sent = joined.End
	}
	rec.Add(jointrace.Span{
		Name: jointrace.SubDebounce, After: []string{jointrace.SFUJoin},
		Start: joined.End, End: sent, Kind: jointrace.KindTimer, Peer: jointrace.PeerSFU,
		Note: "SFU-side wait (debounce or publisher gate), inferred",
	})
	rec.Add(jointrace.Span{
		Name: jointrace.SubOffer, After: []string{jointrace.SubDebounce},
		Start: sent, End: at, Kind: jointrace.KindNet, Peer: jointrace.PeerSFU,
		Note: "RTT_s/2 in flight, inferred",
	})
}

// selectedPairRTT is the RTT ICE measured on the publisher's selected pair, or the
// subscriber's.
func (c *Call) selectedPairRTT() time.Duration {
	if pub := c.publisherPeer(); pub != nil && pub.Transport != nil {
		if rtt := pub.SelectedPairRTT(); rtt > 0 {
			return rtt
		}
	}
	if sub := c.subscriberPeer(); sub != nil && sub.Transport != nil {
		return sub.SelectedPairRTT()
	}
	return 0
}

// afterFirst returns the first of names that was recorded, as a dependency list.
func afterFirst(rec *jointrace.Recorder, names ...string) []string {
	for _, name := range names {
		if rec.Has(name) {
			return []string{name}
		}
	}
	return nil
}
