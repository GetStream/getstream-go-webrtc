package main

import (
	"time"

	"github.com/GetStream/getstream-go-webrtc/jointrace"
)

// Roles of a recorded trace in a run.
const (
	// rolePublisher is the client whose publish time to media is measured (pubsub: alice).
	rolePublisher = "publisher"
	// roleSubscriber is the client whose subscribe time to media is measured (pubsub: bob).
	roleSubscriber = "subscriber"
	// roleBoth is a client measured both ways (one-to-one: bob, the second joiner).
	roleBoth = "both"
	// roleFirst is one-to-one's alice up to hearing bob (peer_subscribe).
	roleFirst = "first"
)

// runResult is one JSON line of the output.
type runResult struct {
	Env           string  `json:"env"`
	Flow          string  `json:"flow"`
	Mode          string  `json:"mode"`
	Scenario      string  `json:"scenario"`
	Run           int     `json:"run"`
	StartedAt     string  `json:"started_at"`
	CallID        string  `json:"call_id"`
	SFU           string  `json:"sfu,omitempty"`
	Location      string  `json:"location,omitempty"`
	InjectedRTTMs float64 `json:"injected_rtt_ms"`
	// BrokenCandidates is -break-candidates: the join fell back past that many SFUs.
	BrokenCandidates int `json:"broken_candidates,omitempty"`
	// BrokenRounds is -break-rounds: the join asked fast_join again that many times.
	BrokenRounds int `json:"broken_rounds,omitempty"`
	// SecondJoinDelayMs is -second-join-delay: bob joined that long after alice's Join.
	SecondJoinDelayMs float64 `json:"second_join_delay_ms,omitempty"`
	// GapMs is -gap: the pause after the previous call.
	GapMs float64 `json:"gap_ms,omitempty"`
	// AudioSlots is -audio-slots, for the fast flow.
	AudioSlots *uint `json:"audio_slots,omitempty"`

	// RTTc and RTTs are the measured round trips to the coordinator and the SFU,
	// averaged over the run's traces; RTTudp the media path's.
	RTTcMs   float64 `json:"rtt_c_ms"`
	RTTsMs   float64 `json:"rtt_s_ms"`
	RTTudpMs float64 `json:"rtt_udp_ms"`
	// RTTcConnectMs is the coordinator's TCP connect time on a fresh connection: the
	// round trip to the load balancer's edge, which can be much nearer than RTT_c.
	RTTcConnectMs float64 `json:"rtt_c_connect_ms,omitempty"`

	Publish   *toMedia `json:"publish,omitempty"`
	Subscribe *toMedia `json:"subscribe,omitempty"`
	// PeerSubscribe (one-to-one) is alice hearing bob: from bob's Call.Join to alice's
	// first packet of his audio, in bob's RTT_s. It has no path: it spans two clients.
	PeerSubscribe *toMedia `json:"peer_subscribe,omitempty"`

	Traces []roleTrace `json:"traces"`
	Error  string      `json:"error,omitempty"`

	// dags are the traces drawn, for -dag.
	dags []string
}

// toMedia is a time to media, from Call.Join (README "How we count").
type toMedia struct {
	Ms float64 `json:"ms"`
	// RTTs is Ms in round trips to the SFU, which the goal assumes equal to RTT_c.
	// Timers and local work count too: it is wall time.
	RTTs float64 `json:"rtts"`
	// NetRTTs are the network round trips on the path from Join to the first packet,
	// each in its own peer's RTT (half a round trip in flight for publish); TimerMs,
	// LocalMs and WaitMs the rest of the path.
	NetRTTs float64 `json:"net_rtts"`
	TimerMs float64 `json:"timer_ms"`
	LocalMs float64 `json:"local_ms"`
	WaitMs  float64 `json:"wait_ms"`
	// Path is the critical path to the first packet, from Join.
	Path []string `json:"path"`
}

type roleTrace struct {
	Role  string           `json:"role"`
	User  string           `json:"user"`
	Trace jointrace.Report `json:"trace"`
}

// refRTT is the round trip time to media is counted in: RTT_s, a clean TCP connect to
// the SFU. RTT_c is taken from the coordinator's websocket exchanges, which carry some
// server time and, on a warm client, date from its first connect.
func refRTT(t jointrace.Trace) time.Duration {
	if s := t.RTT[jointrace.PeerSFU]; s > 0 {
		return s
	}
	return t.RTT[jointrace.PeerCoordinator]
}

// timeToMedia is the publish (or subscribe) time to media of t, or nil when the first
// packet was not reached.
func timeToMedia(t jointrace.Trace, publish bool) *toMedia {
	total, ok := t.SubscribeToMedia()
	terminal := jointrace.SubRTP
	if publish {
		total, ok = t.PublishToMedia()
		terminal = jointrace.PubRTP
	}
	if !ok {
		return nil
	}
	m := &toMedia{Ms: ms(total)}
	if ref := refRTT(t); ref > 0 {
		m.RTTs = round2(float64(total) / float64(ref))
	}
	var net float64
	for _, step := range t.PathTo(terminal).Steps {
		if !step.Span.End.After(t.JoinAt) {
			// The client's own coordinator connection, opened before Join.
			continue
		}
		wait := step.Wait
		if len(m.Path) == 0 {
			// Only the wait after Join counts, not the time before it was called.
			wait = min(wait, max(step.Span.Start.Sub(t.JoinAt), 0))
		}
		m.Path = append(m.Path, step.Span.Name)
		m.WaitMs += ms(wait)
		switch step.Span.Kind {
		case jointrace.KindNet:
			net += step.RTTs
		case jointrace.KindTimer:
			m.TimerMs += ms(step.Cost)
		default:
			m.LocalMs += ms(step.Cost)
		}
	}
	if publish {
		net += 0.5
	}
	m.NetRTTs = round2(net)
	m.TimerMs, m.LocalMs, m.WaitMs = round2(m.TimerMs), round2(m.LocalMs), round2(m.WaitMs)
	return m
}

// peerTimeToMedia is the time from joiner's Call.Join to peer's first received packet,
// or nil when peer received nothing.
func peerTimeToMedia(joiner, peer jointrace.Trace) *toMedia {
	first, ok := peer.Span(jointrace.SubRTP)
	if !ok || joiner.JoinAt.IsZero() {
		return nil
	}
	total := first.End.Sub(joiner.JoinAt)
	m := &toMedia{Ms: ms(total)}
	if ref := refRTT(joiner); ref > 0 {
		m.RTTs = round2(float64(total) / float64(ref))
	}
	return m
}

func ms(d time.Duration) float64 {
	return round2(float64(d) / float64(time.Millisecond))
}

func round2(v float64) float64 {
	if v < 0 {
		return -round2(-v)
	}
	return float64(int64(v*100+0.5)) / 100
}
