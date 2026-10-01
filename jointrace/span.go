// Package jointrace records where the time goes when a client joins a call: one span per
// step of the join, each with the steps it waited for, so the critical path through the
// join and its cost in network round trips can be read off.
//
// Node names follow the join DAG the SDK is measured against (the "3RTT" project's
// research/join-dag.md), so bench output and that document line up.
package jointrace

import "time"

// Kind is what a span spends its time on.
type Kind string

const (
	// KindNet is time waiting for the network: a request and its response.
	KindNet Kind = "net"
	// KindLocal is CPU work on this machine: SDP, key generation, peer connection setup.
	KindLocal Kind = "local"
	// KindTimer is time spent waiting on a timer, such as a negotiation debounce.
	KindTimer Kind = "timer"
)

// Peer is who a network span talks to. Its round trips are counted in that peer's RTT.
type Peer string

const (
	PeerCoordinator Peer = "coordinator"
	PeerSFU         Peer = "sfu"
	// PeerUDP is the media path to the SFU: ICE, DTLS and RTP.
	PeerUDP   Peer = "udp"
	PeerLocal Peer = "local"
)

// The steps of a join as the SDK performs it today (legacy path).
const (
	CoordWSDial      = "coord.ws.dial"
	CoordWSAuth      = "coord.ws.auth"
	CoordJoin        = "coord.join"
	PCsCreate        = "pcs.create"
	SFUWSDial        = "sfu.ws.dial"
	SFUJoin          = "sfu.join"
	PubDebounce      = "pub.debounce"
	PubOffer         = "pub.offer"
	PubSetPublisher  = "pub.setpublisher"
	PubSFUCandidates = "pub.sfu.candidates"
	PubTrickleOut    = "pub.trickle.out"
	PubICE           = "pub.ice"
	PubDTLS          = "pub.dtls"
	PubRTP           = "pub.rtp"
	SubSubscribe     = "sub.subscribe"
	SubGate          = "sub.gate"
	SubDebounce      = "sub.debounce"
	SubOffer         = "sub.offer"
	SubSendAnswer    = "sub.sendanswer"
	SubICE           = "sub.ice"
	SubDTLS          = "sub.dtls"
	SubRTP           = "sub.rtp"
)

// The steps of the fast join path. It shares pcs.create, sfu.ws.dial, pub.sfu.candidates,
// the ICE, DTLS and RTP steps and sub.sendanswer with the legacy path; until the SFU puts
// its candidates in the SDPs they arrive on the websocket, which is why sfu.ws and the
// *.sfu.candidates steps are still there.
const (
	CoordFastJoin    = "coord.fastjoin"
	SFUFastJoin      = "sfu.fastjoin"
	SFUWS            = "sfu.ws"
	SubAnswer        = "sub.answer"
	SubSFUCandidates = "sub.sfu.candidates"
	PubICEDTLS       = "pub.ice+dtls"
	SubICEDTLS       = "sub.ice+dtls"
)

// Suffixes of the detail spans a network step is split into. A detail span is named
// after its parent, "coord.join.tcp" for example, and has Parent set.
const (
	DetailDNS       = ".dns"
	DetailTCP       = ".tcp"
	DetailTLS       = ".tls"
	DetailRequest   = ".request"
	DetailFirstByte = ".first_byte"
	DetailServer    = ".server"
)

// Span is one step of a join.
type Span struct {
	Name string
	// After lists the steps this one waited for.
	After []string
	// Parent is set on detail spans: the network step they are part of. Detail spans are
	// not part of the dependency graph.
	Parent string
	Start  time.Time
	End    time.Time
	Kind   Kind
	Peer   Peer
	// Note says anything unusual about how the span was measured.
	Note string
}

// Duration is how long the span took.
func (s Span) Duration() time.Duration {
	return s.End.Sub(s.Start)
}
