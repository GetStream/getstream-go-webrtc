package red

import (
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// maxPayloadSize keeps a RED packet, with its RTP header and extensions,
// under the 1200 byte MTU the tracks packetize for.
const maxPayloadSize = 1100

// Packetizer turns the Opus packets of an inner packetizer into RED packets.
type Packetizer struct {
	inner       rtp.Packetizer
	encoder     *Encoder
	payloadType uint8
}

var _ rtp.Packetizer = (*Packetizer)(nil)

// NewPacketizer wraps inner, sending its packets as payloadType with
// [DefaultDistance] frames of redundancy.
func NewPacketizer(inner rtp.Packetizer, payloadType webrtc.PayloadType) *Packetizer {
	return &Packetizer{
		inner:       inner,
		encoder:     NewEncoder(DefaultDistance),
		payloadType: uint8(payloadType),
	}
}

// Packetize packetizes payload and wraps each packet in RED.
func (p *Packetizer) Packetize(payload []byte, samples uint32) []*rtp.Packet {
	pkts := p.inner.Packetize(payload, samples)
	for i, pkt := range pkts {
		wrapped := *pkt
		wrapped.PayloadType = p.payloadType
		wrapped.Payload = p.encoder.Encode(pkt, maxPayloadSize)
		pkts[i] = &wrapped
	}
	return pkts
}

// GeneratePadding returns the inner packetizer's padding under the RED payload
// type. Padding carries no audio, so it is neither wrapped nor repeated.
func (p *Packetizer) GeneratePadding(samples uint32) []*rtp.Packet {
	pkts := p.inner.GeneratePadding(samples)
	for _, pkt := range pkts {
		pkt.PayloadType = p.payloadType
	}
	return pkts
}

// EnableAbsSendTime forwards to the inner packetizer.
func (p *Packetizer) EnableAbsSendTime(value int) {
	p.inner.EnableAbsSendTime(value)
}

// SkipSamples forwards to the inner packetizer.
func (p *Packetizer) SkipSamples(skippedSamples uint32) {
	p.inner.SkipSamples(skippedSamples)
}
