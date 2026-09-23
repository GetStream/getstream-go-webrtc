package red

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

const (
	firstSeq = uint16(1000)
	firstTS  = uint32(1 << 20)
	// frameTicks is one 20 ms Opus frame at 48 kHz.
	frameTicks = uint32(960)
)

// opusStream returns count in-order Opus packets with distinct payloads.
func opusStream(count int) []*rtp.Packet {
	pkts := make([]*rtp.Packet, count)
	for i := range pkts {
		pkts[i] = &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				PayloadType:    OpusPayloadType,
				SequenceNumber: firstSeq + uint16(i),
				Timestamp:      firstTS + uint32(i)*frameTicks,
			},
			Payload: fmt.Appendf(nil, "frame-%d", i),
		}
	}
	return pkts
}

// encodeStream wraps each packet the way the SFU and the tracks do.
func encodeStream(primary []*rtp.Packet) []*rtp.Packet {
	encoder := NewEncoder(DefaultDistance)
	out := make([]*rtp.Packet, len(primary))
	for i, pkt := range primary {
		wrapped := *pkt
		wrapped.PayloadType = uint8(DefaultPayloadType)
		wrapped.Payload = encoder.Encode(pkt, maxPayloadSize)
		out[i] = &wrapped
	}
	return out
}

// blocks decodes a RED payload into every encoding it carries, oldest first,
// with the sequence number and timestamp a receiver would give each.
func blocks(t *testing.T, pkt *rtp.Packet) []*rtp.Packet {
	t.Helper()

	redundant, primary, err := parse(pkt.Payload)
	require.NoError(t, err)

	out := make([]*rtp.Packet, 0, len(redundant)+1)
	for i, b := range redundant {
		out = append(out, carried(pkt, pkt.SequenceNumber-uint16(len(redundant)-i), pkt.Timestamp-b.tsOffset, b))
	}
	return append(out, carried(pkt, pkt.SequenceNumber, pkt.Timestamp, primary))
}

func requireSamePackets(t *testing.T, want, got []*rtp.Packet) {
	t.Helper()

	require.Len(t, got, len(want))
	for i := range want {
		require.Equal(t, want[i].SequenceNumber, got[i].SequenceNumber, "packet %d", i)
		require.Equal(t, want[i].Timestamp, got[i].Timestamp, "packet %d", i)
		require.Equal(t, OpusPayloadType, got[i].PayloadType, "packet %d", i)
		require.Equal(t, want[i].Payload, got[i].Payload, "packet %d", i)
	}
}

// blockHeader builds the four byte header of a redundant block.
func blockHeader(tsOffset uint32, length int) []byte {
	header := uint32(followBit|OpusPayloadType)<<24 | tsOffset<<10 | uint32(length)
	return binary.BigEndian.AppendUint32(nil, header)
}

func TestEncoderRepeatsTheFramesBeforeIt(t *testing.T) {
	t.Parallel()

	primary := opusStream(4)
	encoded := encodeStream(primary)

	requireSamePackets(t, primary[:1], blocks(t, encoded[0]))
	requireSamePackets(t, primary[:2], blocks(t, encoded[1]))
	requireSamePackets(t, primary[:3], blocks(t, encoded[2]))
	requireSamePackets(t, primary[1:4], blocks(t, encoded[3]))
}

// A receiver places a redundant block by its position, so a frame behind a
// gap in the sender's own sequence numbers must not be repeated.
func TestEncoderOnlyRepeatsTheRunDirectlyBeforeThePacket(t *testing.T) {
	t.Parallel()

	stream := opusStream(4)
	encoder := NewEncoder(DefaultDistance)
	encoder.Encode(stream[0], maxPayloadSize)
	encoder.Encode(stream[1], maxPayloadSize)
	// stream[2] was dropped before it was ever sent.
	payload := encoder.Encode(stream[3], maxPayloadSize)

	requireSamePackets(t, stream[3:4], blocks(t, &rtp.Packet{Header: stream[3].Header, Payload: payload}))
}

func TestEncoderDropsTheOldestRedundancyThatDoesNotFit(t *testing.T) {
	t.Parallel()

	stream := opusStream(3)
	encoder := NewEncoder(DefaultDistance)
	encoder.Encode(stream[0], maxPayloadSize)
	encoder.Encode(stream[1], maxPayloadSize)

	// Room for the primary and one redundant block, not two.
	limit := primaryHeaderSize + len(stream[2].Payload) + blockHeaderSize + len(stream[1].Payload)
	payload := encoder.Encode(stream[2], limit)

	require.Len(t, payload, limit)
	requireSamePackets(t, stream[1:3], blocks(t, &rtp.Packet{Header: stream[2].Header, Payload: payload}))
}

// The header has 14 bits for the timestamp offset; a frame further back than
// that cannot be described and is left out.
func TestEncoderSkipsAFrameWhoseTimestampOffsetDoesNotFit(t *testing.T) {
	t.Parallel()

	stream := opusStream(2)
	stream[1].Timestamp = stream[0].Timestamp + maxTimestampOffset + 1

	encoder := NewEncoder(DefaultDistance)
	encoder.Encode(stream[0], maxPayloadSize)
	payload := encoder.Encode(stream[1], maxPayloadSize)

	requireSamePackets(t, stream[1:2], blocks(t, &rtp.Packet{Header: stream[1].Header, Payload: payload}))
}

// The encoder keeps its own copy of each frame, so a caller reusing its buffer
// cannot corrupt the redundancy of the next packet.
func TestEncoderCopiesTheFramesItRemembers(t *testing.T) {
	t.Parallel()

	stream := opusStream(2)
	want := append([]byte(nil), stream[0].Payload...)

	encoder := NewEncoder(DefaultDistance)
	encoder.Encode(stream[0], maxPayloadSize)
	clear(stream[0].Payload)
	payload := encoder.Encode(stream[1], maxPayloadSize)

	got := blocks(t, &rtp.Packet{Header: stream[1].Header, Payload: payload})
	require.Equal(t, want, got[0].Payload)
}

func TestDecoderRecoversOnlyWhatItHasNotSeen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// deliver lists the packets that reach the decoder, by offset from the
		// first; the rest were lost in the network.
		deliver []int
		// want is what comes out of each delivery, as offsets again.
		want [][]int
	}{
		{
			name:    "an unbroken stream needs no recovery",
			deliver: []int{0, 1, 2},
			want:    [][]int{{0}, {1}, {2}},
		},
		{
			name:    "one lost packet comes back with the next one",
			deliver: []int{0, 2},
			want:    [][]int{{0}, {1, 2}},
		},
		{
			name:    "two lost packets both come back",
			deliver: []int{0, 3},
			want:    [][]int{{0}, {1, 2, 3}},
		},
		{
			name:    "the third packet back is beyond the redundancy",
			deliver: []int{0, 4},
			want:    [][]int{{0}, {2, 3, 4}},
		},
		{
			name:    "after a long gap only the redundancy comes back",
			deliver: []int{0, 9},
			want:    [][]int{{0}, {7, 8, 9}},
		},
		{
			name:    "a duplicate yields nothing",
			deliver: []int{0, 1, 1},
			want:    [][]int{{0}, {1}, {}},
		},
		{
			name:    "a late packet that was already recovered yields nothing",
			deliver: []int{0, 2, 1},
			want:    [][]int{{0}, {1, 2}, {}},
		},
		{
			name:    "a late packet that was not recovered still comes out",
			deliver: []int{0, 4, 1},
			want:    [][]int{{0}, {2, 3, 4}, {1}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			primary := opusStream(10)
			encoded := encodeStream(primary)
			decoder := NewDecoder()

			for i, offset := range tt.deliver {
				decoded, err := decoder.Decode(encoded[offset])
				require.NoError(t, err)

				want := make([]*rtp.Packet, 0, len(tt.want[i]))
				for _, w := range tt.want[i] {
					want = append(want, primary[w])
				}
				requireSamePackets(t, want, decoded)
			}
		})
	}
}

func TestDecoderAcrossTheSequenceNumberWrap(t *testing.T) {
	t.Parallel()

	primary := opusStream(4)
	for i, pkt := range primary {
		pkt.SequenceNumber = 65534 + uint16(i)
	}
	encoded := encodeStream(primary)
	decoder := NewDecoder()

	decoded, err := decoder.Decode(encoded[0])
	require.NoError(t, err)
	requireSamePackets(t, primary[:1], decoded)

	decoded, err = decoder.Decode(encoded[3])
	require.NoError(t, err)
	requireSamePackets(t, primary[1:4], decoded)
}

func TestDecoderHandlesAPayloadWithOnlyThePrimaryBlock(t *testing.T) {
	t.Parallel()

	pkt := &rtp.Packet{
		Header:  rtp.Header{SequenceNumber: firstSeq, Timestamp: firstTS, PayloadType: uint8(DefaultPayloadType)},
		Payload: append([]byte{OpusPayloadType}, "opus"...),
	}

	decoded, err := NewDecoder().Decode(pkt)
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	require.Equal(t, OpusPayloadType, decoded[0].PayloadType, "the payload type comes from the block header")
	require.Equal(t, []byte("opus"), decoded[0].Payload)
}

func TestDecoderRejectsMalformedPayloads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload []byte
		wantErr error
	}{
		{name: "empty payload", payload: nil, wantErr: ErrIncompleteHeader},
		{name: "a block header cut short", payload: []byte{followBit | OpusPayloadType, 0x00, 0x02}, wantErr: ErrIncompleteHeader},
		{name: "no primary block header", payload: blockHeader(frameTicks, 4), wantErr: ErrIncompleteHeader},
		{
			name:    "a block shorter than its header claims",
			payload: append(append(blockHeader(frameTicks, 10), OpusPayloadType), "abc"...),
			wantErr: ErrIncompleteBlock,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewDecoder().Decode(&rtp.Packet{Payload: tt.payload})
			require.ErrorIs(t, err, tt.wantErr)

			_, err = Primary(tt.payload)
			require.ErrorIs(t, err, tt.wantErr, "Primary walks the same headers")
		})
	}
}

func TestPrimaryReturnsTheNewestEncoding(t *testing.T) {
	t.Parallel()

	primary := opusStream(4)
	for i, pkt := range encodeStream(primary) {
		got, err := Primary(pkt.Payload)
		require.NoError(t, err)
		require.Equal(t, primary[i].Payload, got)
	}
}

// fakeInnerPacketizer stands in for the Opus packetizer: one packet per
// sample, sequence numbers and timestamps advancing in step.
type fakeInnerPacketizer struct {
	seq         uint16
	ts          uint32
	skipped     uint32
	absSendTime int
	padding     int
}

var _ rtp.Packetizer = (*fakeInnerPacketizer)(nil)

func (p *fakeInnerPacketizer) next(payload []byte, samples uint32) *rtp.Packet {
	pkt := &rtp.Packet{
		Header:  rtp.Header{Version: 2, SequenceNumber: p.seq, Timestamp: p.ts},
		Payload: append([]byte(nil), payload...),
	}
	p.seq++
	p.ts += samples
	return pkt
}

func (p *fakeInnerPacketizer) Packetize(payload []byte, samples uint32) []*rtp.Packet {
	return []*rtp.Packet{p.next(payload, samples)}
}

func (p *fakeInnerPacketizer) GeneratePadding(samples uint32) []*rtp.Packet {
	pkts := make([]*rtp.Packet, p.padding)
	for i := range pkts {
		pkts[i] = p.next(nil, samples)
		pkts[i].Padding = true
	}
	return pkts
}

func (p *fakeInnerPacketizer) EnableAbsSendTime(value int) { p.absSendTime = value }
func (p *fakeInnerPacketizer) SkipSamples(samples uint32)  { p.skipped += samples }

func TestPacketizerWrapsEachPacketWithTheOnesBeforeIt(t *testing.T) {
	t.Parallel()

	inner := &fakeInnerPacketizer{seq: firstSeq, ts: firstTS}
	packetizer := NewPacketizer(inner, DefaultPayloadType)

	var sent []*rtp.Packet
	for i := range 3 {
		header := rtp.Header{Version: 2, PayloadType: OpusPayloadType, SequenceNumber: inner.seq, Timestamp: inner.ts}
		payload := fmt.Appendf(nil, "frame-%d", i)

		pkts := packetizer.Packetize(payload, frameTicks)
		require.Len(t, pkts, 1)
		require.Equal(t, uint8(DefaultPayloadType), pkts[0].PayloadType, "packets go out as audio/red")

		sent = append(sent, &rtp.Packet{Header: header, Payload: payload})
		requireSamePackets(t, sent[max(0, i-DefaultDistance):], blocks(t, pkts[0]))
	}
}

// Each packet carries the raw frames before it, never their RED payloads, so
// its size stays fixed however long the stream runs.
func TestPacketizerPayloadSizeStaysBounded(t *testing.T) {
	t.Parallel()

	packetizer := NewPacketizer(&fakeInnerPacketizer{seq: firstSeq, ts: firstTS}, DefaultPayloadType)
	frame := make([]byte, 100)
	want := (DefaultDistance+1)*len(frame) + DefaultDistance*blockHeaderSize + primaryHeaderSize

	for i := range 50 {
		pkts := packetizer.Packetize(frame, frameTicks)
		if i >= DefaultDistance {
			require.Len(t, pkts[0].Payload, want, "packet %d", i)
		}
	}
}

func TestPacketizerPassesPaddingThroughAndForwardsToTheInnerPacketizer(t *testing.T) {
	t.Parallel()

	inner := &fakeInnerPacketizer{seq: firstSeq, ts: firstTS, padding: 1}
	packetizer := NewPacketizer(inner, DefaultPayloadType)

	padding := packetizer.GeneratePadding(frameTicks)
	require.Len(t, padding, 1)
	require.Equal(t, uint8(DefaultPayloadType), padding[0].PayloadType)
	require.Empty(t, padding[0].Payload, "padding is not wrapped in RED")

	packetizer.SkipSamples(480)
	packetizer.EnableAbsSendTime(3)
	require.Equal(t, uint32(480), inner.skipped)
	require.Equal(t, 3, inner.absSendTime)
}
