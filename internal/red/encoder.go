package red

import (
	"encoding/binary"

	"github.com/pion/rtp"
)

// frame is an earlier packet the encoder can repeat.
type frame struct {
	seq     uint16
	ts      uint32
	payload []byte
}

// Encoder wraps packets in RED, each one repeating the frames just before it.
// It is not safe for concurrent use.
type Encoder struct {
	distance int
	// history holds the most recent frames, oldest first.
	history []frame
}

// NewEncoder returns an encoder that repeats up to distance earlier frames in
// every packet.
func NewEncoder(distance int) *Encoder {
	return &Encoder{distance: max(distance, 0)}
}

// Encode returns the RED payload for pkt and remembers pkt for the packets
// that follow. Redundancy that would push the payload past maxSize is dropped,
// oldest first; the primary encoding is always kept.
//
// A block's sequence number is implicit in its position, so only the run of
// frames directly before pkt is repeated: a frame behind a gap would be
// recovered under the wrong sequence number.
func (e *Encoder) Encode(pkt *rtp.Packet, maxSize int) []byte {
	repeat := e.precedingRun(pkt)

	size := primaryHeaderSize + len(pkt.Payload)
	for _, f := range repeat {
		size += blockHeaderSize + len(f.payload)
	}
	for size > maxSize && len(repeat) > 0 {
		size -= blockHeaderSize + len(repeat[0].payload)
		repeat = repeat[1:]
	}

	out := make([]byte, 0, size)
	for _, f := range repeat {
		header := uint32(followBit|OpusPayloadType)<<24 |
			(pkt.Timestamp-f.ts)<<10 |
			uint32(len(f.payload))
		out = binary.BigEndian.AppendUint32(out, header)
	}
	out = append(out, OpusPayloadType)
	for _, f := range repeat {
		out = append(out, f.payload...)
	}
	out = append(out, pkt.Payload...)

	e.remember(pkt)
	return out
}

// precedingRun returns the remembered frames with sequence numbers
// pkt-n..pkt-1, oldest first, stopping at the first one that is missing or
// cannot be described by a block header.
func (e *Encoder) precedingRun(pkt *rtp.Packet) []frame {
	start := len(e.history)
	for i := len(e.history) - 1; i >= 0; i-- {
		f := e.history[i]
		back := len(e.history) - i
		if back > e.distance ||
			pkt.SequenceNumber-f.seq != uint16(back) ||
			pkt.Timestamp-f.ts > maxTimestampOffset ||
			len(f.payload) > maxBlockLength {
			break
		}
		start = i
	}
	return e.history[start:]
}

func (e *Encoder) remember(pkt *rtp.Packet) {
	if e.distance == 0 {
		return
	}
	if len(e.history) == e.distance {
		copy(e.history, e.history[1:])
		e.history = e.history[:len(e.history)-1]
	}
	e.history = append(e.history, frame{
		seq:     pkt.SequenceNumber,
		ts:      pkt.Timestamp,
		payload: append([]byte(nil), pkt.Payload...),
	})
}
