package red

import "github.com/pion/rtp"

// window is how many sequence numbers behind the newest one the decoder
// remembers. Anything older is assumed not to have been seen.
const window = 64

// Decoder unwraps RED packets into the packets they carry, recovering the
// earlier ones that never arrived on their own. Every sequence number comes
// out at most once. It is not safe for concurrent use.
type Decoder struct {
	started bool
	newest  uint16
	// seen has bit i set once newest-i has been handed out.
	seen uint64
}

// NewDecoder returns a decoder with no history.
func NewDecoder() *Decoder {
	return &Decoder{}
}

// Decode returns the packets carried by pkt, oldest first: any earlier packet
// recovered from the redundancy, then the primary one. A packet that was
// already handed out, on its own or recovered from a later one, yields
// nothing. The payloads alias pkt.Payload.
//
// Nothing is recovered from the first packet: the frames it repeats were sent
// before the decoder started listening.
func (d *Decoder) Decode(pkt *rtp.Packet) ([]*rtp.Packet, error) {
	redundant, primary, err := parse(pkt.Payload)
	if err != nil {
		return nil, err
	}

	first := !d.started
	if first {
		d.started, d.newest, d.seen = true, pkt.SequenceNumber, 0
	}
	if !d.markSeen(pkt.SequenceNumber) {
		return nil, nil
	}

	out := make([]*rtp.Packet, 0, len(redundant)+1)
	if !first {
		for i, b := range redundant {
			seq := pkt.SequenceNumber - uint16(len(redundant)-i)
			if d.markSeen(seq) {
				out = append(out, carried(pkt, seq, pkt.Timestamp-b.tsOffset, b))
			}
		}
	}
	return append(out, carried(pkt, pkt.SequenceNumber, pkt.Timestamp, primary)), nil
}

// markSeen records seq as handed out and reports whether it was new.
func (d *Decoder) markSeen(seq uint16) bool {
	if ahead := int16(seq - d.newest); ahead > 0 {
		if ahead >= window {
			d.seen = 0
		} else {
			d.seen <<= uint(ahead)
		}
		d.newest = seq
		d.seen |= 1
		return true
	}

	back := d.newest - seq
	if back >= window {
		return true
	}
	bit := uint64(1) << back
	if d.seen&bit != 0 {
		return false
	}
	d.seen |= bit
	return true
}

func carried(pkt *rtp.Packet, seq uint16, ts uint32, b block) *rtp.Packet {
	header := pkt.Header
	header.SequenceNumber = seq
	header.Timestamp = ts
	header.PayloadType = b.payloadType
	if seq != pkt.SequenceNumber {
		header.Marker = false
	}
	return &rtp.Packet{Header: header, Payload: b.payload}
}
