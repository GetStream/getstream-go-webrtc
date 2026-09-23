// Package red implements RFC 2198 redundant audio for Opus: each outgoing
// packet repeats the frames sent just before it, so a receiver can rebuild a
// lost packet from the one that follows.
package red

import (
	"encoding/binary"
	"errors"

	"github.com/pion/webrtc/v4"
)

const (
	// MimeTypeAudio is the SDP media type for RFC 2198 redundant audio.
	MimeTypeAudio = "audio/red"

	// DefaultPayloadType is the payload type Stream negotiates for RED audio.
	DefaultPayloadType webrtc.PayloadType = 63

	// OpusPayloadType is the payload type of every block. Stream negotiates
	// RED with the fmtp line "111/111", which fixes it.
	OpusPayloadType uint8 = 111

	// DefaultDistance is how many earlier frames each packet repeats.
	DefaultDistance = 2

	// A redundant block header is four bytes: F bit, 7-bit payload type,
	// 14-bit timestamp offset and 10-bit length. The primary block header is
	// the F bit and the payload type alone.
	blockHeaderSize    = 4
	primaryHeaderSize  = 1
	maxBlockLength     = 1<<10 - 1
	maxTimestampOffset = 1<<14 - 1
	followBit          = 0x80
)

var (
	// ErrIncompleteHeader is returned for a payload that ends inside its block
	// headers, or never reaches the primary block header.
	ErrIncompleteHeader = errors.New("red: incomplete block header")
	// ErrIncompleteBlock is returned for a payload shorter than its headers
	// say the redundant blocks are.
	ErrIncompleteBlock = errors.New("red: block shorter than its header")
)

// block is one encoding carried in a RED payload.
type block struct {
	payloadType uint8
	// tsOffset is how far the block's timestamp is behind the packet's.
	tsOffset uint32
	payload  []byte
}

// parse splits a RED payload into its redundant blocks, oldest first, and the
// primary block. The block payloads alias the input.
func parse(payload []byte) (redundant []block, primary block, err error) {
	var lengths []int
	rest := payload
	for {
		if len(rest) < primaryHeaderSize {
			return nil, block{}, ErrIncompleteHeader
		}
		if rest[0]&followBit == 0 {
			primary.payloadType = rest[0] &^ followBit
			rest = rest[primaryHeaderSize:]
			break
		}
		if len(rest) < blockHeaderSize {
			return nil, block{}, ErrIncompleteHeader
		}
		header := binary.BigEndian.Uint32(rest)
		redundant = append(redundant, block{
			payloadType: uint8(header>>24) &^ followBit,
			tsOffset:    header >> 10 & maxTimestampOffset,
		})
		lengths = append(lengths, int(header&maxBlockLength))
		rest = rest[blockHeaderSize:]
	}

	for i, n := range lengths {
		if n > len(rest) {
			return nil, block{}, ErrIncompleteBlock
		}
		redundant[i].payload = rest[:n:n]
		rest = rest[n:]
	}
	primary.payload = rest
	return redundant, primary, nil
}

// Primary returns the primary encoding of a RED payload, skipping the
// redundant blocks in front of it.
func Primary(payload []byte) ([]byte, error) {
	_, primary, err := parse(payload)
	if err != nil {
		return nil, err
	}
	return primary.payload, nil
}
