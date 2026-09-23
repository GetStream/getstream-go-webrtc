package audio

import (
	"fmt"
	"math/bits"
)

// G711 selects between the two companding laws defined by ITU-T G.711. Both
// squeeze a 13- or 12-bit sample into a byte on a logarithmic curve, which is
// what telephony carries: mu-law in North America and Japan, A-law elsewhere.
type G711 uint8

const (
	// MuLaw is the mu-law companding law, ITU-T G.711 clause 2.
	MuLaw G711 = iota
	// ALaw is the A-law companding law, ITU-T G.711 clause 1.
	ALaw
)

// String implements [fmt.Stringer].
func (g G711) String() string {
	switch g {
	case MuLaw:
		return "mulaw"
	case ALaw:
		return "alaw"
	default:
		return fmt.Sprintf("G711(%d)", uint8(g))
	}
}

// G711SampleRate is the sample rate G.711 is defined at. The codec itself only
// companded amplitude, but every telephony deployment runs it at 8 kHz.
const G711SampleRate = 8000

// FromG711 decodes companded telephony bytes into 16-bit PCM. One input byte
// becomes one sample, so a mono stream of n bytes is n frames at 8 kHz.
func FromG711(data []byte, law G711, sampleRate, channels int) (PCM, error) {
	if err := validate(FormatS16, sampleRate, channels); err != nil {
		return PCM{}, err
	}
	if !law.valid() {
		return PCM{}, fmt.Errorf("audio: invalid G.711 law %d", uint8(law))
	}

	table := &muLawDecode
	if law == ALaw {
		table = &aLawDecode
	}

	data = data[:len(data)-len(data)%channels]
	out := make([]int16, len(data))
	for i, b := range data {
		out[i] = table[b]
	}
	return FromInt16(out, sampleRate, channels), nil
}

// ToG711 encodes the buffer as companded telephony bytes, resampling to
// [G711SampleRate] first if needed. The result is one byte per sample.
func (p PCM) ToG711(law G711) ([]byte, error) {
	if !law.valid() {
		return nil, fmt.Errorf("audio: invalid G.711 law %d", uint8(law))
	}

	src := p
	if src.SampleRate != G711SampleRate {
		converted, err := NewResampler(FormatS16, G711SampleRate, max(1, src.Channels)).Resample(src)
		if err != nil {
			return nil, err
		}
		src = converted
	}
	samples := src.ToInt16().Int16()

	out := make([]byte, len(samples))
	encode := encodeMuLaw
	if law == ALaw {
		encode = encodeALaw
	}
	for i, s := range samples {
		out[i] = encode(s)
	}
	return out, nil
}

func (g G711) valid() bool { return g == MuLaw || g == ALaw }

// encodeMuLaw implements the ITU-T G.711 mu-law encoder.
//
// The sample is biased, its exponent found from the position of the highest set
// bit, and the four bits below that exponent kept as the mantissa. The result is
// stored inverted, which is what puts the most common quiet samples near 0xFF
// and gives the format its DC-balance properties on the wire.
func encodeMuLaw(sample int16) byte {
	const (
		bias = 0x84 // 132, added so that the exponent search never sees zero
		clip = 32635
	)

	sign := byte(0)
	if sample < 0 {
		sign = 0x80
		// Negate carefully: -(-32768) does not fit in an int16.
		if sample == -32768 {
			sample = 32767
		} else {
			sample = -sample
		}
	}
	if sample > clip {
		sample = clip
	}

	magnitude := int(sample) + bias
	exponent := g711Exponent[(magnitude>>8)&0xFF]
	mantissa := (magnitude >> (exponent + 3)) & 0x0F

	return ^(sign | byte(exponent<<4) | byte(mantissa))
}

// encodeALaw implements the ITU-T G.711 A-law encoder.
//
// A-law uses a linear segment for the quietest samples instead of mu-law's
// bias, and inverts alternate bits with 0x55 rather than every bit.
func encodeALaw(sample int16) byte {
	const clip = 32635

	sign := byte(0x80)
	if sample < 0 {
		sign = 0
		if sample == -32768 {
			sample = 32767
		} else {
			sample = -sample
		}
	}
	if sample > clip {
		sample = clip
	}

	var encoded byte
	if magnitude := int(sample); magnitude >= 256 {
		exponent := g711Exponent[(magnitude>>8)&0xFF]
		mantissa := (magnitude >> (exponent + 3)) & 0x0F
		encoded = byte(exponent<<4) | byte(mantissa)
	} else {
		encoded = byte(magnitude >> 4)
	}
	return (sign | encoded) ^ 0x55
}

// g711Exponent maps the top byte of a magnitude to its segment number. Both
// laws divide the amplitude range into eight power-of-two segments, so the
// segment is just the index of the highest set bit, capped at 7.
var g711Exponent = buildExponentTable()

func buildExponentTable() [256]byte {
	var t [256]byte
	for i := range t {
		t[i] = byte(min(7, bits.Len8(uint8(i))))
	}
	return t
}

// muLawDecode and aLawDecode are the 256-entry expansion tables. They are built
// once at init from the inverse of the encoders above, rather than pasted in as
// literals, so the two directions cannot drift apart.
var (
	muLawDecode [256]int16
	aLawDecode  [256]int16
)

func init() {
	for i := range 256 {
		muLawDecode[i] = decodeMuLaw(byte(i))
		aLawDecode[i] = decodeALaw(byte(i))
	}
}

func decodeMuLaw(b byte) int16 {
	const bias = 0x84

	b = ^b
	sign := b & 0x80
	exponent := int((b >> 4) & 0x07)
	mantissa := int(b & 0x0F)

	magnitude := ((mantissa << 3) + bias) << exponent
	magnitude -= bias

	if sign != 0 {
		return int16(-magnitude)
	}
	return int16(magnitude)
}

func decodeALaw(b byte) int16 {
	b ^= 0x55
	sign := b & 0x80
	exponent := int((b >> 4) & 0x07)
	mantissa := int(b & 0x0F)

	var magnitude int
	if exponent == 0 {
		magnitude = (mantissa << 4) + 8
	} else {
		magnitude = ((mantissa << 4) + 0x108) << (exponent - 1)
	}

	if sign == 0 {
		return int16(-magnitude)
	}
	return int16(magnitude)
}
