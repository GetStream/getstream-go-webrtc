package audio

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestG711KnownCodes pins the decoders to values published in ITU-T G.711, so
// a refactor cannot quietly shift the companding curve.
func TestG711KnownCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		law  G711
		code byte
		want int16
	}{
		{"mulaw positive zero", MuLaw, 0xFF, 0},
		{"mulaw negative zero", MuLaw, 0x7F, 0},
		{"mulaw most positive", MuLaw, 0x80, 32124},
		{"mulaw most negative", MuLaw, 0x00, -32124},
		{"alaw smallest positive", ALaw, 0xD5, 8},
		{"alaw smallest negative", ALaw, 0x55, -8},
		{"alaw most positive", ALaw, 0xAA, 32256},
		{"alaw most negative", ALaw, 0x2A, -32256},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pcm, err := FromG711([]byte{tt.code}, tt.law, G711SampleRate, 1)
			require.NoError(t, err)
			require.Equal(t, []int16{tt.want}, pcm.Int16())
		})
	}
}

// TestG711CodeRoundTrip checks the defining property of a G.711 codec: each
// code decodes to the midpoint of its quantization interval, so re-encoding
// that value must return the same code.
func TestG711CodeRoundTrip(t *testing.T) {
	t.Parallel()

	for _, law := range []G711{MuLaw, ALaw} {
		t.Run(law.String(), func(t *testing.T) {
			t.Parallel()

			codes := make([]byte, 256)
			for i := range codes {
				codes[i] = byte(i)
			}

			decoded, err := FromG711(codes, law, G711SampleRate, 1)
			require.NoError(t, err)

			reencoded, err := decoded.ToG711(law)
			require.NoError(t, err)
			require.Len(t, reencoded, 256)

			redecoded, err := FromG711(reencoded, law, G711SampleRate, 1)
			require.NoError(t, err)

			// Codes are compared through a second decode because the two
			// zero codes of mu-law collapse onto a single sample value.
			require.Equal(t, decoded.Int16(), redecoded.Int16())
		})
	}
}

// TestG711Monotonic checks that the companding curve never goes backwards,
// which is the cheapest way to catch a wrong segment or mantissa shift.
func TestG711Monotonic(t *testing.T) {
	t.Parallel()

	for _, law := range []G711{MuLaw, ALaw} {
		t.Run(law.String(), func(t *testing.T) {
			t.Parallel()

			var prev int16
			first := true
			for v := -32768; v <= 32767; v += 7 {
				pcm := FromInt16([]int16{int16(v)}, G711SampleRate, 1)
				encoded, err := pcm.ToG711(law)
				require.NoError(t, err)

				decoded, err := FromG711(encoded, law, G711SampleRate, 1)
				require.NoError(t, err)
				got := decoded.Int16()[0]

				if !first {
					require.GreaterOrEqual(t, got, prev,
						"companding curve decreased at input %d", v)
				}
				prev, first = got, false
			}
		})
	}
}

// TestG711Accuracy checks that a round trip stays within the quantization
// error the format allows, rather than merely being self-consistent.
func TestG711Accuracy(t *testing.T) {
	t.Parallel()

	for _, law := range []G711{MuLaw, ALaw} {
		t.Run(law.String(), func(t *testing.T) {
			t.Parallel()

			samples := make([]int16, 0, 4096)
			for v := -32000; v <= 32000; v += 16 {
				samples = append(samples, int16(v))
			}
			src := FromInt16(samples, G711SampleRate, 1)

			encoded, err := src.ToG711(law)
			require.NoError(t, err)
			require.Len(t, encoded, len(samples))

			decoded, err := FromG711(encoded, law, G711SampleRate, 1)
			require.NoError(t, err)

			for i, want := range samples {
				got := decoded.Int16()[i]
				// G.711 keeps roughly 8 bits of a logarithmic scale, so the
				// error grows with amplitude; 8% is inside the spec's
				// quantization interval everywhere.
				tolerance := float64(abs16(want))*0.08 + 64
				require.InDelta(t, float64(want), float64(got), tolerance,
					"sample %d", i)
			}
		})
	}
}

func TestG711Resamples(t *testing.T) {
	t.Parallel()

	// A 48 kHz buffer must come back as 8 kHz bytes, one per frame.
	src := FromInt16(make([]int16, 4800), 48000, 1)
	encoded, err := src.ToG711(MuLaw)
	require.NoError(t, err)
	require.Len(t, encoded, 800)
}

func TestG711Invalid(t *testing.T) {
	t.Parallel()

	_, err := FromG711([]byte{0}, G711(9), G711SampleRate, 1)
	require.Error(t, err)

	_, err = FromInt16([]int16{0}, G711SampleRate, 1).ToG711(G711(9))
	require.Error(t, err)
}

func abs16(v int16) int32 {
	if v < 0 {
		return -int32(v)
	}
	return int32(v)
}
