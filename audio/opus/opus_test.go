package opus

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/audio"
)

// tone builds a mono sine of the given length, which is the easiest signal to
// judge a lossy codec against: the error is dominated by quantisation noise
// rather than by anything structural.
func tone(sampleRate int, freq float64, d time.Duration, amplitude float64) audio.PCM {
	n := int(d * time.Duration(sampleRate) / time.Second)
	samples := make([]float32, n)
	for i := range samples {
		samples[i] = float32(amplitude * math.Sin(2*math.Pi*freq*float64(i)/float64(sampleRate)))
	}
	return audio.FromFloat32(samples, sampleRate, 1)
}

// correlation measures how much of a's shape survives in b, ignoring gain. It
// is the right metric for a codec round trip, where an absolute sample
// comparison would fail on principle.
func correlation(a, b []float32) float64 {
	n := min(len(a), len(b))
	var sumAB, sumAA, sumBB float64
	for i := range n {
		x, y := float64(a[i]), float64(b[i])
		sumAB += x * y
		sumAA += x * x
		sumBB += y * y
	}
	if sumAA == 0 || sumBB == 0 {
		return 0
	}
	return sumAB / math.Sqrt(sumAA*sumBB)
}

func TestConfigDefaults(t *testing.T) {
	t.Parallel()

	enc, err := NewEncoder(Config{})
	require.NoError(t, err)
	require.Equal(t, DefaultSampleRate, enc.Config().SampleRate)
	require.Equal(t, DefaultChannels, enc.Config().Channels)
	require.Equal(t, DefaultFrameDuration, enc.FrameDuration())
	require.Equal(t, 960, enc.FrameSamples())
}

func TestConfigRejectsBadParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
	}{
		{"odd sample rate", Config{SampleRate: 44100}},
		{"too many channels", Config{Channels: 3}},
		{"frame duration not an Opus size", Config{FrameDuration: 15 * time.Millisecond}},
		{"unknown application", Config{Application: Application(9)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewEncoder(tt.cfg)
			require.Error(t, err)
		})
	}
}

func TestEncodeFramesAtFrameDuration(t *testing.T) {
	t.Parallel()

	enc, err := NewEncoder(Config{})
	require.NoError(t, err)

	// One second at the native rate is exactly 50 frames of 20 ms.
	packets, err := enc.Encode(tone(48000, 440, time.Second, 0.5))
	require.NoError(t, err)
	require.Len(t, packets, 50)
	for _, p := range packets {
		require.NotEmpty(t, p)
	}

	// Nothing is left over, so a flush adds nothing.
	tail, err := enc.Flush()
	require.NoError(t, err)
	require.Empty(t, tail)
}

func TestEncodeBuffersPartialFrames(t *testing.T) {
	t.Parallel()

	enc, err := NewEncoder(Config{})
	require.NoError(t, err)

	// 5 ms is a quarter of a frame, so nothing can be emitted yet.
	packets, err := enc.Encode(tone(48000, 440, 5*time.Millisecond, 0.5))
	require.NoError(t, err)
	require.Empty(t, packets)

	// Four more brings the total to 25 ms: one whole frame plus a remainder.
	packets, err = enc.Encode(tone(48000, 440, 20*time.Millisecond, 0.5))
	require.NoError(t, err)
	require.Len(t, packets, 1)

	// The remainder comes out padded on flush.
	tail, err := enc.Flush()
	require.NoError(t, err)
	require.Len(t, tail, 1)
}

// TestRoundTrip is the test that matters: audio in, audio out, still
// recognisably the same signal.
func TestRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sampleRate int
		channels   int
	}{
		{"48k mono", 48000, 1},
		{"48k stereo", 48000, 2},
		{"16k mono", 16000, 1},
		{"8k mono", 8000, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := Config{SampleRate: tt.sampleRate, Channels: tt.channels}
			enc, err := NewEncoder(cfg)
			require.NoError(t, err)
			dec, err := NewDecoder(cfg)
			require.NoError(t, err)

			src := tone(tt.sampleRate, 440, 500*time.Millisecond, 0.5)

			packets, err := enc.Encode(src)
			require.NoError(t, err)
			require.NotEmpty(t, packets)

			var out audio.PCM
			for _, p := range packets {
				frame, err := dec.Decode(p)
				require.NoError(t, err)
				require.Equal(t, tt.channels, frame.Channels)
				require.Equal(t, tt.sampleRate, frame.SampleRate)
				require.NoError(t, out.Append(frame))
			}

			require.Equal(t, src.Len(), out.Len(), "decoded length should match")

			// The source is mono, so compare it against one channel of the
			// output rather than against the interleaved stream.
			left, err := out.Channel(0)
			require.NoError(t, err)

			// Opus delays its output, so line the two signals up before
			// comparing. The delay is well under 10 ms at any supported rate.
			want := src.ToFloat32().Float32()
			got := left.ToFloat32().Float32()
			best := bestCorrelation(want, got, tt.sampleRate/50)
			require.Greater(t, best, 0.95,
				"round-tripped audio should track the source, got correlation %.3f", best)

			// Loudness should survive too, not just shape.
			require.InDelta(t, src.RMS(), left.RMS(), 0.05)
		})
	}
}

// bestCorrelation finds the highest correlation over the codec's output delay.
func bestCorrelation(want, got []float32, maxLag int) float64 {
	best := 0.0
	for lag := 0; lag <= maxLag; lag++ {
		if lag >= len(got) {
			break
		}
		best = math.Max(best, correlation(want, got[lag:]))
	}
	return best
}

// TestEncoderResamples checks the ergonomic promise: hand the encoder whatever
// the TTS engine produced and it deals with it.
func TestEncoderResamples(t *testing.T) {
	t.Parallel()

	enc, err := NewEncoder(Config{})
	require.NoError(t, err)
	dec, err := NewDecoder(Config{})
	require.NoError(t, err)

	// 24 kHz s16 mono, a common TTS output, into a 48 kHz mono encoder.
	src := tone(24000, 440, time.Second, 0.5).ToInt16()
	require.Equal(t, audio.FormatS16, src.Format)

	packets, err := enc.Encode(src)
	require.NoError(t, err)

	// The interpolation filter holds back its last few input samples, so the
	// final frame only appears once the stream is flushed.
	tail, err := enc.Flush()
	require.NoError(t, err)
	packets = append(packets, tail...)
	require.Equal(t, 50, len(packets), "one second is fifty 20 ms frames")

	var out audio.PCM
	for _, p := range packets {
		frame, err := dec.Decode(p)
		require.NoError(t, err)
		require.NoError(t, out.Append(frame))
	}
	require.Equal(t, 48000, out.SampleRate)
	require.InDelta(t, src.RMS(), out.RMS(), 0.05)
}

func TestConcealProducesOneFrame(t *testing.T) {
	t.Parallel()

	dec, err := NewDecoder(Config{})
	require.NoError(t, err)

	// Cold concealment, before any packet has been seen, is silence.
	frame, err := dec.Conceal()
	require.NoError(t, err)
	require.Equal(t, 960, frame.Len())
	require.Equal(t, 20*time.Millisecond, frame.Duration())
}

// TestConcealAfterLoss checks that concealment fills the gap with something
// resembling the audio around it rather than silence, which is what keeps a
// dropped packet from sounding like a click.
func TestConcealAfterLoss(t *testing.T) {
	t.Parallel()

	enc, err := NewEncoder(Config{})
	require.NoError(t, err)
	dec, err := NewDecoder(Config{})
	require.NoError(t, err)

	packets, err := enc.Encode(tone(48000, 440, 200*time.Millisecond, 0.5))
	require.NoError(t, err)
	require.Greater(t, len(packets), 5)

	for _, p := range packets[:5] {
		_, err := dec.Decode(p)
		require.NoError(t, err)
	}

	concealed, err := dec.Conceal()
	require.NoError(t, err)
	require.Equal(t, 960, concealed.Len())
	require.Greater(t, concealed.RMS(), 0.05,
		"concealment should continue the tone, not fall silent")
}

func TestDecodeEmptyPacketConceals(t *testing.T) {
	t.Parallel()

	dec, err := NewDecoder(Config{})
	require.NoError(t, err)

	frame, err := dec.Decode(nil)
	require.NoError(t, err)
	require.Equal(t, 960, frame.Len())
}

func TestDecodeRejectsGarbage(t *testing.T) {
	t.Parallel()

	dec, err := NewDecoder(Config{})
	require.NoError(t, err)

	_, err = dec.Decode([]byte{0xFF, 0xFF, 0xFF, 0xFF})
	require.Error(t, err)
}

func TestEncodeSilence(t *testing.T) {
	t.Parallel()

	enc, err := NewEncoder(Config{})
	require.NoError(t, err)
	dec, err := NewDecoder(Config{})
	require.NoError(t, err)

	packet, err := enc.EncodeSilence()
	require.NoError(t, err)
	require.NotEmpty(t, packet)

	frame, err := dec.Decode(packet)
	require.NoError(t, err)
	require.Equal(t, 960, frame.Len())
	require.Less(t, frame.RMS(), 0.01)
}

// TestFECRecoversLostFrame exercises the loss path a real network needs: the
// packet after a drop carries a redundant copy of the one that went missing.
func TestFECRecoversLostFrame(t *testing.T) {
	t.Parallel()

	cfg := Config{FEC: true, PacketLossPercent: 20, Bitrate: 32000}
	enc, err := NewEncoder(cfg)
	require.NoError(t, err)
	dec, err := NewDecoder(cfg)
	require.NoError(t, err)

	packets, err := enc.Encode(tone(48000, 440, 200*time.Millisecond, 0.5))
	require.NoError(t, err)
	require.Greater(t, len(packets), 5)

	for _, p := range packets[:4] {
		_, err := dec.Decode(p)
		require.NoError(t, err)
	}

	// packets[4] is "lost"; recover it from packets[5], then decode packets[5].
	recovered, err := dec.DecodeFEC(packets[5])
	require.NoError(t, err)
	require.Equal(t, 960, recovered.Len())

	next, err := dec.Decode(packets[5])
	require.NoError(t, err)
	require.Equal(t, 960, next.Len())
}

func TestEncoderReset(t *testing.T) {
	t.Parallel()

	enc, err := NewEncoder(Config{})
	require.NoError(t, err)

	_, err = enc.Encode(tone(48000, 440, 5*time.Millisecond, 0.5))
	require.NoError(t, err)

	enc.Reset()

	// The buffered 5 ms is gone, so a flush has nothing to emit.
	tail, err := enc.Flush()
	require.NoError(t, err)
	require.Empty(t, tail)
}

func TestBitrateIsHonoured(t *testing.T) {
	t.Parallel()

	sizeAt := func(bitrate int) int {
		enc, err := NewEncoder(Config{Bitrate: bitrate})
		require.NoError(t, err)
		packets, err := enc.Encode(tone(48000, 440, 200*time.Millisecond, 0.5))
		require.NoError(t, err)

		total := 0
		for _, p := range packets {
			total += len(p)
		}
		return total
	}

	require.Less(t, sizeAt(16000), sizeAt(64000),
		"a lower bitrate should produce smaller packets")
}
