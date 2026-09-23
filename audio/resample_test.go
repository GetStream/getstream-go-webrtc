package audio

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// sine builds a mono float32 tone, the reference signal for every resampler
// assertion here: a resampler that works reproduces it at the new rate, and one
// that does not shows up immediately as distortion.
func sine(sampleRate int, freq float64, frames int, amplitude float64) PCM {
	s := make([]float32, frames)
	for i := range s {
		s[i] = float32(amplitude * math.Sin(2*math.Pi*freq*float64(i)/float64(sampleRate)))
	}
	return FromFloat32(s, sampleRate, 1)
}

// maxDeviation compares a resampled tone against the ideal tone at the new
// rate, skipping the edges where the filter is still running in or out.
func maxDeviation(t *testing.T, got PCM, freq, amplitude float64, skip int) float64 {
	t.Helper()

	samples := got.ToFloat32().Float32()
	require.Greater(t, len(samples), 2*skip, "not enough samples to compare")

	worst := 0.0
	for i := skip; i < len(samples)-skip; i++ {
		want := amplitude * math.Sin(2*math.Pi*freq*float64(i)/float64(got.SampleRate))
		worst = math.Max(worst, math.Abs(float64(samples[i])-want))
	}
	return worst
}

func TestResamplerPreservesATone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		inRate  int
		outRate int
	}{
		{"upsample 16k to 48k", 16000, 48000},
		{"downsample 48k to 16k", 48000, 16000},
		{"upsample 8k to 48k", 8000, 48000},
		{"downsample 48k to 8k", 48000, 8000},
		{"24k to 48k", 24000, 48000},
		{"non-integer ratio 44.1k to 48k", 44100, 48000},
		{"non-integer ratio 48k to 44.1k", 48000, 44100},
	}

	const freq = 440.0
	const amplitude = 0.5

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := sine(tt.inRate, freq, tt.inRate, amplitude) // one second
			got, err := src.Resample(tt.outRate)
			require.NoError(t, err)

			require.Equal(t, tt.outRate, got.SampleRate)
			require.InDelta(t, tt.outRate, got.Len(), 1,
				"one second in should be one second out")
			require.InDelta(t, 0.0, maxDeviation(t, got, freq, amplitude, 200), 0.005)
		})
	}
}

func TestResamplerHandlesChannelChanges(t *testing.T) {
	t.Parallel()

	t.Run("mono to stereo duplicates", func(t *testing.T) {
		t.Parallel()

		src := FromInt16([]int16{100, 200, 300}, 48000, 1)
		got, err := NewResampler(FormatS16, 48000, 2).Resample(src)
		require.NoError(t, err)
		require.Equal(t, 2, got.Channels)
		require.Equal(t, []int16{100, 100, 200, 200, 300, 300}, got.Int16())
	})

	t.Run("stereo to mono averages", func(t *testing.T) {
		t.Parallel()

		src := FromInt16([]int16{100, 300, 0, 200}, 48000, 2)
		got, err := NewResampler(FormatS16, 48000, 1).Resample(src)
		require.NoError(t, err)
		require.Equal(t, 1, got.Channels)
		require.Equal(t, []int16{200, 100}, got.Int16())
	})

	t.Run("rate and channels together", func(t *testing.T) {
		t.Parallel()

		src := sine(16000, 440, 16000, 0.5)
		got, err := NewResampler(FormatS16, 48000, 2).Resample(src)
		require.NoError(t, err)
		require.Equal(t, 2, got.Channels)
		require.Equal(t, FormatS16, got.Format)
		require.InDelta(t, 48000, got.Len(), 1)

		left, err := got.Channel(0)
		require.NoError(t, err)
		require.InDelta(t, 0.0, maxDeviation(t, left, 440, 0.5, 200), 0.01)
	})
}

func TestResamplerPassesThroughMatchingParameters(t *testing.T) {
	t.Parallel()

	src := FromInt16([]int16{1, 2, 3}, 48000, 1)
	got, err := NewResampler(FormatS16, 48000, 1).Resample(src)
	require.NoError(t, err)
	require.Equal(t, &src.Int16()[0], &got.Int16()[0],
		"a no-op resample should not copy")
}

func TestResamplerConvertsFormatOnly(t *testing.T) {
	t.Parallel()

	src := FromInt16([]int16{0, 32767}, 48000, 1)
	got, err := NewResampler(FormatF32, 48000, 1).Resample(src)
	require.NoError(t, err)
	require.Equal(t, FormatF32, got.Format)
	require.InDelta(t, 1.0, got.Float32()[1], 0.001)
}

func TestResamplerRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	_, err := NewResampler(FormatS16, 48000, 1).Resample(PCM{SampleRate: 0, Channels: 1})
	require.Error(t, err)

	require.Panics(t, func() { NewResampler(FormatS16, 0, 1) })
	require.Panics(t, func() { NewStreamResampler(FormatS16, 48000, 1, -1) })
}

func TestResamplerEmptyInput(t *testing.T) {
	t.Parallel()

	got, err := NewResampler(FormatS16, 48000, 1).Resample(New(FormatF32, 16000, 1))
	require.NoError(t, err)
	require.True(t, got.IsEmpty())
	require.Equal(t, 48000, got.SampleRate)
}

// TestStreamResamplerIsSeamless is the reason StreamResampler exists. Feeding
// the same tone in small pieces must give the same answer as one big call; if
// the filter state were dropped between calls there would be a step
// discontinuity at every boundary, audible as a buzz at the chunk rate.
func TestStreamResamplerIsSeamless(t *testing.T) {
	t.Parallel()

	const (
		inRate  = 16000
		outRate = 48000
		freq    = 440.0
		amp     = 0.5
	)
	src := sine(inRate, freq, inRate, amp)

	oneShot, err := src.Resample(outRate)
	require.NoError(t, err)

	for _, chunkFrames := range []int{1, 7, 160, 320, 4096} {
		t.Run(fmt.Sprintf("%d frame chunks", chunkFrames), func(t *testing.T) {
			t.Parallel()

			sr := NewStreamResampler(FormatF32, outRate, 1, 0)
			var streamed PCM
			for chunk := range src.Chunks(chunkFrames, 0, false) {
				frames, err := sr.Write(chunk)
				require.NoError(t, err)
				for _, f := range frames {
					require.NoError(t, streamed.Append(f))
				}
			}
			tail, err := sr.Flush()
			require.NoError(t, err)
			for _, f := range tail {
				require.NoError(t, streamed.Append(f))
			}

			require.Equal(t, oneShot.Len(), streamed.Len(),
				"chunking must not change the output length")

			want := oneShot.Float32()
			got := streamed.Float32()
			for i := range want {
				require.InDelta(t, want[i], got[i], 1e-5,
					"sample %d differs from the one-shot result", i)
			}
		})
	}
}

// TestStreamResamplerHasNoDiscontinuities looks directly for the artefact:
// a sample-to-sample jump far larger than the tone's own slope.
func TestStreamResamplerHasNoDiscontinuities(t *testing.T) {
	t.Parallel()

	const outRate = 48000
	src := sine(16000, 440, 16000, 0.5)

	sr := NewStreamResampler(FormatF32, outRate, 1, 0)
	var out PCM
	for chunk := range src.Chunks(320, 0, false) { // 20 ms chunks
		frames, err := sr.Write(chunk)
		require.NoError(t, err)
		for _, f := range frames {
			require.NoError(t, out.Append(f))
		}
	}

	// The largest step a 440 Hz sine at 0.5 amplitude can take between
	// adjacent samples at 48 kHz is 2*pi*440/48000*0.5, about 0.029.
	maxStep := 2 * math.Pi * 440 / outRate * 0.5 * 1.5

	samples := out.Float32()
	for i := 1; i < len(samples); i++ {
		require.LessOrEqual(t, math.Abs(float64(samples[i]-samples[i-1])), maxStep,
			"discontinuity at sample %d", i)
	}
}

func TestStreamResamplerFixedFrameSize(t *testing.T) {
	t.Parallel()

	sr := NewStreamResampler(FormatS16, 48000, 1, 960)
	require.Equal(t, 960, sr.FrameSize())

	src := sine(16000, 440, 16000, 0.5)
	total := 0
	for chunk := range src.Chunks(320, 0, false) {
		frames, err := sr.Write(chunk)
		require.NoError(t, err)
		for _, f := range frames {
			require.Equal(t, 960, f.Len(), "every frame from Write must be full")
			require.Equal(t, FormatS16, f.Format)
			total += f.Len()
		}
	}

	tail, err := sr.Flush()
	require.NoError(t, err)
	for _, f := range tail {
		require.LessOrEqual(t, f.Len(), 960)
		total += f.Len()
	}
	require.Equal(t, 48000, total)
}

func TestStreamResamplerFramePTS(t *testing.T) {
	t.Parallel()

	sr := NewStreamResampler(FormatS16, 48000, 1, 960)
	frames, err := sr.Write(sine(48000, 440, 48000, 0.5))
	require.NoError(t, err)
	require.Len(t, frames, 50)

	for i, f := range frames {
		require.Equal(t, time.Duration(i)*20*time.Millisecond, f.PTS)
	}
}

func TestStreamResamplerReconfiguresOnRateChange(t *testing.T) {
	t.Parallel()

	sr := NewStreamResampler(FormatF32, 48000, 1, 0)

	first, err := sr.Write(sine(16000, 440, 1600, 0.5)) // 100 ms
	require.NoError(t, err)
	require.NotEmpty(t, first)

	// Switching the input rate mid-stream drains the old filter first, so no
	// audio is silently discarded.
	second, err := sr.Write(sine(8000, 440, 800, 0.5)) // another 100 ms
	require.NoError(t, err)
	require.NotEmpty(t, second)

	var total int
	for _, f := range append(first, second...) {
		total += f.Len()
	}
	tail, err := sr.Flush()
	require.NoError(t, err)
	for _, f := range tail {
		total += f.Len()
	}
	require.InDelta(t, 9600, total, 4, "200 ms at 48 kHz")
}

func TestStreamResamplerReset(t *testing.T) {
	t.Parallel()

	sr := NewStreamResampler(FormatF32, 48000, 1, 0)
	_, err := sr.Write(sine(16000, 440, 1600, 0.5))
	require.NoError(t, err)

	sr.Reset()

	tail, err := sr.Flush()
	require.NoError(t, err)
	require.Empty(t, tail, "reset should discard buffered audio")
}

func TestStreamResamplerPassThrough(t *testing.T) {
	t.Parallel()

	// Matching rates should skip the filter entirely and be sample-exact.
	sr := NewStreamResampler(FormatS16, 48000, 1, 0)
	src := FromInt16([]int16{1, 2, 3, 4}, 48000, 1)

	frames, err := sr.Write(src)
	require.NoError(t, err)
	require.Len(t, frames, 1)
	require.Equal(t, []int16{1, 2, 3, 4}, frames[0].Int16())
}

func TestStreamResamplerEmptyWrite(t *testing.T) {
	t.Parallel()

	sr := NewStreamResampler(FormatS16, 48000, 1, 0)
	frames, err := sr.Write(New(FormatS16, 16000, 1))
	require.NoError(t, err)
	require.Empty(t, frames)

	tail, err := sr.Flush()
	require.NoError(t, err)
	require.Empty(t, tail)
}

// TestResamplerRejectsAliasing checks the anti-alias filter: a tone above the
// output Nyquist must be attenuated rather than folded back into the audible
// band as a spurious lower tone.
func TestResamplerRejectsAliasing(t *testing.T) {
	t.Parallel()

	// 6 kHz at 48 kHz, downsampled to 8 kHz whose Nyquist is 4 kHz. Without
	// filtering this would reappear as a loud 2 kHz tone.
	src := sine(48000, 6000, 48000, 0.5)
	got, err := src.Resample(8000)
	require.NoError(t, err)

	require.Less(t, got.RMS(), 0.02,
		"a tone above the output Nyquist should be filtered out, not aliased")
}

func TestResamplerPreservesLoudness(t *testing.T) {
	t.Parallel()

	for _, outRate := range []int{8000, 16000, 24000, 44100, 96000} {
		t.Run(fmt.Sprintf("to %d", outRate), func(t *testing.T) {
			t.Parallel()

			src := sine(48000, 440, 48000, 0.5)
			got, err := src.Resample(outRate)
			require.NoError(t, err)
			require.InDelta(t, src.RMS(), got.RMS(), 0.005)
		})
	}
}
