package audio

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFormatBasics(t *testing.T) {
	t.Parallel()

	require.Equal(t, "s16", FormatS16.String())
	require.Equal(t, "f32", FormatF32.String())
	require.Equal(t, 2, FormatS16.BytesPerSample())
	require.Equal(t, 4, FormatF32.BytesPerSample())
	require.True(t, FormatS16.Valid())
	require.False(t, Format(7).Valid())
	require.Contains(t, Format(7).String(), "7")
}

func TestParseFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in      string
		want    Format
		wantErr bool
	}{
		{in: "s16", want: FormatS16},
		{in: "int16", want: FormatS16},
		{in: "f32", want: FormatF32},
		{in: "float32", want: FormatF32},
		{in: "flt", want: FormatF32},
		{in: "u8", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			got, err := ParseFormat(tt.in)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestNewValidatesParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		format     Format
		sampleRate int
		channels   int
	}{
		{"bad format", Format(9), 48000, 1},
		{"zero sample rate", FormatS16, 0, 1},
		{"negative sample rate", FormatS16, -1, 1},
		{"zero channels", FormatS16, 48000, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewChecked(tt.format, tt.sampleRate, tt.channels)
			require.Error(t, err)
			require.Panics(t, func() { New(tt.format, tt.sampleRate, tt.channels) })
		})
	}
}

func TestLenAndDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		pcm          PCM
		wantFrames   int
		wantSamples  int
		wantDuration time.Duration
	}{
		{
			name:         "mono",
			pcm:          FromInt16(make([]int16, 16000), 16000, 1),
			wantFrames:   16000,
			wantSamples:  16000,
			wantDuration: time.Second,
		},
		{
			name: "stereo counts frames not samples",
			// 32000 interleaved samples is 16000 stereo frames.
			pcm:          FromInt16(make([]int16, 32000), 16000, 2),
			wantFrames:   16000,
			wantSamples:  32000,
			wantDuration: time.Second,
		},
		{
			name:         "empty",
			pcm:          New(FormatF32, 48000, 1),
			wantFrames:   0,
			wantSamples:  0,
			wantDuration: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.wantFrames, tt.pcm.Len())
			require.Equal(t, tt.wantSamples, tt.pcm.NumSamples())
			require.Equal(t, tt.wantDuration, tt.pcm.Duration())
		})
	}
}

func TestZeroValueIsSafe(t *testing.T) {
	t.Parallel()

	var p PCM
	require.True(t, p.IsEmpty())
	require.Zero(t, p.Len())
	require.Zero(t, p.Duration())
	require.Zero(t, p.RMS())
	require.NotPanics(t, func() { _ = p.String() })
}

// TestFormatRoundTripIsExact is why the conversion scales by 32768 in both
// directions instead of copying the Python SDK's 32767.
func TestFormatRoundTripIsExact(t *testing.T) {
	t.Parallel()

	samples := make([]int16, 0, 65536)
	for v := math.MinInt16; v <= math.MaxInt16; v++ {
		samples = append(samples, int16(v))
	}
	src := FromInt16(samples, 48000, 1)

	round := src.ToFloat32().ToInt16()
	require.Equal(t, src.Int16(), round.Int16())
}

func TestConversionsAreNoOpsWhenAlreadyRight(t *testing.T) {
	t.Parallel()

	s16 := FromInt16([]int16{1, 2, 3}, 48000, 1)
	require.Equal(t, &s16.Int16()[0], &s16.ToInt16().Int16()[0],
		"ToInt16 on an s16 buffer should not copy")

	f32 := FromFloat32([]float32{0.1}, 48000, 1)
	require.Equal(t, &f32.Float32()[0], &f32.ToFloat32().Float32()[0],
		"ToFloat32 on an f32 buffer should not copy")
}

func TestAccessorsReturnNilForWrongFormat(t *testing.T) {
	t.Parallel()

	s16 := FromInt16([]int16{1}, 48000, 1)
	require.Nil(t, s16.Float32())
	require.NotNil(t, s16.Int16())

	f32 := FromFloat32([]float32{1}, 48000, 1)
	require.Nil(t, f32.Int16())
	require.NotNil(t, f32.Float32())
}

func TestClippingAndNaN(t *testing.T) {
	t.Parallel()

	src := FromFloat32([]float32{2.0, -2.0, 1.0, -1.0, float32(math.NaN())}, 48000, 1)
	got := src.ToInt16().Int16()
	require.Equal(t, []int16{32767, -32768, 32767, -32768, 0}, got)
}

func TestBytesRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pcm      PCM
		wantSize int
	}{
		{"s16 mono", FromInt16([]int16{1, -1, 300, -300}, 48000, 1), 8},
		{"s16 stereo", FromInt16([]int16{1, 2, 3, 4}, 48000, 2), 8},
		{"f32 mono", FromFloat32([]float32{0.5, -0.5}, 48000, 1), 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			raw := tt.pcm.Bytes()
			require.Len(t, raw, tt.wantSize)

			back, err := FromBytes(raw, tt.pcm.Format, tt.pcm.SampleRate, tt.pcm.Channels)
			require.NoError(t, err)
			require.Equal(t, tt.pcm.Bytes(), back.Bytes())
			require.Equal(t, tt.pcm.Len(), back.Len())
		})
	}
}

// TestFromBytesTrimsPartialFrames covers a byte stream chopped at an arbitrary
// point, which is what a network source produces.
func TestFromBytesTrimsPartialFrames(t *testing.T) {
	t.Parallel()

	// Nine bytes is two whole stereo s16 frames plus one stray byte.
	pcm, err := FromBytes(make([]byte, 9), FormatS16, 48000, 2)
	require.NoError(t, err)
	require.Equal(t, 2, pcm.Len())
	require.Equal(t, 4, pcm.NumSamples())
}

func TestFromBytesLittleEndian(t *testing.T) {
	t.Parallel()

	// 0x0100 little-endian is 1; 0xFFFF is -1.
	pcm, err := FromBytes([]byte{0x01, 0x00, 0xFF, 0xFF}, FormatS16, 48000, 1)
	require.NoError(t, err)
	require.Equal(t, []int16{1, -1}, pcm.Int16())
}

func TestCloneIsIndependent(t *testing.T) {
	t.Parallel()

	src := FromInt16([]int16{1, 2, 3}, 48000, 1)
	clone := src.Clone()
	clone.Int16()[0] = 99

	require.Equal(t, int16(1), src.Int16()[0])
	require.Equal(t, int16(99), clone.Int16()[0])
}

func TestAppend(t *testing.T) {
	t.Parallel()

	t.Run("same parameters concatenates", func(t *testing.T) {
		t.Parallel()

		a := FromInt16([]int16{1, 2}, 48000, 1)
		require.NoError(t, a.Append(FromInt16([]int16{3, 4}, 48000, 1)))
		require.Equal(t, []int16{1, 2, 3, 4}, a.Int16())
	})

	t.Run("zero value adopts the first buffer", func(t *testing.T) {
		t.Parallel()

		var acc PCM
		require.NoError(t, acc.Append(FromFloat32([]float32{0.5}, 16000, 2)))
		require.Equal(t, FormatF32, acc.Format)
		require.Equal(t, 16000, acc.SampleRate)
		require.Equal(t, 2, acc.Channels)
	})

	t.Run("converts format", func(t *testing.T) {
		t.Parallel()

		a := FromInt16([]int16{0}, 48000, 1)
		require.NoError(t, a.Append(FromFloat32([]float32{1.0}, 48000, 1)))
		require.Equal(t, []int16{0, 32767}, a.Int16())
	})

	t.Run("resamples a mismatched rate", func(t *testing.T) {
		t.Parallel()

		a := FromInt16(make([]int16, 480), 48000, 1)
		require.NoError(t, a.Append(FromInt16(make([]int16, 160), 16000, 1)))
		// 160 frames at 16 kHz is 480 at 48 kHz.
		require.Equal(t, 960, a.Len())
	})

	t.Run("appending empty is a no-op", func(t *testing.T) {
		t.Parallel()

		a := FromInt16([]int16{1}, 48000, 1)
		require.NoError(t, a.Append(New(FormatS16, 48000, 1)))
		require.Equal(t, []int16{1}, a.Int16())
	})
}

func TestClearKeepsMetadata(t *testing.T) {
	t.Parallel()

	p := FromInt16([]int16{1, 2, 3}, 16000, 2)
	p.PTS = time.Second
	p.Clear()

	require.True(t, p.IsEmpty())
	require.Equal(t, 16000, p.SampleRate)
	require.Equal(t, 2, p.Channels)
	require.Equal(t, time.Second, p.PTS)
}

func TestSliceSharesSamplesAndAdvancesPTS(t *testing.T) {
	t.Parallel()

	src := FromInt16([]int16{1, 2, 3, 4, 5, 6, 7, 8}, 8000, 1)
	got := src.Slice(2, 6)

	require.Equal(t, []int16{3, 4, 5, 6}, got.Int16())
	require.Equal(t, 250*time.Microsecond, got.PTS)

	got.Int16()[0] = 99
	require.Equal(t, int16(99), src.Int16()[2], "a slice should share samples")
}

func TestChannel(t *testing.T) {
	t.Parallel()

	stereo := FromInt16([]int16{1, 10, 2, 20, 3, 30}, 48000, 2)

	left, err := stereo.Channel(0)
	require.NoError(t, err)
	require.Equal(t, 1, left.Channels)
	require.Equal(t, []int16{1, 2, 3}, left.Int16())

	right, err := stereo.Channel(1)
	require.NoError(t, err)
	require.Equal(t, []int16{10, 20, 30}, right.Int16())

	_, err = stereo.Channel(2)
	require.Error(t, err)
	_, err = stereo.Channel(-1)
	require.Error(t, err)
}

func TestRMSAndDBFS(t *testing.T) {
	t.Parallel()

	t.Run("silence", func(t *testing.T) {
		t.Parallel()
		p := FromInt16(make([]int16, 100), 48000, 1)
		require.Zero(t, p.RMS())
		require.True(t, math.IsInf(p.DBFS(), -1))
	})

	t.Run("full scale square wave", func(t *testing.T) {
		t.Parallel()
		p := FromFloat32([]float32{1, -1, 1, -1}, 48000, 1)
		require.InDelta(t, 1.0, p.RMS(), 1e-6)
		require.InDelta(t, 0.0, p.DBFS(), 1e-6)
	})

	t.Run("half amplitude is about -6 dB", func(t *testing.T) {
		t.Parallel()
		p := FromFloat32([]float32{0.5, -0.5}, 48000, 1)
		require.InDelta(t, -6.02, p.DBFS(), 0.01)
	})

	t.Run("s16 and f32 agree", func(t *testing.T) {
		t.Parallel()
		f32 := FromFloat32([]float32{0.25, -0.25, 0.5, -0.5}, 48000, 1)
		require.InDelta(t, f32.RMS(), f32.ToInt16().RMS(), 1e-4)
	})
}

func TestString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		pcm  PCM
		want string
	}{
		{
			name: "mono short",
			pcm:  FromInt16(make([]int16, 320), 16000, 1),
			want: "Mono audio: 16000Hz, s16, 320 samples, 20ms",
		},
		{
			name: "stereo long",
			pcm:  FromFloat32(make([]float32, 96000), 48000, 2),
			want: "Stereo audio: 48000Hz, f32, 48000 samples, 1s",
		},
		{
			name: "multichannel",
			pcm:  FromInt16(make([]int16, 24), 8000, 6),
			want: "6-channel audio: 8000Hz, s16, 4 samples, 500µs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, tt.pcm.String())
		})
	}
}
