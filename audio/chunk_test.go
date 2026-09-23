package audio

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ramp builds a mono buffer whose samples count up, so a test can tell exactly
// which frames ended up in which chunk.
func ramp(n, sampleRate int) PCM {
	s := make([]int16, n)
	for i := range s {
		s[i] = int16(i)
	}
	return FromInt16(s, sampleRate, 1)
}

func TestChunks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		total   int
		size    int
		overlap int
		padLast bool
		want    [][]int16
	}{
		{
			name:  "even division",
			total: 6, size: 3,
			want: [][]int16{{0, 1, 2}, {3, 4, 5}},
		},
		{
			name:  "short final chunk is kept",
			total: 5, size: 3,
			want: [][]int16{{0, 1, 2}, {3, 4}},
		},
		{
			name:  "short final chunk is padded",
			total: 5, size: 3, padLast: true,
			want: [][]int16{{0, 1, 2}, {3, 4, 0}},
		},
		{
			name:  "overlapping windows",
			total: 10, size: 4, overlap: 2,
			want: [][]int16{{0, 1, 2, 3}, {2, 3, 4, 5}, {4, 5, 6, 7}, {6, 7, 8, 9}, {8, 9}},
		},
		{
			name:  "overlap larger than size still advances",
			total: 4, size: 2, overlap: 5,
			want: [][]int16{{0, 1}, {1, 2}, {2, 3}, {3}},
		},
		{
			name:  "empty input yields nothing",
			total: 0, size: 3,
			want: nil,
		},
		{
			name:  "zero size yields nothing",
			total: 4, size: 0,
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var got [][]int16
			for chunk := range ramp(tt.total, 16000).Chunks(tt.size, tt.overlap, tt.padLast) {
				got = append(got, slices.Clone(chunk.Int16()))
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestChunksCarryPTS(t *testing.T) {
	t.Parallel()

	src := ramp(48000, 48000)
	src.PTS = time.Second

	var ptss []time.Duration
	for chunk := range src.Chunks(960, 0, false) {
		ptss = append(ptss, chunk.PTS)
		require.Equal(t, 20*time.Millisecond, chunk.Duration())
	}

	require.Len(t, ptss, 50)
	require.Equal(t, time.Second, ptss[0])
	require.Equal(t, time.Second+20*time.Millisecond, ptss[1])
	require.Equal(t, time.Second+980*time.Millisecond, ptss[49])
}

func TestChunksStopEarly(t *testing.T) {
	t.Parallel()

	// Breaking out of a range-over-func must not deadlock or over-produce.
	count := 0
	for range ramp(1000, 16000).Chunks(10, 0, false) {
		count++
		if count == 3 {
			break
		}
	}
	require.Equal(t, 3, count)
}

func TestSlidingWindow(t *testing.T) {
	t.Parallel()

	// A 25 ms window hopping 10 ms at 16 kHz: 400-frame windows, 160 apart.
	src := ramp(16000, 16000)

	var windows []PCM
	for w := range src.SlidingWindow(25*time.Millisecond, 10*time.Millisecond, false) {
		windows = append(windows, w)
	}

	require.NotEmpty(t, windows)
	require.Equal(t, 400, windows[0].Len())
	require.Equal(t, int16(0), windows[0].Int16()[0])
	require.Equal(t, int16(160), windows[1].Int16()[0])
	require.Equal(t, 25*time.Millisecond, windows[0].Duration())
}

func TestHeadAndTail(t *testing.T) {
	t.Parallel()

	src := ramp(1000, 1000) // one second at 1 kHz keeps the arithmetic obvious

	t.Run("head", func(t *testing.T) {
		t.Parallel()
		got := src.Head(100*time.Millisecond, false, PadEnd)
		require.Equal(t, 100, got.Len())
		require.Equal(t, int16(0), got.Int16()[0])
	})

	t.Run("tail", func(t *testing.T) {
		t.Parallel()
		got := src.Tail(100*time.Millisecond, false, PadEnd)
		require.Equal(t, 100, got.Len())
		require.Equal(t, int16(900), got.Int16()[0])
	})

	t.Run("head longer than buffer without padding", func(t *testing.T) {
		t.Parallel()
		got := src.Head(2*time.Second, false, PadEnd)
		require.Equal(t, 1000, got.Len())
	})

	t.Run("head longer than buffer with padding", func(t *testing.T) {
		t.Parallel()
		got := src.Head(2*time.Second, true, PadEnd)
		require.Equal(t, 2000, got.Len())
		require.Equal(t, int16(999), got.Int16()[999])
		require.Equal(t, int16(0), got.Int16()[1000])
	})

	t.Run("tail pads at the start", func(t *testing.T) {
		t.Parallel()
		got := src.Tail(2*time.Second, true, PadStart)
		require.Equal(t, 2000, got.Len())
		require.Equal(t, int16(0), got.Int16()[0])
		require.Equal(t, int16(0), got.Int16()[1000])
		require.Equal(t, int16(999), got.Int16()[1999])
	})
}

func TestPadTo(t *testing.T) {
	t.Parallel()

	t.Run("pad at end", func(t *testing.T) {
		t.Parallel()
		got := FromInt16([]int16{1, 2}, 1000, 1).PadTo(4, PadEnd)
		require.Equal(t, []int16{1, 2, 0, 0}, got.Int16())
	})

	t.Run("pad at start rewinds PTS", func(t *testing.T) {
		t.Parallel()
		src := FromInt16([]int16{1, 2}, 1000, 1)
		src.PTS = 10 * time.Millisecond

		got := src.PadTo(4, PadStart)
		require.Equal(t, []int16{0, 0, 1, 2}, got.Int16())
		require.Equal(t, 8*time.Millisecond, got.PTS)
	})

	t.Run("pad stereo keeps interleaving", func(t *testing.T) {
		t.Parallel()
		got := FromInt16([]int16{1, 2}, 1000, 2).PadTo(2, PadEnd)
		require.Equal(t, []int16{1, 2, 0, 0}, got.Int16())
	})

	t.Run("already long enough is untouched", func(t *testing.T) {
		t.Parallel()
		src := FromInt16([]int16{1, 2, 3}, 1000, 1)
		require.Equal(t, src.Int16(), src.PadTo(2, PadEnd).Int16())
	})

	t.Run("float", func(t *testing.T) {
		t.Parallel()
		got := FromFloat32([]float32{0.5}, 1000, 1).PadTo(3, PadEnd)
		require.Equal(t, []float32{0.5, 0, 0}, got.Float32())
	})
}
