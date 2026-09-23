package audio

import (
	"bytes"
	"errors"
	"io"
	"iter"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAligner(t *testing.T) {
	t.Parallel()

	t.Run("holds a partial frame until it completes", func(t *testing.T) {
		t.Parallel()

		a := NewAligner(FormatS16, 48000, 2) // 4 bytes per frame

		got, err := a.Write([]byte{1, 0, 2}) // three quarters of a frame
		require.NoError(t, err)
		require.True(t, got.IsEmpty())
		require.Equal(t, 3, a.Pending())

		got, err = a.Write([]byte{0, 3}) // completes it, one byte over
		require.NoError(t, err)
		require.Equal(t, []int16{1, 2}, got.Int16())
		require.Equal(t, 1, a.Pending())
	})

	t.Run("flush pads the remainder", func(t *testing.T) {
		t.Parallel()

		a := NewAligner(FormatS16, 48000, 2)
		_, err := a.Write([]byte{1, 0, 2})
		require.NoError(t, err)

		got, err := a.Flush()
		require.NoError(t, err)
		require.Equal(t, []int16{1, 2}, got.Int16())
		require.Zero(t, a.Pending())
	})

	t.Run("flush with nothing pending", func(t *testing.T) {
		t.Parallel()

		got, err := NewAligner(FormatS16, 48000, 1).Flush()
		require.NoError(t, err)
		require.True(t, got.IsEmpty())
	})

	t.Run("rejects bad parameters", func(t *testing.T) {
		t.Parallel()
		require.Panics(t, func() { NewAligner(FormatS16, 48000, 0) })
	})
}

func TestFromReader(t *testing.T) {
	t.Parallel()

	src := sine(16000, 440, 16000, 0.5).ToInt16()
	raw := src.Bytes()

	tests := []struct {
		name        string
		chunkFrames int
		wantFrames  int
	}{
		{"default chunk size", 0, 320},
		{"explicit chunk size", 160, 160},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var out PCM
			first := true
			for pcm, err := range FromReader(bytes.NewReader(raw), FormatS16, 16000, 1, tt.chunkFrames) {
				require.NoError(t, err)
				if first {
					require.Equal(t, tt.wantFrames, pcm.Len())
					first = false
				}
				require.NoError(t, out.Append(pcm))
			}
			require.Equal(t, src.Int16(), out.Int16())
		})
	}
}

// TestFromReaderRealignsChunks feeds a reader that hands back awkward sizes,
// which is what an HTTP body does.
func TestFromReaderRealignsChunks(t *testing.T) {
	t.Parallel()

	src := FromInt16([]int16{1, 2, 3, 4, 5, 6}, 8000, 2)

	out, err := Collect(FromReader(
		&drippingReader{data: src.Bytes(), per: 3},
		FormatS16, 8000, 2, 1,
	))
	require.NoError(t, err)
	require.Equal(t, src.Int16(), out.Int16())
}

func TestFromReaderPadsTrailingPartialFrame(t *testing.T) {
	t.Parallel()

	// Five bytes is two whole s16 mono frames plus a stray byte.
	out, err := Collect(FromReader(
		bytes.NewReader([]byte{1, 0, 2, 0, 3}), FormatS16, 8000, 1, 1,
	))
	require.NoError(t, err)
	require.Equal(t, []int16{1, 2, 3}, out.Int16())
}

func TestFromReaderPropagatesErrors(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("connection reset")

	var gotErr error
	for _, err := range FromReader(&failingReader{err: wantErr}, FormatS16, 8000, 1, 1) {
		if err != nil {
			gotErr = err
		}
	}
	require.ErrorIs(t, gotErr, wantErr)
}

func TestFromReaderRejectsBadParameters(t *testing.T) {
	t.Parallel()

	_, err := Collect(FromReader(bytes.NewReader(nil), FormatS16, 0, 1, 0))
	require.Error(t, err)
}

func TestFromReaderStopsEarly(t *testing.T) {
	t.Parallel()

	count := 0
	for range FromReader(bytes.NewReader(make([]byte, 4000)), FormatS16, 8000, 1, 10) {
		count++
		if count == 2 {
			break
		}
	}
	require.Equal(t, 2, count)
}

func TestFromByteSeq(t *testing.T) {
	t.Parallel()

	src := FromInt16([]int16{1, 2, 3, 4, 5}, 8000, 1)
	raw := src.Bytes()

	// Split at an odd boundary so the aligner has to carry a byte forward.
	chunks := func(yield func([]byte) bool) {
		for _, c := range [][]byte{raw[:3], raw[3:7], raw[7:]} {
			if !yield(c) {
				return
			}
		}
	}

	out, err := Collect(FromByteSeq(chunks, FormatS16, 8000, 1))
	require.NoError(t, err)
	require.Equal(t, src.Int16(), out.Int16())
}

func TestFromByteSeqStopsEarly(t *testing.T) {
	t.Parallel()

	delivered := 0
	chunks := func(yield func([]byte) bool) {
		for range 10 {
			delivered++
			if !yield(make([]byte, 100)) {
				return
			}
		}
	}

	count := 0
	for range FromByteSeq(chunks, FormatS16, 8000, 1) {
		count++
		break
	}
	require.Equal(t, 1, count)
	require.Equal(t, 1, delivered, "the source should stop being drained")
}

func TestFromByteSeqRejectsBadParameters(t *testing.T) {
	t.Parallel()

	empty := func(func([]byte) bool) {}
	_, err := Collect(FromByteSeq(empty, Format(9), 8000, 1))
	require.Error(t, err)
}

func TestCollectPropagatesErrors(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("boom")
	seq := iter.Seq2[PCM, error](func(yield func(PCM, error) bool) {
		yield(FromInt16([]int16{1}, 8000, 1), nil)
		yield(PCM{}, wantErr)
	})

	got, err := Collect(seq)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, []int16{1}, got.Int16(), "audio read before the error is kept")
}

// drippingReader hands out at most per bytes per call, imitating a socket.
type drippingReader struct {
	data []byte
	per  int
	pos  int
}

func (r *drippingReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := min(len(p), r.per, len(r.data)-r.pos)
	copy(p, r.data[r.pos:r.pos+n])
	r.pos += n
	return n, nil
}

type failingReader struct{ err error }

func (r *failingReader) Read([]byte) (int, error) { return 0, r.err }
