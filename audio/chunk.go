package audio

import (
	"iter"
	"time"
)

// PadPosition says which end of a buffer padding is added to.
type PadPosition uint8

const (
	// PadEnd appends silence, keeping the existing audio at the start.
	PadEnd PadPosition = iota
	// PadStart prepends silence, keeping the existing audio at the end.
	PadStart
)

// Chunks iterates the buffer in fixed-size pieces.
//
// size and overlap are counted in frames. Consecutive chunks start size-overlap
// frames apart, so a non-zero overlap gives the windowed input that feature
// extraction and VAD models expect. Each chunk shares samples with the original
// unless it had to be padded.
//
// The final chunk is short when the buffer does not divide evenly. Set padLast
// to pad it with silence to the full size instead, which is what a model with a
// fixed input size needs.
func (p PCM) Chunks(size, overlap int, padLast bool) iter.Seq[PCM] {
	return func(yield func(PCM) bool) {
		if size <= 0 {
			return
		}
		step := max(1, size-overlap)
		total := p.Len()

		for i := 0; i < total; i += step {
			end := min(i+size, total)
			chunk := p.Slice(i, end)
			if end-i < size && padLast {
				chunk = chunk.PadTo(size, PadEnd)
			}
			if !yield(chunk) {
				return
			}
		}
	}
}

// SlidingWindow iterates fixed-duration windows advancing by hop each step. It
// is [PCM.Chunks] expressed in time rather than frames, which is how window
// sizes are usually specified (25 ms window, 10 ms hop, and so on).
func (p PCM) SlidingWindow(window, hop time.Duration, padLast bool) iter.Seq[PCM] {
	size := p.framesIn(window)
	step := p.framesIn(hop)
	return p.Chunks(size, max(0, size-step), padLast)
}

// Head returns the first d of audio. When the buffer is shorter than d it is
// returned as is, unless pad is set, in which case it is padded with silence to
// exactly d.
func (p PCM) Head(d time.Duration, pad bool, at PadPosition) PCM {
	want := p.framesIn(d)
	if p.Len() >= want {
		return p.Slice(0, want)
	}
	if !pad {
		return p
	}
	return p.PadTo(want, at)
}

// Tail returns the last d of audio. When the buffer is shorter than d it is
// returned as is, unless pad is set, in which case it is padded with silence to
// exactly d.
//
// This is the primitive behind a rolling window of recent audio: keep appending
// and call Tail to bound what you retain.
func (p PCM) Tail(d time.Duration, pad bool, at PadPosition) PCM {
	want := p.framesIn(d)
	if n := p.Len(); n >= want {
		return p.Slice(n-want, n)
	}
	if !pad {
		return p
	}
	return p.PadTo(want, at)
}

// PadTo extends the buffer to frames with silence at the given end. A buffer
// that is already at least that long is returned unchanged.
func (p PCM) PadTo(frames int, at PadPosition) PCM {
	have := p.Len()
	if have >= frames {
		return p
	}
	missing := (frames - have) * p.Channels

	out := p
	switch p.Format {
	case FormatF32:
		out.f32 = make([]float32, frames*p.Channels)
		copy(out.f32[padOffset(at, missing):], p.f32)
	default:
		out.s16 = make([]int16, frames*p.Channels)
		copy(out.s16[padOffset(at, missing):], p.s16)
	}
	if at == PadStart && p.SampleRate > 0 {
		out.PTS = p.PTS - time.Duration(frames-have)*time.Second/time.Duration(p.SampleRate)
	}
	return out
}

func padOffset(at PadPosition, missing int) int {
	if at == PadStart {
		return missing
	}
	return 0
}

// framesIn converts a duration to a frame count at this buffer's sample rate.
func (p PCM) framesIn(d time.Duration) int {
	if p.SampleRate <= 0 || d <= 0 {
		return 0
	}
	return int(d * time.Duration(p.SampleRate) / time.Second)
}
