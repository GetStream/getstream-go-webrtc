package audio

import (
	"errors"
	"io"
	"iter"
)

// Aligner turns an arbitrarily chopped byte stream into whole-frame buffers.
//
// Network sources do not respect sample boundaries: a streaming TTS response
// can end a chunk halfway through a stereo frame. An Aligner holds the
// remainder until the next chunk completes it, so nothing is lost and no buffer
// ever contains half a frame.
//
// An Aligner is not safe for concurrent use.
type Aligner struct {
	format     Format
	sampleRate int
	channels   int
	frameSize  int
	buf        []byte
}

// NewAligner returns an aligner for the given stream parameters. It panics on
// invalid parameters; see [New] for the rationale.
func NewAligner(format Format, sampleRate, channels int) *Aligner {
	if err := validate(format, sampleRate, channels); err != nil {
		panic(err)
	}
	return &Aligner{
		format:     format,
		sampleRate: sampleRate,
		channels:   channels,
		frameSize:  format.BytesPerSample() * channels,
	}
}

// Write appends b and returns every whole frame now available. The returned
// buffer is empty when b did not complete a frame.
func (a *Aligner) Write(b []byte) (PCM, error) {
	a.buf = append(a.buf, b...)

	whole := len(a.buf) - len(a.buf)%a.frameSize
	if whole == 0 {
		return New(a.format, a.sampleRate, a.channels), nil
	}

	pcm, err := FromBytes(a.buf[:whole], a.format, a.sampleRate, a.channels)
	if err != nil {
		return PCM{}, err
	}
	a.buf = append(a.buf[:0], a.buf[whole:]...)
	return pcm, nil
}

// Flush returns the trailing partial frame padded with silence, and resets the
// aligner. It returns an empty buffer when nothing is pending.
func (a *Aligner) Flush() (PCM, error) {
	if len(a.buf) == 0 {
		return New(a.format, a.sampleRate, a.channels), nil
	}
	a.buf = append(a.buf, make([]byte, (a.frameSize-len(a.buf)%a.frameSize)%a.frameSize)...)

	pcm, err := FromBytes(a.buf, a.format, a.sampleRate, a.channels)
	a.buf = a.buf[:0]
	return pcm, err
}

// Pending is the number of buffered bytes that do not yet form a whole frame.
func (a *Aligner) Pending() int { return len(a.buf) }

// FromReader streams raw PCM bytes off r as buffers of up to chunkFrames
// frames. Pass 0 for chunkFrames to use a default sized for real-time audio.
//
// It is the adapter for a source that hands back bytes rather than samples: an
// HTTP response from a TTS engine, a pipe from ffmpeg, a file of headerless
// PCM. Iteration stops at the first error; io.EOF ends it cleanly, after any
// trailing partial frame has been padded and yielded.
func FromReader(r io.Reader, format Format, sampleRate, channels, chunkFrames int) iter.Seq2[PCM, error] {
	return func(yield func(PCM, error) bool) {
		if err := validate(format, sampleRate, channels); err != nil {
			yield(PCM{}, err)
			return
		}
		if chunkFrames <= 0 {
			// 20 ms, the frame size every WebRTC audio path is built around.
			chunkFrames = sampleRate / 50
		}

		aligner := NewAligner(format, sampleRate, channels)
		buf := make([]byte, max(chunkFrames*format.BytesPerSample()*channels, 1))

		for {
			n, err := r.Read(buf)
			if n > 0 {
				pcm, aerr := aligner.Write(buf[:n])
				if aerr != nil {
					yield(PCM{}, aerr)
					return
				}
				if !pcm.IsEmpty() && !yield(pcm, nil) {
					return
				}
			}
			if err != nil {
				if !errors.Is(err, io.EOF) {
					yield(PCM{}, err)
					return
				}
				if tail, ferr := aligner.Flush(); ferr != nil {
					yield(PCM{}, ferr)
				} else if !tail.IsEmpty() {
					yield(tail, nil)
				}
				return
			}
		}
	}
}

// FromByteSeq is [FromReader] for a source that already yields discrete chunks,
// such as a channel drained with range. Chunks are re-aligned to frame
// boundaries, so the caller does not have to care where they were split.
func FromByteSeq(chunks iter.Seq[[]byte], format Format, sampleRate, channels int) iter.Seq2[PCM, error] {
	return func(yield func(PCM, error) bool) {
		if err := validate(format, sampleRate, channels); err != nil {
			yield(PCM{}, err)
			return
		}

		aligner := NewAligner(format, sampleRate, channels)
		stopped := false

		for chunk := range chunks {
			pcm, err := aligner.Write(chunk)
			if err != nil {
				yield(PCM{}, err)
				return
			}
			if !pcm.IsEmpty() && !yield(pcm, nil) {
				stopped = true
				break
			}
		}
		if stopped {
			return
		}

		if tail, err := aligner.Flush(); err != nil {
			yield(PCM{}, err)
		} else if !tail.IsEmpty() {
			yield(tail, nil)
		}
	}
}

// Collect drains an iterator from [FromReader] or [FromByteSeq] into a single
// buffer. It is the convenience for callers that want the whole utterance
// rather than a stream, and it stops at the first error.
func Collect(seq iter.Seq2[PCM, error]) (PCM, error) {
	var out PCM
	for pcm, err := range seq {
		if err != nil {
			return out, err
		}
		if err := out.Append(pcm); err != nil {
			return out, err
		}
	}
	return out, nil
}
