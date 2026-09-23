package audio

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

// PCM is a buffer of interleaved PCM audio.
//
// Samples are interleaved rather than planar: a stereo buffer is laid out
// L,R,L,R. Every consumer this package feeds (Opus, WAV, G.711, RTP) wants
// interleaved data, so storing it that way avoids a layout conversion at every
// boundary.
//
// Exactly one of the two sample slices is populated, selected by Format. Use
// [PCM.Int16] or [PCM.Float32] to reach the active one, or [PCM.ToInt16] and
// [PCM.ToFloat32] to convert first.
//
// A PCM is a value. Copying one shares the underlying sample slice, so treat a
// copy as read-only or take a [PCM.Clone]. The only methods that mutate in
// place take a pointer receiver: [PCM.Append] and [PCM.Clear].
//
// The zero PCM is an empty s16 buffer with no sample rate; it is only useful as
// a destination for Append.
type PCM struct {
	// Format selects which of the sample slices holds data.
	Format Format
	// SampleRate is the number of frames per second, e.g. 48000.
	SampleRate int
	// Channels is the number of interleaved channels, 1 for mono.
	Channels int
	// PTS is the presentation timestamp of the first frame, relative to
	// whatever origin the producer chose. Zero means unset or start-of-stream.
	PTS time.Duration

	s16 []int16
	f32 []float32
}

// New returns an empty buffer with the given parameters. It panics if the
// parameters are invalid, because they are almost always constants at the call
// site; use [NewChecked] when they come from user input.
func New(format Format, sampleRate, channels int) PCM {
	pcm, err := NewChecked(format, sampleRate, channels)
	if err != nil {
		panic(err)
	}
	return pcm
}

// NewChecked returns an empty buffer with the given parameters.
func NewChecked(format Format, sampleRate, channels int) (PCM, error) {
	if err := validate(format, sampleRate, channels); err != nil {
		return PCM{}, err
	}
	return PCM{Format: format, SampleRate: sampleRate, Channels: channels}, nil
}

// FromInt16 wraps interleaved int16 samples. The slice is not copied.
func FromInt16(samples []int16, sampleRate, channels int) PCM {
	pcm := New(FormatS16, sampleRate, channels)
	pcm.s16 = samples
	return pcm
}

// FromFloat32 wraps interleaved float32 samples. The slice is not copied.
func FromFloat32(samples []float32, sampleRate, channels int) PCM {
	pcm := New(FormatF32, sampleRate, channels)
	pcm.f32 = samples
	return pcm
}

// FromBytes decodes little-endian interleaved samples. Trailing bytes that do
// not complete a frame are dropped, so a buffer split at an arbitrary boundary
// still decodes; use [Aligner] if you need to carry the remainder forward.
func FromBytes(b []byte, format Format, sampleRate, channels int) (PCM, error) {
	pcm, err := NewChecked(format, sampleRate, channels)
	if err != nil {
		return PCM{}, err
	}

	frameSize := format.BytesPerSample() * channels
	b = b[:len(b)-len(b)%frameSize]

	switch format {
	case FormatS16:
		pcm.s16 = make([]int16, len(b)/2)
		for i := range pcm.s16 {
			pcm.s16[i] = int16(binary.LittleEndian.Uint16(b[i*2:]))
		}
	case FormatF32:
		pcm.f32 = make([]float32, len(b)/4)
		for i := range pcm.f32 {
			pcm.f32[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
		}
	}
	return pcm, nil
}

func validate(format Format, sampleRate, channels int) error {
	if !format.Valid() {
		return fmt.Errorf("audio: invalid format %d", uint8(format))
	}
	if sampleRate <= 0 {
		return fmt.Errorf("audio: sample rate must be positive, got %d", sampleRate)
	}
	if channels <= 0 {
		return fmt.Errorf("audio: channels must be positive, got %d", channels)
	}
	return nil
}

// Int16 returns the underlying interleaved samples, or nil if Format is not
// [FormatS16]. The slice is live: writing to it writes to the buffer.
func (p PCM) Int16() []int16 {
	if p.Format != FormatS16 {
		return nil
	}
	return p.s16
}

// Float32 returns the underlying interleaved samples, or nil if Format is not
// [FormatF32]. The slice is live: writing to it writes to the buffer.
func (p PCM) Float32() []float32 {
	if p.Format != FormatF32 {
		return nil
	}
	return p.f32
}

// NumSamples is the total number of samples across all channels, i.e. the
// length of the active sample slice.
func (p PCM) NumSamples() int {
	if p.Format == FormatF32 {
		return len(p.f32)
	}
	return len(p.s16)
}

// Len is the number of frames, i.e. samples per channel. Duration is Len over
// SampleRate, so this is the length that matters for timing.
func (p PCM) Len() int {
	if p.Channels <= 0 {
		return 0
	}
	return p.NumSamples() / p.Channels
}

// IsEmpty reports whether the buffer holds no samples.
func (p PCM) IsEmpty() bool { return p.NumSamples() == 0 }

// Duration is how long the buffer takes to play.
func (p PCM) Duration() time.Duration {
	if p.SampleRate <= 0 {
		return 0
	}
	return time.Duration(p.Len()) * time.Second / time.Duration(p.SampleRate)
}

// Bytes encodes the samples as little-endian interleaved bytes in the current
// format. It always allocates.
func (p PCM) Bytes() []byte {
	switch p.Format {
	case FormatF32:
		out := make([]byte, len(p.f32)*4)
		for i, v := range p.f32 {
			binary.LittleEndian.PutUint32(out[i*4:], math.Float32bits(v))
		}
		return out
	default:
		out := make([]byte, len(p.s16)*2)
		for i, v := range p.s16 {
			binary.LittleEndian.PutUint16(out[i*2:], uint16(v))
		}
		return out
	}
}

// ToInt16 returns the buffer in [FormatS16], converting if needed. When the
// buffer is already s16 it is returned unchanged and shares its samples.
func (p PCM) ToInt16() PCM {
	if p.Format == FormatS16 {
		return p
	}
	out := p
	out.Format = FormatS16
	out.f32 = nil
	out.s16 = make([]int16, len(p.f32))
	for i, v := range p.f32 {
		out.s16[i] = f32ToS16(v)
	}
	return out
}

// ToFloat32 returns the buffer in [FormatF32], converting if needed. When the
// buffer is already f32 it is returned unchanged and shares its samples.
func (p PCM) ToFloat32() PCM {
	if p.Format == FormatF32 {
		return p
	}
	out := p
	out.Format = FormatF32
	out.s16 = nil
	out.f32 = make([]float32, len(p.s16))
	for i, v := range p.s16 {
		out.f32[i] = s16ToF32(v)
	}
	return out
}

// To returns the buffer in the requested format.
func (p PCM) To(format Format) PCM {
	if format == FormatF32 {
		return p.ToFloat32()
	}
	return p.ToInt16()
}

// Clone returns a deep copy that shares nothing with the original.
func (p PCM) Clone() PCM {
	out := p
	if p.s16 != nil {
		out.s16 = make([]int16, len(p.s16))
		copy(out.s16, p.s16)
	}
	if p.f32 != nil {
		out.f32 = make([]float32, len(p.f32))
		copy(out.f32, p.f32)
	}
	return out
}

// Slice returns frames [from, to) as a buffer sharing the original samples.
// PTS is advanced to match. It panics on an out-of-range index, like a slice
// expression.
func (p PCM) Slice(from, to int) PCM {
	out := p
	lo, hi := from*p.Channels, to*p.Channels
	switch p.Format {
	case FormatF32:
		out.f32 = p.f32[lo:hi]
	default:
		out.s16 = p.s16[lo:hi]
	}
	if p.SampleRate > 0 {
		out.PTS = p.PTS + time.Duration(from)*time.Second/time.Duration(p.SampleRate)
	}
	return out
}

// Channel de-interleaves one channel into a new mono buffer. It is how you
// hand a single microphone to a VAD or a transcriber that only accepts mono
// without paying for a full downmix.
func (p PCM) Channel(i int) (PCM, error) {
	if i < 0 || i >= p.Channels {
		return PCM{}, fmt.Errorf("audio: channel %d out of range for %d-channel audio", i, p.Channels)
	}
	if p.Channels == 1 {
		return p, nil
	}

	out := p
	out.Channels = 1
	frames := p.Len()
	switch p.Format {
	case FormatF32:
		out.f32 = make([]float32, frames)
		for f := range frames {
			out.f32[f] = p.f32[f*p.Channels+i]
		}
	default:
		out.s16 = make([]int16, frames)
		for f := range frames {
			out.s16[f] = p.s16[f*p.Channels+i]
		}
	}
	return out, nil
}

// Append concatenates other onto p, converting it to p's sample rate, channel
// count and format first. Appending to an empty buffer whose parameters are
// unset adopts other's parameters, so a zero PCM works as an accumulator.
func (p *PCM) Append(other PCM) error {
	if other.IsEmpty() {
		return nil
	}
	if p.SampleRate <= 0 && p.Channels <= 0 && p.IsEmpty() {
		p.Format, p.SampleRate, p.Channels = other.Format, other.SampleRate, other.Channels
		p.PTS = other.PTS
	}
	if err := validate(p.Format, p.SampleRate, p.Channels); err != nil {
		return err
	}

	if other.SampleRate != p.SampleRate || other.Channels != p.Channels {
		converted, err := NewResampler(p.Format, p.SampleRate, p.Channels).Resample(other)
		if err != nil {
			return err
		}
		other = converted
	}
	other = other.To(p.Format)

	switch p.Format {
	case FormatF32:
		p.f32 = append(p.f32, other.f32...)
	default:
		p.s16 = append(p.s16, other.s16...)
	}
	return nil
}

// Clear drops every sample, keeping the format, rate, channel count and PTS.
// The backing array is retained so a reused accumulator stops allocating.
func (p *PCM) Clear() {
	p.s16 = p.s16[:0]
	p.f32 = p.f32[:0]
}

// RMS is the root-mean-square amplitude on the float scale, so 0 is silence
// and 1 is full scale. This is the cheap loudness measure a VAD or an
// audio-level indicator wants.
func (p PCM) RMS() float64 {
	n := p.NumSamples()
	if n == 0 {
		return 0
	}
	var sum float64
	if p.Format == FormatF32 {
		for _, v := range p.f32 {
			sum += float64(v) * float64(v)
		}
	} else {
		for _, v := range p.s16 {
			f := float64(v) * scaleToFloat
			sum += f * f
		}
	}
	return math.Sqrt(sum / float64(n))
}

// DBFS is [PCM.RMS] in decibels relative to full scale, so it is at most 0 and
// silence is negative infinity.
func (p PCM) DBFS() float64 {
	rms := p.RMS()
	if rms <= 0 {
		return math.Inf(-1)
	}
	return 20 * math.Log10(rms)
}

// String implements [fmt.Stringer].
func (p PCM) String() string {
	var layout string
	switch p.Channels {
	case 1:
		layout = "Mono"
	case 2:
		layout = "Stereo"
	default:
		layout = fmt.Sprintf("%d-channel", p.Channels)
	}

	return fmt.Sprintf("%s audio: %dHz, %s, %d samples, %s",
		layout, p.SampleRate, p.Format, p.Len(),
		p.Duration().Round(time.Microsecond))
}
