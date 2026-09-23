package audio

import (
	"fmt"
	"math"
	"time"
)

// Resampler converts buffers to a fixed sample rate, channel count and format.
//
// It is stateless: each call is independent, which makes it the right choice
// for whole utterances and for one-off conversions. Feeding it a stream in
// small pieces will produce a discontinuity at every boundary, because the
// interpolation filter cannot see across calls. Use [StreamResampler] for that.
type Resampler struct {
	format     Format
	sampleRate int
	channels   int
}

// NewResampler returns a resampler targeting the given parameters. It panics on
// invalid parameters; see [New] for the rationale.
func NewResampler(format Format, sampleRate, channels int) *Resampler {
	if err := validate(format, sampleRate, channels); err != nil {
		panic(err)
	}
	return &Resampler{format: format, sampleRate: sampleRate, channels: channels}
}

// Format is the output sample format.
func (r *Resampler) Format() Format { return r.format }

// SampleRate is the output sample rate.
func (r *Resampler) SampleRate() int { return r.sampleRate }

// Channels is the output channel count.
func (r *Resampler) Channels() int { return r.channels }

// Resample converts in to the resampler's parameters. When in already matches,
// it is returned unchanged and shares its samples.
func (r *Resampler) Resample(in PCM) (PCM, error) {
	if err := validate(in.Format, in.SampleRate, in.Channels); err != nil {
		return PCM{}, fmt.Errorf("audio: resample input: %w", err)
	}
	if in.SampleRate == r.sampleRate && in.Channels == r.channels {
		return in.To(r.format), nil
	}
	if in.IsEmpty() {
		out := New(r.format, r.sampleRate, r.channels)
		out.PTS = in.PTS
		return out, nil
	}

	// One-shot use of the streaming engine: write everything, then flush, so
	// the tail runs out through the filter instead of being truncated.
	sr := NewStreamResampler(r.format, r.sampleRate, r.channels, 0)
	frames, err := sr.Write(in)
	if err != nil {
		return PCM{}, err
	}
	tail, err := sr.Flush()
	if err != nil {
		return PCM{}, err
	}

	out := New(r.format, r.sampleRate, r.channels)
	out.PTS = in.PTS
	for _, f := range append(frames, tail...) {
		if err := out.Append(f); err != nil {
			return PCM{}, err
		}
	}
	return out, nil
}

// Resample converts the buffer to the given sample rate, keeping its channel
// count and format. It is shorthand for the common case.
func (p PCM) Resample(sampleRate int) (PCM, error) {
	return NewResampler(p.Format, sampleRate, p.Channels).Resample(p)
}

// StreamResampler converts a continuous stream to a fixed sample rate, channel
// count and format, and optionally cuts the result into fixed-size frames.
//
// It carries interpolation state across calls, so consecutive [StreamResampler.Write]
// calls join seamlessly. This is what a real-time path needs: resampling 20 ms
// chunks independently leaves a step discontinuity at every boundary, which is
// audible as a click or a buzz at the chunk rate.
//
// The input parameters are locked in by the first non-empty Write. If a later
// buffer has a different rate or channel count the resampler drains and
// reconfigures itself, so the change costs a filter reset but no samples.
//
// A StreamResampler is not safe for concurrent use.
type StreamResampler struct {
	format     Format
	sampleRate int
	channels   int
	frameSize  int

	inRate     int
	inChannels int
	configured bool

	filter *sincFilter
	// buf holds pending input frames, interleaved at midChannels, in float32.
	buf []float32
	// mid is the channel count the filter runs at: the smaller of the input and
	// output counts, so a downmix happens before filtering and an upmix after.
	mid int
	// The read cursor into buf is the exact rational posInt + posFrac/stepDen
	// frames, advancing by stepNum/stepDen per output frame. Keeping it in
	// integers rather than a float64 accumulator means a stream that runs for
	// days does not drift away from its true position.
	posInt  int
	posFrac int
	stepNum int
	stepDen int
	// pending accumulates output frames, interleaved at the output channel
	// count, until frameSize of them are ready.
	pending []float32
	weights []float32

	emitted int64
}

// NewStreamResampler returns a resampler targeting the given parameters.
//
// frameSize is the number of frames per emitted buffer; pass 0 to emit
// everything that is ready on each call. With a non-zero frameSize every buffer
// from Write holds exactly frameSize frames, except the tail from
// [StreamResampler.Flush], which may be shorter.
//
// It panics on invalid parameters; see [New] for the rationale.
func NewStreamResampler(format Format, sampleRate, channels, frameSize int) *StreamResampler {
	if err := validate(format, sampleRate, channels); err != nil {
		panic(err)
	}
	if frameSize < 0 {
		panic(fmt.Errorf("audio: frame size must not be negative, got %d", frameSize))
	}
	return &StreamResampler{
		format:     format,
		sampleRate: sampleRate,
		channels:   channels,
		frameSize:  frameSize,
	}
}

// Format is the output sample format.
func (r *StreamResampler) Format() Format { return r.format }

// SampleRate is the output sample rate.
func (r *StreamResampler) SampleRate() int { return r.sampleRate }

// Channels is the output channel count.
func (r *StreamResampler) Channels() int { return r.channels }

// FrameSize is the number of frames in each emitted buffer, or 0 for variable.
func (r *StreamResampler) FrameSize() int { return r.frameSize }

// Write pushes pcm through the resampler and returns the output buffers that
// are complete. It returns no buffers while the resampler is still filling its
// first frame, which is normal.
func (r *StreamResampler) Write(pcm PCM) ([]PCM, error) {
	if pcm.IsEmpty() {
		return nil, nil
	}
	if err := validate(pcm.Format, pcm.SampleRate, pcm.Channels); err != nil {
		return nil, fmt.Errorf("audio: resample input: %w", err)
	}

	var drained []PCM
	if r.configured && (pcm.SampleRate != r.inRate || pcm.Channels != r.inChannels) {
		// The stream changed shape. Run the old filter out so its buffered
		// samples are not dropped, then start over on the new parameters.
		var err error
		if drained, err = r.Flush(); err != nil {
			return nil, err
		}
	}
	if !r.configured {
		r.configure(pcm.SampleRate, pcm.Channels)
	}

	r.feed(pcm)
	r.produce()
	return append(drained, r.collect(false)...), nil
}

// Flush drains the buffered tail and resets the resampler, which is then ready
// for a new stream. The last buffer it returns may hold fewer than FrameSize
// frames.
func (r *StreamResampler) Flush() ([]PCM, error) {
	if !r.configured {
		return nil, nil
	}
	// Pad with enough silence for the filter to run past the last real sample.
	if r.filter != nil {
		r.buf = append(r.buf, make([]float32, r.filter.halfTaps*r.mid)...)
	}
	r.produce()
	out := r.collect(true)
	r.Reset()
	return out, nil
}

// Reset discards all buffered audio and unlocks the input parameters.
func (r *StreamResampler) Reset() {
	r.configured = false
	r.filter = nil
	r.buf = r.buf[:0]
	r.pending = r.pending[:0]
	r.posInt, r.posFrac = 0, 0
	r.emitted = 0
}

func (r *StreamResampler) configure(inRate, inChannels int) {
	r.inRate, r.inChannels = inRate, inChannels
	r.mid = min(inChannels, r.channels)
	r.buf = r.buf[:0]
	r.pending = r.pending[:0]
	r.posInt, r.posFrac = 0, 0

	g := gcd(inRate, r.sampleRate)
	r.stepNum, r.stepDen = inRate/g, r.sampleRate/g

	if inRate != r.sampleRate {
		r.filter = newSincFilter(float64(r.sampleRate) / float64(inRate))
		// Prime with silence so the filter has history for the first output
		// frame; this is the same as treating the stream as zero before it
		// starts, and keeps every index in range.
		r.buf = append(r.buf, make([]float32, r.filter.halfTaps*r.mid)...)
		r.posInt = r.filter.halfTaps
	} else {
		r.filter = nil
	}
	r.configured = true
}

func gcd(a, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// feed converts pcm to float32 at mid channels and appends it to buf.
func (r *StreamResampler) feed(pcm PCM) {
	src := pcm.ToFloat32().f32
	frames := len(src) / pcm.Channels

	switch {
	case pcm.Channels == r.mid:
		r.buf = append(r.buf, src...)
	default:
		// Downmix to mid channels by averaging. mid < pcm.Channels here, and
		// the only layouts in practice are stereo -> mono.
		base := len(r.buf)
		r.buf = append(r.buf, make([]float32, frames*r.mid)...)
		out := r.buf[base:]
		for f := range frames {
			for c := range r.mid {
				var sum float32
				n := 0
				for s := c; s < pcm.Channels; s += r.mid {
					sum += src[f*pcm.Channels+s]
					n++
				}
				out[f*r.mid+c] = sum / float32(n)
			}
		}
	}
}

// produce runs the filter over everything readable in buf, appending output
// frames to pending, then compacts buf.
func (r *StreamResampler) produce() {
	if r.filter == nil {
		r.produceDirect()
		return
	}

	f := r.filter
	frames := len(r.buf) / r.mid
	taps := 2 * f.halfTaps

	if cap(r.weights) < taps {
		r.weights = make([]float32, taps)
	}
	weights := r.weights[:taps]

	for r.posInt+f.halfTaps < frames {
		start := r.posInt - f.halfTaps + 1
		frac := float64(r.posFrac) / float64(r.stepDen)

		var wsum float32
		for i := range taps {
			w := f.at(math.Abs(float64(r.posInt-(start+i)) + frac))
			weights[i] = w
			wsum += w
		}
		if wsum == 0 {
			wsum = 1
		}

		base := len(r.pending)
		r.pending = append(r.pending, make([]float32, r.channels)...)
		for c := range r.mid {
			var acc float32
			for i := range taps {
				acc += r.buf[(start+i)*r.mid+c] * weights[i]
			}
			r.pending[base+c] = acc / wsum
		}
		r.spreadChannels(base)

		r.posFrac += r.stepNum
		r.posInt += r.posFrac / r.stepDen
		r.posFrac %= r.stepDen
	}

	// Keep only the frames the filter can still reach back to.
	if keep := r.posInt - f.halfTaps + 1; keep > 0 {
		r.buf = append(r.buf[:0], r.buf[keep*r.mid:]...)
		r.posInt -= keep
	}
}

// produceDirect is the no-resampling path: rates match, so frames pass through
// with only a channel change.
func (r *StreamResampler) produceDirect() {
	frames := len(r.buf) / r.mid
	for f := range frames {
		base := len(r.pending)
		r.pending = append(r.pending, make([]float32, r.channels)...)
		copy(r.pending[base:], r.buf[f*r.mid:(f+1)*r.mid])
		r.spreadChannels(base)
	}
	r.buf = r.buf[:0]
}

// spreadChannels fills the output channels beyond mid for one frame starting at
// base, duplicating the existing channels round-robin. This is the upmix half
// of the channel conversion; it is a no-op when mid == channels.
func (r *StreamResampler) spreadChannels(base int) {
	for c := r.mid; c < r.channels; c++ {
		r.pending[base+c] = r.pending[base+c%r.mid]
	}
}

// collect cuts pending into output buffers. Unless final is set, a partial
// frame is left behind for the next call.
func (r *StreamResampler) collect(final bool) []PCM {
	perFrame := r.channels
	available := len(r.pending) / perFrame
	if available == 0 {
		return nil
	}

	size := r.frameSize
	if size == 0 {
		size = available
	}

	var out []PCM
	taken := 0
	for taken < available {
		n := min(size, available-taken)
		if n < size && !final {
			break
		}
		samples := make([]float32, n*perFrame)
		copy(samples, r.pending[taken*perFrame:(taken+n)*perFrame])

		pcm := FromFloat32(samples, r.sampleRate, r.channels).To(r.format)
		pcm.PTS = time.Duration(r.emitted) * time.Second / time.Duration(r.sampleRate)
		out = append(out, pcm)

		r.emitted += int64(n)
		taken += n
	}

	r.pending = append(r.pending[:0], r.pending[taken*perFrame:]...)
	return out
}

// sincFilter is a windowed-sinc interpolation kernel sampled into a table.
//
// The kernel is evaluated by linear interpolation between table entries, which
// is the standard trick for arbitrary-ratio resampling: it keeps the per-sample
// cost to a table lookup instead of a transcendental, and the table is dense
// enough that the interpolation error stays far below the stopband.
type sincFilter struct {
	table []float32
	// halfTaps is the kernel's reach in input frames on each side.
	halfTaps int
	// subPhases is how many table entries cover one input frame.
	subPhases float64
}

const (
	// sincZeros is the number of sinc zero crossings per side. More means a
	// sharper transition band at a linear cost per output sample.
	sincZeros = 16
	// sincSubPhases is the table resolution, entries per input frame.
	sincSubPhases = 512
	// sincBeta is the Kaiser window parameter, chosen for roughly 90 dB of
	// stopband attenuation.
	sincBeta = 9.0
	// sincRolloff keeps the cutoff just below Nyquist so the transition band
	// has somewhere to go.
	sincRolloff = 0.95
)

func newSincFilter(ratio float64) *sincFilter {
	cutoff := math.Min(1, ratio) * sincRolloff
	halfWidth := float64(sincZeros) / cutoff

	n := int(halfWidth*sincSubPhases) + 2
	table := make([]float32, n)
	for i := range table {
		x := float64(i) / sincSubPhases
		table[i] = float32(cutoff * sinc(cutoff*x) * kaiser(x/halfWidth, sincBeta))
	}

	return &sincFilter{
		table:     table,
		halfTaps:  int(math.Ceil(halfWidth)),
		subPhases: sincSubPhases,
	}
}

// at evaluates the kernel at distance d (in input frames) from its centre.
func (f *sincFilter) at(d float64) float32 {
	x := d * f.subPhases
	i := int(x)
	if i+1 >= len(f.table) {
		return 0
	}
	frac := float32(x - float64(i))
	return f.table[i] + frac*(f.table[i+1]-f.table[i])
}

func sinc(x float64) float64 {
	if x == 0 {
		return 1
	}
	px := math.Pi * x
	return math.Sin(px) / px
}

// kaiser evaluates the Kaiser window at t in [-1, 1], zero outside.
func kaiser(t, beta float64) float64 {
	if t <= -1 || t >= 1 {
		return 0
	}
	return besselI0(beta*math.Sqrt(1-t*t)) / besselI0(beta)
}

// besselI0 is the zeroth-order modified Bessel function of the first kind,
// evaluated by its power series. The series converges quickly for the beta
// range a Kaiser window uses.
func besselI0(x float64) float64 {
	sum, term := 1.0, 1.0
	quarterSq := x * x / 4
	for k := 1; k < 64; k++ {
		term *= quarterSq / float64(k*k)
		sum += term
		if term < 1e-16*sum {
			break
		}
	}
	return sum
}
