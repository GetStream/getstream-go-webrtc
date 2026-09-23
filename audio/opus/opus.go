// Package opus encodes and decodes Opus using a pure-Go codec.
//
// It wraps github.com/thesyncim/gopus in an API that speaks [audio.PCM] instead
// of raw sample slices, and takes care of the two things every caller
// otherwise reimplements: converting the input to the codec's sample rate and
// channel layout, and cutting it into the fixed-size frames Opus requires.
//
// An [Encoder] accepts buffers of any rate, layout and format and emits whole
// Opus packets. A [Decoder] turns packets back into buffers and can conceal the
// ones that never arrived.
//
// Neither type is safe for concurrent use; give each stream its own.
package opus

import (
	"fmt"
	"time"

	"github.com/thesyncim/gopus"

	"github.com/GetStream/getstream-go-webrtc/audio"
)

// Defaults matching what WebRTC negotiates for audio.
const (
	DefaultSampleRate    = 48000
	DefaultChannels      = 1
	DefaultFrameDuration = 20 * time.Millisecond
)

// maxPacketBytes is the largest packet Opus can produce, so an encode buffer of
// this size never needs to grow.
const maxPacketBytes = 4000

// Application tells the encoder what it is compressing, which shifts its
// internal mode selection.
type Application uint8

const (
	// ApplicationVoIP optimises for speech intelligibility. It is the default
	// because this SDK exists to carry conversations.
	ApplicationVoIP Application = iota
	// ApplicationAudio optimises for general audio and music.
	ApplicationAudio
	// ApplicationLowDelay minimises latency at some cost in quality.
	ApplicationLowDelay
)

func (a Application) toGopus() (gopus.Application, error) {
	switch a {
	case ApplicationVoIP:
		return gopus.ApplicationVoIP, nil
	case ApplicationAudio:
		return gopus.ApplicationAudio, nil
	case ApplicationLowDelay:
		return gopus.ApplicationLowDelay, nil
	default:
		return 0, fmt.Errorf("opus: invalid application %d", uint8(a))
	}
}

// Config configures an [Encoder] or a [Decoder]. Every field may be left zero,
// in which case the documented default applies, so Config{} is a working
// 48 kHz mono voice configuration.
type Config struct {
	// SampleRate of the codec, one of 8000, 12000, 16000, 24000 or 48000.
	// Defaults to 48000. This is the rate packets decode to and the rate the
	// encoder converts its input to; it is not a constraint on the input.
	SampleRate int
	// Channels is 1 or 2, defaulting to 1.
	Channels int
	// FrameDuration is the length of one packet: 2.5, 5, 10, 20, 40, 60, 80,
	// 100 or 120 ms. Defaults to 20 ms, which is what WebRTC uses.
	FrameDuration time.Duration
	// Application biases encoder mode selection. Defaults to
	// [ApplicationVoIP]. Ignored by a Decoder.
	Application Application
	// Bitrate in bits per second. Zero leaves the codec's own choice, which
	// scales with the sample rate and channel count. Ignored by a Decoder.
	Bitrate int
	// Complexity trades CPU for quality, from 1 to 10. Zero leaves the codec
	// default. Ignored by a Decoder.
	Complexity int
	// FEC enables in-band forward error correction, which embeds a
	// low-bitrate copy of the previous frame in each packet so a decoder can
	// recover from a single loss. Ignored by a Decoder.
	FEC bool
	// PacketLossPercent tells the encoder how much loss to expect, which
	// controls how much redundancy FEC adds. Ignored by a Decoder.
	PacketLossPercent int
	// DTX stops sending packets during silence. Ignored by a Decoder.
	DTX bool
}

func (c Config) withDefaults() Config {
	if c.SampleRate == 0 {
		c.SampleRate = DefaultSampleRate
	}
	if c.Channels == 0 {
		c.Channels = DefaultChannels
	}
	if c.FrameDuration == 0 {
		c.FrameDuration = DefaultFrameDuration
	}
	return c
}

// frameSamples is the per-channel sample count of one frame, and the point at
// which an unsupported rate or frame duration is rejected.
func (c Config) frameSamples() (int, error) {
	switch c.SampleRate {
	case 8000, 12000, 16000, 24000, 48000:
	default:
		return 0, fmt.Errorf("opus: sample rate %d is not supported (want 8000, 12000, 16000, 24000 or 48000)", c.SampleRate)
	}
	if c.Channels != 1 && c.Channels != 2 {
		return 0, fmt.Errorf("opus: channels must be 1 or 2, got %d", c.Channels)
	}

	validDurations := []time.Duration{
		2500 * time.Microsecond,
		5 * time.Millisecond,
		10 * time.Millisecond,
		20 * time.Millisecond,
		40 * time.Millisecond,
		60 * time.Millisecond,
		80 * time.Millisecond,
		100 * time.Millisecond,
		120 * time.Millisecond,
	}
	if !contains(validDurations, c.FrameDuration) {
		return 0, fmt.Errorf("opus: frame duration %v is not an Opus frame size", c.FrameDuration)
	}

	n := int(c.FrameDuration * time.Duration(c.SampleRate) / time.Second)
	if n <= 0 {
		return 0, fmt.Errorf("opus: frame duration %v yields no samples at %d Hz", c.FrameDuration, c.SampleRate)
	}
	return n, nil
}

func contains[T comparable](haystack []T, needle T) bool {
	for _, v := range haystack {
		if v == needle {
			return true
		}
	}
	return false
}

// Encoder converts PCM into Opus packets.
//
// Input may be at any sample rate, channel count and format; the encoder
// resamples it and buffers whatever does not fill a whole frame, so a caller
// can write at whatever cadence its source produces. [Encoder.Encode] returns
// only complete packets, and returns none at all while the first frame is
// still filling.
type Encoder struct {
	enc          *gopus.Encoder
	cfg          Config
	frameSamples int
	resampler    *audio.StreamResampler
	packet       []byte
	silence      audio.PCM
}

// NewEncoder returns an encoder for cfg.
func NewEncoder(cfg Config) (*Encoder, error) {
	cfg = cfg.withDefaults()
	frameSamples, err := cfg.frameSamples()
	if err != nil {
		return nil, err
	}
	application, err := cfg.Application.toGopus()
	if err != nil {
		return nil, err
	}

	enc, err := gopus.NewEncoder(gopus.EncoderConfig{
		SampleRate:  cfg.SampleRate,
		Channels:    cfg.Channels,
		Application: application,
	})
	if err != nil {
		return nil, fmt.Errorf("opus: create encoder: %w", err)
	}

	if cfg.Bitrate > 0 {
		if err := enc.SetBitrate(cfg.Bitrate); err != nil {
			return nil, fmt.Errorf("opus: set bitrate: %w", err)
		}
	}
	if cfg.Complexity > 0 {
		if err := enc.SetComplexity(cfg.Complexity); err != nil {
			return nil, fmt.Errorf("opus: set complexity: %w", err)
		}
	}
	if cfg.FEC {
		if err := enc.SetInBandFEC(gopus.InBandFECEnabled); err != nil {
			return nil, fmt.Errorf("opus: enable FEC: %w", err)
		}
	}
	if cfg.PacketLossPercent > 0 {
		if err := enc.SetPacketLoss(cfg.PacketLossPercent); err != nil {
			return nil, fmt.Errorf("opus: set packet loss: %w", err)
		}
	}
	enc.SetDTX(cfg.DTX)

	return &Encoder{
		enc:          enc,
		cfg:          cfg,
		frameSamples: frameSamples,
		// Float32 throughout: the resampler works in float32 and so does the
		// codec's primary entry point, so nothing round-trips through int16.
		resampler: audio.NewStreamResampler(audio.FormatF32, cfg.SampleRate, cfg.Channels, frameSamples),
		packet:    make([]byte, maxPacketBytes),
		silence: audio.New(audio.FormatF32, cfg.SampleRate, cfg.Channels).
			PadTo(frameSamples, audio.PadEnd),
	}, nil
}

// Config returns the effective configuration, with defaults applied.
func (e *Encoder) Config() Config { return e.cfg }

// FrameDuration is the duration each returned packet represents.
func (e *Encoder) FrameDuration() time.Duration { return e.cfg.FrameDuration }

// FrameSamples is the per-channel sample count of one frame.
func (e *Encoder) FrameSamples() int { return e.frameSamples }

// Encode converts pcm and returns the packets that are now complete. Each
// packet covers exactly [Encoder.FrameDuration].
//
// Samples left over from a partial frame stay buffered for the next call. Call
// [Encoder.Flush] at the end of an utterance to emit them.
func (e *Encoder) Encode(pcm audio.PCM) ([][]byte, error) {
	frames, err := e.resampler.Write(pcm)
	if err != nil {
		return nil, err
	}
	return e.encodeFrames(frames)
}

// Flush emits the buffered tail, padding the last frame with silence so it is a
// legal Opus frame, and resets the encoder's resampling state.
func (e *Encoder) Flush() ([][]byte, error) {
	frames, err := e.resampler.Flush()
	if err != nil {
		return nil, err
	}
	return e.encodeFrames(frames)
}

// Reset drops buffered audio and returns the encoder to its initial state,
// ready for an unrelated stream.
func (e *Encoder) Reset() {
	e.resampler.Reset()
	e.enc.Reset()
}

func (e *Encoder) encodeFrames(frames []audio.PCM) ([][]byte, error) {
	if len(frames) == 0 {
		return nil, nil
	}
	out := make([][]byte, 0, len(frames))
	for _, frame := range frames {
		// Only Flush can produce a short frame; Opus has no way to express one.
		if frame.Len() < e.frameSamples {
			frame = frame.PadTo(e.frameSamples, audio.PadEnd)
		}
		n, err := e.enc.Encode(frame.Float32(), e.packet)
		if err != nil {
			return nil, fmt.Errorf("opus: encode: %w", err)
		}
		out = append(out, append([]byte(nil), e.packet[:n]...))
	}
	return out, nil
}

// EncodeSilence returns a single packet of encoded silence, which is what a
// sender emits to keep a stream's timeline continuous while it has nothing to
// say. It does not disturb the buffered stream state.
func (e *Encoder) EncodeSilence() ([]byte, error) {
	n, err := e.enc.Encode(e.silence.Float32(), e.packet)
	if err != nil {
		return nil, fmt.Errorf("opus: encode silence: %w", err)
	}
	return append([]byte(nil), e.packet[:n]...), nil
}

// Decoder converts Opus packets back into PCM.
type Decoder struct {
	dec          *gopus.Decoder
	cfg          Config
	frameSamples int
	buf          []float32
}

// NewDecoder returns a decoder for cfg. Only SampleRate, Channels and
// FrameDuration are used; the encoder-side fields are ignored.
func NewDecoder(cfg Config) (*Decoder, error) {
	cfg = cfg.withDefaults()
	frameSamples, err := cfg.frameSamples()
	if err != nil {
		return nil, err
	}

	gcfg := gopus.DefaultDecoderConfig(cfg.SampleRate, cfg.Channels)
	dec, err := gopus.NewDecoder(gcfg)
	if err != nil {
		return nil, fmt.Errorf("opus: create decoder: %w", err)
	}

	return &Decoder{
		dec:          dec,
		cfg:          cfg,
		frameSamples: frameSamples,
		buf:          make([]float32, gcfg.MaxPacketSamples*cfg.Channels),
	}, nil
}

// Config returns the effective configuration, with defaults applied.
func (d *Decoder) Config() Config { return d.cfg }

// FrameDuration is the duration [Decoder.Conceal] produces.
func (d *Decoder) FrameDuration() time.Duration { return d.cfg.FrameDuration }

// Decode turns one Opus packet into PCM. A packet may carry more than one
// frame, so the result is not always [Decoder.FrameDuration] long. An empty
// packet is treated as a loss and concealed.
func (d *Decoder) Decode(packet []byte) (audio.PCM, error) {
	if len(packet) == 0 {
		return d.Conceal()
	}
	n, err := d.dec.Decode(packet, d.buf)
	if err != nil {
		return audio.PCM{}, fmt.Errorf("opus: decode: %w", err)
	}
	return d.output(n), nil
}

// Conceal synthesises one frame to cover a packet that never arrived.
//
// Handing the decoder an explicit loss rather than skipping it keeps its
// internal state aligned with the sender's and lets it fade out rather than
// click, which matters more than the concealed audio itself.
func (d *Decoder) Conceal() (audio.PCM, error) {
	// gopus derives the concealed length from the buffer it is given.
	n, err := d.dec.Decode(nil, d.buf[:d.frameSamples*d.cfg.Channels])
	if err != nil {
		return audio.PCM{}, fmt.Errorf("opus: conceal: %w", err)
	}
	return d.output(n), nil
}

// DecodeFEC recovers the frame *before* packet from the redundant copy embedded
// in it, for use when the previous packet was lost and the encoder had FEC on.
//
// The normal sequence on a loss is to call this with the next packet that did
// arrive, then decode that packet normally. When it carries no redundant data
// the result is concealed audio, the same as [Decoder.Conceal].
func (d *Decoder) DecodeFEC(packet []byte) (audio.PCM, error) {
	if len(packet) == 0 {
		return d.Conceal()
	}
	n, err := d.dec.DecodeWithFEC(packet, d.buf[:d.frameSamples*d.cfg.Channels], true)
	if err != nil {
		return audio.PCM{}, fmt.Errorf("opus: decode FEC: %w", err)
	}
	return d.output(n), nil
}

// Reset returns the decoder to its initial state, discarding the history it
// uses for concealment.
func (d *Decoder) Reset() { d.dec.Reset() }

func (d *Decoder) output(frames int) audio.PCM {
	samples := make([]float32, frames*d.cfg.Channels)
	copy(samples, d.buf)
	return audio.FromFloat32(samples, d.cfg.SampleRate, d.cfg.Channels)
}
