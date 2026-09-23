package track

import (
	"context"
	"time"

	"github.com/pion/webrtc/v4/pkg/media"
)

// SampleProvider feeds a Local track. The track calls NextSample from a single
// goroutine and paces the calls by each sample's Duration.
type SampleProvider interface {
	// NextSample blocks until the next sample is ready. io.EOF ends the track
	// cleanly; it must return promptly once ctx is done.
	NextSample(ctx context.Context) (media.Sample, error)
	// OnBind is called when the track is negotiated and about to be written.
	OnBind() error
	// OnUnbind is called when the track leaves its peer connection.
	OnUnbind() error
	// Close is called once, when the track is closed.
	Close() error
}

// KeyFramer is implemented by video providers that can produce a key frame on
// demand. The default RTCP handler calls it on PLI and FIR.
type KeyFramer interface {
	ForceKeyFrame() error
}

// AudioSampleProvider is an audio provider that reports the level of what it
// last produced, written to the audio-level header extension when negotiated.
type AudioSampleProvider interface {
	SampleProvider
	// CurrentAudioLevel is the level in -dBov, 0 loudest and 127 silent.
	CurrentAudioLevel() uint8
}

// BaseSampleProvider gives a provider no-op OnBind, OnUnbind and Close.
type BaseSampleProvider struct{}

// OnBind does nothing.
func (p *BaseSampleProvider) OnBind() error { return nil }

// OnUnbind does nothing.
func (p *BaseSampleProvider) OnUnbind() error { return nil }

// Close does nothing.
func (p *BaseSampleProvider) Close() error { return nil }

// nullSamplesPerSecond is the rate NewNullSampleProvider produces samples at.
const nullSamplesPerSecond = 30

// NullSampleProvider produces zeroed samples sized to meet a bitrate.
type NullSampleProvider struct {
	BaseSampleProvider
	BytesPerSample uint32
	SampleDuration time.Duration
}

// NewNullSampleProvider returns a provider that sends bitrate bits a second.
func NewNullSampleProvider(bitrate uint32) *NullSampleProvider {
	p := &NullSampleProvider{SampleDuration: time.Second / nullSamplesPerSecond}
	p.BytesPerSample = bitrate / nullSamplesPerSecond / 8
	return p
}

// NextSample returns a zeroed sample.
func (p *NullSampleProvider) NextSample(context.Context) (media.Sample, error) {
	return media.Sample{
		Data:     make([]byte, p.BytesPerSample),
		Duration: p.SampleDuration,
	}, nil
}
