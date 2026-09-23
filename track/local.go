package track

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/sdp/v3"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"google.golang.org/protobuf/proto"

	"github.com/GetStream/getstream-go-webrtc/internal/red"
	"github.com/GetStream/getstream-go-webrtc/internal/xrand"
	"github.com/GetStream/getstream-go-webrtc/logger"
)

const (
	rtpOutboundMTU = 1200
	rtpInboundMTU  = 1500

	// maxLag is how far a slow provider may fall behind real time before the
	// write loop stops catching up, so a stall does not end in a burst.
	maxLag = 200 * time.Millisecond
)

var (
	errInvalidDurationSample = errors.New("invalid duration sample")
	errOutOfOrderSample      = errors.New("out-of-order sample")
)

// SampleWriteOptions carries per-sample header extension values.
type SampleWriteOptions struct {
	AudioLevel *uint8
}

// Option configures a Local track.
type Option func(s *options)

type options struct {
	onRTCP       func(SampleProvider, rtcp.Packet) error
	videoLayer   *sfu_models.VideoLayer
	trackID      string
	isREDEnabled bool
	log          logger.ILogger
}

// WithSimulcast publishes the track as one simulcast layer. All layers of a
// track must share the same track ID.
func WithSimulcast(layer *sfu_models.VideoLayer) Option {
	return func(s *options) { s.videoLayer = layer }
}

// WithTrackID sets the track ID; the stream ID is derived from it.
func WithTrackID(trackID string) Option {
	return func(s *options) { s.trackID = trackID }
}

// WithRTCPHandler replaces the default RTCP handler, which asks a KeyFramer
// provider for a key frame on PLI and FIR.
func WithRTCPHandler(cb func(SampleProvider, rtcp.Packet) error) Option {
	return func(s *options) { s.onRTCP = cb }
}

// WithRedTranscodingEnabledForAudio sends opus audio wrapped in RED (RFC 2198),
// so each packet also carries the frames before it.
func WithRedTranscodingEnabledForAudio() Option {
	return func(s *options) { s.isREDEnabled = true }
}

// WithLogger sets the logger for write and RTCP failures.
func WithLogger(l logger.ILogger) Option {
	return func(s *options) { s.log = l }
}

func requestKeyFrame(p SampleProvider, pkt rtcp.Packet) error {
	switch pkt.(type) {
	case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
		if keyFramer, ok := p.(KeyFramer); ok {
			return keyFramer.ForceKeyFrame()
		}
	}
	return nil
}

// Local is a track published to the SFU. Given a SampleProvider it paces,
// packetizes and timestamps the samples itself, and it stamps the header
// extensions the SFU needs.
type Local struct {
	options
	rtp         *webrtc.TrackLocalStaticRTP
	trackInfo   *sfu_models.TrackInfo
	transceiver atomic.Pointer[webrtc.RTPTransceiver]
	binding     atomic.Pointer[binding]
	muted       atomic.Bool

	mu         sync.Mutex
	provider   SampleProvider
	onComplete func()
	pump       *pump
	onBind     func()
	onUnbind   func()
	closed     bool
}

// binding is the state of one negotiation of the track.
type binding struct {
	ssrc         webrtc.SSRC
	audioLevelID uint8
	midID        uint8
	ridID        uint8
	// acked is set once the SFU reports on the SSRC: it has mapped it to the
	// layer, so mid and rid no longer need to ride on every packet.
	acked atomic.Bool

	// mu keeps a sample's packets together and in order on the wire.
	mu         sync.Mutex
	sequencer  rtp.Sequencer
	packetizer rtp.Packetizer
	clockRate  uint32
	clock      sampleClock
}

type pump struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// NewLocalTrack creates a track for the codec. Audio tracks get their own copy
// of info, with Red set to match WithRedTranscodingEnabledForAudio.
func NewLocalTrack(info *sfu_models.TrackInfo, c webrtc.RTPCodecCapability, opts ...Option) (*Local, error) {
	s := &Local{options: options{
		onRTCP:  requestKeyFrame,
		trackID: "trackID-" + xrand.NewRandom(24),
		log:     logger.Noop{},
	}}
	for _, o := range opts {
		o(&s.options)
	}
	if strings.HasPrefix(strings.ToLower(c.MimeType), "audio") {
		info = proto.Clone(info).(*sfu_models.TrackInfo)
		info.Red = s.isREDEnabled
		if s.isREDEnabled {
			c = webrtc.RTPCodecCapability{
				MimeType:    red.MimeTypeAudio,
				ClockRate:   48000,
				Channels:    2,
				SDPFmtpLine: "111/111",
			}
		}
	}
	rtpTrack, err := webrtc.NewTrackLocalStaticRTP(c, s.trackID, "streamID-"+s.trackID, webrtc.WithRTPStreamID(rid(s.videoLayer)))
	if err != nil {
		return nil, err
	}
	s.rtp = rtpTrack
	s.trackInfo = info
	return s, nil
}

// rid is the RTP stream ID the SFU keys a simulcast layer on.
func rid(layer *sfu_models.VideoLayer) string {
	if layer == nil {
		return ""
	}
	switch layer.Quality {
	case sfu_models.VideoQuality_VIDEO_QUALITY_HIGH:
		return "f"
	case sfu_models.VideoQuality_VIDEO_QUALITY_MID:
		return "h"
	case sfu_models.VideoQuality_VIDEO_QUALITY_LOW_UNSPECIFIED:
		return "q"
	}
	return ""
}

// Bind is part of webrtc.TrackLocal and is called by pion on negotiation.
func (s *Local) Bind(ctx webrtc.TrackLocalContext) (webrtc.RTPCodecParameters, error) {
	codec, err := s.rtp.Bind(ctx)
	if err != nil {
		return codec, err
	}
	b, err := newBinding(ctx, codec)
	if err != nil {
		_ = s.rtp.Unbind(ctx)
		return codec, err
	}
	s.binding.Store(b)
	go s.readRTCP(b, ctx.RTCPReader())

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.provider != nil && !s.closed {
		if err := s.provider.OnBind(); err != nil {
			s.binding.Store(nil)
			_ = s.rtp.Unbind(ctx)
			return codec, err
		}
		s.startPumpLocked(s.provider, s.onComplete)
	}
	if s.onBind != nil {
		go s.onBind()
	}
	return codec, nil
}

// Unbind is part of webrtc.TrackLocal and is called by pion when the track
// leaves its peer connection.
func (s *Local) Unbind(ctx webrtc.TrackLocalContext) error {
	if err := s.rtp.Unbind(ctx); err != nil {
		return err
	}
	s.binding.Store(nil)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopPumpLocked()
	var err error
	if s.provider != nil {
		err = s.provider.OnUnbind()
	}
	if s.onUnbind != nil {
		go s.onUnbind()
	}
	return err
}

// StartWrite makes provider the source of the track. It starts writing once
// the track is bound, or straight away if it already is, replacing the
// current provider. onComplete runs when the write loop ends.
func (s *Local) StartWrite(provider SampleProvider, onComplete func()) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.provider == provider {
		return nil
	}
	if s.binding.Load() != nil && !s.closed {
		if err := provider.OnBind(); err != nil {
			return err
		}
		s.stopPumpLocked()
		if s.provider != nil {
			if err := s.provider.OnUnbind(); err != nil {
				s.log.Warnw("could not unbind the replaced sample provider", err, "track", s.ID())
			}
		}
		s.startPumpLocked(provider, onComplete)
	}
	s.provider = provider
	s.onComplete = onComplete
	return nil
}

// Close stops the write loop and closes the provider.
func (s *Local) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.stopPumpLocked()
	if s.provider != nil {
		return s.provider.Close()
	}
	return nil
}

// SetTransceiver records the transceiver the track is sent on; its mid is
// stamped on simulcast packets.
func (s *Local) SetTransceiver(transceiver *webrtc.RTPTransceiver) {
	s.transceiver.Store(transceiver)
}

// Transceiver returns the transceiver set by SetTransceiver.
func (s *Local) Transceiver() *webrtc.RTPTransceiver {
	return s.transceiver.Load()
}

// Track returns the underlying pion track.
func (s *Local) Track() *webrtc.TrackLocalStaticRTP {
	return s.rtp
}

// TrackInfo returns what the SFU is told about the track.
func (s *Local) TrackInfo() *sfu_models.TrackInfo {
	return s.trackInfo
}

// VideoLayer returns the simulcast layer, or nil.
func (s *Local) VideoLayer() *sfu_models.VideoLayer {
	return s.videoLayer
}

// ID is the track ID.
func (s *Local) ID() string { return s.rtp.ID() }

// RID is the RTP stream ID, empty unless the track is a simulcast layer.
func (s *Local) RID() string { return s.rtp.RID() }

// StreamID is the stream ID, derived from the track ID.
func (s *Local) StreamID() string { return s.rtp.StreamID() }

// Kind is audio or video, from the codec.
func (s *Local) Kind() webrtc.RTPCodecType { return s.rtp.Kind() }

// Codec is the codec the track offers.
func (s *Local) Codec() webrtc.RTPCodecCapability { return s.rtp.Codec() }

// IsBound reports whether the track is negotiated on a peer connection.
func (s *Local) IsBound() bool {
	return s.binding.Load() != nil
}

// SSRC is the negotiated SSRC, or 0 while unbound.
func (s *Local) SSRC() webrtc.SSRC {
	if b := s.binding.Load(); b != nil {
		return b.ssrc
	}
	return 0
}

// OnBind sets a callback run after the track is bound.
func (s *Local) OnBind(f func()) {
	s.mu.Lock()
	s.onBind = f
	s.mu.Unlock()
}

// OnUnbind sets a callback run after the track is unbound.
func (s *Local) OnUnbind(f func()) {
	s.mu.Lock()
	s.onUnbind = f
	s.mu.Unlock()
}

// SetMuted stops or resumes putting this track's samples on the wire.
//
// The sample provider keeps running and its samples are discarded, rather than
// the write loop being stopped, so the track resumes in place with no
// renegotiation. That is what makes this the right primitive for switching a
// simulcast layer off when the SFU asks for less bandwidth.
func (s *Local) SetMuted(muted bool) {
	s.muted.Store(muted)
}

// Muted reports whether this track is currently dropping its samples.
func (s *Local) Muted() bool {
	return s.muted.Load()
}

// WriteSample packetizes a sample and writes it. It is dropped while the track
// is unbound.
//
// The sample's RTP time comes from PacketTimestamp if set, else from the wall
// clock Timestamp, else from Duration. It never lands closer to the previous
// sample than that sample's length, and PrevDroppedPackets leaves a gap of
// that many sequence numbers.
func (s *Local) WriteSample(sample media.Sample, opts *SampleWriteOptions) error {
	b := s.binding.Load()
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	skip, span, err := b.clock.place(sample, b.clockRate)
	if err != nil {
		return err
	}
	for range sample.PrevDroppedPackets {
		b.sequencer.NextSequenceNumber()
	}
	b.packetizer.SkipSamples(skip)

	var firstErr error
	for _, p := range b.packetizer.Packetize(sample.Data, span) {
		if err := s.write(b, p, opts); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// WriteRTP stamps the negotiated header extensions on p and writes it.
func (s *Local) WriteRTP(p *rtp.Packet, opts *SampleWriteOptions) error {
	return s.write(s.binding.Load(), p, opts)
}

func (s *Local) write(b *binding, p *rtp.Packet, opts *SampleWriteOptions) error {
	if b != nil {
		if err := s.stamp(b, p, opts); err != nil {
			return err
		}
	}
	return s.rtp.WriteRTP(p)
}

func (s *Local) stamp(b *binding, p *rtp.Packet, opts *SampleWriteOptions) error {
	if b.audioLevelID != 0 && opts != nil && opts.AudioLevel != nil {
		level, err := rtp.AudioLevelExtension{Level: *opts.AudioLevel}.Marshal()
		if err != nil {
			return err
		}
		if err := p.SetExtension(b.audioLevelID, level); err != nil {
			return err
		}
	}

	if s.RID() == "" || b.acked.Load() {
		return nil
	}
	transceiver := s.transceiver.Load()
	if transceiver == nil || transceiver.Mid() == "" {
		return nil
	}
	if b.midID != 0 {
		if err := p.SetExtension(b.midID, []byte(transceiver.Mid())); err != nil {
			return err
		}
	}
	if b.ridID != 0 {
		if err := p.SetExtension(b.ridID, []byte(s.RID())); err != nil {
			return err
		}
	}
	return nil
}

// readRTCP drains the sender's RTCP until the binding goes away. Pion's
// interceptors only see RTCP that someone reads.
func (s *Local) readRTCP(b *binding, r interceptor.RTCPReader) {
	buf := make([]byte, rtpInboundMTU)
	for {
		n, _, err := r.Read(buf, nil)
		if err != nil {
			return
		}
		pkts, err := rtcp.Unmarshal(buf[:n])
		if err != nil {
			s.log.Debugw("dropping malformed rtcp", "track", s.ID(), "error", err)
			continue
		}
		for _, pkt := range pkts {
			if rr, ok := pkt.(*rtcp.ReceiverReport); ok && reportsOn(rr, b.ssrc) {
				b.acked.Store(true)
			}
			if s.onRTCP == nil {
				continue
			}
			s.mu.Lock()
			provider := s.provider
			s.mu.Unlock()
			if err := s.onRTCP(provider, pkt); err != nil {
				s.log.Warnw("rtcp handler failed", err, "track", s.ID())
			}
		}
	}
}

func reportsOn(rr *rtcp.ReceiverReport, ssrc webrtc.SSRC) bool {
	for _, r := range rr.Reports {
		if webrtc.SSRC(r.SSRC) == ssrc {
			return true
		}
	}
	return false
}

// startPumpLocked starts the write loop. There is never more than one, so the
// provider is never read concurrently.
func (s *Local) startPumpLocked(provider SampleProvider, onComplete func()) {
	ctx, cancel := context.WithCancel(context.Background())
	p := &pump{cancel: cancel, done: make(chan struct{})}
	s.pump = p
	go func() {
		s.runPump(ctx, provider)
		close(p.done)
		if onComplete != nil {
			onComplete()
		}
	}()
}

func (s *Local) stopPumpLocked() {
	if s.pump == nil {
		return
	}
	s.pump.cancel()
	<-s.pump.done
	s.pump = nil
}

// runPump writes the provider's samples in real time until it runs out or ctx
// is done. It must not take s.mu: stopPumpLocked waits for it under that lock.
func (s *Local) runPump(ctx context.Context, provider SampleProvider) {
	audio, isAudio := provider.(AudioSampleProvider)
	timer := time.NewTimer(0)
	defer timer.Stop()

	next := time.Now()
	for ctx.Err() == nil {
		sample, err := provider.NextSample(ctx)
		if err != nil {
			if !errors.Is(err, io.EOF) && ctx.Err() == nil {
				s.log.Warnw("sample provider failed", err, "track", s.ID())
			}
			return
		}

		if !s.muted.Load() {
			var opts *SampleWriteOptions
			if isAudio {
				level := audio.CurrentAudioLevel()
				opts = &SampleWriteOptions{AudioLevel: &level}
			}
			if err := s.WriteSample(sample, opts); err != nil {
				s.log.Debugw("could not write sample", "track", s.ID(), "error", err)
			}
		}

		next = next.Add(sample.Duration)
		wait := time.Until(next)
		if wait < -maxLag {
			next = time.Now()
		}
		if wait <= 0 {
			continue
		}
		timer.Reset(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			return
		}
	}
}

func newBinding(ctx webrtc.TrackLocalContext, codec webrtc.RTPCodecParameters) (*binding, error) {
	payloader, err := payloaderForCodec(codec.RTPCodecCapability)
	if err != nil {
		return nil, err
	}
	b := &binding{
		ssrc:      ctx.SSRC(),
		sequencer: rtp.NewRandomSequencer(),
		clockRate: codec.ClockRate,
	}
	for _, ext := range ctx.HeaderExtensions() {
		switch ext.URI {
		case sdp.AudioLevelURI:
			b.audioLevelID = uint8(ext.ID)
		case sdp.SDESMidURI:
			b.midID = uint8(ext.ID)
		case sdp.SDESRTPStreamIDURI:
			b.ridID = uint8(ext.ID)
		}
	}
	// SSRC and payload type are left at 0: TrackLocalStaticRTP sets them.
	b.packetizer = rtp.NewPacketizer(rtpOutboundMTU, 0, 0, payloader, b.sequencer, codec.ClockRate)
	if strings.EqualFold(codec.MimeType, red.MimeTypeAudio) {
		b.packetizer = red.NewPacketizer(b.packetizer, codec.PayloadType)
	}
	return b, nil
}

// sampleClock places samples on the RTP timeline. It counts ticks from an
// arbitrary origin: only the distance between samples reaches the packetizer,
// which picks its own random base.
type sampleClock struct {
	started bool
	// start is where the previous sample began and end where it finished.
	start, end uint32
	// wall is the previous sample's wall clock time, if it had one.
	wall time.Time
}

// place returns how many ticks to skip before the sample and how many each of
// its packets spans.
func (c *sampleClock) place(s media.Sample, rate uint32) (skip, span uint32, err error) {
	if s.Duration < 0 {
		return 0, 0, errInvalidDurationSample
	}
	span = uint32(int64(s.Duration) * int64(rate) / int64(time.Second))
	dropped := uint32(s.PrevDroppedPackets) * span

	if !c.started && s.PacketTimestamp != 0 {
		c.end = s.PacketTimestamp - dropped
	}
	start := c.end + dropped
	if c.started {
		var target uint32
		switch {
		case s.PacketTimestamp != 0:
			if int32(s.PacketTimestamp-c.start) < 0 {
				return 0, 0, errOutOfOrderSample
			}
			target = s.PacketTimestamp
		case !s.Timestamp.IsZero() && !c.wall.IsZero():
			if s.Timestamp.Before(c.wall) {
				return 0, 0, errOutOfOrderSample
			}
			target = c.start + uint32(int64(s.Timestamp.Sub(c.wall))*int64(rate)/int64(time.Second))
		default:
			target = start
		}
		if int32(target-start) > 0 {
			start = target
		}
	}

	skip = start - c.end
	c.started = true
	c.start, c.end, c.wall = start, start+span, s.Timestamp
	return skip, span, nil
}

// payloaderForCodec mirrors pion's own codec-to-payloader table, which it does
// not export.
func payloaderForCodec(codec webrtc.RTPCodecCapability) (rtp.Payloader, error) {
	switch strings.ToLower(codec.MimeType) {
	case strings.ToLower(webrtc.MimeTypeH264):
		return &codecs.H264Payloader{}, nil
	// RED carries opus frames; the RED packetizer wraps them afterwards.
	case strings.ToLower(webrtc.MimeTypeOpus), strings.ToLower(red.MimeTypeAudio):
		return &codecs.OpusPayloader{}, nil
	case strings.ToLower(webrtc.MimeTypeVP8):
		return &codecs.VP8Payloader{EnablePictureID: true}, nil
	case strings.ToLower(webrtc.MimeTypeVP9):
		return &codecs.VP9Payloader{}, nil
	case strings.ToLower(webrtc.MimeTypeG722):
		return &codecs.G722Payloader{}, nil
	case strings.ToLower(webrtc.MimeTypePCMU), strings.ToLower(webrtc.MimeTypePCMA):
		return &codecs.G711Payloader{}, nil
	case strings.ToLower(webrtc.MimeTypeAV1):
		return &codecs.AV1Payloader{}, nil
	default:
		return nil, webrtc.ErrNoPayloaderForCodec
	}
}
