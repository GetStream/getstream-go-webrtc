package track

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

// The fakes below stand in for the pieces a PeerConnection would supply after
// negotiation, so the whole bind/packetize/write path can be driven without a
// peer connection, an SFU or a network.

type recordedPacket struct {
	header  rtp.Header
	payload []byte
}

// recordingWriter is the TrackLocalWriter a binding writes to.
type recordingWriter struct {
	mu      sync.Mutex
	packets []recordedPacket
	err     error
}

func (w *recordingWriter) WriteRTP(header *rtp.Header, payload []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.err != nil {
		return 0, w.err
	}
	w.packets = append(w.packets, recordedPacket{
		header:  *header,
		payload: append([]byte(nil), payload...),
	})
	return len(payload), nil
}

func (w *recordingWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.err != nil {
		return 0, w.err
	}
	return len(b), nil
}

func (w *recordingWriter) written() []recordedPacket {
	w.mu.Lock()
	defer w.mu.Unlock()

	return append([]recordedPacket(nil), w.packets...)
}

func (w *recordingWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	return len(w.packets)
}

// fakeRTCPReader feeds the track's rtcpWorker packets the SFU would send.
type fakeRTCPReader struct {
	packets chan []byte
	closed  chan struct{}
	once    sync.Once
}

func newFakeRTCPReader() *fakeRTCPReader {
	return &fakeRTCPReader{packets: make(chan []byte, 8), closed: make(chan struct{})}
}

func (r *fakeRTCPReader) Read(b []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
	select {
	case raw := <-r.packets:
		return copy(b, raw), a, nil
	case <-r.closed:
		return 0, a, io.EOF
	}
}

func (r *fakeRTCPReader) send(t *testing.T, packets ...rtcp.Packet) {
	t.Helper()

	raw, err := rtcp.Marshal(packets)
	if err != nil {
		t.Fatalf("marshal rtcp: %v", err)
	}
	select {
	case r.packets <- raw:
	case <-time.After(time.Second):
		t.Fatal("rtcp worker never read the packet")
	}
}

func (r *fakeRTCPReader) close() {
	r.once.Do(func() { close(r.closed) })
}

// fakeTrackContext is the negotiated context pion hands to TrackLocal.Bind.
type fakeTrackContext struct {
	id         string
	codecs     []webrtc.RTPCodecParameters
	extensions []webrtc.RTPHeaderExtensionParameter
	ssrc       webrtc.SSRC
	writer     *recordingWriter
	rtcp       *fakeRTCPReader
}

var _ webrtc.TrackLocalContext = (*fakeTrackContext)(nil)

func newTrackContext(t *testing.T, codecs []webrtc.RTPCodecParameters, extensions ...webrtc.RTPHeaderExtensionParameter) *fakeTrackContext {
	t.Helper()

	reader := newFakeRTCPReader()
	t.Cleanup(reader.close)

	return &fakeTrackContext{
		id:         "binding-1",
		codecs:     codecs,
		extensions: extensions,
		ssrc:       webrtc.SSRC(0x1234abcd),
		writer:     &recordingWriter{},
		rtcp:       reader,
	}
}

func (c *fakeTrackContext) CodecParameters() []webrtc.RTPCodecParameters { return c.codecs }

func (c *fakeTrackContext) HeaderExtensions() []webrtc.RTPHeaderExtensionParameter {
	return c.extensions
}

func (c *fakeTrackContext) SSRC() webrtc.SSRC                       { return c.ssrc }
func (c *fakeTrackContext) SSRCRetransmission() webrtc.SSRC         { return c.ssrc + 1 }
func (c *fakeTrackContext) SSRCForwardErrorCorrection() webrtc.SSRC { return c.ssrc + 2 }
func (c *fakeTrackContext) WriteStream() webrtc.TrackLocalWriter    { return c.writer }
func (c *fakeTrackContext) ID() string                              { return c.id }
func (c *fakeTrackContext) RTCPReader() interceptor.RTCPReader      { return c.rtcp }

// countingProvider is a SampleProvider that hands out a fixed sample and counts
// what the track asked of it.
type countingProvider struct {
	mu sync.Mutex

	duration time.Duration
	payload  []byte
	eofAfter int // 0 means never
	nextErr  error
	bindErr  error

	polled      int
	bindCalls   int
	unbindCalls int
	closeCalls  int

	level atomic.Uint32
}

var _ AudioSampleProvider = (*countingProvider)(nil)

func newCountingProvider(payload []byte) *countingProvider {
	return &countingProvider{duration: 20 * time.Millisecond, payload: payload}
}

func (p *countingProvider) NextSample(ctx context.Context) (media.Sample, error) {
	select {
	case <-ctx.Done():
		return media.Sample{}, ctx.Err()
	default:
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.polled++
	if p.nextErr != nil {
		return media.Sample{}, p.nextErr
	}
	if p.eofAfter > 0 && p.polled > p.eofAfter {
		return media.Sample{}, io.EOF
	}
	return media.Sample{
		Data:     append([]byte(nil), p.payload...),
		Duration: p.duration,
	}, nil
}

func (p *countingProvider) OnBind() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.bindCalls++
	return p.bindErr
}

func (p *countingProvider) OnUnbind() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.unbindCalls++
	return nil
}

func (p *countingProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closeCalls++
	return nil
}

func (p *countingProvider) CurrentAudioLevel() uint8 { return uint8(p.level.Load()) }

func (p *countingProvider) counts() (polled, binds, unbinds, closes int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.polled, p.bindCalls, p.unbindCalls, p.closeCalls
}

// plainProvider deliberately does not implement AudioSampleProvider, so the
// write loop has no audio level to attach.
type plainProvider struct {
	inner *countingProvider
}

func (p plainProvider) NextSample(ctx context.Context) (media.Sample, error) {
	return p.inner.NextSample(ctx)
}

func (p plainProvider) OnBind() error   { return p.inner.OnBind() }
func (p plainProvider) OnUnbind() error { return p.inner.OnUnbind() }
func (p plainProvider) Close() error    { return p.inner.Close() }

// keyFrameProvider reports key frame requests, which is how the default RTCP
// handler reacts to PLI and FIR.
type keyFrameProvider struct {
	*countingProvider
	keyFrames atomic.Int64
}

var _ KeyFramer = (*keyFrameProvider)(nil)

func (p *keyFrameProvider) ForceKeyFrame() error {
	p.keyFrames.Add(1)
	return nil
}
