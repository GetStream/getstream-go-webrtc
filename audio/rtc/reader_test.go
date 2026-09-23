package rtc

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/audio"
	"github.com/GetStream/getstream-go-webrtc/audio/opus"
	"github.com/GetStream/getstream-go-webrtc/internal/red"
)

const (
	testOpusPT    = 111
	testBaseSeq   = 1000
	testBaseTS    = uint32(1) << 20
	testFrameTick = 960 // 20 ms at the 48 kHz Opus clock
)

// fakeRTPSource replays a fixed list of packets, then reports the end of the
// stream. It stands in for a live TrackRemote.
type fakeRTPSource struct {
	packets []*rtp.Packet
	idx     int
	endErr  error
}

func (f *fakeRTPSource) ReadRTP() (*rtp.Packet, interceptor.Attributes, error) {
	if f.idx >= len(f.packets) {
		if f.endErr != nil {
			return nil, nil, f.endErr
		}
		return nil, nil, io.EOF
	}
	pkt := f.packets[f.idx]
	f.idx++
	return pkt, nil, nil
}

// opusStream encodes a tone and wraps each packet in RTP with consecutive
// sequence numbers, which is what an undisturbed sender produces.
func opusStream(t *testing.T, frames int) []*rtp.Packet {
	t.Helper()

	enc, err := opus.NewEncoder(opus.Config{})
	require.NoError(t, err)

	packets, err := enc.Encode(tone(48000, 440, time.Duration(frames)*20*time.Millisecond, 0.5))
	require.NoError(t, err)
	require.Len(t, packets, frames)

	out := make([]*rtp.Packet, frames)
	for i, payload := range packets {
		out[i] = &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				PayloadType:    testOpusPT,
				SequenceNumber: uint16(testBaseSeq + i),
				Timestamp:      testBaseTS + uint32(i)*testFrameTick,
				SSRC:           0xDEADBEEF,
			},
			Payload: payload,
		}
	}
	return out
}

func opusCodec() webrtc.RTPCodecCapability {
	return webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: OpusClockRate, Channels: 1}
}

// readAll drains a reader, which must terminate at the end of its source.
func readAll(t *testing.T, r *TrackReader) []audio.PCM {
	t.Helper()

	var out []audio.PCM
	for pcm, err := range r.Frames() {
		require.NoError(t, err)
		out = append(out, pcm)
	}
	return out
}

func TestTrackReaderDecodesAStream(t *testing.T) {
	t.Parallel()

	src := &fakeRTPSource{packets: opusStream(t, 25)}
	r, err := NewRTPReader(src, opusCodec(), ReaderConfig{})
	require.NoError(t, err)

	frames := readAll(t, r)
	require.Len(t, frames, 25)

	var joined audio.PCM
	for i, f := range frames {
		require.Equal(t, 48000, f.SampleRate)
		require.Equal(t, 1, f.Channels)
		require.Equal(t, 960, f.Len())
		require.Equal(t, time.Duration(i)*20*time.Millisecond, f.PTS)
		require.NoError(t, joined.Append(f))
	}

	require.Equal(t, 500*time.Millisecond, joined.Duration())
	require.Greater(t, joined.RMS(), 0.2, "the tone should survive the round trip")
	require.NoError(t, r.Close())
}

func TestTrackReaderDecodesToARequestedRate(t *testing.T) {
	t.Parallel()

	// 16 kHz mono is what a speech recogniser wants, and the decoder can
	// produce it directly instead of making the caller resample.
	src := &fakeRTPSource{packets: opusStream(t, 10)}
	r, err := NewRTPReader(src, opusCodec(), ReaderConfig{
		Opus: opus.Config{SampleRate: 16000},
	})
	require.NoError(t, err)

	frames := readAll(t, r)
	require.Len(t, frames, 10)
	for _, f := range frames {
		require.Equal(t, 16000, f.SampleRate)
		require.Equal(t, 320, f.Len())
	}
}

// TestTrackReaderConcealsLoss covers the everyday network case: a packet never
// arrives, and the output has to stay continuous anyway.
func TestTrackReaderConcealsLoss(t *testing.T) {
	t.Parallel()

	packets := opusStream(t, 10)
	// Drop the sixth packet, leaving a one-packet hole in the sequence.
	lossy := append(append([]*rtp.Packet{}, packets[:5]...), packets[6:]...)

	src := &fakeRTPSource{packets: lossy}
	r, err := NewRTPReader(src, opusCodec(), ReaderConfig{})
	require.NoError(t, err)

	frames := readAll(t, r)
	require.Len(t, frames, 10, "the gap should be filled, not skipped")

	// PTS stays on the 20 ms grid straight through the hole.
	for i, f := range frames {
		require.Equal(t, time.Duration(i)*20*time.Millisecond, f.PTS)
	}
	require.Greater(t, frames[5].RMS(), 0.05,
		"concealment should continue the tone rather than fall silent")
}

func TestTrackReaderConcealsMultiPacketGaps(t *testing.T) {
	t.Parallel()

	packets := opusStream(t, 20)
	lossy := append(append([]*rtp.Packet{}, packets[:5]...), packets[9:]...)

	src := &fakeRTPSource{packets: lossy}
	r, err := NewRTPReader(src, opusCodec(), ReaderConfig{})
	require.NoError(t, err)

	frames := readAll(t, r)
	require.Len(t, frames, 20)
	for i, f := range frames {
		require.Equal(t, time.Duration(i)*20*time.Millisecond, f.PTS)
	}
}

func TestTrackReaderConcealmentCanBeDisabled(t *testing.T) {
	t.Parallel()

	packets := opusStream(t, 10)
	lossy := append(append([]*rtp.Packet{}, packets[:5]...), packets[6:]...)

	src := &fakeRTPSource{packets: lossy}
	r, err := NewRTPReader(src, opusCodec(), ReaderConfig{DisableConcealment: true})
	require.NoError(t, err)

	require.Len(t, readAll(t, r), 9, "the gap should be left as a gap")
}

// TestTrackReaderCapsConcealment stops a long silence from being replaced by
// several seconds of invented audio.
func TestTrackReaderCapsConcealment(t *testing.T) {
	t.Parallel()

	packets := opusStream(t, 4)
	// Jump the sequence forward by 100 packets, two seconds of nothing.
	far := packets[3]
	far.SequenceNumber += 100
	far.Timestamp += 100 * testFrameTick

	src := &fakeRTPSource{packets: packets}
	r, err := NewRTPReader(src, opusCodec(), ReaderConfig{MaxConceal: 100 * time.Millisecond})
	require.NoError(t, err)

	frames := readAll(t, r)
	// All four real packets, plus five concealed ones rather than the hundred
	// the sequence gap asks for: the 100 ms cap is five 20 ms frames.
	require.Len(t, frames, 4+5)
}

func TestTrackReaderIgnoresDuplicatesAndLatePackets(t *testing.T) {
	t.Parallel()

	packets := opusStream(t, 5)
	// Deliver packet 2 again after packet 4, the way a retransmission or a
	// duplicated path would.
	withDupes := append(append([]*rtp.Packet{}, packets...), packets[2])

	src := &fakeRTPSource{packets: withDupes}
	r, err := NewRTPReader(src, opusCodec(), ReaderConfig{})
	require.NoError(t, err)

	require.Len(t, readAll(t, r), 5, "a repeat of an old packet should be dropped")
}

func TestTrackReaderHandlesSequenceWraparound(t *testing.T) {
	t.Parallel()

	packets := opusStream(t, 5)
	for i, p := range packets {
		p.SequenceNumber = uint16(65534 + i) // wraps after the second packet
	}

	src := &fakeRTPSource{packets: packets}
	r, err := NewRTPReader(src, opusCodec(), ReaderConfig{})
	require.NoError(t, err)

	require.Len(t, readAll(t, r), 5)
}

// TestTrackReaderUnwrapsRED exercises the redundancy Stream negotiates for
// audio: each packet carries copies of its predecessors, so a single loss is
// recovered rather than concealed.
func TestTrackReaderUnwrapsRED(t *testing.T) {
	t.Parallel()

	primary := opusStream(t, 10)
	redPackets := encodeRED(t, primary)

	t.Run("clean stream", func(t *testing.T) {
		t.Parallel()

		src := &fakeRTPSource{packets: redPackets}
		r, err := NewRTPReader(src, redCodec(), ReaderConfig{})
		require.NoError(t, err)

		frames := readAll(t, r)
		require.Len(t, frames, 10)
		for _, f := range frames {
			require.Equal(t, 960, f.Len())
		}
	})

	t.Run("recovers a dropped packet", func(t *testing.T) {
		t.Parallel()

		lossy := append(append([]*rtp.Packet{}, redPackets[:5]...), redPackets[6:]...)
		src := &fakeRTPSource{packets: lossy}
		r, err := NewRTPReader(src, redCodec(), ReaderConfig{DisableConcealment: true})
		require.NoError(t, err)

		// With concealment off, every frame here came from real audio, so a
		// full ten means the redundancy did the recovering.
		require.Len(t, readAll(t, r), 10)
	})
}

func redCodec() webrtc.RTPCodecCapability {
	return webrtc.RTPCodecCapability{MimeType: red.MimeTypeAudio, ClockRate: OpusClockRate, Channels: 1}
}

// encodeRED wraps a stream of Opus packets in RFC 2198 redundancy using the
// same encoder the SFU does, so the test exercises the real wire format.
func encodeRED(t *testing.T, primary []*rtp.Packet) []*rtp.Packet {
	t.Helper()

	enc := red.NewEncoder(red.DefaultDistance)
	out := make([]*rtp.Packet, len(primary))

	for i, p := range primary {
		wrapped := *p
		wrapped.PayloadType = uint8(red.DefaultPayloadType)
		wrapped.Payload = enc.Encode(p, 1200)
		out[i] = &wrapped
	}
	return out
}

func TestTrackReaderConcealsGarbage(t *testing.T) {
	t.Parallel()

	packets := opusStream(t, 5)
	packets[2].Payload = []byte{0xFF, 0xFF, 0xFF, 0xFF}

	src := &fakeRTPSource{packets: packets}
	r, err := NewRTPReader(src, opusCodec(), ReaderConfig{})
	require.NoError(t, err)

	require.Len(t, readAll(t, r), 5,
		"a corrupt packet should be concealed, not fatal")
}

func TestTrackReaderSurfacesGarbageWhenConcealmentIsOff(t *testing.T) {
	t.Parallel()

	packets := opusStream(t, 5)
	packets[2].Payload = []byte{0xFF, 0xFF, 0xFF, 0xFF}

	src := &fakeRTPSource{packets: packets}
	r, err := NewRTPReader(src, opusCodec(), ReaderConfig{DisableConcealment: true})
	require.NoError(t, err)

	var gotErr error
	for _, err := range r.Frames() {
		if err != nil {
			gotErr = err
			break
		}
	}
	require.Error(t, gotErr)
}

func TestTrackReaderEndOfStream(t *testing.T) {
	t.Parallel()

	src := &fakeRTPSource{packets: opusStream(t, 2)}
	r, err := NewRTPReader(src, opusCodec(), ReaderConfig{})
	require.NoError(t, err)

	for range 2 {
		_, err := r.Read()
		require.NoError(t, err)
	}

	_, err = r.Read()
	require.ErrorIs(t, err, io.EOF)
}

func TestTrackReaderPropagatesErrors(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("track closed unexpectedly")
	src := &fakeRTPSource{packets: opusStream(t, 1), endErr: wantErr}
	r, err := NewRTPReader(src, opusCodec(), ReaderConfig{})
	require.NoError(t, err)

	var gotErr error
	for _, err := range r.Frames() {
		if err != nil {
			gotErr = err
		}
	}
	require.ErrorIs(t, gotErr, wantErr)
}

func TestTrackReaderFramesStopsEarly(t *testing.T) {
	t.Parallel()

	src := &fakeRTPSource{packets: opusStream(t, 50)}
	r, err := NewRTPReader(src, opusCodec(), ReaderConfig{})
	require.NoError(t, err)

	count := 0
	for range r.Frames() {
		count++
		if count == 3 {
			break
		}
	}
	require.Equal(t, 3, count)
}

func TestTrackReaderDefaultsClockRate(t *testing.T) {
	t.Parallel()

	src := &fakeRTPSource{packets: opusStream(t, 3)}
	// A codec with no clock rate set should fall back to Opus's 48 kHz.
	r, err := NewRTPReader(src, webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, ReaderConfig{})
	require.NoError(t, err)

	frames := readAll(t, r)
	require.Len(t, frames, 3)
	require.Equal(t, 20*time.Millisecond, frames[1].PTS)
}

func TestNewTrackReaderRejectsBadInput(t *testing.T) {
	t.Parallel()

	_, err := NewTrackReader(nil, ReaderConfig{})
	require.Error(t, err)

	_, err = NewRTPReader(nil, opusCodec(), ReaderConfig{})
	require.Error(t, err)

	_, err = NewRTPReader(&fakeRTPSource{}, opusCodec(), ReaderConfig{
		Opus: opus.Config{SampleRate: 44100},
	})
	require.Error(t, err)
}
