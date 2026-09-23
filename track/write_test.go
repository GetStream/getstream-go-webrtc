package track

import (
	"errors"
	"testing"
	"time"

	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/sdp/v3"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/red"
)

const (
	opusPayloadType = webrtc.PayloadType(111)
	redPayloadType  = webrtc.PayloadType(63)
	vp8PayloadType  = webrtc.PayloadType(96)

	// 20 ms at 48 kHz.
	opusSamplesPerFrame = uint32(960)
)

func opusCodecParams() []webrtc.RTPCodecParameters {
	return []webrtc.RTPCodecParameters{{
		RTPCodecCapability: opusCapability(),
		PayloadType:        opusPayloadType,
	}}
}

func redCodecParams() []webrtc.RTPCodecParameters {
	return []webrtc.RTPCodecParameters{{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:    red.MimeTypeAudio,
			ClockRate:   48000,
			Channels:    2,
			SDPFmtpLine: "111/111",
		},
		PayloadType: redPayloadType,
	}}
}

func vp8CodecParams() []webrtc.RTPCodecParameters {
	return []webrtc.RTPCodecParameters{{
		RTPCodecCapability: vp8Capability(),
		PayloadType:        vp8PayloadType,
	}}
}

func audioSample(payload []byte) media.Sample {
	return media.Sample{Data: payload, Duration: 20 * time.Millisecond}
}

func TestBindRecordsTheNegotiatedParameters(t *testing.T) {
	t.Parallel()

	local, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)

	ctx := newTrackContext(t, opusCodecParams(),
		webrtc.RTPHeaderExtensionParameter{URI: sdp.AudioLevelURI, ID: 1},
		webrtc.RTPHeaderExtensionParameter{URI: sdp.SDESMidURI, ID: 4},
		webrtc.RTPHeaderExtensionParameter{URI: sdp.SDESRTPStreamIDURI, ID: 5},
		webrtc.RTPHeaderExtensionParameter{URI: "urn:ietf:params:rtp-hdrext:toffset", ID: 9},
	)

	codec, err := local.Bind(ctx)
	require.NoError(t, err)
	require.Equal(t, opusPayloadType, codec.PayloadType)
	require.True(t, local.IsBound())
	require.Equal(t, ctx.ssrc, local.SSRC())

	b := local.binding.Load()
	require.Equal(t, uint8(1), b.audioLevelID)
	require.Equal(t, uint8(4), b.midID)
	require.Equal(t, uint8(5), b.ridID)
	require.Equal(t, uint32(48000), b.clockRate)
	require.NotNil(t, b.packetizer)
	require.False(t, b.acked.Load(), "nothing has acked the ssrc yet")
}

func TestBindRejectsACodecThePeerDidNotNegotiate(t *testing.T) {
	t.Parallel()

	local, err := NewLocalTrack(videoTrackInfo(), vp8Capability())
	require.NoError(t, err)

	_, err = local.Bind(newTrackContext(t, opusCodecParams()))
	require.ErrorIs(t, err, webrtc.ErrUnsupportedCodec)
	require.False(t, local.IsBound())
}

// Opus is negotiated as two channels even when the payload is mono, and pion
// matches on channel count in both passes of its codec search. A track that
// declares one channel therefore never binds, and the failure is a codec error
// rather than anything that points at the channel count.
func TestAudioTrackChannelCountDecidesWhetherItBinds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		channels uint16
		wantErr  error
	}{
		{name: "two channels, as negotiated", channels: 2},
		{name: "unset falls back to the codec default", channels: 0},
		{name: "one channel never binds", channels: 1, wantErr: webrtc.ErrUnsupportedCodec},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			capability := opusCapability()
			capability.Channels = tt.channels

			local, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, capability)
			require.NoError(t, err)

			_, err = local.Bind(newTrackContext(t, opusCodecParams()))
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestWriteSampleBeforeBindIsDropped(t *testing.T) {
	t.Parallel()

	local, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)

	require.NoError(t, local.WriteSample(audioSample([]byte("frame")), nil))
}

func TestWriteSamplePacketizesAndAdvancesTheClock(t *testing.T) {
	t.Parallel()

	local, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)

	ctx := newTrackContext(t, opusCodecParams())
	_, err = local.Bind(ctx)
	require.NoError(t, err)

	for range 3 {
		require.NoError(t, local.WriteSample(audioSample([]byte("opus-frame")), nil))
	}

	written := ctx.writer.written()
	require.Len(t, written, 3)
	for _, pkt := range written {
		require.Equal(t, uint8(opusPayloadType), pkt.header.PayloadType)
		require.Equal(t, uint32(ctx.ssrc), pkt.header.SSRC)
		require.Equal(t, []byte("opus-frame"), pkt.payload)
		require.False(t, pkt.header.Extension, "no extension was negotiated")
	}

	// One 20 ms sample is 960 RTP ticks at 48 kHz, and each frame is one packet.
	for i := 1; i < len(written); i++ {
		require.Equal(t, opusSamplesPerFrame, written[i].header.Timestamp-written[i-1].header.Timestamp)
		require.Equal(t, uint16(1), written[i].header.SequenceNumber-written[i-1].header.SequenceNumber)
	}
}

// A sample that arrives after a gap keeps its own RTP timestamp: the gap is
// absorbed by skipping timestamps, not sequence numbers, because nothing was
// actually sent during the gap.
func TestWriteSampleAbsorbsAGapInTheTimestampsOnly(t *testing.T) {
	t.Parallel()

	local, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)

	ctx := newTrackContext(t, opusCodecParams())
	_, err = local.Bind(ctx)
	require.NoError(t, err)

	require.NoError(t, local.WriteSample(media.Sample{
		Data:            []byte("a"),
		Duration:        20 * time.Millisecond,
		PacketTimestamp: 100_000,
	}, nil))
	// Five seconds later, in RTP ticks.
	require.NoError(t, local.WriteSample(media.Sample{
		Data:            []byte("b"),
		Duration:        20 * time.Millisecond,
		PacketTimestamp: 100_000 + 5*48_000,
	}, nil))

	// The RTP timestamps are relative to the packetizer's own random base, so
	// the gap shows up as the delta between the two packets.
	written := ctx.writer.written()
	require.Len(t, written, 2)
	require.Equal(t, uint32(5*48_000), written[1].header.Timestamp-written[0].header.Timestamp)
	require.Equal(t, uint16(1), written[1].header.SequenceNumber-written[0].header.SequenceNumber)
}

// Samples that arrive faster than their duration still advance the clock by the
// duration, so the receiver's jitter buffer is not asked to play them early.
func TestWriteSampleLowerBoundsTheTimestampByTheSampleDuration(t *testing.T) {
	t.Parallel()

	local, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)

	ctx := newTrackContext(t, opusCodecParams())
	_, err = local.Bind(ctx)
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, local.WriteSample(media.Sample{
		Data:      []byte("a"),
		Duration:  20 * time.Millisecond,
		Timestamp: now,
	}, nil))
	require.NoError(t, local.WriteSample(media.Sample{
		Data:      []byte("b"),
		Duration:  20 * time.Millisecond,
		Timestamp: now.Add(time.Millisecond),
	}, nil))

	written := ctx.writer.written()
	require.Len(t, written, 2)
	require.Equal(t, opusSamplesPerFrame, written[1].header.Timestamp-written[0].header.Timestamp)
}

func TestWriteSampleRejectsANegativeDuration(t *testing.T) {
	t.Parallel()

	local, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)

	ctx := newTrackContext(t, opusCodecParams())
	_, err = local.Bind(ctx)
	require.NoError(t, err)

	err = local.WriteSample(media.Sample{Data: []byte("frame"), Duration: -time.Millisecond}, nil)
	require.ErrorIs(t, err, errInvalidDurationSample)
	require.Zero(t, ctx.writer.count())
}

func TestWriteSampleRejectsSamplesThatMoveBackwards(t *testing.T) {
	t.Parallel()

	now := time.Now()

	tests := []struct {
		name   string
		first  media.Sample
		second media.Sample
	}{
		{
			name:   "rtp timestamp jumps back more than half the timestamp space",
			first:  media.Sample{Data: []byte("a"), Duration: 20 * time.Millisecond, PacketTimestamp: 1 << 31},
			second: media.Sample{Data: []byte("b"), Duration: 20 * time.Millisecond, PacketTimestamp: 1000},
		},
		{
			name:   "wall clock goes backwards",
			first:  media.Sample{Data: []byte("a"), Duration: 20 * time.Millisecond, Timestamp: now},
			second: media.Sample{Data: []byte("b"), Duration: 20 * time.Millisecond, Timestamp: now.Add(-time.Second)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			local, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
			require.NoError(t, err)

			ctx := newTrackContext(t, opusCodecParams())
			_, err = local.Bind(ctx)
			require.NoError(t, err)

			require.NoError(t, local.WriteSample(tt.first, nil))
			require.ErrorIs(t, local.WriteSample(tt.second, nil), errOutOfOrderSample)
			require.Equal(t, 1, ctx.writer.count(), "only the first sample goes out")
		})
	}
}

// A publisher that reports dropped packets has to leave a matching hole in the
// sequence numbers, otherwise the receiver never learns that anything was lost.
func TestWriteSampleSkipsSequenceNumbersForDroppedPackets(t *testing.T) {
	t.Parallel()

	local, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)

	ctx := newTrackContext(t, opusCodecParams())
	_, err = local.Bind(ctx)
	require.NoError(t, err)

	require.NoError(t, local.WriteSample(audioSample([]byte("a")), nil))
	require.NoError(t, local.WriteSample(media.Sample{
		Data:               []byte("b"),
		Duration:           20 * time.Millisecond,
		PrevDroppedPackets: 2,
	}, nil))

	written := ctx.writer.written()
	require.Len(t, written, 2)
	require.Equal(t, uint16(3), written[1].header.SequenceNumber-written[0].header.SequenceNumber)
	require.Equal(t, 3*opusSamplesPerFrame, written[1].header.Timestamp-written[0].header.Timestamp)
}

// The gap before a sample is measured from where the previous one ended, so
// samples of different lengths keep the timestamps the publisher gave them.
func TestWriteSampleKeepsPacketTimestampsAcrossSampleLengths(t *testing.T) {
	t.Parallel()

	local, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)

	ctx := newTrackContext(t, opusCodecParams())
	_, err = local.Bind(ctx)
	require.NoError(t, err)

	for _, s := range []media.Sample{
		{Data: []byte("a"), Duration: 40 * time.Millisecond, PacketTimestamp: 10_000},
		{Data: []byte("b"), Duration: 20 * time.Millisecond, PacketTimestamp: 10_000 + 2*opusSamplesPerFrame},
		{Data: []byte("c"), Duration: 20 * time.Millisecond, PacketTimestamp: 10_000 + 3*opusSamplesPerFrame},
	} {
		require.NoError(t, local.WriteSample(s, nil))
	}

	written := ctx.writer.written()
	require.Len(t, written, 3)
	require.Equal(t, 2*opusSamplesPerFrame, written[1].header.Timestamp-written[0].header.Timestamp)
	require.Equal(t, opusSamplesPerFrame, written[2].header.Timestamp-written[1].header.Timestamp)
}

func TestWriteSampleAttachesTheAudioLevel(t *testing.T) {
	t.Parallel()

	local, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)

	ctx := newTrackContext(t, opusCodecParams(),
		webrtc.RTPHeaderExtensionParameter{URI: sdp.AudioLevelURI, ID: 3})
	_, err = local.Bind(ctx)
	require.NoError(t, err)

	level := uint8(42)
	require.NoError(t, local.WriteSample(audioSample([]byte("loud")), &SampleWriteOptions{AudioLevel: &level}))
	require.NoError(t, local.WriteSample(audioSample([]byte("quiet")), nil))

	written := ctx.writer.written()
	require.Len(t, written, 2)

	require.True(t, written[0].header.Extension)
	var ext rtp.AudioLevelExtension
	require.NoError(t, ext.Unmarshal(written[0].header.GetExtension(3)))
	require.Equal(t, level, ext.Level)

	require.False(t, written[1].header.Extension, "no level, no extension")
}

// Without the extension in the answer there is nowhere to put the level, and
// writing one anyway would produce a packet the SFU cannot parse.
func TestAudioLevelIsDroppedWhenTheExtensionWasNotNegotiated(t *testing.T) {
	t.Parallel()

	local, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)

	ctx := newTrackContext(t, opusCodecParams())
	_, err = local.Bind(ctx)
	require.NoError(t, err)

	level := uint8(42)
	require.NoError(t, local.WriteSample(audioSample([]byte("loud")), &SampleWriteOptions{AudioLevel: &level}))

	written := ctx.writer.written()
	require.Len(t, written, 1)
	require.False(t, written[0].header.Extension)
}

// RED wraps each opus frame together with the previous ones, so a receiver that
// loses a packet recovers it from the next one.
func TestRedTrackWrapsEachFrameWithTheOnesBeforeIt(t *testing.T) {
	t.Parallel()

	info := &sfu_models.TrackInfo{TrackId: "audio", TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO}
	local, err := NewLocalTrack(info, opusCapability(), WithRedTranscodingEnabledForAudio())
	require.NoError(t, err)

	ctx := newTrackContext(t, redCodecParams())
	codec, err := local.Bind(ctx)
	require.NoError(t, err)
	require.Equal(t, redPayloadType, codec.PayloadType)

	payloads := [][]byte{[]byte("frame-one"), []byte("frame-two"), []byte("frame-three")}
	for _, payload := range payloads {
		require.NoError(t, local.WriteSample(audioSample(payload), nil))
	}

	written := ctx.writer.written()
	require.Len(t, written, 3)

	for i, pkt := range written {
		require.Equal(t, uint8(redPayloadType), pkt.header.PayloadType, "packets go out as audio/red")

		primary, err := red.Primary(pkt.payload)
		require.NoError(t, err)
		require.Equal(t, payloads[i], primary, "the primary encoding is the frame that was written")
	}

	// Feed a decoder the first and third packets: the frame from the packet that
	// went missing comes back out of the redundancy.
	decoder := red.NewDecoder()

	recovered, err := redDecodedPayloads(decoder, written[0])
	require.NoError(t, err)
	require.Equal(t, [][]byte{payloads[0]}, recovered)

	recovered, err = redDecodedPayloads(decoder, written[2])
	require.NoError(t, err)
	require.Equal(t, [][]byte{payloads[1], payloads[2]}, recovered,
		"the dropped frame is recovered from the redundancy, followed by the primary")
}

// redDecodedPayloads returns the payloads a receiver recovers from a RED packet.
func redDecodedPayloads(decoder *red.Decoder, pkt recordedPacket) ([][]byte, error) {
	decoded, err := decoder.Decode(&rtp.Packet{Header: pkt.header, Payload: pkt.payload})
	if err != nil {
		return nil, err
	}

	payloads := make([][]byte, 0, len(decoded))
	for _, p := range decoded {
		payloads = append(payloads, p.Payload)
	}
	return payloads, nil
}

// Until the SFU acknowledges the ssrc, a simulcast layer stamps its mid and rid
// into every packet: that is how the SFU learns which rid an ssrc belongs to.
func TestSimulcastLayerStampsMidAndRidUntilTheSsrcIsAcked(t *testing.T) {
	t.Parallel()

	local, err := NewLocalTrack(videoTrackInfo(), vp8Capability(),
		WithSimulcast(&sfu_models.VideoLayer{Quality: sfu_models.VideoQuality_VIDEO_QUALITY_HIGH}))
	require.NoError(t, err)
	require.Equal(t, "f", local.RID())

	ctx := newTrackContext(t, vp8CodecParams(),
		webrtc.RTPHeaderExtensionParameter{URI: sdp.SDESMidURI, ID: 4},
		webrtc.RTPHeaderExtensionParameter{URI: sdp.SDESRTPStreamIDURI, ID: 5},
	)
	_, err = local.Bind(ctx)
	require.NoError(t, err)

	local.SetTransceiver(negotiatedTransceiver(t))
	require.NotEmpty(t, local.Transceiver().Mid())

	require.NoError(t, local.WriteSample(media.Sample{Data: videoFrame(), Duration: 33 * time.Millisecond}, nil))

	written := ctx.writer.written()
	require.NotEmpty(t, written)
	require.Equal(t, []byte(local.Transceiver().Mid()), written[0].header.GetExtension(4))
	require.Equal(t, []byte("f"), written[0].header.GetExtension(5))

	// A receiver report naming the ssrc is the ack, and the header extensions
	// stop once the mapping is known.
	ctx.rtcp.send(t, &rtcp.ReceiverReport{
		Reports: []rtcp.ReceptionReport{{SSRC: uint32(ctx.ssrc)}},
	})
	require.Eventually(t, func() bool {
		return local.binding.Load().acked.Load()
	}, 2*time.Second, 5*time.Millisecond)

	before := ctx.writer.count()
	require.NoError(t, local.WriteSample(media.Sample{Data: videoFrame(), Duration: 33 * time.Millisecond}, nil))

	written = ctx.writer.written()
	require.Greater(t, len(written), before)
	require.Empty(t, written[before].header.GetExtension(4))
	require.Empty(t, written[before].header.GetExtension(5))
}

// A track with no rid is not simulcast, so it never stamps mid or rid even
// while the ssrc is unacked.
func TestNonSimulcastTrackNeverStampsMidOrRid(t *testing.T) {
	t.Parallel()

	local, err := NewLocalTrack(videoTrackInfo(), vp8Capability())
	require.NoError(t, err)

	ctx := newTrackContext(t, vp8CodecParams(),
		webrtc.RTPHeaderExtensionParameter{URI: sdp.SDESMidURI, ID: 4},
		webrtc.RTPHeaderExtensionParameter{URI: sdp.SDESRTPStreamIDURI, ID: 5},
	)
	_, err = local.Bind(ctx)
	require.NoError(t, err)
	local.SetTransceiver(negotiatedTransceiver(t))

	require.NoError(t, local.WriteSample(media.Sample{Data: videoFrame(), Duration: 33 * time.Millisecond}, nil))

	written := ctx.writer.written()
	require.NotEmpty(t, written)
	require.False(t, written[0].header.Extension)
}

// negotiatedTransceiver returns a transceiver with a mid assigned, which only
// happens once a local description has been applied.
func negotiatedTransceiver(t *testing.T) *webrtc.RTPTransceiver {
	t.Helper()

	peer, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Close() })

	transceiver, err := peer.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly})
	require.NoError(t, err)

	offer, err := peer.CreateOffer(nil)
	require.NoError(t, err)
	require.NoError(t, peer.SetLocalDescription(offer))

	return transceiver
}

// videoFrame is a VP8 keyframe payload header, enough for the payloader to
// produce a packet.
func videoFrame() []byte {
	return []byte{0x10, 0x00, 0x00, 0x9d, 0x01, 0x2a, 0x00, 0x10, 0x00, 0x10}
}

// WriteRTP is the path for callers that packetize themselves; it still owns the
// header extensions, because those describe the transport rather than the media.
func TestWriteRTPStampsExtensionsOnCallerSuppliedPackets(t *testing.T) {
	t.Parallel()

	local, err := NewLocalTrack(videoTrackInfo(), vp8Capability(),
		WithSimulcast(&sfu_models.VideoLayer{Quality: sfu_models.VideoQuality_VIDEO_QUALITY_MID}))
	require.NoError(t, err)

	ctx := newTrackContext(t, vp8CodecParams(),
		webrtc.RTPHeaderExtensionParameter{URI: sdp.AudioLevelURI, ID: 3},
		webrtc.RTPHeaderExtensionParameter{URI: sdp.SDESMidURI, ID: 4},
		webrtc.RTPHeaderExtensionParameter{URI: sdp.SDESRTPStreamIDURI, ID: 5},
	)
	_, err = local.Bind(ctx)
	require.NoError(t, err)
	local.SetTransceiver(negotiatedTransceiver(t))

	level := uint8(7)
	packet := &rtp.Packet{
		Header:  rtp.Header{Version: 2, SequenceNumber: 5, Timestamp: 90000, PayloadType: uint8(vp8PayloadType)},
		Payload: videoFrame(),
	}
	require.NoError(t, local.WriteRTP(packet, &SampleWriteOptions{AudioLevel: &level}))

	written := ctx.writer.written()
	require.Len(t, written, 1)
	require.Equal(t, uint16(5), written[0].header.SequenceNumber, "the caller owns the sequence number")

	var ext rtp.AudioLevelExtension
	require.NoError(t, ext.Unmarshal(written[0].header.GetExtension(3)))
	require.Equal(t, level, ext.Level)
	require.Equal(t, []byte(local.Transceiver().Mid()), written[0].header.GetExtension(4))
	require.Equal(t, []byte("h"), written[0].header.GetExtension(5))
}

// A custom RTCP handler replaces the default one, so nothing forces a key frame
// behind the caller's back.
func TestWithRTCPHandlerReplacesTheDefaultHandler(t *testing.T) {
	t.Parallel()

	provider := &keyFrameProvider{countingProvider: newCountingProvider(videoFrame())}

	seen := make(chan rtcp.Packet, 4)
	local, err := NewLocalTrack(videoTrackInfo(), vp8Capability(),
		WithRTCPHandler(func(p SampleProvider, packet rtcp.Packet) error {
			require.Same(t, provider, p)
			seen <- packet
			return errors.New("handler errors are logged, not fatal")
		}))
	require.NoError(t, err)
	require.NoError(t, local.StartWrite(provider, nil))

	ctx := newTrackContext(t, vp8CodecParams())
	_, err = local.Bind(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = local.Close() })

	ctx.rtcp.send(t, &rtcp.PictureLossIndication{MediaSSRC: uint32(ctx.ssrc)})

	select {
	case packet := <-seen:
		require.IsType(t, &rtcp.PictureLossIndication{}, packet)
	case <-time.After(2 * time.Second):
		t.Fatal("the custom handler never saw the packet")
	}
	require.Zero(t, provider.keyFrames.Load(), "the default key frame handler must not also run")

	// A second packet proves the worker survived the handler's error.
	ctx.rtcp.send(t, &rtcp.PictureLossIndication{MediaSSRC: uint32(ctx.ssrc)})
	select {
	case <-seen:
	case <-time.After(2 * time.Second):
		t.Fatal("the rtcp worker stopped after a handler error")
	}
}

func TestKindFollowsTheCodec(t *testing.T) {
	t.Parallel()

	audio, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)
	require.Equal(t, webrtc.RTPCodecTypeAudio, audio.Kind())
	require.NotNil(t, audio.Track(), "the underlying pion track is what gets added to the peer connection")

	video, err := NewLocalTrack(videoTrackInfo(), vp8Capability())
	require.NoError(t, err)
	require.Equal(t, webrtc.RTPCodecTypeVideo, video.Kind())
}

func TestRtcpForcesAKeyFrameOnPliAndFir(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		packet        rtcp.Packet
		wantKeyFrames int64
	}{
		{
			name:          "picture loss indication",
			packet:        &rtcp.PictureLossIndication{MediaSSRC: 1},
			wantKeyFrames: 1,
		},
		{
			name: "full intra request",
			packet: &rtcp.FullIntraRequest{
				MediaSSRC: 1,
				FIR:       []rtcp.FIREntry{{SSRC: 1, SequenceNumber: 3}},
			},
			wantKeyFrames: 1,
		},
		{
			// A nack is answered by the retransmission path, not by a new key
			// frame; asking for one would throw away the whole GOP.
			name:          "transport layer nack",
			packet:        &rtcp.TransportLayerNack{MediaSSRC: 1, Nacks: []rtcp.NackPair{{PacketID: 5}}},
			wantKeyFrames: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			provider := &keyFrameProvider{countingProvider: newCountingProvider(videoFrame())}
			local, err := NewLocalTrack(videoTrackInfo(), vp8Capability())
			require.NoError(t, err)
			require.NoError(t, local.StartWrite(provider, nil))

			ctx := newTrackContext(t, vp8CodecParams())
			_, err = local.Bind(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = local.Close() })

			ctx.rtcp.send(t, tt.packet)

			if tt.wantKeyFrames == 0 {
				// Give the worker a chance to get it wrong.
				time.Sleep(50 * time.Millisecond)
				require.Zero(t, provider.keyFrames.Load())
				return
			}
			require.Eventually(t, func() bool {
				return provider.keyFrames.Load() == tt.wantKeyFrames
			}, 2*time.Second, 5*time.Millisecond)
		})
	}
}

func TestWriteWorkerRunsUntilTheProviderIsDone(t *testing.T) {
	t.Parallel()

	provider := newCountingProvider([]byte("opus-frame"))
	provider.eofAfter = 3

	local, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)

	completed := make(chan struct{})
	require.NoError(t, local.StartWrite(provider, func() { close(completed) }))

	ctx := newTrackContext(t, opusCodecParams())
	_, err = local.Bind(ctx)
	require.NoError(t, err)

	select {
	case <-completed:
	case <-time.After(5 * time.Second):
		t.Fatal("the write loop never reported completion")
	}

	require.Equal(t, 3, ctx.writer.count(), "one packet per sample, then EOF")
	polled, binds, _, _ := provider.counts()
	require.Equal(t, 4, polled, "the loop polls once more to see the EOF")
	require.Equal(t, 1, binds, "Bind glues the provider to the track exactly once")
}

func TestWriteWorkerStopsOnAProviderError(t *testing.T) {
	t.Parallel()

	provider := newCountingProvider([]byte("opus-frame"))
	provider.nextErr = errors.New("capture device went away")

	local, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)

	completed := make(chan struct{})
	require.NoError(t, local.StartWrite(provider, func() { close(completed) }))

	ctx := newTrackContext(t, opusCodecParams())
	_, err = local.Bind(ctx)
	require.NoError(t, err)

	select {
	case <-completed:
	case <-time.After(5 * time.Second):
		t.Fatal("the write loop never reported completion")
	}
	require.Zero(t, ctx.writer.count())
}

// Muting keeps the provider running and drops the samples, rather than stopping
// the loop: the track has to resume in place when it is unmuted.
func TestMutedTrackDrainsTheProviderWithoutWriting(t *testing.T) {
	t.Parallel()

	provider := newCountingProvider([]byte("opus-frame"))

	local, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)
	local.SetMuted(true)
	require.NoError(t, local.StartWrite(provider, nil))

	ctx := newTrackContext(t, opusCodecParams())
	_, err = local.Bind(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = local.Close() })

	require.Eventually(t, func() bool {
		polled, _, _, _ := provider.counts()
		return polled >= 2
	}, 2*time.Second, 10*time.Millisecond)
	require.Zero(t, ctx.writer.count(), "a muted track puts nothing on the wire")

	local.SetMuted(false)
	require.Eventually(t, func() bool {
		return ctx.writer.count() > 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestUnbindStopsWritingAndReleasesTheProvider(t *testing.T) {
	t.Parallel()

	provider := newCountingProvider([]byte("opus-frame"))

	local, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)
	require.NoError(t, local.StartWrite(provider, nil))

	unbound := make(chan struct{})
	local.OnUnbind(func() { close(unbound) })

	ctx := newTrackContext(t, opusCodecParams())
	_, err = local.Bind(ctx)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return ctx.writer.count() > 0 }, 2*time.Second, 10*time.Millisecond)
	require.NoError(t, local.Unbind(ctx))
	require.False(t, local.IsBound())

	select {
	case <-unbound:
	case <-time.After(2 * time.Second):
		t.Fatal("the unbind callback never fired")
	}

	_, _, unbinds, _ := provider.counts()
	require.Equal(t, 1, unbinds)

	// The write loop is cancelled, so the packet count settles.
	require.Eventually(t, func() bool {
		settled := ctx.writer.count()
		time.Sleep(100 * time.Millisecond)
		return ctx.writer.count() == settled
	}, 3*time.Second, 10*time.Millisecond)
}

func TestUnbindOfAnUnknownBindingFails(t *testing.T) {
	t.Parallel()

	local, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)

	require.Error(t, local.Unbind(newTrackContext(t, opusCodecParams())))
}

func TestStartWriteWithTheSameProviderIsANoop(t *testing.T) {
	t.Parallel()

	provider := newCountingProvider([]byte("opus-frame"))

	local, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)
	require.NoError(t, local.StartWrite(provider, nil))

	ctx := newTrackContext(t, opusCodecParams())
	_, err = local.Bind(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = local.Close() })

	require.Eventually(t, func() bool { return ctx.writer.count() > 0 }, 2*time.Second, 10*time.Millisecond)

	require.NoError(t, local.StartWrite(provider, nil))
	_, binds, unbinds, _ := provider.counts()
	require.Equal(t, 1, binds, "re-registering the same provider must not rebind it")
	require.Zero(t, unbinds)
}

func TestStartWriteSwapsTheProviderWhileBound(t *testing.T) {
	t.Parallel()

	first := newCountingProvider([]byte("first"))
	second := newCountingProvider([]byte("second"))

	local, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)
	require.NoError(t, local.StartWrite(first, nil))

	ctx := newTrackContext(t, opusCodecParams())
	_, err = local.Bind(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = local.Close() })

	require.Eventually(t, func() bool { return ctx.writer.count() > 0 }, 2*time.Second, 10*time.Millisecond)
	require.NoError(t, local.StartWrite(second, nil))

	_, _, firstUnbinds, _ := first.counts()
	require.Equal(t, 1, firstUnbinds, "the outgoing provider is released")
	_, secondBinds, _, _ := second.counts()
	require.Equal(t, 1, secondBinds)

	require.Eventually(t, func() bool {
		for _, pkt := range ctx.writer.written() {
			if string(pkt.payload) == "second" {
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond)
}

func TestStartWriteReportsAProviderThatRefusesToBind(t *testing.T) {
	t.Parallel()

	local, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)

	ctx := newTrackContext(t, opusCodecParams())
	_, err = local.Bind(ctx)
	require.NoError(t, err)

	refusing := newCountingProvider([]byte("frame"))
	refusing.bindErr = errors.New("no capture device")
	require.ErrorContains(t, local.StartWrite(refusing, nil), "no capture device")
}

func TestCloseStopsTheLoopAndClosesTheProvider(t *testing.T) {
	t.Parallel()

	provider := newCountingProvider([]byte("opus-frame"))

	local, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)
	require.NoError(t, local.StartWrite(provider, nil))

	ctx := newTrackContext(t, opusCodecParams())
	_, err = local.Bind(ctx)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return ctx.writer.count() > 0 }, 2*time.Second, 10*time.Millisecond)
	require.NoError(t, local.Close())

	_, _, _, closes := provider.counts()
	require.Equal(t, 1, closes)
}

// The write loop only attaches an audio level when the provider can report one.
func TestWriteLoopAttachesTheProviderAudioLevel(t *testing.T) {
	t.Parallel()

	audio := newCountingProvider([]byte("opus-frame"))
	audio.level.Store(17)

	withLevel, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)
	require.NoError(t, withLevel.StartWrite(audio, nil))

	levelCtx := newTrackContext(t, opusCodecParams(),
		webrtc.RTPHeaderExtensionParameter{URI: sdp.AudioLevelURI, ID: 3})
	_, err = withLevel.Bind(levelCtx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = withLevel.Close() })

	require.Eventually(t, func() bool { return levelCtx.writer.count() > 0 }, 2*time.Second, 10*time.Millisecond)

	var ext rtp.AudioLevelExtension
	require.NoError(t, ext.Unmarshal(levelCtx.writer.written()[0].header.GetExtension(3)))
	require.Equal(t, uint8(17), ext.Level)

	// The same track fed by a provider that reports no level writes no
	// extension, even though the extension was negotiated.
	plain, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)
	require.NoError(t, plain.StartWrite(plainProvider{inner: newCountingProvider([]byte("opus-frame"))}, nil))

	plainCtx := newTrackContext(t, opusCodecParams(),
		webrtc.RTPHeaderExtensionParameter{URI: sdp.AudioLevelURI, ID: 3})
	_, err = plain.Bind(plainCtx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = plain.Close() })

	require.Eventually(t, func() bool { return plainCtx.writer.count() > 0 }, 2*time.Second, 10*time.Millisecond)
	require.False(t, plainCtx.writer.written()[0].header.Extension)
}

// A write failure on one sample must not stop the track: the SFU can drop a
// packet and the next one still has to go out.
func TestWriteSampleReportsWriteFailuresAndKeepsGoing(t *testing.T) {
	t.Parallel()

	local, err := NewLocalTrack(&sfu_models.TrackInfo{TrackId: "audio"}, opusCapability())
	require.NoError(t, err)

	ctx := newTrackContext(t, opusCodecParams())
	_, err = local.Bind(ctx)
	require.NoError(t, err)

	ctx.writer.mu.Lock()
	ctx.writer.err = errors.New("transport closed")
	ctx.writer.mu.Unlock()

	require.ErrorContains(t, local.WriteSample(audioSample([]byte("frame")), nil), "transport closed")

	ctx.writer.mu.Lock()
	ctx.writer.err = nil
	ctx.writer.mu.Unlock()

	require.NoError(t, local.WriteSample(audioSample([]byte("frame")), nil))
	require.Equal(t, 1, ctx.writer.count())
}
