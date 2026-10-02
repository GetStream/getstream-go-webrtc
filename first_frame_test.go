package rtc

import (
	"context"
	"encoding/binary"
	"sync/atomic"
	"testing"
	"time"

	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/jointrace"
	"github.com/GetStream/getstream-go-webrtc/track"
)

// stampedAudio is an audio source that returns at once, as joinbench's, the audio
// writer's and the agent's do. Each 20 ms frame carries its index and when it was
// made, so the receiver can tell how old the first one was and that none repeats.
type stampedAudio struct {
	track.BaseSampleProvider
	next atomic.Uint32
}

func (s *stampedAudio) NextSample(ctx context.Context) (media.Sample, error) {
	if err := ctx.Err(); err != nil {
		return media.Sample{}, err
	}
	frame := make([]byte, 12)
	binary.BigEndian.PutUint32(frame, s.next.Add(1))
	binary.BigEndian.PutUint64(frame[4:], uint64(time.Now().UnixNano()))
	return media.Sample{Data: frame, Duration: 20 * time.Millisecond}, nil
}

func (*stampedAudio) CurrentAudioLevel() uint8 { return 127 }

func stampOf(p *rtp.Packet) (uint32, time.Time) {
	return binary.BigEndian.Uint32(p.Payload), time.Unix(0, int64(binary.BigEndian.Uint64(p.Payload[4:])))
}

// TestFastJoinSendsTheFirstAudioFrameAtConnect: over a simulated 100 ms network, the
// audio written while the publisher connects is not lost to the next frame tick. The
// newest frame goes out the moment DTLS is up, and the SFU sees one timeline from it.
func TestFastJoinSendsTheFirstAudioFrameAtConnect(t *testing.T) {
	t.Parallel()

	const (
		rtt     = 100 * time.Millisecond
		packets = 20
	)
	m := newMediaSFU(t)
	m.pub.candidatesInSDP.Store(true)
	m.sub.candidatesInSDP.Store(true)
	received := make(chan *rtp.Packet, packets)
	m.pub.PC.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		for range packets {
			p, _, err := remote.ReadRTP()
			if err != nil {
				return
			}
			received <- p
		}
	})
	f := newFakeCoordinator(t, 0, false)
	f.serveFastJoin(m.fake)
	call := fastJoinCall(t, f, "first-audio-frame", WithNetworkDelay(rtt))
	traces := make(chan jointrace.Trace, 1)
	call.OnJoinTrace(func(tr jointrace.Trace) { traces <- tr })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go writeSamplesUntilDone(ctx, m.aliceAudio)
	info := &sfu_models.TrackInfo{TrackId: "published-audio", TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO}
	audio, err := track.NewAudioTrack(info, &stampedAudio{},
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2})
	require.NoError(t, err)
	require.NoError(t, joinFast(t, call,
		WithTrack(info, audio),
		WithOnTrack(SubscriberFunc(func(OnTrackReceived) {})),
		WithPublisherPeerConfiguration(loopbackPeerConfig()),
		WithSubscriberPeerConfiguration(loopbackPeerConfig())))
	var trace jointrace.Trace
	select {
	case trace = <-traces:
	case <-time.After(iceTimeout):
		t.Fatalf("no media both ways; recorded so far:\n%s", call.JoinTrace())
	}
	t.Logf("\n%s", trace)

	first, ok := trace.Span(jointrace.PubRTP)
	require.True(t, ok, "no %s span", jointrace.PubRTP)
	require.LessOrEqual(t, first.Duration(), 2*time.Millisecond,
		"the first RTP waits for the next frame tick after %s", jointrace.PubDTLS)

	var got []*rtp.Packet
	for len(got) < packets {
		select {
		case p := <-received:
			got = append(got, p)
		case <-time.After(iceTimeout):
			t.Fatalf("the SFU received %d of %d packets", len(got), packets)
		}
	}
	_, made := stampOf(got[0])
	require.LessOrEqual(t, first.End.Sub(made), 40*time.Millisecond, "the first frame is stale")
	for i := 1; i < len(got); i++ {
		prev, cur := got[i-1], got[i]
		prevIndex, _ := stampOf(prev)
		index, _ := stampOf(cur)
		require.Equal(t, prevIndex+1, index, "packet %d: every frame once, in order", i)
		require.Equal(t, prev.SequenceNumber+1, cur.SequenceNumber, "packet %d", i)
		require.Equal(t, prev.Timestamp+960, cur.Timestamp, "packet %d: 20 ms at 48 kHz", i)
	}
}
