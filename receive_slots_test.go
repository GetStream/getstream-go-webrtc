package rtc

import (
	"context"
	"testing"
	"time"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
)

// slotJoin is a fast join whose subscriber offer has alice's audio and one receive
// slot, as the SFU offers them, and in which bob has just joined.
type slotJoin struct {
	m      *mediaSFU
	call   *Call
	slot   *webrtc.TrackLocalStaticSample
	mid    string
	tracks chan OnTrackReceived
}

func joinWithReceiveSlot(t *testing.T, name string, opts ...Option) slotJoin {
	t.Helper()

	m := newMediaSFU(t)
	// The SFU's slot: an audio sender with the receive-slot msid, which keeps it
	// once the publisher's audio is bound to it.
	slot, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, "receive-slot-0", receiveSlotStreamID)
	require.NoError(t, err)
	slotSender, err := m.sub.PC.AddTrack(slot)
	require.NoError(t, err)

	f := newFakeCoordinator(t, 0, false)
	f.serveFastJoin(m.fake)
	call := fastJoinCall(t, f, name, opts...)
	tracks := make(chan OnTrackReceived, 4)
	require.NoError(t, joinFast(t, call,
		WithOnTrack(SubscriberFunc(func(track OnTrackReceived) { tracks <- track })),
		WithPublisherPeerConfiguration(loopbackPeerConfig()),
		WithSubscriberPeerConfiguration(loopbackPeerConfig())))
	req, err := testutil.NextRPCRequest[*signal_rpc.FastJoinRequest](m.fake, time.Second)
	require.NoError(t, err)
	require.Equal(t, uint32(DefaultAudioReceiveSlots), req.GetAudioReceiveSlots())

	j := slotJoin{m: m, call: call, slot: slot, tracks: tracks}
	for _, transceiver := range m.sub.PC.GetTransceivers() {
		if transceiver.Sender() == slotSender {
			j.mid = transceiver.Mid()
		}
	}
	require.NotEmpty(t, j.mid)

	require.NoError(t, m.fake.Send(&sfu_events.SfuEvent{EventPayload: &sfu_events.SfuEvent_ParticipantJoined{
		ParticipantJoined: &sfu_events.ParticipantJoined{Participant: &sfu_models.Participant{
			UserId: "bob", SessionId: "session-b", TrackLookupPrefix: "prefix-b",
		}},
	}}, iceTimeout))
	return j
}

// bind tells the client bob's audio is on the slot.
func (j slotJoin) bind(t *testing.T) {
	t.Helper()

	require.NoError(t, j.m.fake.Send(&sfu_events.SfuEvent{EventPayload: &sfu_events.SfuEvent_AudioReceiveSlotBound{
		AudioReceiveSlotBound: &sfu_events.AudioReceiveSlotBound{
			Mid: j.mid, UserId: "bob", SessionId: "session-b", TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO,
		},
	}}, iceTimeout))
}

func (j slotJoin) requireBob(t *testing.T) {
	t.Helper()

	select {
	case got := <-j.tracks:
		require.Equal(t, ParticipantID{UserID: "bob", SessionID: "session-b"}, got.ParticipantID)
		require.NotNil(t, got.Participant)
		require.Equal(t, sfu_models.TrackType_TRACK_TYPE_AUDIO, got.TrackType)
		require.Equal(t, receiveSlotStreamID, got.Track.StreamID())
	case <-time.After(iceTimeout):
		t.Fatal("bob's audio never reached OnTrack")
	}
}

// TestFastJoinReceivesOnAnAudioReceiveSlot: audio the SFU sends on a receive slot it
// offered in the FastJoin reaches OnTrack as the participant AudioReceiveSlotBound
// names, whether that event comes before the media or after it.
func TestFastJoinReceivesOnAnAudioReceiveSlot(t *testing.T) {
	t.Parallel()

	for name, eventFirst := range map[string]bool{"event first": true, "media first": false} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			j := joinWithReceiveSlot(t, "receive-slot-"+name)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			if eventFirst {
				j.bind(t)
			}
			go writeSamplesUntilDone(ctx, j.slot)
			if !eventFirst {
				require.Eventually(t, func() bool {
					sub := j.call.subscriberPeer()
					sub.slots.mu.Lock()
					defer sub.slots.mu.Unlock()
					_, parked := sub.slots.parked[j.mid]
					return parked
				}, iceTimeout, 10*time.Millisecond, "the slot's track waits for its binding")
				select {
				case got := <-j.tracks:
					t.Fatalf("a track reached OnTrack before its binding: %+v", got.ParticipantID)
				default:
				}
				j.bind(t)
			}
			j.requireBob(t)
		})
	}
}

// TestAudioReceiveSlotWithNetworkDelay: over a simulated 100 ms network, audio a
// participant starts publishing after the join reaches OnTrack half a round trip
// after the SFU sends it on a slot, where a renegotiation would cost an offer, an
// answer and only then the media: a round trip and a half.
func TestAudioReceiveSlotWithNetworkDelay(t *testing.T) {
	t.Parallel()

	const rtt = 100 * time.Millisecond
	j := joinWithReceiveSlot(t, "receive-slot-delay", WithNetworkDelay(rtt))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go writeSamplesUntilDone(ctx, j.m.aliceAudio)
	select {
	case got := <-j.tracks:
		require.Equal(t, UserID("alice"), got.ParticipantID.UserID, "the subscriber is up")
	case <-time.After(iceTimeout):
		t.Fatalf("alice's audio never reached OnTrack; recorded so far:\n%s", j.call.JoinTrace())
	}

	// As the SFU does on binding: the event and the media leave together.
	sent := time.Now()
	j.bind(t)
	require.NoError(t, j.slot.WriteSample(media.Sample{Data: []byte{0x00}, Duration: 20 * time.Millisecond}))
	go writeSamplesUntilDone(ctx, j.slot)
	j.requireBob(t)
	took := time.Since(sent)
	t.Logf("bob's audio reached OnTrack %s (%.2f RTT) after the SFU bound it", took, float64(took)/float64(rtt))
	require.InDelta(t, float64(rtt/2), float64(took), float64(rtt/10), "want 0.5 RTT, got %s", took)
}

func TestWithAudioReceiveSlots(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU()
	t.Cleanup(sfu.Close)
	f := newFakeCoordinator(t, 0, false)
	f.serveFastJoin(sfu)
	call := fastJoinCall(t, f, "no-receive-slots")
	require.NoError(t, joinFast(t, call, WithAudioReceiveSlots(0)))
	req, err := testutil.NextRPCRequest[*signal_rpc.FastJoinRequest](sfu, time.Second)
	require.NoError(t, err)
	require.Zero(t, req.GetAudioReceiveSlots())
}

func TestLookupParticipantByTrackIgnoresForeignStreamIDs(t *testing.T) {
	t.Parallel()

	store := NewParticipantStore(nil, &sfu_models.CallState{})
	p, trackType := store.LookupParticipantByTrack(receiveSlotStreamID)
	require.Nil(t, p)
	require.Zero(t, trackType)
}
