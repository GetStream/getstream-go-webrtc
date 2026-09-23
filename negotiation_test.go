package rtc

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"
	"github.com/pion/ice/v4"
	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
	"github.com/GetStream/getstream-go-webrtc/pc"
)

// iceTimeout is how long a real loopback ICE connection is given. It is
// generous: the assertion is that the negotiation completes at all, and a slow
// CI machine failing it would say nothing about the code.
const iceTimeout = 30 * time.Second

// loopbackSettingEngine keeps ICE inside the machine. Link-local candidates make
// two in-process peers try to reach each other across interfaces, and multicast
// DNS hides the host candidates behind .local names.
func loopbackSettingEngine() webrtc.SettingEngine {
	var se webrtc.SettingEngine
	se.SetIPFilter(func(ip net.IP) bool { return !ip.IsLinkLocalUnicast() })
	se.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
	return se
}

// loopbackPeerConfig configures an SDK peer for a loopback negotiation. The
// media engine is deliberately left nil so the publisher and the subscriber
// build their production ones.
func loopbackPeerConfig() pc.PeerConfig {
	return pc.PeerConfig{SettingEngine: loopbackSettingEngine()}
}

// sfuWebRTCPeer is the SFU's media half of a negotiation: a real peer connection
// that answers the publisher's offers, offers to the subscriber, and trickles its
// candidates over the fake SFU's websocket the way the SFU does.
//
// This is where this suite goes further than the JS client's Publisher and
// Subscriber tests, which stub the SFU client and assert on the arguments: the
// same assertions are made here, and then the negotiation is actually completed,
// so an offer or answer the peer connection would reject cannot pass.
type sfuWebRTCPeer struct {
	fake *testutil.FakeSFU
	PC   *webrtc.PeerConnection

	mu        sync.Mutex
	remoteSet bool
	pending   []webrtc.ICECandidateInit
}

// newSFUWebRTCPeer starts the SFU-side peer. peerType is the client peer it
// negotiates with, which is how the client routes the trickled candidates.
func newSFUWebRTCPeer(t *testing.T, fake *testutil.FakeSFU, peerType sfu_models.PeerType) *sfuWebRTCPeer {
	t.Helper()

	mediaEngine := &webrtc.MediaEngine{}
	require.NoError(t, mediaEngine.RegisterDefaultCodecs())
	registry := &interceptor.Registry{}
	require.NoError(t, webrtc.RegisterDefaultInterceptors(mediaEngine, registry))

	api := webrtc.NewAPI(
		webrtc.WithMediaEngine(mediaEngine),
		webrtc.WithInterceptorRegistry(registry),
		webrtc.WithSettingEngine(loopbackSettingEngine()),
	)
	peerConnection, err := api.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = peerConnection.Close() })

	peer := &sfuWebRTCPeer{fake: fake, PC: peerConnection}
	peerConnection.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}
		encoded, err := json.Marshal(candidate.ToJSON())
		if err != nil {
			return
		}
		// Errors are ignored on purpose: the connection is torn down at the
		// end of the test while gathering may still be running.
		_ = fake.Send(&sfu_events.SfuEvent{
			EventPayload: &sfu_events.SfuEvent_IceTrickle{
				IceTrickle: &sfu_models.ICETrickle{
					PeerType:     peerType,
					IceCandidate: string(encoded),
				},
			},
		}, iceTimeout)
	})
	return peer
}

// AddRemoteCandidate applies a candidate the client trickled, holding it back
// until the offer/answer exchange has given this peer a remote description.
func (s *sfuWebRTCPeer) AddRemoteCandidate(raw string) error {
	var candidate webrtc.ICECandidateInit
	if err := json.Unmarshal([]byte(raw), &candidate); err != nil {
		return err
	}

	s.mu.Lock()
	if !s.remoteSet {
		s.pending = append(s.pending, candidate)
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	return s.PC.AddICECandidate(candidate)
}

func (s *sfuWebRTCPeer) setRemoteDescription(sd webrtc.SessionDescription) error {
	if err := s.PC.SetRemoteDescription(sd); err != nil {
		return err
	}

	s.mu.Lock()
	pending := s.pending
	s.pending = nil
	s.remoteSet = true
	s.mu.Unlock()

	for _, candidate := range pending {
		if err := s.PC.AddICECandidate(candidate); err != nil {
			return err
		}
	}
	return nil
}

// Answer takes the client's offer and produces the answer the SFU sends back.
func (s *sfuWebRTCPeer) Answer(offerSDP string) (string, error) {
	if err := s.setRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  offerSDP,
	}); err != nil {
		return "", err
	}
	answer, err := s.PC.CreateAnswer(nil)
	if err != nil {
		return "", err
	}
	if err := s.PC.SetLocalDescription(answer); err != nil {
		return "", err
	}
	return answer.SDP, nil
}

// AcceptAnswer applies the subscriber's answer to the offer the SFU sent.
func (s *sfuWebRTCPeer) AcceptAnswer(answerSDP string) error {
	return s.setRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  answerSDP,
	})
}

// Offer creates the offer the SFU pushes to the subscriber.
func (s *sfuWebRTCPeer) Offer() (string, error) {
	offer, err := s.PC.CreateOffer(nil)
	if err != nil {
		return "", err
	}
	if err := s.PC.SetLocalDescription(offer); err != nil {
		return "", err
	}
	return offer.SDP, nil
}

// iceUfrag returns the ICE username fragment an SDP offers. A restart is
// required to change it, so it is how a restart is told from a renegotiation.
func iceUfrag(t *testing.T, sdp string) string {
	t.Helper()

	for _, line := range strings.Split(sdp, "\n") {
		if ufrag, ok := strings.CutPrefix(strings.TrimSpace(line), "a=ice-ufrag:"); ok {
			return ufrag
		}
	}
	t.Fatalf("no ice-ufrag in %q", sdp)
	return ""
}

func requireConnected(t *testing.T, name string, state func() webrtc.PeerConnectionState) {
	t.Helper()

	require.Eventually(t, func() bool {
		return state() == webrtc.PeerConnectionStateConnected
	}, iceTimeout, 20*time.Millisecond, "the %s peer connection never connected", name)
}

// TestPublisherNegotiation covers the publish path end to end: adding a track
// makes the publisher offer, the offer reaches the SFU as SetPublisher with the
// announced tracks, the answer is applied, ICE trickles in both directions and
// the connection comes up. Then an SFU-requested ICE restart re-offers with a new
// ufrag on the same peer connection.
func TestPublisherNegotiation(t *testing.T) {
	t.Parallel()

	var peer atomic.Pointer[sfuWebRTCPeer]
	fake := testutil.NewFakeSFU(testutil.WithSignalRPC(testutil.SignalRPC{
		SetPublisher: func(_ context.Context, req *signal_rpc.SetPublisherRequest) (*signal_rpc.SetPublisherResponse, error) {
			answer, err := peer.Load().Answer(req.GetSdp())
			if err != nil {
				return nil, err
			}
			return &signal_rpc.SetPublisherResponse{Sdp: answer}, nil
		},
		IceTrickle: func(_ context.Context, trickle *sfu_models.ICETrickle) (*signal_rpc.ICETrickleResponse, error) {
			if err := peer.Load().AddRemoteCandidate(trickle.GetIceCandidate()); err != nil {
				return nil, err
			}
			return &signal_rpc.ICETrickleResponse{}, nil
		},
	}))
	defer fake.Close()

	peer.Store(newSFUWebRTCPeer(t, fake, sfu_models.PeerType_PEER_TYPE_PUBLISHER_UNSPECIFIED))

	call, _ := joinFakeSFU(t, fake,
		WithPublisherPeerConfiguration(loopbackPeerConfig()),
		WithSubscriberPeerConfiguration(loopbackPeerConfig()))

	audioTrack, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, "audio", "prefix:TRACK_TYPE_AUDIO")
	require.NoError(t, err)
	transceiver, err := call.AddTrack(&sfu_models.TrackInfo{
		TrackId:   "published-audio",
		TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO,
	}, audioTrack)
	require.NoError(t, err)
	require.NotNil(t, transceiver)

	offer, err := testutil.NextRPCRequest[*signal_rpc.SetPublisherRequest](fake, iceTimeout)
	require.NoError(t, err)
	require.Equal(t, call.SessionID.Load(), offer.GetSessionId())
	require.Contains(t, offer.GetSdp(), "m=audio")
	require.Len(t, offer.GetTracks(), 1, "the offer announces the tracks it carries")
	require.Equal(t, "published-audio", offer.GetTracks()[0].GetTrackId())
	require.Equal(t, "0", offer.GetTracks()[0].GetMid(),
		"the SFU matches the announced track to the m-line by mid")

	requireConnected(t, "publisher", call.PublisherPC().ConnectionState)
	requireConnected(t, "SFU", peer.Load().PC.ConnectionState)

	// The SFU asks for an ICE restart over the websocket when it loses the
	// client's candidates. It has to reuse the peer connection: a restart that
	// rebuilt it would drop the tracks that are already flowing.
	require.NoError(t, fake.Send(&sfu_events.SfuEvent{
		EventPayload: &sfu_events.SfuEvent_IceRestart{
			IceRestart: &sfu_events.ICERestart{
				PeerType: sfu_models.PeerType_PEER_TYPE_PUBLISHER_UNSPECIFIED,
			},
		},
	}, iceTimeout))

	restarted, err := testutil.NextRPCRequest[*signal_rpc.SetPublisherRequest](fake, iceTimeout)
	require.NoError(t, err)
	require.NotEqual(t, iceUfrag(t, offer.GetSdp()), iceUfrag(t, restarted.GetSdp()),
		"an ICE restart has to offer a new ufrag")
	require.Len(t, restarted.GetTracks(), 1, "the restarted offer still announces the track")
	requireConnected(t, "publisher after the ICE restart", call.PublisherPC().ConnectionState)
}

// TestSubscriberNegotiation covers the subscribe path end to end: the SFU's offer
// arrives over the websocket, the subscriber answers it over twirp with the
// negotiation ID it was given, the connection comes up, and the track the SFU
// sends is handed to the application resolved to the participant that published
// it.
func TestSubscriberNegotiation(t *testing.T) {
	t.Parallel()

	var peer atomic.Pointer[sfuWebRTCPeer]
	fake := testutil.NewFakeSFU(
		testutil.WithJoinResponse(&sfu_events.JoinResponse{
			CallState: &sfu_models.CallState{
				Participants: []*sfu_models.Participant{
					{UserId: "alice", SessionId: "session-a", TrackLookupPrefix: "prefix-a"},
				},
				ParticipantCount: &sfu_models.ParticipantCount{Total: 2},
			},
		}),
		testutil.WithSignalRPC(testutil.SignalRPC{
			SendAnswer: func(_ context.Context, req *signal_rpc.SendAnswerRequest) (*signal_rpc.SendAnswerResponse, error) {
				if err := peer.Load().AcceptAnswer(req.GetSdp()); err != nil {
					return nil, err
				}
				return &signal_rpc.SendAnswerResponse{}, nil
			},
			IceTrickle: func(_ context.Context, trickle *sfu_models.ICETrickle) (*signal_rpc.ICETrickleResponse, error) {
				if err := peer.Load().AddRemoteCandidate(trickle.GetIceCandidate()); err != nil {
					return nil, err
				}
				return &signal_rpc.ICETrickleResponse{}, nil
			},
		}))
	defer fake.Close()

	peer.Store(newSFUWebRTCPeer(t, fake, sfu_models.PeerType_PEER_TYPE_SUBSCRIBER))

	// The SFU forwards alice's video. The stream ID is how the client resolves
	// an incoming track back to the participant that published it.
	videoTrack, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8}, "video", "prefix-a:TRACK_TYPE_VIDEO")
	require.NoError(t, err)
	_, err = peer.Load().PC.AddTrack(videoTrack)
	require.NoError(t, err)

	received := make(chan OnTrackReceived, 4)
	call, _ := joinFakeSFU(t, fake,
		WithOnTrack(SubscriberFunc(func(track OnTrackReceived) {
			select {
			case received <- track:
			default:
			}
		})),
		WithPublisherPeerConfiguration(loopbackPeerConfig()),
		WithSubscriberPeerConfiguration(loopbackPeerConfig()))

	offerSDP, err := peer.Load().Offer()
	require.NoError(t, err)
	require.NoError(t, fake.Send(&sfu_events.SfuEvent{
		EventPayload: &sfu_events.SfuEvent_SubscriberOffer{
			SubscriberOffer: &sfu_events.SubscriberOffer{Sdp: offerSDP, NegotiationId: 42},
		},
	}, iceTimeout))

	answer, err := testutil.NextRPCRequest[*signal_rpc.SendAnswerRequest](fake, iceTimeout)
	require.NoError(t, err)
	require.Equal(t, sfu_models.PeerType_PEER_TYPE_SUBSCRIBER, answer.GetPeerType())
	require.Equal(t, call.SessionID.Load(), answer.GetSessionId())
	require.Equal(t, uint32(42), answer.GetNegotiationId(),
		"the negotiation ID tells the SFU which offer this answers")
	require.Contains(t, answer.GetSdp(), "m=video")

	requireConnected(t, "subscriber", call.SubscriberPC().ConnectionState)

	// pion surfaces a remote track on its first packet, so the SFU has to send
	// media for OnTrack to fire.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go writeSamplesUntilDone(ctx, videoTrack)

	select {
	case track := <-received:
		require.Equal(t, UserID("alice"), track.ParticipantID.UserID)
		require.Equal(t, SessionID("session-a"), track.ParticipantID.SessionID)
		require.Equal(t, sfu_models.TrackType_TRACK_TYPE_VIDEO, track.TrackType)
		require.NotNil(t, track.Participant)
		require.Equal(t, webrtc.MimeTypeVP8, track.Track.Codec().MimeType)
	case <-time.After(iceTimeout):
		t.Fatal("the subscribed track never reached the application")
	}
}

func writeSamplesUntilDone(ctx context.Context, track *webrtc.TrackLocalStaticSample) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := track.WriteSample(media.Sample{
				Data:     []byte{0x00, 0x01, 0x02, 0x03},
				Duration: 20 * time.Millisecond,
			}); err != nil {
				return
			}
		}
	}
}
