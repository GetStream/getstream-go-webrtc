package rtc

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"
	"github.com/pion/turn/v5"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
	"github.com/GetStream/getstream-go-webrtc/jointrace"
)

// testTURN is a TURN server with one credential. Like the SFU's own TURN, it accepts the
// credential only once the SFU has authenticated the token it comes with.
type testTURN struct {
	server models.ICEServerResponse
	// accepted counts the requests it authenticated, refused those that came too early.
	accepted, refused atomic.Int32
}

func newTestTURN(t *testing.T, ip net.IP, authenticated func() bool) *testTURN {
	t.Helper()

	const realm, username, password = "stream-test", "fake-app/default:call/ws-user/turn-session", "fake-turn-password"
	conn, err := net.ListenPacket("udp4", net.JoinHostPort(ip.String(), "0"))
	require.NoError(t, err)
	s := &testTURN{server: models.ICEServerResponse{
		Urls: []string{"turn:" + conn.LocalAddr().String() + "?transport=udp"}, Username: username, Password: password,
	}}
	key := turn.GenerateAuthKey(username, realm, password)
	server, err := turn.NewServer(turn.ServerConfig{
		Realm: realm,
		AuthHandler: func(ra *turn.RequestAttributes) (string, []byte, bool) {
			if ra.Username != username {
				return "", nil, false
			}
			if !authenticated() {
				s.refused.Add(1)
				return "", nil, false
			}
			s.accepted.Add(1)
			return username, key, true
		},
		PacketConnConfigs: []turn.PacketConnConfig{{
			PacketConn:            conn,
			RelayAddressGenerator: &turn.RelayAddressGeneratorStatic{RelayAddress: ip, Address: ip.String()},
		}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	return s
}

// relayIP is an address of this machine the fake SFU's peers gather candidates on: they
// skip the loopback interface.
func relayIP(t *testing.T) net.IP {
	t.Helper()

	addrs, err := net.InterfaceAddrs()
	require.NoError(t, err)
	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok {
			if ip := ipNet.IP.To4(); ip != nil && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() {
				return ip
			}
		}
	}
	t.Skip("no IPv4 address outside the loopback interface to relay on")
	return nil
}

// TestFastJoinRelayOnly: a client that reaches the SFU only through TURN, with no ICE
// servers of its own, gets the candidate's TURN server before either peer connection
// gathers. Its tracks are published after the FastJoin, so neither peer connection uses
// the credential before the SFU knows it. Media flows both ways on the first attempt,
// without an ICE restart.
func TestFastJoinRelayOnly(t *testing.T) {
	t.Parallel()

	m := newMediaSFU(t)
	turnServer := newTestTURN(t, relayIP(t), m.fastJoined.Load)
	f := newFakeCoordinator(t, 0, false)
	f.serveFastJoin(m.fake)
	f.iceServers.Store(&[]models.ICEServerResponse{turnServer.server})
	call := fastJoinCall(t, f, "relay-only")
	traces := make(chan jointrace.Trace, 1)
	call.OnJoinTrace(func(tr jointrace.Trace) { traces <- tr })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	audio, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, "audio", "prefix:TRACK_TYPE_AUDIO")
	require.NoError(t, err)
	relay := loopbackPeerConfig()
	relay.Config.ICETransportPolicy = webrtc.ICETransportPolicyRelay
	require.NoError(t, joinFast(t, call,
		WithTrack(&sfu_models.TrackInfo{TrackId: "published-audio", TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO}, audio),
		WithOnTrack(SubscriberFunc(func(OnTrackReceived) {})),
		WithPublisherPeerConfiguration(relay),
		WithSubscriberPeerConfiguration(relay)))
	require.Equal(t, JoinFlowFast, call.JoinFlow())
	go writeSamplesUntilDone(ctx, audio)
	go writeSamplesUntilDone(ctx, m.aliceAudio)

	var trace jointrace.Trace
	select {
	case trace = <-traces:
	case <-time.After(iceTimeout):
		t.Fatalf("no media both ways; recorded so far:\n%s", call.JoinTrace())
	}
	t.Logf("\n%s", trace)

	var fastJoins, setPublishers int
	for _, rpc := range rpcsOf[any](m.fake) {
		switch req := rpc.(type) {
		case *signal_rpc.FastJoinRequest:
			fastJoins++
			require.Empty(t, req.GetPublisherSdp(), "an offer gathered before the TURN server")
			require.Empty(t, req.GetTracks())
			require.Contains(t, req.GetSubscriberSdp(), "a=recvonly")
		case *signal_rpc.SetPublisherRequest:
			setPublishers++
			require.Contains(t, req.GetSdp(), "m=audio")
		}
	}
	require.Equal(t, 1, fastJoins)
	require.Equal(t, 1, setPublishers, "the tracks published once, with no ICE restart")
	require.Zero(t, turnServer.refused.Load(), "TURN asked before the FastJoin")
	require.Positive(t, turnServer.accepted.Load(), "media went another way than TURN")

	for _, name := range []string{
		jointrace.SFUFastJoin, jointrace.PubDebounce, jointrace.PubOffer, jointrace.PubSetPublisher,
		jointrace.PubICE, jointrace.PubRTP, jointrace.SubAnswer, jointrace.SubICE, jointrace.SubRTP,
	} {
		_, ok := trace.Span(name)
		require.True(t, ok, "no %s span", name)
	}
	debounce, _ := trace.Span(jointrace.PubDebounce)
	require.Equal(t, []string{jointrace.SFUFastJoin}, debounce.After, "published after the FastJoin")
	ice, _ := trace.Span(jointrace.PubICE)
	require.Contains(t, ice.After, jointrace.PubSetPublisher)
}

// TestFastJoinPublishesInTheFastJoin: with ICE servers of its own, or with every
// transport allowed, the publisher's offer goes in the FastJoin, as on the common path.
func TestFastJoinPublishesInTheFastJoin(t *testing.T) {
	t.Parallel()

	track := trackWithInfo{info: &sfu_models.TrackInfo{TrackType: sfu_models.TrackType_TRACK_TYPE_AUDIO}}
	relay := webrtc.Configuration{ICETransportPolicy: webrtc.ICETransportPolicyRelay}
	ownServers := relay
	ownServers.ICEServers = []webrtc.ICEServer{{URLs: []string{"turn:127.0.0.1:3478"}}}
	for name, tc := range map[string]struct {
		config webrtc.Configuration
		tracks []trackWithInfo
		after  bool
	}{
		"all transports":         {config: webrtc.Configuration{}, tracks: []trackWithInfo{track}},
		"relay with own servers": {config: ownServers, tracks: []trackWithInfo{track}},
		"relay":                  {config: relay, tracks: []trackWithInfo{track}, after: true},
		"relay without tracks":   {config: relay},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			options := joinOptions{tracks: tc.tracks}
			options.publisherPeerConfig.Config = tc.config
			require.Equal(t, tc.after, options.publishAfterFastJoin())
		})
	}
}
