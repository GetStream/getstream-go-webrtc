package pc

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/dtls/v4/pkg/protocol/handshake"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
)

// TestTheClientDoesNotAskForAHelloVerifyCookie connects a transport that is the DTLS server,
// as the publisher is, and counts the peer's ClientHellos: a HelloVerifyRequest would make the
// peer send a second one carrying the cookie, a round trip later.
func TestTheClientDoesNotAskForAHelloVerifyCookie(t *testing.T) {
	st := newPCTest(t)

	var clientHellos atomic.Int32
	cfg := newPCTestPeerConfig(t)
	cfg.SettingEngine.SetDTLSClientHelloMessageHook(func(msg handshake.MessageClientHello) handshake.Message {
		clientHellos.Add(1)
		return &msg
	})
	remote := newRemotePeerWithConfig(t, st.tr, cfg)
	st.handler.onICECandidateSender = remote.ICECandidateSender

	st.tr.Negotiate()
	answer := remote.Answer(st.waitForOffer())
	require.Contains(t, answer.SDP, "a=setup:active", "the peer answers as DTLS client, so the transport is the server")
	st.tr.HandleRemoteDescription(answer)
	st.waitForPCState(webrtc.PeerConnectionStateConnected, 5*time.Second)

	require.Equal(t, int32(1), clientHellos.Load(), "one ClientHello: no HelloVerifyRequest round trip")
}
