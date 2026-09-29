package pc

import (
	"testing"
	"time"

	"github.com/pion/dtls/v4/pkg/protocol"
	"github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/logger"
	"github.com/GetStream/protocol/protobuf/video/sfu/models"
)

// TestWARPNegotiation connects a publisher transport, which offers, to a pion peer
// configured like an SFU. A WARP SFU (DTLS 1.2 to 1.3, SPED, the DTLS server role when
// the offer has SPED) gets DTLS 1.3 and SPED; an SFU without WARP gets DTLS 1.2 without
// SPED, as before.
func TestWARPNegotiation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		warp        bool
		wantVersion protocol.Version
	}{
		{name: "WARPSFU", warp: true, wantVersion: protocol.Version1_3},
		{name: "LegacySFU", warp: false, wantVersion: protocol.Version1_2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := newPCTestWithTransportParams(t, TransportParams{
				Transport:  models.PeerType_PEER_TYPE_PUBLISHER_UNSPECIFIED,
				Logger:     logger.Noop{},
				IsOfferer:  true,
				PeerConfig: newPCTestPeerConfig(t),
			})

			sfu := newPCTestPeerConfig(t)
			sfu.SettingEngine.SetLite(true)
			if tc.warp {
				require.NoError(t, sfu.SettingEngine.SetDTLSVersionRange(protocol.Version1_2, protocol.Version1_3))
				sfu.SettingEngine.EnableSped(true)
				sfu.SettingEngine.SetAnsweringDTLSRoleWithSPED(webrtc.DTLSRoleServer)
			}
			remote := newRemotePeerWithConfig(t, st.tr, sfu)
			st.handler.onICECandidateSender = remote.ICECandidateSender

			st.tr.Negotiate(true)
			st.tr.HandleRemoteDescription(remote.Answer(st.waitForOffer()))
			st.waitForPCState(webrtc.PeerConnectionStateConnected, 5*time.Second)

			// The two ends finish the handshake one flight apart.
			require.Eventually(t, func() bool {
				return remote.PC.ConnectionState() == webrtc.PeerConnectionStateConnected
			}, 5*time.Second, 10*time.Millisecond)
			local, sfuState := st.tr.PC.WARPState(), remote.PC.WARPState()
			require.Equal(t, tc.wantVersion, local.DTLSVersion)
			require.Equal(t, tc.wantVersion, sfuState.DTLSVersion)
			if !tc.warp {
				require.Equal(t, ice.SPEDStateDisabled, local.SPED)
				require.Equal(t, ice.SPEDStateDisabled, sfuState.SPED)
				return
			}
			// SPED completes once everything sent is acknowledged or media arrives.
			for _, state := range []ice.SPEDState{local.SPED, sfuState.SPED} {
				require.Contains(t, []ice.SPEDState{ice.SPEDStatePending, ice.SPEDStateComplete}, state)
			}
		})
	}
}
