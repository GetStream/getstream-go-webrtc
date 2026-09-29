package pc

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/pion/dtls/v4/pkg/protocol"
	"github.com/pion/webrtc/v5"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/logger"
	"github.com/GetStream/protocol/protobuf/video/sfu/models"
)

// keyLog records what pion/dtls writes to its key log. DTLS 1.2 logs a CLIENT_RANDOM line
// and DTLS 1.3 logs nothing: pion/webrtc v5 has no accessor for the negotiated version.
type keyLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (k *keyLog) Write(p []byte) (int, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.buf.Write(p)
}

func (k *keyLog) version() protocol.Version {
	k.mu.Lock()
	defer k.mu.Unlock()
	if bytes.Contains(k.buf.Bytes(), []byte("CLIENT_RANDOM")) {
		return protocol.Version1_2
	}
	return protocol.Version1_3
}

// TestWARPNegotiation connects a publisher transport, which offers, to a pion peer
// configured like an SFU. An SFU with DTLS 1.2 to 1.3 gets DTLS 1.3; an SFU limited to
// DTLS 1.2 gets DTLS 1.2, as before. pion/webrtc v5 has no SPED, so neither gets it.
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
			localLog := &keyLog{}
			local := newPCTestPeerConfig(t)
			local.SettingEngine.SetDTLSKeyLogWriter(localLog)
			st := newPCTestWithTransportParams(t, TransportParams{
				Transport:  models.PeerType_PEER_TYPE_PUBLISHER_UNSPECIFIED,
				Logger:     logger.Noop{},
				IsOfferer:  true,
				PeerConfig: local,
			})

			sfuLog := &keyLog{}
			sfu := newPCTestPeerConfig(t)
			sfu.SettingEngine.SetLite(true)
			sfu.SettingEngine.SetDTLSKeyLogWriter(sfuLog)
			sfu.SettingEngine.SetDTLSMinVersion(protocol.Version1_2)
			if tc.warp {
				sfu.SettingEngine.SetDTLSMaxVersion(protocol.Version1_3)
			} else {
				sfu.SettingEngine.SetDTLSMaxVersion(protocol.Version1_2)
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
			require.Equal(t, tc.wantVersion, localLog.version())
			require.Equal(t, tc.wantVersion, sfuLog.version())
		})
	}
}
