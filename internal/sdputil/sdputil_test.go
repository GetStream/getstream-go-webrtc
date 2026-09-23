package sdputil_test

import (
	"testing"

	"github.com/pion/sdp/v3"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/sdputil"
)

// The fixtures below are inline SDP literals in the style of
// stream-video-js' packages/client/src/rtc/helpers/__tests__/sdp.test.ts and
// iceCandiates.test.ts: the input is a whole session description, and the
// assertion is on what the helper reads out of it.

// A subscriber offer as the SFU sends it: ICE credentials at media level only,
// which is what browsers and pion both emit.
const offerWithMediaLevelCredentials = `v=0
o=- 8380609262679842857 2 IN IP4 127.0.0.1
s=-
t=0 0
a=group:BUNDLE 0 1
m=audio 9 UDP/TLS/RTP/SAVPF 111
c=IN IP4 0.0.0.0
a=ice-ufrag:BRD8
a=ice-pwd:8e+C7eV4BeLTWG9HvRNQZ52S
a=ice-options:trickle
a=mid:0
a=recvonly
a=rtpmap:111 opus/48000/2
m=video 9 UDP/TLS/RTP/SAVPF 96
c=IN IP4 0.0.0.0
a=ice-ufrag:BRD8
a=ice-pwd:8e+C7eV4BeLTWG9HvRNQZ52S
a=ice-options:trickle
a=mid:1
a=recvonly
a=rtpmap:96 VP8/90000
`

func parseSDP(t *testing.T, raw string) *sdp.SessionDescription {
	t.Helper()

	desc := &sdp.SessionDescription{}
	require.NoError(t, desc.UnmarshalString(raw))
	return desc
}

func TestGetMidValue(t *testing.T) {
	t.Parallel()

	desc := parseSDP(t, offerWithMediaLevelCredentials)
	require.Len(t, desc.MediaDescriptions, 2)

	require.Equal(t, "0", sdputil.GetMidValue(desc.MediaDescriptions[0]))
	require.Equal(t, "1", sdputil.GetMidValue(desc.MediaDescriptions[1]))
}

func TestGetMidValueNamedMids(t *testing.T) {
	t.Parallel()

	// The SFU labels mids by track type rather than by index, and a publisher
	// offer for simulcast repeats the same mid across rids.
	desc := parseSDP(t, `v=0
o=- 1 2 IN IP4 127.0.0.1
s=-
t=0 0
m=video 9 UDP/TLS/RTP/SAVPF 96
c=IN IP4 0.0.0.0
a=mid:video
a=rid:f send
a=rid:h send
`)

	require.Equal(t, "video", sdputil.GetMidValue(desc.MediaDescriptions[0]))
}

func TestGetMidValueMissing(t *testing.T) {
	t.Parallel()

	desc := parseSDP(t, `v=0
o=- 1 2 IN IP4 127.0.0.1
s=-
t=0 0
m=application 9 DTLS/SCTP 5000
c=IN IP4 0.0.0.0
a=sctpmap:5000 webrtc-datachannel 1024
`)

	require.Empty(t, sdputil.GetMidValue(desc.MediaDescriptions[0]))
}

// A media section carrying "a=mid:" with nothing after it parses as a mid
// attribute with an empty value, which is indistinguishable from no mid at all.
func TestGetMidValueEmptyValue(t *testing.T) {
	t.Parallel()

	media := &sdp.MediaDescription{
		Attributes: []sdp.Attribute{{Key: sdp.AttrKeyMID, Value: ""}},
	}

	require.Empty(t, sdputil.GetMidValue(media))
}

func TestGetMidValueFirstWins(t *testing.T) {
	t.Parallel()

	media := &sdp.MediaDescription{
		Attributes: []sdp.Attribute{
			{Key: "recvonly"},
			{Key: sdp.AttrKeyMID, Value: "0"},
			{Key: sdp.AttrKeyMID, Value: "1"},
		},
	}

	require.Equal(t, "0", sdputil.GetMidValue(media))
}

func TestExtractICECredentialFromMediaLevel(t *testing.T) {
	t.Parallel()

	ufrag, pwd, err := sdputil.ExtractICECredential(parseSDP(t, offerWithMediaLevelCredentials))
	require.NoError(t, err)
	require.Equal(t, "BRD8", ufrag)
	require.Equal(t, "8e+C7eV4BeLTWG9HvRNQZ52S", pwd)
}

func TestExtractICECredentialFromSessionLevel(t *testing.T) {
	t.Parallel()

	// Session-level credentials with no media-level override: legal per RFC 8839
	// and what an ICE-lite server may send.
	ufrag, pwd, err := sdputil.ExtractICECredential(parseSDP(t, `v=0
o=- 1 2 IN IP4 127.0.0.1
s=-
t=0 0
a=ice-ufrag:F7gIaBcD
a=ice-pwd:sessionpwd
m=audio 9 UDP/TLS/RTP/SAVPF 111
c=IN IP4 0.0.0.0
a=mid:0
`))
	require.NoError(t, err)
	require.Equal(t, "F7gIaBcD", ufrag)
	require.Equal(t, "sessionpwd", pwd)
}

func TestExtractICECredentialErrors(t *testing.T) {
	t.Parallel()

	const header = `v=0
o=- 1 2 IN IP4 127.0.0.1
s=-
t=0 0
`

	tests := []struct {
		name    string
		sdp     string
		wantErr error
	}{
		{
			name: "no ice attributes at all",
			sdp: header + `m=audio 9 UDP/TLS/RTP/SAVPF 111
c=IN IP4 0.0.0.0
a=mid:0
`,
			wantErr: webrtc.ErrSessionDescriptionMissingIceUfrag,
		},
		{
			name: "ufrag without pwd",
			sdp: header + `m=audio 9 UDP/TLS/RTP/SAVPF 111
c=IN IP4 0.0.0.0
a=ice-ufrag:BRD8
a=mid:0
`,
			wantErr: webrtc.ErrSessionDescriptionMissingIcePwd,
		},
		{
			// A half-applied ICE restart: the second m-section still carries the
			// previous generation's ufrag. Answering with either one would fail
			// the integrity check on one of the two candidate pairs.
			name: "media sections disagree on ufrag",
			sdp: header + `m=audio 9 UDP/TLS/RTP/SAVPF 111
c=IN IP4 0.0.0.0
a=ice-ufrag:BRD8
a=ice-pwd:samepwd
a=mid:0
m=video 9 UDP/TLS/RTP/SAVPF 96
c=IN IP4 0.0.0.0
a=ice-ufrag:OTHER
a=ice-pwd:samepwd
a=mid:1
`,
			wantErr: webrtc.ErrSessionDescriptionConflictingIceUfrag,
		},
		{
			name: "media sections disagree on pwd",
			sdp: header + `m=audio 9 UDP/TLS/RTP/SAVPF 111
c=IN IP4 0.0.0.0
a=ice-ufrag:BRD8
a=ice-pwd:onepwd
a=mid:0
m=video 9 UDP/TLS/RTP/SAVPF 96
c=IN IP4 0.0.0.0
a=ice-ufrag:BRD8
a=ice-pwd:anotherpwd
a=mid:1
`,
			wantErr: webrtc.ErrSessionDescriptionConflictingIcePwd,
		},
		{
			name: "session level disagrees with media level",
			sdp: header + `a=ice-ufrag:SESSION
a=ice-pwd:samepwd
m=audio 9 UDP/TLS/RTP/SAVPF 111
c=IN IP4 0.0.0.0
a=ice-ufrag:MEDIA
a=ice-pwd:samepwd
a=mid:0
`,
			wantErr: webrtc.ErrSessionDescriptionConflictingIceUfrag,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ufrag, pwd, err := sdputil.ExtractICECredential(parseSDP(t, tt.sdp))
			require.ErrorIs(t, err, tt.wantErr)
			require.Empty(t, ufrag)
			require.Empty(t, pwd)
		})
	}
}

// Credentials repeated identically across the session and every m-section are
// the common bundled case and must not read as a conflict.
func TestExtractICECredentialAgreeingDuplicates(t *testing.T) {
	t.Parallel()

	ufrag, pwd, err := sdputil.ExtractICECredential(parseSDP(t, `v=0
o=- 1 2 IN IP4 127.0.0.1
s=-
t=0 0
a=ice-ufrag:BRD8
a=ice-pwd:samepwd
m=audio 9 UDP/TLS/RTP/SAVPF 111
c=IN IP4 0.0.0.0
a=ice-ufrag:BRD8
a=ice-pwd:samepwd
a=mid:0
m=video 9 UDP/TLS/RTP/SAVPF 96
c=IN IP4 0.0.0.0
a=ice-ufrag:BRD8
a=ice-pwd:samepwd
a=mid:1
`))
	require.NoError(t, err)
	require.Equal(t, "BRD8", ufrag)
	require.Equal(t, "samepwd", pwd)
}
