// Package sdputil holds small helpers for reading values out of parsed SDP.
package sdputil

import (
	"github.com/pion/sdp/v3"
	"github.com/pion/webrtc/v4"
)

// GetMidValue returns the a=mid value of a media section, or "" when absent.
func GetMidValue(media *sdp.MediaDescription) string {
	for _, attr := range media.Attributes {
		if attr.Key == sdp.AttrKeyMID {
			return attr.Value
		}
	}
	return ""
}

// ExtractICECredential returns the ice-ufrag and ice-pwd of a session
// description. Session-level and media-level values must all agree.
func ExtractICECredential(desc *sdp.SessionDescription) (string, string, error) {
	remotePwds := []string{}
	remoteUfrags := []string{}

	if ufrag, haveUfrag := desc.Attribute("ice-ufrag"); haveUfrag {
		remoteUfrags = append(remoteUfrags, ufrag)
	}
	if pwd, havePwd := desc.Attribute("ice-pwd"); havePwd {
		remotePwds = append(remotePwds, pwd)
	}

	for _, m := range desc.MediaDescriptions {
		if ufrag, haveUfrag := m.Attribute("ice-ufrag"); haveUfrag {
			remoteUfrags = append(remoteUfrags, ufrag)
		}
		if pwd, havePwd := m.Attribute("ice-pwd"); havePwd {
			remotePwds = append(remotePwds, pwd)
		}
	}

	if len(remoteUfrags) == 0 {
		return "", "", webrtc.ErrSessionDescriptionMissingIceUfrag
	} else if len(remotePwds) == 0 {
		return "", "", webrtc.ErrSessionDescriptionMissingIcePwd
	}

	for _, m := range remoteUfrags {
		if m != remoteUfrags[0] {
			return "", "", webrtc.ErrSessionDescriptionConflictingIceUfrag
		}
	}

	for _, m := range remotePwds {
		if m != remotePwds[0] {
			return "", "", webrtc.ErrSessionDescriptionConflictingIcePwd
		}
	}

	return remoteUfrags[0], remotePwds[0], nil
}
