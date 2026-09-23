package rtc

import (
	"sync"
	"sync/atomic"
	"time"

	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/thesyncim/skipset"

	"github.com/GetStream/getstream-go-webrtc/internal/atomicx"
)

// UserID identifies a Stream user.
type UserID string

// SessionID identifies one participant session inside a call. The same user
// joining twice gets two session IDs.
type SessionID string

// ParticipantID identifies a participant: the user plus the session they
// joined with. It is comparable so it can be used as a map key.
type ParticipantID struct {
	UserID    UserID    `json:"user_id"`
	SessionID SessionID `json:"session_id"`
}

func (p ParticipantID) IsZero() bool {
	return p.UserID == "" && p.SessionID == ""
}

func (p ParticipantID) Equal(other ParticipantID) bool {
	return p.UserID == other.UserID && p.SessionID == other.SessionID
}

type Participant struct {
	Call *Call
	ParticipantID
	PublishedTracks   *skipset.SkipSet[sfu_models.TrackType]
	JoinedAt          time.Time
	TrackLookupPrefix string
	ConnectionQuality atomicx.AtomicValue[sfu_models.ConnectionQuality]
	IsSpeaking        atomic.Bool
	IsDominantSpeaker atomic.Bool
	AudioLevel        atomicx.AtomicValue[float32]
	Name              string
	Image             string
	Custom            map[string]any
	// todo make roles thread safe
	Roles []string

	// pausedTracks holds the track types the SFU has stopped forwarding for this
	// participant. It is only populated for clients that advertise the
	// subscriber-video-pause capability, which this SDK does by default.
	pausedTracksMu sync.Mutex
	pausedTracks   map[sfu_models.TrackType]bool
}

// SetTrackPaused records whether the SFU has paused forwarding the given track
// type for this participant.
func (p *Participant) SetTrackPaused(trackType sfu_models.TrackType, paused bool) {
	p.pausedTracksMu.Lock()
	defer p.pausedTracksMu.Unlock()
	if p.pausedTracks == nil {
		p.pausedTracks = make(map[sfu_models.TrackType]bool)
	}
	if paused {
		p.pausedTracks[trackType] = true
		return
	}
	delete(p.pausedTracks, trackType)
}

// IsTrackPaused reports whether the SFU has paused forwarding the given track
// type for this participant.
//
// A paused track produces no media even though it is still subscribed, so
// without this an application cannot tell "the SFU is deliberately not sending
// this" from "the track is broken".
func (p *Participant) IsTrackPaused(trackType sfu_models.TrackType) bool {
	p.pausedTracksMu.Lock()
	defer p.pausedTracksMu.Unlock()
	return p.pausedTracks[trackType]
}

// PausedTracks returns the track types the SFU is currently not forwarding for
// this participant.
func (p *Participant) PausedTracks() []sfu_models.TrackType {
	p.pausedTracksMu.Lock()
	defer p.pausedTracksMu.Unlock()
	types := make([]sfu_models.TrackType, 0, len(p.pausedTracks))
	for trackType := range p.pausedTracks {
		types = append(types, trackType)
	}
	return types
}

func NewParticipant(call *Call, p *sfu_models.Participant) *Participant {
	participant := &Participant{
		Call: call,
		ParticipantID: ParticipantID{
			UserID:    UserID(p.UserId),
			SessionID: SessionID(p.SessionId),
		},
		PublishedTracks: skipset.New[sfu_models.TrackType](func(a, b sfu_models.TrackType) bool {
			return a < b
		}),
		JoinedAt:          p.JoinedAt.AsTime(),
		TrackLookupPrefix: p.TrackLookupPrefix,
		ConnectionQuality: atomicx.AtomicValue[sfu_models.ConnectionQuality]{},
		Name:              p.Name,
		Image:             p.Image,
		Custom:            p.Custom.AsMap(),
		Roles:             p.Roles,
	}
	for _, t := range p.PublishedTracks {
		participant.PublishedTracks.Set(t)
	}
	participant.AudioLevel.Store(p.AudioLevel)
	participant.ConnectionQuality.Store(p.ConnectionQuality)
	participant.IsSpeaking.Store(p.IsSpeaking)
	participant.IsDominantSpeaker.Store(p.IsDominantSpeaker)
	return participant
}
