package rtc

import (
	"strings"
	"sync/atomic"

	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/thesyncim/skipset"
)

// anonymousUserID is the user ID the coordinator assigns to anonymous
// participants; they are counted separately from named ones.
const anonymousUserID = "!anon"

type ParticipantStore struct {
	pByID          *skipset.SkipSet[*Participant]
	pByTrackPrefix *skipset.SkipSet[*Participant]
	Total          atomic.Int32
	Anonymous      atomic.Int32
}

func NewParticipantStore(call *Call, callState *sfu_models.CallState) *ParticipantStore {
	s := &ParticipantStore{
		pByID: skipset.New[*Participant](func(a, b *Participant) bool {
			if a.UserID != b.UserID {
				return a.UserID < b.UserID
			}
			return a.SessionID < b.SessionID
		}),
		pByTrackPrefix: skipset.New[*Participant](func(a, b *Participant) bool {
			return a.TrackLookupPrefix < b.TrackLookupPrefix
		}),
	}
	// Read the call state through the generated getters: protobuf has no
	// required fields, so a join response that omits the call state or the
	// counts is a message this client has to survive, not crash on.
	for _, p := range callState.GetParticipants() {
		pp := NewParticipant(call, p)
		s.Set(pp)
	}
	s.Total.Store(int32(callState.GetParticipantCount().GetTotal()))
	s.Anonymous.Store(int32(callState.GetParticipantCount().GetAnonymous()))
	return s
}

func (s *ParticipantStore) LookupParticipantByTrack(streamID string) (*Participant, sfu_models.TrackType) {
	parts := strings.Split(streamID, ":")
	prefix := parts[0]
	p := s.GetByTrackPrefix(prefix)
	if p == nil {
		return nil, 0
	}
	return p, sfu_models.TrackType(sfu_models.TrackType_value[parts[1]])
}

func (s *ParticipantStore) GetByID(id ParticipantID) *Participant {
	p, _ := s.pByID.Load(&Participant{ParticipantID: id})
	return p
}

func (s *ParticipantStore) GetByTrackPrefix(prefix string) *Participant {
	p, _ := s.pByTrackPrefix.Load(&Participant{TrackLookupPrefix: prefix})
	return p
}

func (s *ParticipantStore) Set(p *Participant) {
	s.pByID.Set(p)
	s.pByTrackPrefix.Set(p)
	s.Total.Add(1)
	if p.UserID == anonymousUserID {
		s.Anonymous.Add(1)
	}
}

func (s *ParticipantStore) Remove(p *Participant) {
	s.pByID.Remove(p)
	s.pByTrackPrefix.Remove(p)
	s.Total.Add(-1)
	if p.UserID == anonymousUserID {
		s.Anonymous.Add(-1)
	}
}
