package rtc

import (
	"sync"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	"github.com/pion/webrtc/v4"
)

// receiveSlotStreamID is the msid stream id of an audio receive slot. A slot
// keeps it after the SFU binds a participant's audio to it, so the participant
// comes from the AudioReceiveSlotBound event for the slot's mid instead.
const receiveSlotStreamID = "receive-slot"

// receiveSlots pairs the tracks arriving on audio receive slots with the
// AudioReceiveSlotBound events naming their participant. Media and event take
// different paths from the SFU, so either can come first.
type receiveSlots struct {
	mu sync.Mutex
	// bound is keyed by the slot's mid.
	bound map[string]*sfu_events.AudioReceiveSlotBound
	// parked holds the tracks that arrived before their slot's event, by mid.
	parked map[string]parkedSlotTrack
}

type parkedSlotTrack struct {
	track    *webrtc.TrackRemote
	receiver *webrtc.RTPReceiver
}

func (s *subscriber) onReceiveSlotTrack(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
	mid := s.receiverMid(receiver)
	if mid == "" {
		s.logger.Warn("dropping a receive slot track without a mid")
		return
	}
	s.slots.mu.Lock()
	bound, ok := s.slots.bound[mid]
	if !ok {
		if s.slots.parked == nil {
			s.slots.parked = map[string]parkedSlotTrack{}
		}
		s.slots.parked[mid] = parkedSlotTrack{track: track, receiver: receiver}
	}
	s.slots.mu.Unlock()
	if ok {
		s.deliverSlotTrack(bound, track, receiver)
	}
}

// receiveSlotBound records which participant's audio the SFU bound to a slot,
// and delivers the slot's track if it is already here.
func (s *subscriber) receiveSlotBound(bound *sfu_events.AudioReceiveSlotBound) {
	mid := bound.GetMid()
	s.slots.mu.Lock()
	if s.slots.bound == nil {
		s.slots.bound = map[string]*sfu_events.AudioReceiveSlotBound{}
	}
	s.slots.bound[mid] = bound
	parked, ok := s.slots.parked[mid]
	delete(s.slots.parked, mid)
	s.slots.mu.Unlock()
	if ok {
		// Off the signalling read loop: OnTrack handlers may block, as pion runs
		// them on their own goroutine.
		go s.deliverSlotTrack(bound, parked.track, parked.receiver)
	}
}

func (s *subscriber) deliverSlotTrack(bound *sfu_events.AudioReceiveSlotBound, track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
	id := ParticipantID{UserID: UserID(bound.GetUserId()), SessionID: SessionID(bound.GetSessionId())}
	store := s.c.participantStore()
	var participant *Participant
	if store != nil {
		participant = store.GetByID(id)
	}
	if participant == nil {
		s.logger.WithField("participant", id).Warn("dropping a receive slot track from an unknown participant")
		return
	}
	s.deliverTrack(participant, bound.GetTrackType(), track, receiver)
}

func (s *subscriber) receiverMid(receiver *webrtc.RTPReceiver) string {
	for _, transceiver := range s.PC.GetTransceivers() {
		if transceiver.Receiver() == receiver {
			return transceiver.Mid()
		}
	}
	return ""
}
