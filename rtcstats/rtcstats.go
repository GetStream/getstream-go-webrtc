// Package rtcstats collects the WebRTC trace events the SDK reports to the SFU.
//
// The emitted format is part of the SFU wire protocol: a TraceBuffer holds one
// JSON array per event, `[eventName, pcID, payload, epochMillis]`, and the
// drained lines are spliced into the RtcStats field of the SFU's SendStats RPC.
// Changing the tuple layout, the event names or the pcID format breaks the
// server-side parser, so treat all of it as protocol rather than as internal
// detail.
package rtcstats

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

// TraceEvent is the decoded form of one emitted tuple. It is not produced by
// this package; it documents the layout consumers parse back out.
type TraceEvent struct {
	Type      string          // column 0
	PCID      *string         // column 1 – may be nil
	Value     json.RawMessage // column 2
	Timestamp int64           // column 3 (ms)
	Raw       json.RawMessage // original line
}

type StatsEvent struct {
	Timestamp float64         `json:"timestamp"` // µs
	Raw       json.RawMessage `json:"-"`
}

type rawLine struct {
	ts int64
	b  []byte
}

// TraceBuffer accumulates trace events for one namespace and peer connection.
// It is safe for concurrent use, and every method tolerates a nil receiver so
// callers can emit unconditionally.
type TraceBuffer struct {
	mu   sync.Mutex
	buf  []rawLine
	ns   string
	pcID *string
}

const (
	pubTag = "pub"
	subTag = "sub"
)

// Event names. These strings are matched by the SFU and by the rtcstats
// viewer; do not rename them.
const (
	GetStatsEvent = "getstats"

	SignalRPCSetPublisherEvent                = "signal.rpc.setpublisher"
	SignalRPCSetPublisherResponseEvent        = "signal.rpc.setpublisher.response"
	SignalRPCSendAnswerEvent                  = "signal.rpc.sendanswer"
	SignalRPCSendAnswerResponseEvent          = "signal.rpc.sendanswer.response"
	SignalRPCIceTrickleEvent                  = "signal.rpc.icetrickle"
	SignalRPCUpdateSubscriptionsEvent         = "signal.rpc.updatesubscriptions"
	SignalRPCUpdateSubscriptionsResponseEvent = "signal.rpc.updatesubscriptions.response"
	SignalRPCUpdateMuteStatesEvent            = "signal.rpc.updatemutestates"
	SignalRPCIceRestartEvent                  = "signal.rpc.icerestart"
	SignalRPCStartNoiseCancellationEvent      = "signal.rpc.startnoisecancellation"
	SignalRPCStopNoiseCancellationEvent       = "signal.rpc.stopnoisecancellation"

	SignalWSOpenEvent                     = "signal.ws.open"
	SignalWSJoinRequestEvent              = "signal.ws.joinrequest"
	SignalWSJoinResponseEvent             = "signal.ws.joinresponse"
	SignalWSErrorEvent                    = "signal.ws.error"
	SignalWSCallEndedEvent                = "signal.ws.callended"
	SignalWSCallGrantsUpdatedEvent        = "signal.ws.callgrantsupdated"
	SignalWSGoAwayEvent                   = "signal.ws.goaway"
	SignalWSIceRestartEvent               = "signal.ws.icerestart"
	SignalWSSubscriberOfferEvent          = "signal.ws.subscriberoffer"
	SignalWSPublisherAnswerEvent          = "signal.ws.publisheranswer"
	SignalWSConnectionQualityChangedEvent = "signal.ws.connectionqualitychanged"
	SignalWSIceTrickleEvent               = "signal.ws.icetrickle"
	SignalWSChangePublishQualityEvent     = "signal.ws.changepublishquality"
	SignalWSParticipantJoinedEvent        = "signal.ws.participantjoined"
	SignalWSParticipantLeftEvent          = "signal.ws.participantleft"
	SignalWSTrackPublishedEvent           = "signal.ws.trackpublished"
	SignalWSTrackUnpublishedEvent         = "signal.ws.trackunpublished"

	CoordinatorWSConnectEvent        = "coordinator.ws.connect"
	CoordinatorWSConnectedEvent      = "coordinator.ws.connected"
	CoordinatorJoinCallEvent         = "coordinator.joincall"
	CoordinatorJoinCallResponseEvent = "coordinator.joincall.response"
	CoordinatorConnectEvent          = "coordinator.connect"
	CoordinatorConnectedEvent        = "coordinator.connected"

	PeerCreateEvent                      = "create"
	PeerOnTrackEvent                     = "ontrack"
	PeerAddICECandidateEvent             = "addicecandidate"
	PeerAddICECandidateSuccessEvent      = "addicecandidatesuccess"
	PeerConnectionStateChangeEvent       = "connectionstatechange"
	PeerICECandidateEvent                = "icecandidate"
	PeerICEConnectionStateChangeEvent    = "iceconnectionstatechange"
	PeerICEGatheringStateChangeEvent     = "icegatheringstatechange"
	PeerNegotiationNeededEvent           = "negotiationneeded"
	PeerSetLocalDescriptionEvent         = "setlocaldescription"
	PeerSetLocalDescriptionSuccessEvent  = "setlocaldescriptionsuccess"
	PeerSetRemoteDescriptionEvent        = "setremotedescription"
	PeerSetRemoteDescriptionSuccessEvent = "setremotedescriptionsuccess"
	PeerSignalingStateChangeEvent        = "signalingstatechange"

	RembBitrateChangeEvent = "remb_bitrate_change"
	TrackMappingEvent      = "track.mapping"
)

// eventBlacklist holds events that are dropped before reaching the buffer.
// Participant churn is already known server-side and is high volume.
var eventBlacklist = map[string]struct{}{
	SignalWSParticipantLeftEvent:   {},
	SignalWSParticipantJoinedEvent: {},
}

func buildPcId(tag *string, version *int64, sfuID *string) *string {
	if tag != nil && version != nil && sfuID != nil {
		id := fmt.Sprintf("%s-%d-%s", *tag, *version+1, *sfuID)
		return &id
	} else if version != nil && sfuID != nil {
		id := fmt.Sprintf("%d-%s", *version+1, *sfuID)
		return &id
	}
	return nil
}

func buildNs(ns string) string {
	var eventNs string
	if ns != "" {
		eventNs = fmt.Sprintf("%s.", ns)
	}
	return eventNs
}

func newTraceBuffer(ns string, tag *string, version *int64, sfuID *string) TraceBuffer {
	return TraceBuffer{
		buf:  []rawLine{},
		pcID: buildPcId(tag, version, sfuID),
		ns:   buildNs(ns),
	}
}

// NewClientTraceBuffer returns a buffer for client-scoped events, which carry
// no peer-connection ID.
func NewClientTraceBuffer(ns string) *TraceBuffer {
	tb := newTraceBuffer(ns, nil, nil, nil)
	return &tb
}

// NewCallTraceBuffer returns a buffer for call-scoped events, tagged
// "<version+1>-<sfuID>".
func NewCallTraceBuffer(ns string, version int64, sfuID string) *TraceBuffer {
	tb := newTraceBuffer(ns, nil, &version, &sfuID)
	return &tb
}

// NewPubTraceBuffer returns a buffer for publisher events, tagged
// "pub-<version+1>-<sfuID>".
func NewPubTraceBuffer(ns string, version int64, sfuID string) *TraceBuffer {
	tag := pubTag
	tb := newTraceBuffer(ns, &tag, &version, &sfuID)
	return &tb
}

// NewSubTraceBuffer returns a buffer for subscriber events, tagged
// "sub-<version+1>-<sfuID>".
func NewSubTraceBuffer(ns string, version int64, sfuID string) *TraceBuffer {
	tag := subTag
	tb := newTraceBuffer(ns, &tag, &version, &sfuID)
	return &tb
}

// Emit queues `[event, pcID|null, payload, epochMs]` in a thread-safe way.
func (tb *TraceBuffer) Emit(event string, payload any) {
	if tb == nil {
		return
	}
	if _, blacklisted := eventBlacklist[event]; blacklisted {
		return
	}

	ts := time.Now().UnixMilli()
	ns := tb.ns
	if event == GetStatsEvent {
		ns = ""
	}
	tuple := []any{ns + event, tb.pcID, payload, ts}
	b, _ := json.Marshal(tuple) // json.Marshal never returns error for sane data

	tb.mu.Lock()
	tb.buf = append(tb.buf, rawLine{ts: ts, b: b})
	tb.mu.Unlock()
}

// EmitError is a convenience wrapper to log errors with optional context.
func (tb *TraceBuffer) EmitError(event string, err error, ctx any) {
	if tb == nil {
		return
	}
	var pay any
	if ctx == nil {
		pay = map[string]any{"error": err.Error()}
	} else {
		pay = map[string]any{"error": err.Error(), "context": ctx}
	}
	tb.Emit("sfu."+event+"Error", pay)
}

// Drain returns all queued lines (chronologically sorted) and resets the
// buffer. Lines are newline separated with a trailing newline.
func (tb *TraceBuffer) Drain() []byte {
	lines := tb.take()
	if len(lines) == 0 {
		return nil
	}
	return append(joinLines(lines, '\n'), '\n')
}

// DrainWithComma returns all queued lines comma separated, optionally with a
// trailing comma. This is what the SDK uses to splice several buffers into one
// JSON array for the SendStats RPC.
func (tb *TraceBuffer) DrainWithComma(trailing bool) []byte {
	lines := tb.take()
	if len(lines) == 0 {
		return nil
	}
	out := joinLines(lines, ',')
	if trailing {
		return append(out, ',')
	}
	return out
}

func (tb *TraceBuffer) take() []rawLine {
	if tb == nil {
		return nil
	}
	tb.mu.Lock()
	lines := tb.buf
	tb.buf = nil // relinquish ownership
	tb.mu.Unlock()

	sort.SliceStable(lines, func(i, j int) bool { return lines[i].ts < lines[j].ts })
	return lines
}

func joinLines(lines []rawLine, sep byte) []byte {
	arr := make([][]byte, len(lines))
	for i, l := range lines {
		arr[i] = l.b
	}
	return bytes.Join(arr, []byte{sep})
}

// DeltaCompressionRtcStats strips from newStats every field that is unchanged
// from oldStats, deletes the per-report "id" (it is the map key already) and
// hoists the largest report timestamp to newStats["timestamp"], zeroing the
// per-report copies of it. Both maps must be the JSON-decoded form of a
// getStats report, i.e. map[reportID]map[string]any.
func DeltaCompressionRtcStats(oldStats, newStats map[string]any) {
	if newStats == nil {
		return
	}
	var maxTimestamp float64 = -1
	for id, val := range newStats {
		// every record is a getstats record map (e.g. webrtc.OutboundRTPStreamStats, webrtc.RemoteInboundRTPStreamStats etc.)
		report, ok := val.(map[string]any)
		if !ok {
			continue
		}
		// Delete id to reduce size
		delete(report, "id")
		// Search for the largest timestamp among the records
		if ts, ok := report["timestamp"].(float64); ok && ts > maxTimestamp {
			maxTimestamp = ts
		}
		oldVal, ok := oldStats[id]
		if !ok {
			// Keep the new record if the old record is missing
			continue
		}
		// If the old report is present, compare and delete identical entries
		oldReport, ok := oldVal.(map[string]any)
		if !ok {
			continue
		}
		for name, value := range report {
			if oldValue, ok := oldReport[name]; ok && value == oldValue {
				// Delete entry from new stats if the old value is the same
				delete(report, name)
			}
		}
	}

	// Avoid duplicating max timestamp
	for _, val := range newStats {
		report, ok := val.(map[string]any)
		if !ok {
			continue
		}
		if ts, ok := report["timestamp"].(float64); ok && ts == maxTimestamp {
			report["timestamp"] = 0
		}
	}
	newStats["timestamp"] = maxTimestamp
}
