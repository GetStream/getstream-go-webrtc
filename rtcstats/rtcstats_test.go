package rtcstats

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
)

// helper to split trailing new-line terminated buffers into non-empty JSON lines
func splitLines(b []byte) [][]byte {
	b = bytes.TrimSpace(b) // Drop the trailing newline added by Drain()
	if len(b) == 0 {
		return nil
	}
	return bytes.Split(b, []byte{'\n'})
}

func TestTraceBufferPcId(t *testing.T) {
	t.Parallel()

	var it int64 = 0
	sfuid := "sfu1"

	// Generic Tracer with nil id
	tb := NewClientTraceBuffer("sfu")
	if tb.pcID != nil {
		t.Fatalf("expected nil PC id, got %s", *tb.pcID)
	}

	// Connection Tracer with format #-sfuid
	tb = NewCallTraceBuffer("sfu", it, sfuid)
	if tb.pcID == nil || *tb.pcID != fmt.Sprintf("%d-%s", it+1, sfuid) {
		t.Fatalf("unexpected PC id")
	}

	// Pub Tracer with format pub-#-sfuid
	tb = NewPubTraceBuffer("sfu", it, sfuid)
	if tb.pcID == nil || *tb.pcID != fmt.Sprintf("pub-%d-%s", it+1, sfuid) {
		t.Fatalf("unexpected PC id")
	}

	// Sub Tracer with format sub-#-sfuid
	tb = NewSubTraceBuffer("sfu", it, sfuid)
	if tb.pcID == nil || *tb.pcID != fmt.Sprintf("sub-%d-%s", it+1, sfuid) {
		t.Fatalf("unexpected PC id")
	}
}

func TestTraceBufferEmitAndDrain(t *testing.T) {
	t.Parallel()

	tb := NewCallTraceBuffer("sfu", 1, "sfu1")
	pay := map[string]any{"foo": "bar"}
	tb.Emit("testEvent", pay)

	raw := tb.Drain()
	if raw == nil {
		t.Fatalf("Drain returned nil, expected data")
	}

	lines := splitLines(raw)
	if len(lines) != 1 {
		t.Fatalf("expected 1 JSON line, got %d", len(lines))
	}

	var tuple []any
	if err := json.Unmarshal(lines[0], &tuple); err != nil {
		t.Fatalf("failed to unmarshal tuple: %v", err)
	}
	if len(tuple) != 4 {
		t.Fatalf("expected tuple of len 4, got %d", len(tuple))
	}

	// column 0 – event name
	if ev, ok := tuple[0].(string); !ok || ev != "sfu.testEvent" {
		t.Fatalf("unexpected event: %#v", tuple[0])
	}
	// column 1 – pcID
	if pcid, ok := tuple[1].(string); !ok || pcid != "2-sfu1" {
		t.Fatalf("unexpected pcID: %#v", tuple[1])
	}
	// column 2 – payload
	if plen, ok := tuple[2].(map[string]any); !ok || plen["foo"] != "bar" {
		t.Fatalf("unexpected payload: %#v", tuple[2])
	}
	// column 3 – timestamp (float64 after json decode)
	if _, ok := tuple[3].(float64); !ok {
		t.Fatalf("timestamp should be a number, got %#v", tuple[3])
	}

	// second Drain should be empty (buffer reset)
	if again := tb.Drain(); again != nil {
		t.Fatalf("expected empty buffer after Drain, got %q", string(again))
	}
}

// A client trace buffer has no peer connection, so column 1 must be JSON null.
func TestTraceBufferNilPcIdIsNull(t *testing.T) {
	t.Parallel()

	tb := NewClientTraceBuffer("")
	tb.Emit(CoordinatorConnectEvent, map[string]any{"a": 1})

	lines := splitLines(tb.Drain())
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	var tuple []any
	if err := json.Unmarshal(lines[0], &tuple); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if tuple[0] != CoordinatorConnectEvent {
		t.Fatalf("empty namespace must not prefix the event: %#v", tuple[0])
	}
	if tuple[1] != nil {
		t.Fatalf("expected null pcID, got %#v", tuple[1])
	}
}

// getstats events are reported without the namespace prefix, unlike every
// other event.
func TestTraceBufferGetStatsHasNoNamespace(t *testing.T) {
	t.Parallel()

	tb := NewPubTraceBuffer("sfu", 0, "sfu1")
	tb.Emit(GetStatsEvent, map[string]any{"timestamp": 1})

	var tuple []any
	if err := json.Unmarshal(splitLines(tb.Drain())[0], &tuple); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if tuple[0] != GetStatsEvent {
		t.Fatalf("expected %q, got %#v", GetStatsEvent, tuple[0])
	}
}

func TestTraceBufferConcurrency(t *testing.T) {
	t.Parallel()

	const N = 100
	tb := NewCallTraceBuffer("sfu", 2, "concurrent")

	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tb.Emit("concurrentEvent", map[string]int{"idx": idx})
		}(i)
	}
	wg.Wait()

	raw := tb.Drain()
	lines := splitLines(raw)
	if len(lines) != N {
		t.Fatalf("expected %d lines, got %d", N, len(lines))
	}

	// ensure timestamps are monotonically non-decreasing (Drain sorts)
	var lastTS float64
	for i, l := range lines {
		var tuple []any
		if err := json.Unmarshal(l, &tuple); err != nil {
			t.Fatalf("json decode line %d: %v", i, err)
		}
		ts, ok := tuple[3].(float64)
		if !ok {
			t.Fatalf("timestamp field is not number: %#v", tuple[3])
		}
		if i > 0 && ts < lastTS {
			t.Fatalf("timestamps not sorted: %v then %v", lastTS, ts)
		}
		lastTS = ts
	}
}

func TestTraceBufferEmitError(t *testing.T) {
	t.Parallel()

	tb := NewCallTraceBuffer("sfu", 3, "err")
	boom := errors.New("boom")

	tb.EmitError("myFunc", boom, nil)
	lines := splitLines(tb.Drain())
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	var tuple []any
	if err := json.Unmarshal(lines[0], &tuple); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	expectedEvent := "sfu.sfu.myFuncError" // note: double "sfu." as per current implementation
	if ev, _ := tuple[0].(string); ev != expectedEvent {
		t.Fatalf("unexpected error event name: got %s, want %s", ev, expectedEvent)
	}
	pay, ok := tuple[2].(map[string]any)
	if !ok || pay["error"] != boom.Error() {
		t.Fatalf("unexpected payload: %#v", tuple[2])
	}

	// with context
	ctx := "details"
	tb.EmitError("myFunc", boom, ctx)
	tuple = nil
	if err := json.Unmarshal(splitLines(tb.Drain())[0], &tuple); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	pay, ok = tuple[2].(map[string]any)
	if !ok || pay["context"] != ctx {
		t.Fatalf("context missing from payload: %#v", tuple[2])
	}
}

func TestTraceBufferBlacklist(t *testing.T) {
	t.Parallel()

	tb := NewCallTraceBuffer("sfu", 1, "sfu1")
	pay := map[string]any{"foo": "bar"}

	for ev := range eventBlacklist {
		tb.Emit(ev, pay)
	}

	raw := tb.Drain()
	if raw != nil {
		t.Fatalf("Drain returned something, expected nil")
	}
}

// Every method must tolerate a nil receiver, because callers emit through
// pointers that are nil before a peer connection exists.
func TestTraceBufferNilReceiver(t *testing.T) {
	t.Parallel()

	var tb *TraceBuffer
	tb.Emit("event", nil)
	tb.EmitError("event", errors.New("boom"), nil)
	if got := tb.Drain(); got != nil {
		t.Fatalf("expected nil, got %q", got)
	}
	if got := tb.DrainWithComma(false); got != nil {
		t.Fatalf("expected nil, got %q", got)
	}
}

// TestSendStatsRtcStatsShape reproduces how the SDK assembles the RtcStats
// string of the SFU SendStats RPC: several buffers are drained with comma
// separators and spliced into one JSON array. The result must parse as an
// array of 4-element tuples.
func TestSendStatsRtcStatsShape(t *testing.T) {
	t.Parallel()

	client := NewClientTraceBuffer("")
	call := NewCallTraceBuffer("sfu", 0, "sfu1")
	pub := NewPubTraceBuffer("sfu", 0, "sfu1")
	sub := NewSubTraceBuffer("sfu", 0, "sfu1")

	client.Emit(CoordinatorJoinCallEvent, map[string]any{"type": "default"})
	call.Emit(SignalWSJoinRequestEvent, map[string]any{"session_id": "s1"})
	pub.Emit(GetStatsEvent, map[string]any{"timestamp": 1234.0})
	sub.Emit(PeerSetRemoteDescriptionEvent, map[string]any{"type": "offer"})

	var samples [][]byte
	for _, tb := range []*TraceBuffer{client, call, pub, sub} {
		if s := tb.DrainWithComma(false); len(s) > 0 {
			samples = append(samples, s)
		}
	}

	var buf bytes.Buffer
	buf.WriteByte('[')
	buf.Write(bytes.Join(samples, []byte{','}))
	buf.WriteByte(']')

	var tuples [][]any
	if err := json.Unmarshal(buf.Bytes(), &tuples); err != nil {
		t.Fatalf("spliced RtcStats payload is not valid JSON: %v\n%s", err, buf.String())
	}
	if len(tuples) != 4 {
		t.Fatalf("expected 4 tuples, got %d: %s", len(tuples), buf.String())
	}
	for i, tuple := range tuples {
		if len(tuple) != 4 {
			t.Fatalf("tuple %d has %d columns, want 4: %#v", i, len(tuple), tuple)
		}
		if _, ok := tuple[0].(string); !ok {
			t.Fatalf("tuple %d column 0 must be the event name: %#v", i, tuple[0])
		}
		if _, ok := tuple[3].(float64); !ok {
			t.Fatalf("tuple %d column 3 must be an epoch-millis number: %#v", i, tuple[3])
		}
	}

	// An empty set of buffers still has to produce a valid empty array.
	if got := client.DrainWithComma(false); got != nil {
		t.Fatalf("expected drained buffer to be empty, got %q", got)
	}
}

func TestDrainWithCommaTrailing(t *testing.T) {
	t.Parallel()

	tb := NewCallTraceBuffer("sfu", 0, "sfu1")
	tb.Emit("a", nil)
	tb.Emit("b", nil)

	got := tb.DrainWithComma(true)
	if len(got) == 0 || got[len(got)-1] != ',' {
		t.Fatalf("expected trailing comma, got %q", got)
	}
	if n := bytes.Count(got, []byte{','}); n < 2 {
		t.Fatalf("expected separator plus trailing comma, got %q", got)
	}
}

func TestDeltaCompressionRtcStats(t *testing.T) {
	t.Parallel()

	oldStats := map[string]any{
		"outbound-rtp-1": map[string]any{
			"id":              "outbound-rtp-1",
			"timestamp":       1000.0,
			"kind":            "audio",
			"bytesSent":       100.0,
			"packetsSent":     10.0,
			"trackIdentifier": "t1",
		},
	}
	newStats := map[string]any{
		"outbound-rtp-1": map[string]any{
			"id":              "outbound-rtp-1",
			"timestamp":       2000.0,
			"kind":            "audio",
			"bytesSent":       200.0,
			"packetsSent":     10.0,
			"trackIdentifier": "t1",
		},
		"inbound-rtp-1": map[string]any{
			"id":        "inbound-rtp-1",
			"timestamp": 1500.0,
			"kind":      "video",
		},
	}

	DeltaCompressionRtcStats(oldStats, newStats)

	if ts, ok := newStats["timestamp"].(float64); !ok || ts != 2000.0 {
		t.Fatalf("expected hoisted max timestamp 2000, got %#v", newStats["timestamp"])
	}

	outbound := newStats["outbound-rtp-1"].(map[string]any)
	if _, ok := outbound["id"]; ok {
		t.Fatalf("id must be stripped: %#v", outbound)
	}
	// Unchanged fields are dropped.
	for _, dropped := range []string{"kind", "packetsSent", "trackIdentifier"} {
		if _, ok := outbound[dropped]; ok {
			t.Fatalf("unchanged field %q must be dropped: %#v", dropped, outbound)
		}
	}
	// Changed fields survive.
	if outbound["bytesSent"] != 200.0 {
		t.Fatalf("changed field must survive: %#v", outbound)
	}
	// The report holding the max timestamp has it zeroed to avoid duplication.
	if outbound["timestamp"] != 0 {
		t.Fatalf("expected zeroed timestamp, got %#v", outbound["timestamp"])
	}

	// A report with no counterpart in oldStats is kept whole, minus its id.
	inbound := newStats["inbound-rtp-1"].(map[string]any)
	if inbound["kind"] != "video" || inbound["timestamp"] != 1500.0 {
		t.Fatalf("new report must be kept intact: %#v", inbound)
	}
}

func TestDeltaCompressionRtcStatsNilNewStats(t *testing.T) {
	t.Parallel()

	DeltaCompressionRtcStats(map[string]any{"a": map[string]any{}}, nil)
}

// The SDK stores the previous report as the raw Go value returned by
// GetRtcStats, whose entries are webrtc stats structs rather than decoded JSON
// maps. Delta compression must degrade to "no compression" instead of panicking
// on those, because the panic would take down the stats worker goroutine.
func TestDeltaCompressionRtcStatsMismatchedOldStats(t *testing.T) {
	t.Parallel()

	type outboundStats struct {
		BytesSent uint64
	}

	oldStats := map[string]any{
		"outbound-rtp-1": outboundStats{BytesSent: 100},
	}
	newStats := map[string]any{
		"outbound-rtp-1": map[string]any{
			"id":        "outbound-rtp-1",
			"timestamp": 2000.0,
			"bytesSent": 200.0,
		},
		"not-a-report": "garbage",
	}

	DeltaCompressionRtcStats(oldStats, newStats)

	report := newStats["outbound-rtp-1"].(map[string]any)
	if report["bytesSent"] != 200.0 {
		t.Fatalf("report must be preserved: %#v", report)
	}
	if newStats["timestamp"] != 2000.0 {
		t.Fatalf("expected hoisted timestamp, got %#v", newStats["timestamp"])
	}
}
