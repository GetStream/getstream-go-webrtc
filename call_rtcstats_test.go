package rtc

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/valyala/bytebufferpool"
)

func TestRTCStatsTracingCollector(t *testing.T) {
	t.Parallel()

	call := GetDummyCall(t, "dummy-call-user-id", true)

	call.cc.Tracing.Load().Emit("client-event", nil)
	call.Tracing.Load().Emit("call-event", nil)
	call.getPeer().publisher.Tracing.Load().Emit("pub-event", nil)
	call.getPeer().subscriber.Tracing.Load().Emit("sub-event", nil)

	pubStats := call.getPeer().publisher.GetRtcStats()
	require.NotNil(t, pubStats)
	call.getPeer().publisher.Tracing.Load().Emit("getstats", pubStats)
	subStats := call.getPeer().subscriber.GetRtcStats()
	require.NotNil(t, subStats)
	call.getPeer().subscriber.Tracing.Load().Emit("getstats", subStats)

	rtcstatsBuffer := bytebufferpool.Get()
	defer bytebufferpool.Put(rtcstatsBuffer)
	call.collectTracing(rtcstatsBuffer)

	var arr []json.RawMessage
	require.NoError(t, json.Unmarshal(rtcstatsBuffer.Bytes(), &arr))
	// Two more "create" events on top of the six emitted above, one per PC.
	require.Len(t, arr, 8, "should have 8 tracing events")

	for i := range arr {
		var entry []any
		require.NoError(t, json.Unmarshal(arr[i], &entry))
		event, ok := entry[0].(string)
		require.True(t, ok)
		switch event {
		case "create", "getstats":
			_, ok := entry[2].(map[string]any)
			require.True(t, ok)
		case "client-event", "call-event", "pub-event", "sub-event":
		default:
			require.FailNow(t, "unexpected event: "+event)
		}
	}
}

func TestRTCStatsWithoutTracing(t *testing.T) {
	t.Parallel()

	call := GetDummyCall(t, "dummy-call-user-id", false)

	call.cc.Tracing.Load().Emit("client-event", nil)
	call.Tracing.Load().Emit("call-event", nil)
	call.getPeer().publisher.Tracing.Load().Emit("pub-event", nil)
	call.getPeer().subscriber.Tracing.Load().Emit("sub-event", nil)

	pubStats := call.getPeer().publisher.GetRtcStats()
	require.NotNil(t, pubStats)
	call.getPeer().publisher.Tracing.Load().Emit("getstats", pubStats)
	subStats := call.getPeer().subscriber.GetRtcStats()
	require.NotNil(t, subStats)
	call.getPeer().subscriber.Tracing.Load().Emit("getstats", subStats)

	rtcstatsBuffer := bytebufferpool.Get()
	defer bytebufferpool.Put(rtcstatsBuffer)
	call.collectTracing(rtcstatsBuffer)

	var arr []json.RawMessage
	require.NoError(t, json.Unmarshal(rtcstatsBuffer.Bytes(), &arr))
	require.Empty(t, arr, "should have no tracing events")
}
