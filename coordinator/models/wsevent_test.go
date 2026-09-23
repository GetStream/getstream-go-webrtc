package models_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
)

func TestParseWebsocketEventConnected(t *testing.T) {
	t.Parallel()

	raw := []byte(`{
		"type": "connection.ok",
		"connection_id": "5c3bd4f8-1234-4a1b-9c02-0f0f5a1b2c3d",
		"created_at": "2026-08-15T09:12:33.123456Z",
		"me": {
			"created_at": "2026-01-02T03:04:05Z",
			"updated_at": "2026-01-02T03:04:05Z",
			"custom": {"color": "blue"},
			"devices": [],
			"id": "thierry",
			"language": "en",
			"role": "user",
			"teams": []
		}
	}`)

	event, err := models.ParseWebsocketEvent(raw)
	require.NoError(t, err)
	require.Equal(t, "connection.ok", event.GetEventType())

	connected, ok := event.(*models.ConnectedEvent)
	require.True(t, ok, "expected *ConnectedEvent, got %T", event)
	require.Equal(t, "5c3bd4f8-1234-4a1b-9c02-0f0f5a1b2c3d", connected.ConnectionID)
	require.Equal(t, "thierry", connected.Me.ID)
	require.Equal(t, "user", connected.Me.Role)
	require.Equal(t, map[string]any{"color": "blue"}, connected.Me.Custom)
}

func TestParseWebsocketEventHealthCheck(t *testing.T) {
	t.Parallel()

	raw := []byte(`{
		"type": "health.check",
		"cid": "*",
		"connection_id": "conn-1",
		"created_at": "2026-08-15T09:12:53Z",
		"custom": {}
	}`)

	event, err := models.ParseWebsocketEvent(raw)
	require.NoError(t, err)

	healthCheck, ok := event.(*models.HealthCheckEvent)
	require.True(t, ok, "expected *HealthCheckEvent, got %T", event)
	require.NotNil(t, healthCheck.Cid)
	require.Equal(t, "*", *healthCheck.Cid)
	require.Equal(t, "health.check", healthCheck.GetEventType())
}

func TestParseWebsocketEventCallCreated(t *testing.T) {
	t.Parallel()

	raw := []byte(`{
		"type": "call.created",
		"call_cid": "default:my-call",
		"created_at": "2026-08-15T09:13:00Z",
		"members": [],
		"call": {
			"backstage": false,
			"cid": "default:my-call",
			"created_at": "2026-08-15T09:13:00Z",
			"created_by": {
				"created_at": "2026-01-02T03:04:05Z",
				"updated_at": "2026-01-02T03:04:05Z",
				"banned": false,
				"custom": {},
				"id": "thierry",
				"language": "en",
				"online": true,
				"role": "user",
				"teams": [],
				"blocked_user_ids": []
			},
			"current_session_id": "session-1",
			"custom": {},
			"id": "my-call",
			"recording": false,
			"transcribing": false,
			"captioning": false,
			"type": "default",
			"updated_at": "2026-08-15T09:13:00Z",
			"blocked_user_ids": [],
			"egress": {
				"broadcasting": false,
				"rtmps": [],
				"frame_recording": {"status": "off"}
			},
			"ingress": {"rtmp": {"address": ""}},
			"settings": {}
		}
	}`)

	event, err := models.ParseWebsocketEvent(raw)
	require.NoError(t, err)

	created, ok := event.(*models.CallCreatedEvent)
	require.True(t, ok, "expected *CallCreatedEvent, got %T", event)
	require.Equal(t, "default:my-call", created.CallCid)
	require.Equal(t, "my-call", created.Call.ID)
	require.Equal(t, time.Date(2026, 8, 15, 9, 13, 0, 0, time.UTC), created.CreatedAt.Time)
}

func TestParseWebsocketEventConnectionError(t *testing.T) {
	t.Parallel()

	raw := []byte(`{
		"type": "connection.error",
		"connection_id": "",
		"created_at": "2026-08-15T09:14:00Z",
		"error": {
			"StatusCode": 401,
			"code": 40,
			"details": [],
			"duration": "0.1ms",
			"message": "token expired",
			"more_info": "https://getstream.io/chat/docs/api_errors_response"
		}
	}`)

	event, err := models.ParseWebsocketEvent(raw)
	require.NoError(t, err)

	connErr, ok := event.(*models.ConnectionErrorEvent)
	require.True(t, ok, "expected *ConnectionErrorEvent, got %T", event)
	require.Equal(t, int32(40), connErr.Error.Code)
	require.Equal(t, "token expired", connErr.Error.Message)
}

// The interface must be satisfied by both the value and the pointer, since
// ParseWebsocketEvent hands back pointers while callers may construct values.
func TestWebsocketEventImplementedByValueAndPointer(t *testing.T) {
	t.Parallel()

	var byValue models.WebsocketEvent = models.HealthCheckEvent{}
	var byPointer models.WebsocketEvent = &models.HealthCheckEvent{}
	require.Equal(t, "health.check", byValue.GetEventType())
	require.Equal(t, "health.check", byPointer.GetEventType())
}

func TestParseWebsocketEventUnknownType(t *testing.T) {
	t.Parallel()

	_, err := models.ParseWebsocketEvent([]byte(`{"type":"call.teleported"}`))
	require.ErrorContains(t, err, "call.teleported")
}

func TestParseWebsocketEventInvalidJSON(t *testing.T) {
	t.Parallel()

	_, err := models.ParseWebsocketEvent([]byte(`not json`))
	require.Error(t, err)
}

func TestGetEventTypeFromRaw(t *testing.T) {
	t.Parallel()

	require.Equal(t, "call.ended", models.GetEventType([]byte(`{"type":"call.ended"}`)))
	require.Empty(t, models.GetEventType([]byte(`not json`)))
}

// Events must survive a round trip so the SDK can re-emit what it received.
func TestParsedEventRoundTrips(t *testing.T) {
	t.Parallel()

	raw := []byte(`{"type":"call.ring","call_cid":"default:ring-me","created_at":"2026-08-15T09:15:00Z"}`)

	event, err := models.ParseWebsocketEvent(raw)
	require.NoError(t, err)

	encoded, err := json.Marshal(event)
	require.NoError(t, err)

	reparsed, err := models.ParseWebsocketEvent(encoded)
	require.NoError(t, err)
	require.Equal(t, event, reparsed)
}
