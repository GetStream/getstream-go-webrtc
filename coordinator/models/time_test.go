package models_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
)

func TestTimeReadsEpochNanoseconds(t *testing.T) {
	t.Parallel()

	var stamp models.Time
	require.NoError(t, json.Unmarshal([]byte("1786819855475907000"), &stamp))
	require.Equal(t, time.Unix(0, 1786819855475907000).UTC(), stamp.Time)
}

func TestTimeReadsRFC3339(t *testing.T) {
	t.Parallel()

	var stamp models.Time
	require.NoError(t, json.Unmarshal([]byte(`"2026-08-15T09:13:00Z"`), &stamp))
	require.Equal(t, time.Date(2026, 8, 15, 9, 13, 0, 0, time.UTC), stamp.Time)
}

func TestTimeLeavesNullAsTheZeroTime(t *testing.T) {
	t.Parallel()

	var stamp models.Time
	require.NoError(t, json.Unmarshal([]byte("null"), &stamp))
	require.True(t, stamp.IsZero())
}

func TestTimeRejectsWhatIsNeitherForm(t *testing.T) {
	t.Parallel()

	var stamp models.Time
	require.Error(t, json.Unmarshal([]byte("{}"), &stamp))
}

func TestTimeIsSentAsRFC3339(t *testing.T) {
	t.Parallel()

	stamp := models.Time{Time: time.Date(2026, 8, 15, 9, 13, 0, 0, time.UTC)}
	encoded, err := json.Marshal(stamp)
	require.NoError(t, err)
	require.JSONEq(t, `"2026-08-15T09:13:00Z"`, string(encoded))
}

// The coordinator answers /join with epoch nanoseconds, which used to fail to decode and
// leave Client.connectWithRetries retrying a request that had already succeeded.
func TestJoinCallResponseDecodesEpochNanoseconds(t *testing.T) {
	t.Parallel()

	raw := []byte(`{
		"created": true,
		"duration": "42ms",
		"call": {
			"id": "my-call",
			"type": "default",
			"cid": "default:my-call",
			"created_at": 1786819855475907000,
			"updated_at": 1786819855475907000,
			"ended_at": null,
			"backstage": false,
			"blocked_user_ids": [],
			"captioning": false,
			"recording": false,
			"transcribing": false,
			"current_session_id": "session-1",
			"custom": {},
			"egress": {"broadcasting": false, "rtmps": [], "frame_recording": {"status": "off"}},
			"ingress": {"rtmp": {"address": ""}},
			"settings": {},
			"created_by": {
				"id": "thierry",
				"created_at": 1786819855278887000,
				"updated_at": 1786819855278887000,
				"custom": {},
				"language": "en",
				"role": "user",
				"teams": [],
				"blocked_user_ids": []
			}
		},
		"members": [],
		"own_capabilities": [],
		"blocked_users": [],
		"credentials": {
			"token": "sfu-token",
			"ice_servers": [],
			"server": {"edge_name": "ams1", "url": "https://sfu.example/twirp", "ws_endpoint": "wss://sfu.example/ws"}
		},
		"stats_options": {"reporting_interval_ms": 10000}
	}`)

	var response models.JoinCallResponse
	require.NoError(t, json.Unmarshal(raw, &response))
	require.Equal(t, "default:my-call", response.Call.Cid)
	require.Equal(t, time.Unix(0, 1786819855475907000).UTC(), response.Call.CreatedAt.Time)
	require.Nil(t, response.Call.EndedAt)
	require.Equal(t, "sfu-token", response.Credentials.Token)
}
