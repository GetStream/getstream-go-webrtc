package models

import (
	"bytes"
	"strconv"
	"time"

	"github.com/GetStream/getstream-go-webrtc/internal/xerr"
)

// Time is a coordinator timestamp.
//
// The spec types these fields as date-time, but the coordinator answers with epoch
// nanoseconds ("created_at": 1786819855475907000), which time.Time rejects: its
// UnmarshalJSON accepts nothing but an RFC 3339 string. So every response carrying a
// timestamp failed to decode, which for /join meant a retry loop rather than a joined call.
//
// Requests still go out as RFC 3339, which is what the coordinator accepts on the way in;
// that is the same asymmetry getstream-go's Timestamp settles on. Marshalling comes from
// the embedded time.Time, so only the reading half is spelled out here.
type Time struct {
	time.Time
}

// UnmarshalJSON accepts epoch nanoseconds or an RFC 3339 string.
func (t *Time) UnmarshalJSON(data []byte) error {
	raw := bytes.TrimSpace(data)
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	if raw[0] == '"' {
		return t.Time.UnmarshalJSON(raw)
	}

	nanos, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return xerr.Errorf("timestamp %s is neither epoch nanoseconds nor an RFC 3339 string", raw)
	}
	t.Time = time.Unix(0, nanos).UTC()
	return nil
}
