package pc_test

import (
	"errors"
	"testing"

	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/pc"
)

// TestNewNegotiationError pins how a negotiation failure is reported. It is the
// error the publisher hands the application when SetPublisher is rejected, so
// which of the two sources wins -- the SFU's structured error or the local one --
// decides whether the caller sees an error code it can act on.
func TestNewNegotiationError(t *testing.T) {
	t.Parallel()

	underlying := errors.New("set remote description failed")

	tests := []struct {
		name     string
		reason   string
		err      error
		sfuErr   *sfu_models.Error
		wantCode sfu_models.ErrorCode
		wantMsg  string
		wantText string
		wantWrap error
	}{
		{
			name:     "an SFU error carries its code and message",
			reason:   "SetPublisher failed",
			sfuErr:   &sfu_models.Error{Code: sfu_models.ErrorCode_ERROR_CODE_LIVE_ENDED, Message: "call is over"},
			wantCode: sfu_models.ErrorCode_ERROR_CODE_LIVE_ENDED,
			wantMsg:  "call is over",
			wantText: "negotiation failed: SetPublisher failed (code: ERROR_CODE_LIVE_ENDED, message: call is over)",
		},
		{
			name:   "the SFU error wins over a local one",
			reason: "SetPublisher failed",
			err:    underlying,
			sfuErr: &sfu_models.Error{Code: sfu_models.ErrorCode_ERROR_CODE_PARTICIPANT_NOT_FOUND, Message: "gone"},
			// The local error is dropped rather than wrapped, so a caller
			// matching on it will not find it.
			wantCode: sfu_models.ErrorCode_ERROR_CODE_PARTICIPANT_NOT_FOUND,
			wantMsg:  "gone",
			wantText: "negotiation failed: SetPublisher failed (code: ERROR_CODE_PARTICIPANT_NOT_FOUND, message: gone)",
		},
		{
			name:     "a local error is wrapped so callers can match on it",
			reason:   "failed to apply the answer",
			err:      underlying,
			wantText: "negotiation failed: failed to apply the answer: set remote description failed",
			wantWrap: underlying,
		},
		{
			name:     "neither, just a reason",
			reason:   "no answer from the SFU",
			wantText: "negotiation failed: no answer from the SFU",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := pc.NewNegotiationError(tt.reason, tt.err, tt.sfuErr)
			require.Equal(t, tt.reason, err.Reason)
			require.Equal(t, tt.wantCode, err.Code)
			require.Equal(t, tt.wantMsg, err.Message)
			require.Equal(t, tt.wantText, err.Error())

			if tt.wantWrap != nil {
				require.ErrorIs(t, err, tt.wantWrap)
				return
			}
			require.NoError(t, errors.Unwrap(err))
		})
	}
}
