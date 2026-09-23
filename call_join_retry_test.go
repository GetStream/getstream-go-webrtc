package rtc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/gobwas/ws"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/coordinator"
	"github.com/GetStream/getstream-go-webrtc/signal"
)

// testNetError implements net.Error for testing purposes.
type testNetError struct {
	timeout   bool
	temporary bool
}

func (e *testNetError) Error() string   { return "test net error" }
func (e *testNetError) Timeout() bool   { return e.timeout }
func (e *testNetError) Temporary() bool { return e.temporary } //nolint:staticcheck

// Compile-time check that testNetError implements net.Error.
var _ net.Error = (*testNetError)(nil)

func TestIsJoinErrorRetryable(t *testing.T) {
	t.Parallel()

	call := &Call{}

	tests := []struct {
		name      string
		err       error
		retryable bool
	}{
		// nil
		{
			name:      "nil error is not retryable",
			err:       nil,
			retryable: false,
		},

		// context errors
		{
			name:      "context.DeadlineExceeded is retryable",
			err:       context.DeadlineExceeded,
			retryable: true,
		},
		{
			name:      "wrapped context.DeadlineExceeded is retryable",
			err:       fmt.Errorf("timeout: %w", context.DeadlineExceeded),
			retryable: true,
		},
		{
			name:      "context.Canceled is not retryable",
			err:       context.Canceled,
			retryable: false,
		},
		{
			name:      "wrapped context.Canceled is not retryable",
			err:       fmt.Errorf("cancelled: %w", context.Canceled),
			retryable: false,
		},

		// net.Error
		{
			name:      "net.Error timeout is retryable",
			err:       &testNetError{timeout: true},
			retryable: true,
		},
		{
			name:      "net.Error non-timeout is retryable",
			err:       &testNetError{timeout: false},
			retryable: true,
		},
		{
			name:      "wrapped net.Error is retryable",
			err:       fmt.Errorf("connect: %w", &testNetError{timeout: true}),
			retryable: true,
		},

		// ws.StatusError
		{
			name:      "ws 503 Service Unavailable is retryable",
			err:       ws.StatusError(503),
			retryable: true,
		},
		{
			name:      "ws 502 Bad Gateway is retryable",
			err:       ws.StatusError(502),
			retryable: true,
		},
		{
			name:      "ws 504 Gateway Timeout is retryable",
			err:       ws.StatusError(504),
			retryable: true,
		},
		{
			name:      "ws 500 Internal Server Error is not retryable",
			err:       ws.StatusError(500),
			retryable: false,
		},
		{
			name:      "ws 401 Unauthorized is not retryable",
			err:       ws.StatusError(401),
			retryable: false,
		},
		{
			name:      "ws 403 Forbidden is not retryable",
			err:       ws.StatusError(403),
			retryable: false,
		},
		{
			name:      "wrapped ws 503 is retryable",
			err:       fmt.Errorf("dial failed: %w", ws.StatusError(503)),
			retryable: true,
		},
		{
			name:      "wrapped ws 401 is not retryable",
			err:       fmt.Errorf("dial failed: %w", ws.StatusError(401)),
			retryable: false,
		},

		// coordinator.Error
		{
			name:      "coordinator error with ShouldRetry=true is retryable",
			err:       coordinator.NewError(int(sfu_models.ErrorCode_ERROR_CODE_INTERNAL_SERVER_ERROR), "server error", true),
			retryable: true,
		},
		{
			name:      "coordinator error with ShouldRetry=false is not retryable",
			err:       coordinator.NewError(int(sfu_models.ErrorCode_ERROR_CODE_PERMISSION_DENIED), "denied", false),
			retryable: false,
		},
		{
			name:      "wrapped coordinator error with ShouldRetry=true is retryable",
			err:       fmt.Errorf("getCred: %w", coordinator.NewError(int(sfu_models.ErrorCode_ERROR_CODE_INTERNAL_SERVER_ERROR), "server error", true)),
			retryable: true,
		},
		{
			name:      "wrapped coordinator error with ShouldRetry=false is not retryable",
			err:       fmt.Errorf("getCred: %w", coordinator.NewError(int(sfu_models.ErrorCode_ERROR_CODE_PERMISSION_DENIED), "denied", false)),
			retryable: false,
		},

		// signal.Error
		{
			name:      "signal error is always retryable",
			err:       signal.NewError(sfu_models.ErrorCode_ERROR_CODE_INTERNAL_SERVER_ERROR, "sfu error", false),
			retryable: true,
		},
		{
			name:      "signal error with ShouldRetry=true is retryable",
			err:       signal.NewError(sfu_models.ErrorCode_ERROR_CODE_INTERNAL_SERVER_ERROR, "sfu error", true),
			retryable: true,
		},
		{
			name:      "wrapped signal error is retryable",
			err:       fmt.Errorf("signal: %w", signal.NewError(sfu_models.ErrorCode_ERROR_CODE_INTERNAL_SERVER_ERROR, "sfu error", false)),
			retryable: true,
		},

		// unknown errors
		{
			name:      "unknown error is not retryable",
			err:       errors.New("something unexpected"),
			retryable: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.retryable, call.isJoinErrorRetryable(tt.err))
		})
	}
}
