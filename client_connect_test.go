package rtc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/coordinator"
	"github.com/GetStream/getstream-go-webrtc/coordinator/mocks"
	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
	"github.com/GetStream/getstream-go-webrtc/event"
	"github.com/GetStream/getstream-go-webrtc/rtcstats"
)

// newInterceptorStore is the GetInterceptor stub every mock below needs.
func newInterceptorStore() *event.Store[models.WebsocketEvent] {
	return event.NewStore(func(e models.WebsocketEvent) any { return e })
}

// newMockedClient wires a Client around a coordinator mock. Only the fields
// connectWithRetries reads are populated, so no network or websocket is
// involved.
func newMockedClient(mock *mocks.CoordinatorClientInterfaceMock) *Client {
	client := &Client{CoordinatorClientInterface: mock}
	client.ConnectionID.Store("test-connection-id")
	client.Tracing.Store(rtcstats.NewClientTraceBuffer("test"))
	return client
}

func TestConnectWithRetries(t *testing.T) {
	t.Parallel()

	t.Run("succeeds on first attempt", func(t *testing.T) {
		t.Parallel()

		mockClient := &mocks.CoordinatorClientInterfaceMock{
			JoinCallFunc: func(ctx context.Context, _type, id string, joinCallRequest models.JoinCallRequest, connectionID *string) (models.JoinCallResponse, error) {
				return models.JoinCallResponse{
					Call: models.CallResponse{ID: "test-call"},
				}, nil
			},
			GetInterceptorFunc: newInterceptorStore,
		}

		result, err := newMockedClient(mockClient).connectWithRetries(
			context.Background(),
			"test-type",
			"test-id",
			models.JoinCallRequest{},
		)

		require.NoError(t, err)
		assert.Equal(t, "test-call", result.Call.ID)
		assert.Len(t, mockClient.JoinCallCalls(), 1)
	})

	t.Run("retries on retryable error", func(t *testing.T) {
		t.Parallel()

		retryableErr := coordinator.NewError(int(sfu_models.ErrorCode_ERROR_CODE_INTERNAL_SERVER_ERROR), "Retryable error", true)

		callCount := 0
		mockClient := &mocks.CoordinatorClientInterfaceMock{
			JoinCallFunc: func(ctx context.Context, _type, id string, joinCallRequest models.JoinCallRequest, connectionID *string) (models.JoinCallResponse, error) {
				callCount++
				if callCount < 3 {
					return models.JoinCallResponse{}, retryableErr
				}
				return models.JoinCallResponse{
					Call: models.CallResponse{ID: "test-call"},
				}, nil
			},
			GetInterceptorFunc: newInterceptorStore,
		}

		result, err := newMockedClient(mockClient).connectWithRetries(
			context.Background(),
			"test-type",
			"test-id",
			models.JoinCallRequest{},
		)

		require.NoError(t, err)
		assert.Equal(t, "test-call", result.Call.ID)
		assert.Len(t, mockClient.JoinCallCalls(), 3)
	})

	t.Run("fails immediately on non-retryable error", func(t *testing.T) {
		t.Parallel()

		nonRetryableErr := coordinator.NewError(int(sfu_models.ErrorCode_ERROR_CODE_PERMISSION_DENIED), "Non-retryable error", false)

		mockClient := &mocks.CoordinatorClientInterfaceMock{
			JoinCallFunc: func(ctx context.Context, _type, id string, joinCallRequest models.JoinCallRequest, connectionID *string) (models.JoinCallResponse, error) {
				return models.JoinCallResponse{}, nonRetryableErr
			},
			GetInterceptorFunc: newInterceptorStore,
		}

		result, err := newMockedClient(mockClient).connectWithRetries(
			context.Background(),
			"test-type",
			"test-id",
			models.JoinCallRequest{},
		)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.Len(t, mockClient.JoinCallCalls(), 1)
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		t.Parallel()

		retryableErr := coordinator.NewError(int(sfu_models.ErrorCode_ERROR_CODE_INTERNAL_SERVER_ERROR), "Retryable error", true)

		// blockCh releases the mock if the test is shutting down.
		blockCh := make(chan struct{})
		callCount := 0

		mockClient := &mocks.CoordinatorClientInterfaceMock{
			JoinCallFunc: func(ctx context.Context, _type, id string, joinCallRequest models.JoinCallRequest, connectionID *string) (models.JoinCallResponse, error) {
				callCount++

				// The first call blocks until the context is cancelled.
				if callCount == 1 {
					select {
					case <-ctx.Done():
						return models.JoinCallResponse{}, ctx.Err()
					case <-blockCh:
					}
				}

				return models.JoinCallResponse{}, retryableErr
			},
			GetInterceptorFunc: newInterceptorStore,
		}

		client := newMockedClient(mockClient)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		resultCh := make(chan error, 1)
		go func() {
			result, err := client.connectWithRetries(
				ctx,
				"test-type",
				"test-id",
				models.JoinCallRequest{},
			)

			switch {
			case err == nil:
				resultCh <- errors.New("expected error, got nil")
			case result != nil:
				resultCh <- fmt.Errorf("expected nil result, got %v", result)
			default:
				resultCh <- nil
			}
		}()

		// Give the goroutine time to make the first (blocking) call.
		require.Eventually(t, func() bool {
			return len(mockClient.JoinCallCalls()) == 1
		}, time.Second, 5*time.Millisecond)

		cancel()

		select {
		case err := <-resultCh:
			require.NoError(t, err, "connectWithRetries returned unexpected error")
		case <-time.After(time.Second):
			close(blockCh) // Unblock the mock if we're timing out
			t.Fatal("test timed out waiting for connectWithRetries to return")
		}

		// Still exactly one call should have been made.
		assert.Len(t, mockClient.JoinCallCalls(), 1)
	})

	t.Run("ws handshake retries", func(t *testing.T) {
		t.Parallel()

		// Fail once with a retryable error, then succeed.
		retryErr := coordinator.NewError(int(sfu_models.ErrorCode_ERROR_CODE_INTERNAL_SERVER_ERROR), "temporary", true)
		calls := 0
		mockClient := &mocks.CoordinatorClientInterfaceMock{
			ConnectFunc: func(ctx context.Context, _ *models.WSAuthMessage) (*models.ConnectedEvent, error) {
				calls++
				if calls == 1 {
					return nil, retryErr
				}
				return &models.ConnectedEvent{ConnectionID: "conn-123"}, nil
			},
			GetInterceptorFunc: newInterceptorStore,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		resp, err := connectWsWithRetries(ctx, mockClient, &models.WSAuthMessage{})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "conn-123", resp.ConnectionID)
		assert.Len(t, mockClient.ConnectCalls(), 2)
	})

	t.Run("network error does not trigger unretryable handler", func(t *testing.T) {
		t.Parallel()

		// A plain error, NOT a coordinator.Error: it must be treated as
		// retryable, the way a connection refused or EOF would be.
		networkErr := errors.New("dial tcp 127.0.0.1:443: connect: connection refused")

		mockClient := &mocks.CoordinatorClientInterfaceMock{
			JoinCallFunc: func(ctx context.Context, _type, id string, joinCallRequest models.JoinCallRequest, connectionID *string) (models.JoinCallResponse, error) {
				return models.JoinCallResponse{}, networkErr
			},
			GetInterceptorFunc: newInterceptorStore,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()

		_, err := newMockedClient(mockClient).connectWithRetries(
			ctx,
			"test-type",
			"test-id",
			models.JoinCallRequest{},
		)

		// Should fail once the context deadline passes, having kept retrying.
		require.Error(t, err)
		assert.Greater(t, len(mockClient.JoinCallCalls()), 1, "network error should be retried")
	})

	t.Run("non-retryable coordinator error fails immediately", func(t *testing.T) {
		t.Parallel()

		nonRetryableErr := coordinator.NewError(int(sfu_models.ErrorCode_ERROR_CODE_PERMISSION_DENIED), "permission denied", false)

		mockClient := &mocks.CoordinatorClientInterfaceMock{
			JoinCallFunc: func(ctx context.Context, _type, id string, joinCallRequest models.JoinCallRequest, connectionID *string) (models.JoinCallResponse, error) {
				return models.JoinCallResponse{}, nonRetryableErr
			},
			GetInterceptorFunc: newInterceptorStore,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := newMockedClient(mockClient).connectWithRetries(
			ctx,
			"test-type",
			"test-id",
			models.JoinCallRequest{},
		)

		require.Error(t, err)
		assert.Len(t, mockClient.JoinCallCalls(), 1, "non-retryable error should not be retried")
	})
}
