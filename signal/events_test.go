package signal_test

import (
	"context"
	"testing"
	"time"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
	"github.com/GetStream/getstream-go-webrtc/signal"
)

const awaitTimeout = 5 * time.Second

// TestEventsReachHandlersAndAwaiters walks every server-initiated event the
// migration path depends on through the real machinery: fake SFU websocket ->
// Client.readLoop -> Client.handle -> event store -> handler and awaiter.
//
// ParticipantMigrationComplete is the regression: it used to be listed in the
// Events constraint as the inner protobuf message rather than the SfuEvent oneof
// wrapper, and since both HandleEvent and the store match on
// SfuEvent.GetEventPayload(), nothing ever matched it. The other cases are
// controls -- events that demonstrably worked -- so a failure here points at the
// harness rather than at the constraint.
func TestEventsReachHandlersAndAwaiters(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU()
	defer sfu.Close()
	client := connectToFakeSFU(t, sfu)

	t.Run("ParticipantMigrationComplete", func(t *testing.T) {
		assertEventDelivered[*sfu_events.SfuEvent_ParticipantMigrationComplete](t, sfu, client,
			&sfu_events.SfuEvent{EventPayload: &sfu_events.SfuEvent_ParticipantMigrationComplete{
				ParticipantMigrationComplete: &sfu_events.ParticipantMigrationComplete{},
			}})
	})

	t.Run("SubscriberOffer", func(t *testing.T) {
		assertEventDelivered[*sfu_events.SfuEvent_SubscriberOffer](t, sfu, client,
			&sfu_events.SfuEvent{EventPayload: &sfu_events.SfuEvent_SubscriberOffer{
				SubscriberOffer: &sfu_events.SubscriberOffer{Sdp: "v=0"},
			}})
	})

	t.Run("IceTrickle", func(t *testing.T) {
		assertEventDelivered[*sfu_events.SfuEvent_IceTrickle](t, sfu, client,
			&sfu_events.SfuEvent{EventPayload: &sfu_events.SfuEvent_IceTrickle{
				IceTrickle: &sfu_models.ICETrickle{PeerType: sfu_models.PeerType_PEER_TYPE_SUBSCRIBER},
			}})
	})

	t.Run("GoAway", func(t *testing.T) {
		assertEventDelivered[*sfu_events.SfuEvent_GoAway](t, sfu, client,
			&sfu_events.SfuEvent{EventPayload: &sfu_events.SfuEvent_GoAway{
				GoAway: &sfu_events.GoAway{Reason: sfu_models.GoAwayReason_GO_AWAY_REASON_REBALANCE},
			}})
	})
}

// assertEventDelivered pushes event from the SFU and requires that both a
// HandleEvent registration and an AwaitEvent awaiter for T see it.
func assertEventDelivered[T signal.Events](t *testing.T, sfu *testutil.FakeSFU, client *signal.Client, event *sfu_events.SfuEvent) {
	t.Helper()

	handled := make(chan T, 1)
	remove := signal.HandleEvent(client, func(e T) {
		select {
		case handled <- e:
		default:
		}
	})
	defer remove()

	awaiter := signal.AwaitEvent(client, func(T) bool { return true })
	require.NoError(t, sfu.Send(event, awaitTimeout))

	got, err := awaiter.Await(awaitTimeout)
	require.NoError(t, err, "awaiter never matched the event")
	require.NotNil(t, got)

	select {
	case <-handled:
	case <-time.After(awaitTimeout):
		t.Fatal("handler never saw the event")
	}
}

func connectToFakeSFU(t *testing.T, sfu *testutil.FakeSFU) *signal.Client {
	t.Helper()

	client := signal.NewClient(models.Credentials{
		Server: models.SFUResponse{URL: sfu.URL(), WsEndpoint: sfu.WsEndpoint()},
	}, signal.NoOpHandler{})

	ctx, cancel := context.WithTimeout(context.Background(), awaitTimeout)
	defer cancel()
	_, err := client.Connect(ctx, &sfu_events.JoinRequest{SessionId: "test-session"})
	require.NoError(t, err)

	t.Cleanup(func() { _ = client.Close() })
	return client
}
