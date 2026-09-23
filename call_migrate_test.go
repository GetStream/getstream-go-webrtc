package rtc

import (
	"testing"
	"time"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
)

// TestAwaitMigrationCompleteFires drives a ParticipantMigrationComplete from a
// fake SFU through the signal read loop and the event store into the awaiter
// Call.migrate blocks on. The awaiter has to match the type the store keys on --
// the SfuEvent oneof wrapper, not the inner message -- or every MIGRATE
// reconnect burns its timeout and escalates to REJOIN.
func TestAwaitMigrationCompleteFires(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU()
	defer sfu.Close()

	call := getCallOnFakeSFU(t, sfu)

	awaiter := awaitMigrationCompleteOn(call.Client())
	require.NoError(t, sfu.Send(&sfu_events.SfuEvent{
		EventPayload: &sfu_events.SfuEvent_ParticipantMigrationComplete{
			ParticipantMigrationComplete: &sfu_events.ParticipantMigrationComplete{},
		},
	}, 5*time.Second))

	event, err := awaiter.Await(5 * time.Second)
	require.NoError(t, err, "migration completion awaiter never fired")
	require.NotNil(t, event)
}
