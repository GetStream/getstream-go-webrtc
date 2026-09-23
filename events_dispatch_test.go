package rtc

import (
	"testing"
	"time"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
)

// TestCallEventDispatchIsExhaustive registers a handler for every type the
// CallEvents constraint admits and requires that each one reaches a store.
//
// The switch behind HandleCallEvent used to be hand-maintained while the
// constraint was generated, so 19 of the 78 admitted types fell through to a
// panic. Both sides are generated now; this is what fails if one of them is
// hand-edited, or regenerated without the other.
func TestCallEventDispatchIsExhaustive(t *testing.T) {
	t.Parallel()

	call := GetDummyCall(t, "dispatch-user", false)

	require.NotEmpty(t, callEventDispatchCases, "no dispatch cases generated")
	for _, c := range callEventDispatchCases {
		unregister, ok := c.Register(call)
		require.Truef(t, ok, "HandleCallEvent does not dispatch %s; rerun ./generate.sh", c.Type)
		require.NotNilf(t, unregister, "%s: dispatch returned no unregister function", c.Type)
		unregister()
	}
}

// TestHandleCallEventDeliversBothFamilies drives one event from each family
// through HandleCallEvent, using two types the hand-written switch used to panic
// on. Registering is not enough on its own: the handler has to land on the store
// that actually carries the event.
func TestHandleCallEventDeliversBothFamilies(t *testing.T) {
	t.Parallel()

	sfu := testutil.NewFakeSFU()
	defer sfu.Close()
	call := getCallOnFakeSFU(t, sfu)

	dtmf := make(chan *models.CallDTMFEvent, 1)
	removeDTMF := HandleCallEvent(call, func(e *models.CallDTMFEvent) { dtmf <- e })
	defer removeDTMF()

	migrated := make(chan *sfu_events.SfuEvent_ParticipantMigrationComplete, 1)
	removeMigrated := HandleCallEvent(call, func(e *sfu_events.SfuEvent_ParticipantMigrationComplete) {
		migrated <- e
	})
	defer removeMigrated()

	call.cc.GetInterceptor().Intercept(&models.CallDTMFEvent{CallCid: call.CID()})
	require.NoError(t, sfu.Send(&sfu_events.SfuEvent{
		EventPayload: &sfu_events.SfuEvent_ParticipantMigrationComplete{
			ParticipantMigrationComplete: &sfu_events.ParticipantMigrationComplete{},
		},
	}, 5*time.Second))

	select {
	case <-dtmf:
	case <-time.After(5 * time.Second):
		t.Fatal("coordinator event never reached the handler")
	}

	select {
	case <-migrated:
	case <-time.After(5 * time.Second):
		t.Fatal("signal event never reached the handler")
	}
}
