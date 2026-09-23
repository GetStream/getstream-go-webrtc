package rtc

import (
	"context"
	"testing"
	"time"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
	"github.com/GetStream/getstream-go-webrtc/internal/testutil"
)

// migrationFixture is a call joined to one fake SFU with a second one standing
// by to migrate to.
type migrationFixture struct {
	call       *Call
	from       *testutil.FakeSFU
	to         *testutil.FakeSFU
	fromCred   models.Credentials
	targetCred models.Credentials
	// old is the peer the call was using before the migration started.
	old *peer
}

func newMigrationFixture(t *testing.T, targetOpts ...testutil.FakeSFUOption) *migrationFixture {
	t.Helper()

	from := testutil.NewFakeSFU()
	t.Cleanup(from.Close)
	to := testutil.NewFakeSFU(targetOpts...)
	t.Cleanup(to.Close)

	call, _ := joinFakeSFU(t, from)
	old := call.getPeer()
	require.NotNil(t, old.publisher)
	require.NotNil(t, old.subscriber)

	fromCred := *call.cred.Load()
	return &migrationFixture{
		call:       call,
		from:       from,
		to:         to,
		fromCred:   fromCred,
		targetCred: fakeSFUCredentials(to, "sfu-migration-target", fromCred.Token),
		old:        old,
	}
}

// peersAreOpen reports whether the peer's connections are still able to carry
// media.
func peersAreOpen(p *peer) bool {
	return p.publisher.PC.ConnectionState() != webrtc.PeerConnectionStateClosed &&
		p.subscriber.PC.ConnectionState() != webrtc.PeerConnectionStateClosed
}

// TestMigrationKeepsTheOldPeerPublishingUntilTheSFUConfirms is the whole point
// of phase 5. The migration used to tear the old peer connections down before
// dialling the new SFU, which left a hole in the media for the entire join --
// several seconds on a bad network, and the join can still fail. The old peer
// now stays up, detached from the call's state but still sending and receiving,
// until the old SFU says the handover is done.
func TestMigrationKeepsTheOldPeerPublishingUntilTheSFUConfirms(t *testing.T) {
	t.Parallel()

	f := newMigrationFixture(t)

	migrated := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		migrated <- f.call.migrate(ctx, f.fromCred, f.targetCred)
	}()

	// The new SFU has been joined, so the expensive part of the migration is
	// behind us and the old peer has had every chance to be torn down.
	join, err := testutil.NextRequestOf[*sfu_events.SfuRequest_JoinRequest](f.to, 10*time.Second)
	require.NoError(t, err)
	require.Equal(t, sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_MIGRATE, join.JoinRequest.GetReconnectDetails().GetStrategy())

	require.True(t, peersAreOpen(f.old), "the old peer must keep carrying media across the migration")
	require.Equal(t, CallConnectionStateMigrating, f.call.connState.Load())
	require.NotSame(t, f.old, f.call.getPeer(), "the call should be driven by the new peer by now")

	// Only the old SFU can confirm the handover, and it is the confirmation
	// that releases the old peer.
	require.NoError(t, f.from.Send(&sfu_events.SfuEvent{
		EventPayload: &sfu_events.SfuEvent_ParticipantMigrationComplete{
			ParticipantMigrationComplete: &sfu_events.ParticipantMigrationComplete{},
		},
	}, 5*time.Second))

	select {
	case err := <-migrated:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("migrate never returned after the sfu confirmed")
	}

	require.False(t, peersAreOpen(f.old), "the old peer should be released once the migration is confirmed")
	require.Equal(t, CallConnectionStateConnected, f.call.connState.Load())
	require.True(t, peersAreOpen(f.call.getPeer()), "the new peer should be the live one")
}

// TestMigrationLeavesNothingBehindWhenTheNewSFUFails covers the failure path.
// Two peers exist at once during a migration, so a botched cleanup leaks a
// whole set of peer connections and a websocket for the lifetime of the
// process. The caller escalates to a rejoin, which builds its own peers, so
// both of these have to be gone.
func TestMigrationLeavesNothingBehindWhenTheNewSFUFails(t *testing.T) {
	t.Parallel()

	// A target that accepts the websocket and then never answers the join, so
	// the migration fails the way a wedged SFU makes it fail.
	f := newMigrationFixture(t, testutil.WithJoinHandler(
		func(*sfu_events.JoinRequest) *sfu_events.SfuEvent { return nil },
	))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := f.call.migrate(ctx, f.fromCred, f.targetCred)
	require.Error(t, err, "a migration to an sfu that never answers must fail")

	require.False(t, peersAreOpen(f.old), "the old peer must not outlive a failed migration")
	if current := f.call.getPeer(); current != nil && current.publisher != nil && current.subscriber != nil {
		require.False(t, peersAreOpen(current), "the half-built new peer must be released too")
	}
	requireClosed(t, f.from, "the old sfu websocket should be closed")
}

// TestMigrationDetachesTheOldPeerFromTheCall checks the other half of keeping
// the old peer alive: it must stop driving the call. Its websocket is still
// open and the old SFU keeps sending on it, and an event from an SFU the call
// has already moved off would otherwise reconfigure peers that belong to the
// new one.
func TestMigrationDetachesTheOldPeerFromTheCall(t *testing.T) {
	t.Parallel()

	f := newMigrationFixture(t)

	migrated := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		migrated <- f.call.migrate(ctx, f.fromCred, f.targetCred)
	}()

	_, err := testutil.NextRequestOf[*sfu_events.SfuRequest_JoinRequest](f.to, 10*time.Second)
	require.NoError(t, err)

	oldClient := f.old.client.Load()
	require.NotNil(t, oldClient)
	require.True(t, oldClient.IsDetached(), "the old signalling client should no longer dispatch to the call")

	// An error from the SFU being migrated away from must not schedule a
	// reconnect: the call has already moved on.
	before := getNextReconnectStrategy(f.call)
	require.NoError(t, f.from.Send(&sfu_events.SfuEvent{
		EventPayload: &sfu_events.SfuEvent_Error{Error: &sfu_events.Error{
			ReconnectStrategy: sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_REJOIN,
		}},
	}, 5*time.Second))
	require.Never(t, func() bool { return getNextReconnectStrategy(f.call) != before },
		500*time.Millisecond, 50*time.Millisecond,
		"an event from the old sfu should not steer the migrated call")

	require.NoError(t, f.from.Send(&sfu_events.SfuEvent{
		EventPayload: &sfu_events.SfuEvent_ParticipantMigrationComplete{
			ParticipantMigrationComplete: &sfu_events.ParticipantMigrationComplete{},
		},
	}, 5*time.Second))
	select {
	case err := <-migrated:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("migrate never returned after the sfu confirmed")
	}
}

// TestMigrationFailsWhenTheSFUNeverConfirms makes sure the confirmation is a
// real gate and not a formality. Without a timeout the call would sit in
// Migrating forever with two peers open, so a silent old SFU has to surface as
// a failure the caller can escalate on.
func TestMigrationFailsWhenTheSFUNeverConfirms(t *testing.T) {
	t.Parallel()

	f := newMigrationFixture(t)
	f.call.cc.reconnectConfig.MigrationCompleteTimeout = 300 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err := f.call.migrate(ctx, f.fromCred, f.targetCred)
	require.Error(t, err, "a migration the sfu never confirms must not be treated as done")
	require.False(t, peersAreOpen(f.old))
}

func requireClosed(t *testing.T, sfu *testutil.FakeSFU, msg string) {
	t.Helper()
	select {
	case <-sfu.Closes:
	case <-time.After(5 * time.Second):
		t.Fatal(msg)
	}
}
