package rtc

import (
	"reflect"
	"testing"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/signal"
)

// TestEverySfuEventIsSafeOnACallWithoutPeers calls every signal.Handler method
// on a Call whose peer connections do not exist.
//
// That is the state a call is in for the whole of a reconnect, and events keep
// arriving on the websocket throughout it. They are all dispatched on the
// signalling read loop, so a handler that panics -- as three of them used to --
// takes down the goroutine and with it the embedding process. A server-side SDK
// must never do that.
//
// The cases are derived from the interface's method set rather than listed by
// hand, so a handler added for a new SFU event is covered the moment Call
// implements it.
func TestEverySfuEventIsSafeOnACallWithoutPeers(t *testing.T) {
	t.Parallel()

	call := newUnconnectedCall(t)
	require.Nil(t, call.publisherPeer(), "the fixture must have no peers, or this proves nothing")
	require.Nil(t, call.subscriberPeer())

	handlerType := reflect.TypeOf((*signal.Handler)(nil)).Elem()
	callValue := reflect.ValueOf(signal.Handler(call))

	require.Positive(t, handlerType.NumMethod())
	for i := range handlerType.NumMethod() {
		method := handlerType.Method(i)
		require.Equal(t, 1, method.Type.NumIn(), "%s: expected a single event argument", method.Name)

		// Two shapes reach handlers in practice: the oneof wrapper with its
		// payload present, and -- from a malformed or truncated message -- the
		// wrapper with a nil payload.
		for _, populated := range []bool{true, false} {
			name := method.Name
			if !populated {
				name += "/nil_payload"
			}
			t.Run(name, func(t *testing.T) {
				arg := newEventArg(method.Type.In(0), populated)
				require.NotPanics(t, func() {
					callValue.MethodByName(method.Name).Call([]reflect.Value{arg})
				})
			})
		}
	}
}

// newEventArg builds an argument for a signal.Handler method: a pointer to the
// oneof wrapper struct, with its single payload field allocated when populated.
func newEventArg(argType reflect.Type, populated bool) reflect.Value {
	if argType.Kind() != reflect.Pointer || argType.Elem().Kind() != reflect.Struct {
		return reflect.Zero(argType)
	}
	arg := reflect.New(argType.Elem())
	if !populated {
		return arg
	}
	elem := arg.Elem()
	for i := range elem.NumField() {
		field := elem.Field(i)
		if field.Kind() == reflect.Pointer && field.CanSet() {
			field.Set(reflect.New(field.Type().Elem()))
		}
	}
	return arg
}

// TestSubscriberOfferWithoutASubscriberIsDropped is the specific case that used
// to panic outright: an offer racing the teardown a reconnect performs.
func TestSubscriberOfferWithoutASubscriberIsDropped(t *testing.T) {
	t.Parallel()

	call := newUnconnectedCall(t)

	require.NotPanics(t, func() {
		call.OnSubscriberOffer(&sfu_events.SfuEvent_SubscriberOffer{
			SubscriberOffer: &sfu_events.SubscriberOffer{Sdp: "v=0"},
		})
	})
}

// TestIceTrickleWithoutPeersIsDropped covers the nil dereference the trickle
// handler used to perform, for both peer types.
func TestIceTrickleWithoutPeersIsDropped(t *testing.T) {
	t.Parallel()

	call := newUnconnectedCall(t)

	for _, peerType := range []sfu_models.PeerType{
		sfu_models.PeerType_PEER_TYPE_SUBSCRIBER,
		sfu_models.PeerType_PEER_TYPE_PUBLISHER_UNSPECIFIED,
	} {
		require.NotPanics(t, func() {
			call.OnIceTrickle(&sfu_events.SfuEvent_IceTrickle{
				IceTrickle: &sfu_models.ICETrickle{
					PeerType:     peerType,
					IceCandidate: `{"candidate":"candidate:1 1 udp 2130706431 127.0.0.1 1234 typ host"}`,
				},
			})
		}, peerType.String())
	}
}

// TestPeerOperationsWithoutPeersReturnErrors: operations that cannot be
// satisfied without a peer connection must say so rather than crash.
func TestPeerOperationsWithoutPeersReturnErrors(t *testing.T) {
	t.Parallel()

	call := newUnconnectedCall(t)

	_, err := call.AddTrack(dummyAudioTrackInfo(), nil)
	require.ErrorIs(t, err, errNoPeerConnection)

	_, err = call.AddSimulcastTracks(dummyAudioTrackInfo())
	require.ErrorIs(t, err, errNoPeerConnection)

	require.ErrorIs(t, call.SendSubscriberRTCP(nil), errNoPeerConnection)
	require.ErrorIs(t, call.restorePublishedTracks(), errNoPeerConnection)
	require.Nil(t, call.PublisherPC())
	require.Nil(t, call.SubscriberPC())
}
