package signal

import (
	sfuevent "github.com/GetStream/protocol/protobuf/video/sfu/event"

	"github.com/GetStream/getstream-go-webrtc/event"
)

// The Events constraint is generated into events.gen.go from the SfuEvent oneof.

type RemoveHandler func()

func HandleEvent[T Events](client *Client, onEvent func(T)) RemoveHandler {
	handler := func(e *sfuevent.SfuEvent) {
		if t, ok := e.GetEventPayload().(T); ok {
			onEvent(t)
		}
	}
	return RemoveHandler(client.signalEventStore.AddInterceptor(handler))
}

func AwaitEvent[T Events](client *Client, match func(T) bool) *event.EventAwaiter[T, *sfuevent.SfuEvent] {
	return event.NewEventAwaiter(client.signalEventStore, match)
}
