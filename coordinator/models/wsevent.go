package models

import (
	"encoding/json"
	"reflect"

	"github.com/GetStream/getstream-go-webrtc/internal/xerr"
)

// WebsocketEvent is implemented by every event the coordinator websocket can
// deliver. The OpenAPI spec models those events as the VideoEvent union, which
// oapi-codegen renders as an opaque json.RawMessage wrapper; this interface is
// what lets callers type switch on the concrete event instead.
//
// The GetEventType implementations live in wsevent_types.gen.go and are
// generated from the spec by internal/cmd/genwsevent.
type WebsocketEvent interface {
	GetEventType() string
}

// GetEventType reads the discriminator out of a raw event payload without
// decoding the rest of it.
func GetEventType(rawEvent []byte) string {
	var discriminator struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(rawEvent, &discriminator); err != nil {
		return ""
	}
	return discriminator.Type
}

// ParseWebsocketEvent decodes a raw websocket frame into the concrete event
// its "type" field names. Unknown event types are an error.
func ParseWebsocketEvent(rawEvent []byte) (WebsocketEvent, error) {
	var union VideoEvent
	if err := union.UnmarshalJSON(rawEvent); err != nil {
		return nil, xerr.Wrapf(err, "decode websocket event")
	}

	value, err := union.ValueByDiscriminator()
	if err != nil {
		return nil, xerr.Wrapf(err, "websocket event %q", GetEventType(rawEvent))
	}

	// ValueByDiscriminator returns the event by value; callers type switch on
	// pointers. Boxing reflectively avoids restating the spec's 70-way switch.
	boxed := reflect.New(reflect.TypeOf(value))
	boxed.Elem().Set(reflect.ValueOf(value))

	event, ok := boxed.Interface().(WebsocketEvent)
	if !ok {
		return nil, xerr.Errorf("websocket event %q does not implement WebsocketEvent", GetEventType(rawEvent))
	}
	return event, nil
}
