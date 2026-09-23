package rtc

import (
	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"google.golang.org/protobuf/proto"
)

// PublishOptions returns the encoding configuration the SFU wants this client to
// publish with: codec, target bitrate, frame rate, and how many spatial and
// temporal layers to produce.
//
// This SDK does not own an encoder -- the application supplies already-encoded
// samples -- so the options are advisory. Read them at join time, and register
// OnPublishOptionsChanged to be told when the SFU changes its mind mid-call.
func (c *Call) PublishOptions() []*sfu_models.PublishOption {
	c.publishOptionsMu.Lock()
	defer c.publishOptionsMu.Unlock()
	return clonePublishOptions(c.publishOptions)
}

// OnPublishOptionsChanged registers a callback fired whenever the SFU sends new
// publish options, either in the join response or through a mid-call
// ChangePublishOptions event. The handler runs on the signalling read loop, so
// it must not block.
func (c *Call) OnPublishOptionsChanged(handler func([]*sfu_models.PublishOption)) {
	c.publishOptionsMu.Lock()
	defer c.publishOptionsMu.Unlock()
	c.publishOptionsHandler = handler
}

// OnChangePublishOptions records the SFU's new encoding configuration and hands
// it to the application.
//
// Swift reconciles this by cloning tracks onto a transceiver per publish option,
// so it can publish the same source under two codecs at once. That needs the SDK
// to own the encoder, which this one does not, so it reports the change and
// leaves the response to the application. Dual-codec publishing is a follow-up.
func (c *Call) OnChangePublishOptions(options *sfu_events.SfuEvent_ChangePublishOptions) {
	change := options.ChangePublishOptions
	c.setPublishOptions(change.GetPublishOptions(), change.GetReason())
}

// setPublishOptions stores new options and notifies the application. reason is
// logged so it is clear whether they came from the join or from a mid-call
// change.
func (c *Call) setPublishOptions(opts []*sfu_models.PublishOption, reason string) {
	if len(opts) == 0 {
		return
	}
	stored := clonePublishOptions(opts)

	c.publishOptionsMu.Lock()
	c.publishOptions = stored
	handler := c.publishOptionsHandler
	c.publishOptionsMu.Unlock()

	c.logger.WithFields(map[string]any{
		"count":  len(stored),
		"reason": reason,
	}).Debug("publish options updated")

	if handler != nil {
		handler(clonePublishOptions(stored))
	}
}

// clonePublishOptions deep-copies the options so callers cannot mutate the
// call's copy, and vice versa.
func clonePublishOptions(opts []*sfu_models.PublishOption) []*sfu_models.PublishOption {
	if opts == nil {
		return nil
	}
	out := make([]*sfu_models.PublishOption, 0, len(opts))
	for _, o := range opts {
		out = append(out, proto.Clone(o).(*sfu_models.PublishOption))
	}
	return out
}
