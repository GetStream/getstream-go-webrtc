package signal

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/GetStream/getstream-go-webrtc/internal/xerr"
	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	sfu_signal_rpc "github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"
	"github.com/gobwas/ws"
	"github.com/twitchtv/twirp"

	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
	"github.com/GetStream/getstream-go-webrtc/event"
	"github.com/GetStream/getstream-go-webrtc/internal/rtretry"
	"github.com/GetStream/getstream-go-webrtc/logger"
	"github.com/GetStream/getstream-go-webrtc/rtcstats"
	"github.com/GetStream/getstream-go-webrtc/websocket"
)

type Option func(*options)

// Health check timings. The client sends a health check every HealthCheckInterval
// and treats the connection as dead once HealthCheckTimeout has passed with no
// response. These match the Swift SDK; being roughly twice as fast as a 10s/20s
// pair to notice a dead SFU directly shortens every outage.
const (
	DefaultHealthCheckInterval = 5 * time.Second
	DefaultHealthCheckTimeout  = 15 * time.Second

	// DefaultReadTimeout is the websocket read deadline. It is the last-resort
	// backstop behind the health check, so it only needs to be comfortably
	// longer than DefaultHealthCheckTimeout.
	DefaultReadTimeout = 30 * time.Second
)

type options struct {
	reconnect   bool
	format      websocket.Format
	logger      logger.ILogger
	withTracing bool

	healthCheckInterval time.Duration
	healthCheckTimeout  time.Duration
	readTimeout         time.Duration
}

func WithLogger(l logger.ILogger) Option {
	return func(o *options) {
		o.logger = l
	}
}

func WithTracing() Option {
	return func(o *options) {
		o.withTracing = true
	}
}

// WithHealthCheck overrides how often health checks are sent and how long the
// client waits for a response before declaring the connection dead. Non-positive
// values keep the defaults.
func WithHealthCheck(interval, timeout time.Duration) Option {
	return func(o *options) {
		if interval > 0 {
			o.healthCheckInterval = interval
		}
		if timeout > 0 {
			o.healthCheckTimeout = timeout
		}
	}
}

// WithReadTimeout overrides the websocket read deadline. Non-positive values
// keep the default.
func WithReadTimeout(timeout time.Duration) Option {
	return func(o *options) {
		if timeout > 0 {
			o.readTimeout = timeout
		}
	}
}

var defaultOptions = options{
	healthCheckInterval: DefaultHealthCheckInterval,
	healthCheckTimeout:  DefaultHealthCheckTimeout,
	readTimeout:         DefaultReadTimeout,
}

var _ sfu_signal_rpc.SignalServer = (*Client)(nil)

type Client struct {
	rpc atomic.Value
	options

	signalEventStore *event.Store[*sfu_events.SfuEvent]
	handler          Handler

	lastHealthCheckNanos atomic.Int64
	conn                 atomic.Pointer[websocket.Connection[sfu_events.SfuEvent, sfu_events.SfuRequest]]
	cred                 atomic.Pointer[models.Credentials]

	disconnected atomic.Bool
	// detached stops events from reaching the Handler while still recording them
	// in the event store. Used during a migration, where the old SFU keeps
	// streaming media but must no longer drive the call's state.
	detached atomic.Bool

	Tracing atomic.Pointer[rtcstats.TraceBuffer]
}

func (c *Client) getRPC() sfu_signal_rpc.SignalServer {
	return c.rpc.Load().(sfu_signal_rpc.SignalServer)
}

func (c *Client) setRPC(rpc sfu_signal_rpc.SignalServer) {
	c.rpc.Store(rpc)
}

func (c *Client) SetPublisher(ctx context.Context, request *sfu_signal_rpc.SetPublisherRequest) (*sfu_signal_rpc.SetPublisherResponse, error) {
	c.Tracing.Load().Emit(rtcstats.SignalRPCSetPublisherEvent, request)
	resp, err := c.getRPC().SetPublisher(ctx, request)
	if err != nil {
		return resp, err
	}
	c.Tracing.Load().Emit(rtcstats.SignalRPCSetPublisherResponseEvent, resp)
	return resp, nil
}

func (c *Client) SendAnswer(ctx context.Context, request *sfu_signal_rpc.SendAnswerRequest) (*sfu_signal_rpc.SendAnswerResponse, error) {
	c.Tracing.Load().Emit(rtcstats.SignalRPCSendAnswerEvent, request)
	resp, err := c.getRPC().SendAnswer(ctx, request)
	if err != nil {
		return resp, err
	}
	c.Tracing.Load().Emit(rtcstats.SignalRPCSendAnswerResponseEvent, resp)
	return resp, nil
}

func (c *Client) IceTrickle(ctx context.Context, trickle *sfu_models.ICETrickle) (*sfu_signal_rpc.ICETrickleResponse, error) {
	c.Tracing.Load().Emit(rtcstats.SignalRPCIceTrickleEvent, trickle)
	return c.getRPC().IceTrickle(ctx, trickle)
}

func (c *Client) UpdateSubscriptions(ctx context.Context, request *sfu_signal_rpc.UpdateSubscriptionsRequest) (*sfu_signal_rpc.UpdateSubscriptionsResponse, error) {
	c.Tracing.Load().Emit(rtcstats.SignalRPCUpdateSubscriptionsEvent, request)
	resp, err := c.getRPC().UpdateSubscriptions(ctx, request)
	if err != nil {
		return resp, err
	}
	c.Tracing.Load().Emit(rtcstats.SignalRPCUpdateSubscriptionsResponseEvent, resp)
	return resp, nil
}

func (c *Client) UpdateMuteStates(ctx context.Context, request *sfu_signal_rpc.UpdateMuteStatesRequest) (*sfu_signal_rpc.UpdateMuteStatesResponse, error) {
	c.Tracing.Load().Emit(rtcstats.SignalRPCUpdateMuteStatesEvent, request)
	return c.getRPC().UpdateMuteStates(ctx, request)
}

func (c *Client) IceRestart(ctx context.Context, request *sfu_signal_rpc.ICERestartRequest) (*sfu_signal_rpc.ICERestartResponse, error) {
	c.Tracing.Load().Emit(rtcstats.SignalRPCIceRestartEvent, request)
	return c.getRPC().IceRestart(ctx, request)
}

func (c *Client) SendStats(ctx context.Context, request *sfu_signal_rpc.SendStatsRequest) (*sfu_signal_rpc.SendStatsResponse, error) {
	return c.getRPC().SendStats(ctx, request)
}

func (c *Client) SendMetrics(ctx context.Context, request *sfu_signal_rpc.SendMetricsRequest) (*sfu_signal_rpc.SendMetricsResponse, error) {
	return c.getRPC().SendMetrics(ctx, request)
}

func (c *Client) StartNoiseCancellation(ctx context.Context, request *sfu_signal_rpc.StartNoiseCancellationRequest) (*sfu_signal_rpc.StartNoiseCancellationResponse, error) {
	c.Tracing.Load().Emit(rtcstats.SignalRPCStartNoiseCancellationEvent, request)
	return c.getRPC().StartNoiseCancellation(ctx, request)
}

func (c *Client) StopNoiseCancellation(ctx context.Context, request *sfu_signal_rpc.StopNoiseCancellationRequest) (*sfu_signal_rpc.StopNoiseCancellationResponse, error) {
	c.Tracing.Load().Emit(rtcstats.SignalRPCStopNoiseCancellationEvent, request)
	return c.getRPC().StopNoiseCancellation(ctx, request)
}

func NewClient(cred models.Credentials, handler Handler, opts ...Option) *Client {
	o := defaultOptions
	for _, opt := range opts {
		opt(&o)
	}
	if o.logger == nil {
		o.logger = logger.Noop{}
	}
	client := &Client{
		options: o,
		signalEventStore: event.NewStore(func(e *sfu_events.SfuEvent) any {
			return e.GetEventPayload()
		}),
		handler: handler,
	}
	if o.withTracing {
		client.Tracing.Store(rtcstats.NewClientTraceBuffer(""))
	}
	client.setRPC(client.getSignalRPCClient(cred))
	client.cred.Store(&cred)
	return client
}

func (c *Client) getSignalRPCClient(cred models.Credentials) sfu_signal_rpc.SignalServer {
	clientCreator := sfu_signal_rpc.NewSignalServerProtobufClient
	if c.format == websocket.FormatText {
		clientCreator = sfu_signal_rpc.NewSignalServerJSONClient
	}
	rpcClient := clientCreator(
		cred.Server.URL,
		&http.Client{
			Timeout:   5 * time.Second,
			Transport: rtretry.NewRoundTripperRetryer(http.DefaultTransport),
		},
		twirp.WithClientPathPrefix(""),
		twirp.WithClientInterceptors(twirpAuthInterceptor(cred.Token)),
	)
	return rpcClient
}

func (c *Client) SetCredentials(cred models.Credentials) {
	c.cred.Store(&cred)
	c.setRPC(c.getSignalRPCClient(cred))
}

func (c *Client) Connect(ctx context.Context, joinRequest *sfu_events.JoinRequest) (*sfu_events.JoinResponse, error) {
	endpoint := c.cred.Load().Server.WsEndpoint
	wsConn, _, _, err := ws.DefaultDialer.Dial(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	c.Tracing.Load().Emit(rtcstats.SignalWSOpenEvent, map[string]any{
		"url": endpoint,
	})
	codec := websocket.NewCodec[sfu_events.SfuEvent, sfu_events.SfuRequest](
		// encode
		func(s *sfu_events.SfuRequest) ([]byte, error) {
			return s.MarshalVT()
		},
		// decode
		func(bytes []byte) (*sfu_events.SfuEvent, error) {
			var msg sfu_events.SfuEvent
			err := msg.UnmarshalVT(bytes)
			if err != nil {
				return nil, err
			}
			return &msg, nil
		})

	conn := websocket.NewConnection[sfu_events.SfuEvent, sfu_events.SfuRequest](wsConn, true, websocket.FormatBinary, codec)

	// Every path out of here other than a JoinResponse abandons conn: without
	// this the websocket stays open with nothing reading it, and a first join
	// retrying twenty times leaks twenty of them.
	joined := false
	defer func() {
		if !joined {
			_ = conn.Close()
		}
	}()

	c.Tracing.Load().Emit(rtcstats.SignalWSJoinRequestEvent, joinRequest)
	msg := &sfu_events.SfuRequest{
		RequestPayload: &sfu_events.SfuRequest_JoinRequest{
			JoinRequest: joinRequest,
		},
	}
	if err := conn.Write(msg); err != nil {
		return nil, xerr.Wrap(err)
	}

	// An SFU that accepts the websocket and then goes quiet produces no
	// transport error, so this read is the only place the caller's join
	// deadline can be enforced.
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetReadDeadline(deadline); err != nil {
			return nil, xerr.Wrap(err)
		}
	}
	response, err := conn.Read()
	if err != nil {
		return nil, xerr.Wrap(err)
	}
	// readLoop sets a deadline per read; clear this one so it is not what a
	// later read is measured against.
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return nil, xerr.Wrap(err)
	}

	switch evt := response.EventPayload.(type) {
	case *sfu_events.SfuEvent_JoinResponse:
		joined = true
		go c.readLoop(conn)
		go c.pingHandler(conn)
		// todo what to do with prev connection
		c.conn.Store(conn)
		c.handle(response)
		return response.GetJoinResponse(), nil
	case *sfu_events.SfuEvent_Error:
		c.handler.RawHandler(response)
		c.handler.OnError(evt)
		merr := response.GetError().GetError()
		err = NewError(merr.Code, merr.Message, merr.ShouldRetry)
		c.handle(response)
		return nil, fmt.Errorf("error response from server(%s): %w", endpoint, err)
	default:
		return nil, fmt.Errorf("unexpected response from server(%s): %v", endpoint, response)
	}
}

func (c *Client) GetConnection() *websocket.Connection[sfu_events.SfuEvent, sfu_events.SfuRequest] {
	return c.conn.Load()
}

func (c *Client) pingHandler(conn *websocket.Connection[sfu_events.SfuEvent, sfu_events.SfuRequest]) {
	defer conn.Close()
	ticker := time.NewTicker(c.healthCheckInterval)
	defer ticker.Stop()

	for range ticker.C {
		// Closing the connection is what wakes the call's health monitor, so the
		// sooner a silent SFU is detected the sooner the reconnect starts.
		if time.Since(time.Unix(0, c.lastHealthCheckNanos.Load())) > c.healthCheckTimeout {
			c.logger.Warn("sfu health check failed, closing connection")
			return
		}

		if conn.IsClosed() {
			return
		}

		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		err := conn.Write(&sfu_events.SfuRequest{
			RequestPayload: &sfu_events.SfuRequest_HealthCheckRequest{
				HealthCheckRequest: &sfu_events.HealthCheckRequest{},
			},
		})
		//
		if err != nil {
			c.logger.Error("failed to send health check request", err)
		}
	}
}

func (c *Client) readLoop(conn *websocket.Connection[sfu_events.SfuEvent, sfu_events.SfuRequest]) {
	c.lastHealthCheckNanos.Store(time.Now().UnixNano())
	defer func() {
		_ = conn.Close()
		// Only clear the pointer if it still refers to this connection. A
		// reconnect may already have installed a newer one, and blindly storing
		// nil would make the healthy connection look lost.
		c.conn.CompareAndSwap(conn, nil)
	}()
	for {
		if err := conn.SetReadDeadline(time.Now().Add(c.readTimeout)); err != nil {
			c.logger.Error("failed to set read deadline", xerr.Wrap(err))
			return
		}
		msg, err := conn.Read()
		if err != nil {
			var decError websocket.DecodeError
			if errors.As(err, &decError) {
				continue
			}
			if !c.disconnected.Load() {
				c.logger.Error("failed to read message", xerr.Wrap(err))
			}
			return
		}

		if _, ok := msg.GetEventPayload().(*sfu_events.SfuEvent_HealthCheckResponse); ok {
			c.lastHealthCheckNanos.Store(time.Now().UnixNano())
		}
		c.handle(msg)
	}
}

func (c *Client) handle(msg *sfu_events.SfuEvent) {
	// A handler panic would otherwise take down readLoop's goroutine, and with it
	// the whole process. Log which event caused it so the panic is actionable.
	defer func() {
		if r := recover(); r != nil {
			c.logger.Errorw("panic handling sfu event", fmt.Errorf("%v", r),
				"event", fmt.Sprintf("%T", msg.GetEventPayload()),
				"stack", string(debug.Stack()))
		}
	}()
	if c.signalEventStore != nil {
		c.signalEventStore.Intercept(msg)
	}
	// The store is fed first on purpose: a detached client still has to deliver
	// ParticipantMigrationComplete to whoever is awaiting it.
	if c.detached.Load() {
		return
	}
	c.handler.RawHandler(msg)
	switch m := msg.EventPayload.(type) {
	case *sfu_events.SfuEvent_HealthCheckResponse:
		c.handler.OnHealthCheckResponse(m)
	case *sfu_events.SfuEvent_JoinResponse:
		c.Tracing.Load().Emit(rtcstats.SignalWSJoinResponseEvent, m.JoinResponse)
		c.handler.OnJoinResponse(m)
	case *sfu_events.SfuEvent_Error:
		c.Tracing.Load().Emit(rtcstats.SignalWSErrorEvent, m.Error)
		c.handler.OnError(m)
	case *sfu_events.SfuEvent_CallEnded:
		c.Tracing.Load().Emit(rtcstats.SignalWSCallEndedEvent, m.CallEnded)
		c.handler.OnCallEnded(m)
	case *sfu_events.SfuEvent_CallGrantsUpdated:
		c.Tracing.Load().Emit(rtcstats.SignalWSCallGrantsUpdatedEvent, m.CallGrantsUpdated)
		c.handler.OnCallGrantsUpdated(m)
	case *sfu_events.SfuEvent_GoAway:
		c.Tracing.Load().Emit(rtcstats.SignalWSGoAwayEvent, m.GoAway)
		c.handler.OnGoAway(m)
	case *sfu_events.SfuEvent_IceRestart:
		c.Tracing.Load().Emit(rtcstats.SignalWSIceRestartEvent, m.IceRestart)
		c.handler.OnIceRestart(m)
	case *sfu_events.SfuEvent_PinsUpdated:
		c.handler.OnPinsUpdated(m)
	case *sfu_events.SfuEvent_ParticipantMigrationComplete:
		// Consumed through the event store by Call.migrate, which awaits it;
		// there is no Handler callback for it.
		c.logger.Debug("participant migration complete")
	case *sfu_events.SfuEvent_SubscriberOffer:
		c.Tracing.Load().Emit(rtcstats.SignalWSSubscriberOfferEvent, m.SubscriberOffer)
		c.handler.OnSubscriberOffer(m)
	case *sfu_events.SfuEvent_PublisherAnswer:
		c.Tracing.Load().Emit(rtcstats.SignalWSPublisherAnswerEvent, m.PublisherAnswer)
		c.handler.OnPublisherAnswer(m)
	case *sfu_events.SfuEvent_ConnectionQualityChanged:
		c.Tracing.Load().Emit(rtcstats.SignalWSConnectionQualityChangedEvent, m.ConnectionQualityChanged)
		c.handler.OnConnectionQualityChanged(m)
	case *sfu_events.SfuEvent_AudioLevelChanged:
		c.handler.OnAudioLevelChanged(m)
	case *sfu_events.SfuEvent_IceTrickle:
		c.Tracing.Load().Emit(rtcstats.SignalWSIceTrickleEvent, m.IceTrickle)
		c.handler.OnIceTrickle(m)
	case *sfu_events.SfuEvent_ChangePublishQuality:
		c.Tracing.Load().Emit(rtcstats.SignalWSChangePublishQualityEvent, m.ChangePublishQuality)
		c.handler.OnChangePublishQuality(m)
	case *sfu_events.SfuEvent_ChangePublishOptions:
		c.handler.OnChangePublishOptions(m)
	case *sfu_events.SfuEvent_InboundStateNotification:
		c.handler.OnInboundStateNotification(m)
	case *sfu_events.SfuEvent_ParticipantJoined:
		c.Tracing.Load().Emit(rtcstats.SignalWSParticipantJoinedEvent, m.ParticipantJoined)
		c.handler.OnParticipantJoined(m)
	case *sfu_events.SfuEvent_ParticipantLeft:
		c.Tracing.Load().Emit(rtcstats.SignalWSParticipantLeftEvent, m.ParticipantLeft)
		c.handler.OnParticipantLeft(m)
	case *sfu_events.SfuEvent_DominantSpeakerChanged:
		c.handler.OnDominantSpeakerChanged(m)
	case *sfu_events.SfuEvent_TrackPublished:
		c.Tracing.Load().Emit(rtcstats.SignalWSTrackPublishedEvent, m.TrackPublished)
		c.handler.OnTrackPublished(m)
	case *sfu_events.SfuEvent_TrackUnpublished:
		c.Tracing.Load().Emit(rtcstats.SignalWSTrackUnpublishedEvent, m.TrackUnpublished)
		c.handler.OnTrackUnpublished(m)
	default:
		c.logger.Warnf("unexpected message: %v", msg)
	}
}

// SendLeaveCallRequest tells the SFU the participant is leaving gracefully.
//
// sessionID must be the session ID of the current session, i.e. the one sent in
// JoinRequest.SessionId and in every signalling RPC. It changes on a REJOIN, so
// the caller passes it per request rather than the client caching it.
func (c *Client) SendLeaveCallRequest(sessionID, reason string) error {
	conn := c.conn.Load()
	if conn == nil {
		return nil
	}
	leaveRequest := &sfu_events.SfuRequest{
		RequestPayload: &sfu_events.SfuRequest_LeaveCallRequest{
			LeaveCallRequest: &sfu_events.LeaveCallRequest{
				SessionId: sessionID,
				Reason:    reason,
			},
		},
	}
	if err := conn.Write(leaveRequest); err != nil {
		return err
	}
	return nil
}

// Detach stops delivering events to the Handler. The websocket stays open and
// the event store keeps recording, so awaiters still fire. This is what lets a
// migration keep the old SFU's media flowing while the new one takes over the
// call's state.
func (c *Client) Detach() {
	c.detached.Store(true)
}

// IsDetached reports whether Detach has been called.
func (c *Client) IsDetached() bool {
	return c.detached.Load()
}

func (c *Client) Close() error {
	conn := c.conn.Load()
	if conn == nil {
		return nil
	}
	return conn.Close()
}

func (c *Client) Disconnect(forGood bool) error {
	if forGood {
		c.disconnected.Store(true)
	}
	// Drop the handle first so GetConnection reports the disconnect even if
	// the close frame races the read loop already tearing the socket down.
	conn := c.conn.Swap(nil)
	if conn == nil {
		return nil
	}
	err := conn.Disconnect(forGood)
	if err != nil && conn.IsClosed() {
		return nil
	}
	return err
}

func twirpAuthInterceptor(token string) twirp.Interceptor {
	return func(next twirp.Method) twirp.Method {
		return func(ctx context.Context, req any) (any, error) {
			headers := http.Header{
				"Authorization": []string{"Bearer " + token},
			}
			ctx, err := twirp.WithHTTPRequestHeaders(ctx, headers)
			if err != nil {
				return nil, err
			}
			return next(ctx, req)
		}
	}
}
