package testutil

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	sfu_signal_rpc "github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"
	"github.com/gobwas/ws"
	"github.com/twitchtv/twirp"

	"github.com/GetStream/getstream-go-webrtc/internal/xerr"
	"github.com/GetStream/getstream-go-webrtc/websocket"
)

// FakeSFU is the SFU signalling surface, reduced to what a client needs to
// complete a join and negotiate: the websocket that answers a JoinRequest with a
// JoinResponse and a HealthCheckRequest with a HealthCheckResponse, plus the
// twirp SignalServer RPCs. Everything the client sends is recorded for the test
// to assert on, and the fake can push arbitrary SfuEvents back.
//
// The zero configuration is a cooperative SFU: joins succeed with an empty call
// state and every RPC answers with an empty successful response. Use the
// With... options to make it uncooperative.
type FakeSFU struct {
	srv *httptest.Server

	// Requests receives every SfuRequest the client sends over the websocket,
	// in order. Read it with NextRequest or NextRequestOf.
	Requests chan *sfu_events.SfuRequest

	// RPCRequests receives every twirp signalling request the client sends, in
	// order. Read it with NextRPCRequest.
	RPCRequests chan any

	// Authorizations receives the Authorization header of every twirp request,
	// which is where the SFU token the client was handed has to end up.
	Authorizations chan string

	// Connects receives once per accepted websocket connection, so a test can
	// tell a reconnect happened, and Closes once per connection that goes away
	// again, so a test can tell the client dropped one.
	Connects chan struct{}
	Closes   chan struct{}

	joinResponse  *sfu_events.JoinResponse
	onJoinRequest func(*sfu_events.JoinRequest) *sfu_events.SfuEvent
	rpc           SignalRPC

	mu   sync.Mutex
	conn *websocket.Connection[sfu_events.SfuRequest, sfu_events.SfuEvent]

	connected chan struct{}
	once      sync.Once
}

// FakeSFUOption configures a FakeSFU. Options are applied before the server
// accepts anything, so the fake's behaviour is fixed by the time a client can
// observe it.
type FakeSFUOption func(*FakeSFU)

// WithJoinResponse sets the JoinResponse the fake answers a JoinRequest with.
func WithJoinResponse(resp *sfu_events.JoinResponse) FakeSFUOption {
	return func(f *FakeSFU) {
		f.joinResponse = resp
	}
}

// WithJoinHandler replaces the whole answer to a JoinRequest, so a test can
// answer with an SfuEvent_Error, answer conditionally on the request, or return
// nil to answer nothing at all and let the client's join time out.
func WithJoinHandler(handler func(*sfu_events.JoinRequest) *sfu_events.SfuEvent) FakeSFUOption {
	return func(f *FakeSFU) {
		f.onJoinRequest = handler
	}
}

// WithSignalRPC overrides the twirp signalling RPC answers.
func WithSignalRPC(rpc SignalRPC) FakeSFUOption {
	return func(f *FakeSFU) {
		f.rpc = rpc
	}
}

// SignalRPC answers the twirp signalling RPCs. A nil field answers with an empty
// successful response, which is what the SFU sends when it simply accepts a
// request.
type SignalRPC struct {
	SetPublisher           func(context.Context, *sfu_signal_rpc.SetPublisherRequest) (*sfu_signal_rpc.SetPublisherResponse, error)
	SendAnswer             func(context.Context, *sfu_signal_rpc.SendAnswerRequest) (*sfu_signal_rpc.SendAnswerResponse, error)
	IceTrickle             func(context.Context, *sfu_models.ICETrickle) (*sfu_signal_rpc.ICETrickleResponse, error)
	UpdateSubscriptions    func(context.Context, *sfu_signal_rpc.UpdateSubscriptionsRequest) (*sfu_signal_rpc.UpdateSubscriptionsResponse, error)
	UpdateMuteStates       func(context.Context, *sfu_signal_rpc.UpdateMuteStatesRequest) (*sfu_signal_rpc.UpdateMuteStatesResponse, error)
	IceRestart             func(context.Context, *sfu_signal_rpc.ICERestartRequest) (*sfu_signal_rpc.ICERestartResponse, error)
	SendStats              func(context.Context, *sfu_signal_rpc.SendStatsRequest) (*sfu_signal_rpc.SendStatsResponse, error)
	SendMetrics            func(context.Context, *sfu_signal_rpc.SendMetricsRequest) (*sfu_signal_rpc.SendMetricsResponse, error)
	StartNoiseCancellation func(context.Context, *sfu_signal_rpc.StartNoiseCancellationRequest) (*sfu_signal_rpc.StartNoiseCancellationResponse, error)
	StopNoiseCancellation  func(context.Context, *sfu_signal_rpc.StopNoiseCancellationRequest) (*sfu_signal_rpc.StopNoiseCancellationResponse, error)
}

// NewFakeSFU starts a fake SFU. Close it when the test is done.
func NewFakeSFU(opts ...FakeSFUOption) *FakeSFU {
	f := &FakeSFU{
		Requests:       make(chan *sfu_events.SfuRequest, 64),
		RPCRequests:    make(chan any, 64),
		Connects:       make(chan struct{}, 16),
		Closes:         make(chan struct{}, 16),
		Authorizations: make(chan string, 64),
		joinResponse:   &sfu_events.JoinResponse{},
		connected:      make(chan struct{}),
	}
	for _, opt := range opts {
		opt(f)
	}

	// The signal client dials WsEndpoint for the websocket and posts the twirp
	// RPCs to URL with an empty path prefix, so the two share one server and
	// are told apart by path.
	mux := http.NewServeMux()
	mux.HandleFunc(wsPath, f.serve)
	mux.Handle("/", f.recordAuthorization(sfu_signal_rpc.NewSignalServerServer(
		&signalRPCService{f: f}, twirp.WithServerPathPrefix(""))))
	f.srv = httptest.NewServer(mux)
	return f
}

const wsPath = "/ws"

// WsEndpoint is the ws:// URL a signal client dials.
func (f *FakeSFU) WsEndpoint() string {
	return "ws" + strings.TrimPrefix(f.srv.URL, "http") + wsPath
}

// URL is the base URL for the twirp RPCs.
func (f *FakeSFU) URL() string {
	return f.srv.URL
}

// Close shuts the server down and drops the client connection.
func (f *FakeSFU) Close() {
	f.mu.Lock()
	conn := f.conn
	f.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	f.srv.Close()
}

// CloseConnection drops the current websocket connection without stopping the
// server, so the client sees its signal connection die and can re-dial.
func (f *FakeSFU) CloseConnection() error {
	f.mu.Lock()
	conn := f.conn
	f.mu.Unlock()
	if conn == nil {
		return xerr.Error("no client connected to the fake sfu")
	}
	return conn.Close()
}

// Send pushes an event to the connected client, waiting up to timeout for a
// client to show up.
//
// It writes to the most recent connection. After a reconnect, wait for that
// connection's JoinRequest before sending: the fake records a request only once
// the connection carrying it is installed.
func (f *FakeSFU) Send(event *sfu_events.SfuEvent, timeout time.Duration) error {
	select {
	case <-f.connected:
	case <-time.After(timeout):
		return xerr.Error("no client connected to the fake sfu")
	}

	f.mu.Lock()
	conn := f.conn
	f.mu.Unlock()
	if conn == nil {
		return xerr.Error("fake sfu connection is gone")
	}
	return conn.Write(event)
}

// NextRequest returns the next request the client sent, or an error if none
// arrives within timeout.
func (f *FakeSFU) NextRequest(timeout time.Duration) (*sfu_events.SfuRequest, error) {
	select {
	case req := <-f.Requests:
		return req, nil
	case <-time.After(timeout):
		return nil, xerr.Error("timed out waiting for an sfu request")
	}
}

// NextRequestOf returns the payload of the next websocket request of type T,
// skipping the health checks and anything else that arrives in between.
func NextRequestOf[T any](f *FakeSFU, timeout time.Duration) (T, error) {
	var zero T
	deadline := time.After(timeout)
	for {
		select {
		case req := <-f.Requests:
			if payload, ok := req.GetRequestPayload().(T); ok {
				return payload, nil
			}
		case <-deadline:
			return zero, xerr.Errorf("timed out waiting for an sfu request of type %T", zero)
		}
	}
}

// NextRPCRequest returns the next twirp signalling request of type T, skipping
// the other RPCs that arrive in between.
func NextRPCRequest[T any](f *FakeSFU, timeout time.Duration) (T, error) {
	var zero T
	deadline := time.After(timeout)
	for {
		select {
		case req := <-f.RPCRequests:
			if typed, ok := req.(T); ok {
				return typed, nil
			}
		case <-deadline:
			return zero, xerr.Errorf("timed out waiting for an rpc request of type %T", zero)
		}
	}
}

func (f *FakeSFU) serve(w http.ResponseWriter, r *http.Request) {
	netConn, _, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		return
	}

	codec := websocket.NewCodec[sfu_events.SfuRequest, sfu_events.SfuEvent](
		func(e *sfu_events.SfuEvent) ([]byte, error) { return e.MarshalVT() },
		func(b []byte) (*sfu_events.SfuRequest, error) {
			var msg sfu_events.SfuRequest
			if err := msg.UnmarshalVT(b); err != nil {
				return nil, err
			}
			return &msg, nil
		},
	)
	conn := websocket.NewConnection[sfu_events.SfuRequest, sfu_events.SfuEvent](netConn, false, websocket.FormatBinary, codec)
	defer func() {
		_ = conn.Close()
		select {
		case f.Closes <- struct{}{}:
		default:
		}
	}()

	f.mu.Lock()
	f.conn = conn
	f.mu.Unlock()
	f.once.Do(func() { close(f.connected) })
	select {
	case f.Connects <- struct{}{}:
	default:
	}

	for {
		req, err := conn.Read()
		if err != nil {
			return
		}

		select {
		case f.Requests <- req:
		default:
		}

		switch payload := req.GetRequestPayload().(type) {
		case *sfu_events.SfuRequest_JoinRequest:
			answer := f.joinAnswer(payload.JoinRequest)
			if answer == nil {
				continue
			}
			err = conn.Write(answer)
		case *sfu_events.SfuRequest_HealthCheckRequest:
			err = conn.Write(&sfu_events.SfuEvent{
				EventPayload: &sfu_events.SfuEvent_HealthCheckResponse{
					HealthCheckResponse: &sfu_events.HealthCheckResponse{
						ParticipantCount: &sfu_models.ParticipantCount{},
					},
				},
			})
		}
		if err != nil {
			return
		}
	}
}

func (f *FakeSFU) joinAnswer(req *sfu_events.JoinRequest) *sfu_events.SfuEvent {
	if f.onJoinRequest != nil {
		return f.onJoinRequest(req)
	}
	return &sfu_events.SfuEvent{
		EventPayload: &sfu_events.SfuEvent_JoinResponse{JoinResponse: f.joinResponse},
	}
}

func (f *FakeSFU) recordAuthorization(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case f.Authorizations <- r.Header.Get("Authorization"):
		default:
		}
		next.ServeHTTP(w, r)
	})
}

func (f *FakeSFU) recordRPC(req any) {
	select {
	case f.RPCRequests <- req:
	default:
	}
}

// signalRPCService serves the twirp SignalServer, recording every request and
// delegating to the test's overrides.
type signalRPCService struct {
	f *FakeSFU
}

var _ sfu_signal_rpc.SignalServer = (*signalRPCService)(nil)

func (s *signalRPCService) SetPublisher(ctx context.Context, req *sfu_signal_rpc.SetPublisherRequest) (*sfu_signal_rpc.SetPublisherResponse, error) {
	s.f.recordRPC(req)
	if h := s.f.rpc.SetPublisher; h != nil {
		return h(ctx, req)
	}
	return &sfu_signal_rpc.SetPublisherResponse{}, nil
}

func (s *signalRPCService) SendAnswer(ctx context.Context, req *sfu_signal_rpc.SendAnswerRequest) (*sfu_signal_rpc.SendAnswerResponse, error) {
	s.f.recordRPC(req)
	if h := s.f.rpc.SendAnswer; h != nil {
		return h(ctx, req)
	}
	return &sfu_signal_rpc.SendAnswerResponse{}, nil
}

func (s *signalRPCService) IceTrickle(ctx context.Context, req *sfu_models.ICETrickle) (*sfu_signal_rpc.ICETrickleResponse, error) {
	s.f.recordRPC(req)
	if h := s.f.rpc.IceTrickle; h != nil {
		return h(ctx, req)
	}
	return &sfu_signal_rpc.ICETrickleResponse{}, nil
}

func (s *signalRPCService) UpdateSubscriptions(ctx context.Context, req *sfu_signal_rpc.UpdateSubscriptionsRequest) (*sfu_signal_rpc.UpdateSubscriptionsResponse, error) {
	s.f.recordRPC(req)
	if h := s.f.rpc.UpdateSubscriptions; h != nil {
		return h(ctx, req)
	}
	return &sfu_signal_rpc.UpdateSubscriptionsResponse{}, nil
}

func (s *signalRPCService) UpdateMuteStates(ctx context.Context, req *sfu_signal_rpc.UpdateMuteStatesRequest) (*sfu_signal_rpc.UpdateMuteStatesResponse, error) {
	s.f.recordRPC(req)
	if h := s.f.rpc.UpdateMuteStates; h != nil {
		return h(ctx, req)
	}
	return &sfu_signal_rpc.UpdateMuteStatesResponse{}, nil
}

func (s *signalRPCService) IceRestart(ctx context.Context, req *sfu_signal_rpc.ICERestartRequest) (*sfu_signal_rpc.ICERestartResponse, error) {
	s.f.recordRPC(req)
	if h := s.f.rpc.IceRestart; h != nil {
		return h(ctx, req)
	}
	return &sfu_signal_rpc.ICERestartResponse{}, nil
}

func (s *signalRPCService) SendStats(ctx context.Context, req *sfu_signal_rpc.SendStatsRequest) (*sfu_signal_rpc.SendStatsResponse, error) {
	s.f.recordRPC(req)
	if h := s.f.rpc.SendStats; h != nil {
		return h(ctx, req)
	}
	return &sfu_signal_rpc.SendStatsResponse{}, nil
}

func (s *signalRPCService) SendMetrics(ctx context.Context, req *sfu_signal_rpc.SendMetricsRequest) (*sfu_signal_rpc.SendMetricsResponse, error) {
	s.f.recordRPC(req)
	if h := s.f.rpc.SendMetrics; h != nil {
		return h(ctx, req)
	}
	return &sfu_signal_rpc.SendMetricsResponse{}, nil
}

func (s *signalRPCService) StartNoiseCancellation(ctx context.Context, req *sfu_signal_rpc.StartNoiseCancellationRequest) (*sfu_signal_rpc.StartNoiseCancellationResponse, error) {
	s.f.recordRPC(req)
	if h := s.f.rpc.StartNoiseCancellation; h != nil {
		return h(ctx, req)
	}
	return &sfu_signal_rpc.StartNoiseCancellationResponse{}, nil
}

func (s *signalRPCService) StopNoiseCancellation(ctx context.Context, req *sfu_signal_rpc.StopNoiseCancellationRequest) (*sfu_signal_rpc.StopNoiseCancellationResponse, error) {
	s.f.recordRPC(req)
	if h := s.f.rpc.StopNoiseCancellation; h != nil {
		return h(ctx, req)
	}
	return &sfu_signal_rpc.StopNoiseCancellationResponse{}, nil
}
