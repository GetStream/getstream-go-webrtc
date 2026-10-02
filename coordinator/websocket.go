package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
	"github.com/GetStream/getstream-go-webrtc/internal/wsdial"
	"github.com/GetStream/getstream-go-webrtc/internal/xerr"
	"github.com/GetStream/getstream-go-webrtc/websocket"
)

const (
	// defaultHealthCheckInterval is how often a health check is sent. The coordinator
	// answers every frame with one, and closes a connection silent for 35 s.
	defaultHealthCheckInterval = 20 * time.Second
	// defaultHealthCheckTimeout is how long a health check may go unanswered.
	defaultHealthCheckTimeout = 3 * time.Second
	// readDeadline bounds how long the read loop waits for any frame.
	readDeadline = 60 * time.Second
)

type wsclient struct {
	url string
	c   *Client

	conn                 atomic.Pointer[websocket.Connection[ReadEvent, WriteEvent]]
	lastHealthCheckNanos atomic.Int64
	// closed is closed when the connection the last successful Connect opened is gone.
	closed atomic.Pointer[chan struct{}]
}

func newWsClient(url string, c *Client) *wsclient {
	return &wsclient{
		url: url,
		c:   c,
	}
}

// pingHandler sends a health check every interval. A connection that answers none for
// two intervals, or none within the timeout, is closed: that is what makes the client
// reconnect.
func (wsc *wsclient) pingHandler(conn *websocket.Connection[ReadEvent, WriteEvent], done <-chan struct{}) {
	defer conn.Close()
	ticker := time.NewTicker(wsc.c.healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
		}
		if time.Now().UnixNano()-wsc.lastHealthCheckNanos.Load() > int64(wsc.c.healthCheckInterval*2) {
			wsc.c.logger.Warn("coordinator health check failed, closing connection")
			return
		}

		_ = conn.SetReadDeadline(time.Now().Add(wsc.c.healthCheckTimeout))
		err := conn.Write(&WriteEvent{
			Event: models.HealthCheckEvent{
				Cid: ptrTo(AnyCall),
			},
		})
		if err != nil {
			wsc.c.logger.Warn("failed to send coordinator health check, closing connection", err)
			return
		}
	}
}

func (wsc *wsclient) readLoop(conn *websocket.Connection[ReadEvent, WriteEvent], done chan<- struct{}) {
	wsc.lastHealthCheckNanos.Store(time.Now().UnixNano())
	defer func() {
		_ = conn.Close()
		wsc.conn.CompareAndSwap(conn, nil)
		close(done)
	}()
	for {
		if err := conn.SetReadDeadline(time.Now().Add(readDeadline)); err != nil {
			wsc.c.logger.Error("failed to set read deadline", err)
			return
		}
		msg, err := conn.Read()
		if err != nil {
			var decError websocket.DecodeError
			if errors.As(err, &decError) {
				continue
			}
			if !conn.IsClosed() {
				wsc.c.logger.Warn("coordinator websocket closed", err)
			}
			return
		}

		if _, ok := msg.Event.(*models.HealthCheckEvent); ok {
			wsc.lastHealthCheckNanos.Store(time.Now().UnixNano())
		}
		wsc.handle(msg.Event)
	}
}

func (wsc *wsclient) handle(event models.WebsocketEvent) {
	wsc.c.interceptor.Intercept(event)
	if !dispatch(wsc.c.handler, event) {
		wsc.c.logger.Warnf("unhandled event: %v", event)
	}
}

// Connect opens the coordinator websocket, authenticates with joinRequest and
// waits for the connection.ok event that carries the connection ID. There is no
// automatic reconnect: callers own that policy, and Disconnected tells them when
// the connection is gone. Ending ctx ends a Connect in flight.
func (wsc *wsclient) Connect(ctx context.Context, joinRequest *models.WSAuthMessage) (*models.ConnectedEvent, error) {
	if wsc == nil {
		return nil, xerr.Error("ws client is nil")
	}

	wsConn, err := wsdial.Dial(ctx, wsc.url, wsc.c.dial, nil)
	if err != nil {
		return nil, xerr.Wrapf(err, "dial %s", wsc.url)
	}

	codec := websocket.NewCodec[ReadEvent, WriteEvent](
		func(s *WriteEvent) ([]byte, error) {
			encodedBytes, err := json.Marshal(s.Event)
			if err != nil {
				return nil, xerr.Wrapf(err, "encode websocket event")
			}
			return encodedBytes, nil
		},
		func(bytes []byte) (*ReadEvent, error) {
			msg, err := models.ParseWebsocketEvent(bytes)
			if err != nil {
				return nil, err
			}
			return &ReadEvent{Event: msg}, nil
		})

	conn := websocket.NewConnection[ReadEvent, WriteEvent](wsConn, true, websocket.FormatText, codec)
	// The auth exchange has no deadline of its own: closing the connection ends it.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })

	if err := conn.Write(&WriteEvent{Event: joinRequest}); err != nil {
		stop()
		_ = conn.Close()
		return nil, xerr.Wrapf(err, "send auth message")
	}

	response, err := conn.Read()
	if !stop() {
		_ = conn.Close()
		return nil, xerr.Wrapf(ctx.Err(), "read auth response")
	}
	if err != nil {
		_ = conn.Close()
		return nil, xerr.Wrapf(err, "read auth response")
	}

	switch v := response.Event.(type) {
	case *models.ConnectedEvent:
		done := make(chan struct{})
		wsc.closed.Store(&done)
		wsc.conn.Store(conn)
		go wsc.run(func() { wsc.pingHandler(conn, done) })
		go wsc.run(func() { wsc.readLoop(conn, done) })
		wsc.handle(v)
		return v, nil
	case *models.ConnectionErrorEvent:
		_ = conn.Close()
		wsc.handle(v)
		return nil, fmt.Errorf("error response from server(%s): %w", wsc.url, connectionError(v.Error))
	default:
		_ = conn.Close()
		return nil, fmt.Errorf("unexpected response from server(%s): %v", wsc.url, response)
	}
}

// connectionError is the coordinator refusing a websocket connect. Like a REST error,
// it is retryable when the server rather than the request is at fault; a timeout, as
// for the coordinator's own "try again" close code, too.
func connectionError(e models.APIError) *Error {
	status := int(e.StatusCode)
	retry := status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status/100 == 5
	err := NewError(int(e.Code), e.Message, retry)
	err.Status = status
	return err
}

// Disconnected returns a channel that is closed once the connection the last
// successful Connect opened is gone: closed by the server, by a failed health check
// or by Close. It is nil before the first successful Connect.
func (wsc *wsclient) Disconnected() <-chan struct{} {
	if wsc == nil {
		return nil
	}
	if done := wsc.closed.Load(); done != nil {
		return *done
	}
	return nil
}

// run executes fn on the current goroutine, logging any panic instead of
// taking the process down with it.
func (wsc *wsclient) run(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			wsc.c.logger.Error("panic in coordinator websocket goroutine", r, string(debug.Stack()))
		}
	}()
	fn()
}

func (wsc *wsclient) Close() error {
	conn := wsc.conn.Load()
	if conn == nil {
		return nil
	}
	return conn.Close()
}

// WriteEvent is anything the client sends on the coordinator websocket.
type WriteEvent struct {
	Event any
}

// ReadEvent is a decoded event received from the coordinator.
type ReadEvent struct {
	Event models.WebsocketEvent
}

func ptrTo[T any](v T) *T {
	return &v
}
