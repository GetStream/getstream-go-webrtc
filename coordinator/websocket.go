package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/gobwas/ws"

	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
	"github.com/GetStream/getstream-go-webrtc/internal/xerr"
	"github.com/GetStream/getstream-go-webrtc/websocket"
)

const (
	// pingInterval is how often a health check is sent. The connection is
	// considered dead after two intervals without one coming back.
	pingInterval = 20 * time.Second
	// readDeadline bounds how long the read loop waits for any frame.
	readDeadline = 60 * time.Second
)

type wsclient struct {
	url string
	c   *Client

	conn                 atomic.Pointer[websocket.Connection[ReadEvent, WriteEvent]]
	lastHealthCheckNanos atomic.Int64
}

func newWsClient(url string, c *Client) *wsclient {
	return &wsclient{
		url: url,
		c:   c,
	}
}

func (wsc *wsclient) pingHandler(conn *websocket.Connection[ReadEvent, WriteEvent]) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for range ticker.C {
		if time.Now().UnixNano()-wsc.lastHealthCheckNanos.Load() > int64(pingInterval*2) {
			wsc.c.logger.Warn("health check failed, closing connection")
			return
		}

		if conn.IsClosed() {
			return
		}

		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		err := conn.Write(&WriteEvent{
			Event: models.HealthCheckEvent{
				Cid: ptrTo(AnyCall),
			},
		})
		if err != nil {
			wsc.c.logger.Error("failed to send health check request", err)
		}
	}
}

func (wsc *wsclient) readLoop(conn *websocket.Connection[ReadEvent, WriteEvent]) {
	wsc.lastHealthCheckNanos.Store(time.Now().UnixNano())
	defer func() {
		_ = conn.Close()
		wsc.conn.Store(nil)
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
			wsc.c.logger.Error("failed to read message", err)
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
// automatic reconnect: callers own that policy.
func (wsc *wsclient) Connect(ctx context.Context, joinRequest *models.WSAuthMessage) (*models.ConnectedEvent, error) {
	if wsc == nil {
		return nil, xerr.Error("ws client is nil")
	}

	wsConn, _, _, err := ws.DefaultDialer.Dial(ctx, wsc.url)
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

	if err := conn.Write(&WriteEvent{Event: joinRequest}); err != nil {
		return nil, xerr.Wrapf(err, "send auth message")
	}

	response, err := conn.Read()
	if err != nil {
		return nil, xerr.Wrapf(err, "read auth response")
	}

	switch v := response.Event.(type) {
	case *models.ConnectedEvent:
		wsc.conn.Store(conn)
		go wsc.run(func() { wsc.pingHandler(conn) })
		go wsc.run(func() { wsc.readLoop(conn) })
		wsc.handle(v)
		return v, nil
	case *models.ConnectionErrorEvent:
		err = NewError(int(v.Error.Code), v.Error.Message, false)
		wsc.handle(v)
		return nil, fmt.Errorf("error response from server(%s): %w", wsc.url, err)
	default:
		return nil, fmt.Errorf("unexpected response from server(%s): %v", wsc.url, response)
	}
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
