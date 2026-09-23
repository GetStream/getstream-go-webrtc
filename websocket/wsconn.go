package websocket

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"

	"github.com/GetStream/getstream-go-webrtc/internal/xerr"
)

type Format int

const (
	FormatBinary Format = iota
	FormatText
)

// Connection is a websocket connection that reads values of type R and writes
// values of type W through a Codec.
type Connection[R, W any] struct {
	mu             sync.Mutex
	closed         atomic.Bool
	c              net.Conn
	isClient       bool
	format         Format
	codec          Codec[R, W]
	disconnectChan chan struct{}
}

func NewConnection[R, W any](c net.Conn, isClient bool, format Format, codec Codec[R, W]) *Connection[R, W] {
	return &Connection[R, W]{
		c:              c,
		isClient:       isClient,
		format:         format,
		codec:          codec,
		disconnectChan: make(chan struct{}, 1),
	}
}

func (w *Connection[R, W]) IsClosed() bool {
	return w.closed.Load()
}

func (w *Connection[R, W]) Close() error {
	if w.closed.CompareAndSwap(false, true) {
		close(w.disconnectChan)
		return w.c.Close()
	}
	return nil
}

// Liveness returns a channel that is closed when the connection goes away.
func (w *Connection[R, W]) Liveness() chan struct{} {
	return w.disconnectChan
}

// Disconnect closes the connection, optionally sending a close frame first so
// the peer knows the client is leaving for good rather than reconnecting.
func (w *Connection[R, W]) Disconnect(forGood bool) error {
	if w.IsClosed() {
		return xerr.Error("connection already closed")
	}

	if forGood {
		w.mu.Lock()
		defer w.mu.Unlock()
		f := ws.NewCloseFrame(ws.NewCloseFrameBody(ws.StatusGoingAway, "user disconnected"))
		// Only a client masks; a masked frame from a server is a protocol error
		// the peer must reject (RFC 6455 section 5.1).
		if w.isClient {
			f = ws.MaskFrameInPlace(f)
		}
		if err := ws.WriteFrame(w.c, f); err != nil {
			return xerr.Wrapf(err, "write close frame")
		}
	}

	return w.Close()
}

func (w *Connection[R, W]) SetWriteDeadline(t time.Time) error {
	return w.c.SetWriteDeadline(t)
}

func (w *Connection[R, W]) SetReadDeadline(t time.Time) error {
	return w.c.SetReadDeadline(t)
}

func (w *Connection[R, W]) Write(msg *W) error {
	out, err := w.codec.Encode(msg)
	if err != nil {
		return xerr.Wrapf(err, "encode message")
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	var frame ws.Frame
	switch w.format {
	case FormatText:
		frame = ws.NewTextFrame(out)
	case FormatBinary:
		frame = ws.NewBinaryFrame(out)
	}
	if w.isClient {
		frame = ws.MaskFrameInPlace(frame)
	}

	if err := ws.WriteFrame(w.c, frame); err != nil {
		return xerr.Wrapf(err, "write frame")
	}
	return nil
}

// DecodeError wraps a codec failure so callers can tell an undecodable message
// apart from a dead connection and keep reading.
type DecodeError struct {
	error
}

func (w *Connection[R, W]) Read() (*R, error) {
	state := ws.StateServerSide
	if w.isClient {
		state = ws.StateClientSide
	}

	for {
		hdr, r, err := wsutil.NextReader(w.c, state)
		if err != nil {
			return nil, xerr.Wrapf(err, "next frame")
		}
		switch hdr.OpCode {
		case ws.OpClose:
			return nil, io.EOF

		case ws.OpPing, ws.OpPong:
			continue

		case ws.OpBinary, ws.OpText:
			payload, err := io.ReadAll(r)
			if err != nil {
				return nil, err
			}
			msg, decErr := w.codec.Decode(payload)
			if decErr != nil {
				return nil, DecodeError{error: xerr.Wrapf(decErr, "decode message")}
			}
			return msg, nil

		default:
			continue
		}
	}
}
