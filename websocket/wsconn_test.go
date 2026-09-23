package websocket_test

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/websocket"
)

// message is the payload both ends of the test connection exchange. The real
// SDK uses protobuf on the SFU socket and JSON on the coordinator socket; JSON
// keeps the fixtures readable and exercises the same codec seam.
type message struct {
	Type string `json:"type"`
	Body string `json:"body"`
}

func jsonCodec() websocket.Codec[message, message] {
	return websocket.NewCodec(
		func(m *message) ([]byte, error) { return json.Marshal(m) },
		func(b []byte) (*message, error) {
			var m message
			if err := json.Unmarshal(b, &m); err != nil {
				return nil, err
			}
			return &m, nil
		},
	)
}

// serverConn is the server half of a live websocket, handed to the test once a
// client has connected.
type serverConn struct {
	conn *websocket.Connection[message, message]
	raw  net.Conn
}

// dial starts an httptest server that upgrades one connection, and returns the
// client and server halves of it. Both are closed when the test ends.
//
// internal/testutil.FakeSFU is the module's shared websocket double, but it
// speaks the SFU's protobuf request/event protocol and lives in a package that
// imports this one, so it cannot serve as the harness for the transport itself.
func dial(t *testing.T, format websocket.Format) (*websocket.Connection[message, message], *serverConn) {
	t.Helper()

	accepted := make(chan *serverConn, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		netConn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		accepted <- &serverConn{
			conn: websocket.NewConnection(netConn, false, format, jsonCodec()),
			raw:  netConn,
		}
		// Hold the handler open; the connection outlives the HTTP request.
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)

	netConn, _, _, err := ws.Dial(t.Context(), "ws"+strings.TrimPrefix(srv.URL, "http"))
	require.NoError(t, err)

	client := websocket.NewConnection(netConn, true, format, jsonCodec())
	t.Cleanup(func() { _ = client.Close() })

	var server *serverConn
	select {
	case server = <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("server never accepted the connection")
	}
	t.Cleanup(func() { _ = server.conn.Close() })

	return client, server
}

func TestRoundTripBothDirections(t *testing.T) {
	t.Parallel()

	client, server := dial(t, websocket.FormatBinary)

	require.NoError(t, client.Write(&message{Type: "join", Body: "from client"}))
	got, err := server.conn.Read()
	require.NoError(t, err)
	require.Equal(t, &message{Type: "join", Body: "from client"}, got)

	require.NoError(t, server.conn.Write(&message{Type: "join.response", Body: "from server"}))
	got, err = client.Read()
	require.NoError(t, err)
	require.Equal(t, &message{Type: "join.response", Body: "from server"}, got)
}

// The coordinator socket is text, the SFU socket is binary; both must survive
// the round trip and the client half must mask its frames either way.
func TestRoundTripPerFormat(t *testing.T) {
	t.Parallel()

	for name, format := range map[string]websocket.Format{
		"binary": websocket.FormatBinary,
		"text":   websocket.FormatText,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			client, server := dial(t, format)

			require.NoError(t, client.Write(&message{Type: "health.check"}))
			got, err := server.conn.Read()
			require.NoError(t, err)
			require.Equal(t, "health.check", got.Type)
		})
	}
}

func TestReadSkipsPingAndPong(t *testing.T) {
	t.Parallel()

	client, server := dial(t, websocket.FormatBinary)

	require.NoError(t, ws.WriteFrame(server.raw, ws.NewPingFrame(nil)))
	require.NoError(t, ws.WriteFrame(server.raw, ws.NewPongFrame(nil)))
	require.NoError(t, server.conn.Write(&message{Type: "after.pings"}))

	got, err := client.Read()
	require.NoError(t, err)
	require.Equal(t, "after.pings", got.Type, "control frames must not surface as messages")
}

// The server half must send an unmasked close frame, which the client reads as
// a graceful end of stream rather than as a protocol error.
func TestReadReportsCloseFrameFromTheServerAsEOF(t *testing.T) {
	t.Parallel()

	client, server := dial(t, websocket.FormatBinary)

	require.NoError(t, server.conn.Disconnect(true))

	_, err := client.Read()
	require.ErrorIs(t, err, io.EOF)
}

// An undecodable payload must be distinguishable from a dead connection, and
// must not cost the caller the connection: the read loop keeps going.
func TestUndecodableMessageIsADecodeErrorAndTheConnectionSurvives(t *testing.T) {
	t.Parallel()

	client, server := dial(t, websocket.FormatBinary)

	require.NoError(t, ws.WriteFrame(server.raw, ws.NewBinaryFrame([]byte("not json"))))

	_, err := client.Read()
	var decodeErr websocket.DecodeError
	require.ErrorAs(t, err, &decodeErr)
	require.ErrorContains(t, err, "decode message")
	require.False(t, client.IsClosed())

	require.NoError(t, server.conn.Write(&message{Type: "still.alive"}))
	got, err := client.Read()
	require.NoError(t, err)
	require.Equal(t, "still.alive", got.Type)
}

func TestReadReportsTransportFailureNotDecodeError(t *testing.T) {
	t.Parallel()

	client, server := dial(t, websocket.FormatBinary)

	// A dropped TCP connection, as opposed to a graceful close frame.
	require.NoError(t, server.raw.Close())

	_, err := client.Read()
	require.Error(t, err)
	var decodeErr websocket.DecodeError
	require.False(t, errors.As(err, &decodeErr), "a transport failure is not a decode failure")
}

func TestWriteSurfacesEncodeFailures(t *testing.T) {
	t.Parallel()

	failing := websocket.NewCodec(
		func(*message) ([]byte, error) { return nil, errors.New("boom") },
		func([]byte) (*message, error) { return nil, errors.New("unused") },
	)

	client, _ := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })

	conn := websocket.NewConnection(client, true, websocket.FormatBinary, failing)
	err := conn.Write(&message{Type: "join"})
	require.ErrorContains(t, err, "encode message")
	require.ErrorContains(t, err, "boom")
}

func TestCloseIsIdempotentAndClosesLiveness(t *testing.T) {
	t.Parallel()

	client, _ := dial(t, websocket.FormatBinary)

	require.False(t, client.IsClosed())
	select {
	case <-client.Liveness():
		t.Fatal("liveness must stay open while the connection is up")
	default:
	}

	require.NoError(t, client.Close())
	require.True(t, client.IsClosed())
	select {
	case <-client.Liveness():
	case <-time.After(time.Second):
		t.Fatal("liveness must be closed once the connection is gone")
	}

	// monitorHealth and Leave can both close the same connection; the second
	// close must neither error nor close the liveness channel twice.
	require.NotPanics(t, func() { require.NoError(t, client.Close()) })
}

func TestDisconnectForGoodSendsACloseFrame(t *testing.T) {
	t.Parallel()

	client, server := dial(t, websocket.FormatBinary)

	require.NoError(t, client.Disconnect(true))
	require.True(t, client.IsClosed())

	_, err := server.conn.Read()
	require.ErrorIs(t, err, io.EOF, "the peer must see a close frame, not a dropped socket")
}

func TestDisconnectWithoutCloseFrame(t *testing.T) {
	t.Parallel()

	client, _ := dial(t, websocket.FormatBinary)

	require.NoError(t, client.Disconnect(false))
	require.True(t, client.IsClosed())
}

func TestDisconnectOnAClosedConnectionErrors(t *testing.T) {
	t.Parallel()

	client, _ := dial(t, websocket.FormatBinary)

	require.NoError(t, client.Close())
	require.ErrorContains(t, client.Disconnect(true), "already closed")
	require.ErrorContains(t, client.Disconnect(false), "already closed")
}

func TestReadDeadlineExpires(t *testing.T) {
	t.Parallel()

	client, _ := dial(t, websocket.FormatBinary)

	require.NoError(t, client.SetReadDeadline(time.Now().Add(50*time.Millisecond)))

	_, err := client.Read()
	require.Error(t, err)
	var netErr net.Error
	require.ErrorAs(t, err, &netErr)
	require.True(t, netErr.Timeout())
}

func TestWriteDeadlineIsSetOnTheUnderlyingConnection(t *testing.T) {
	t.Parallel()

	client, _ := dial(t, websocket.FormatBinary)

	require.NoError(t, client.SetWriteDeadline(time.Now().Add(time.Hour)))
	require.NoError(t, client.Write(&message{Type: "join"}))

	// A deadline in the past fails the very next write.
	require.NoError(t, client.SetWriteDeadline(time.Now().Add(-time.Second)))
	require.Error(t, client.Write(&message{Type: "join"}))
}

// Every SDK send goes through one Connection shared by the health-check ticker
// and the caller, so frames must not interleave.
func TestConcurrentWritesProduceIntactFrames(t *testing.T) {
	t.Parallel()

	client, server := dial(t, websocket.FormatBinary)

	const writers = 8
	errs := make(chan error, writers)
	for i := range writers {
		go func() {
			errs <- client.Write(&message{Type: "health.check", Body: strings.Repeat("x", 100+i)})
		}()
	}
	for range writers {
		require.NoError(t, <-errs)
	}

	for range writers {
		got, err := server.conn.Read()
		require.NoError(t, err)
		require.Equal(t, "health.check", got.Type)
		require.GreaterOrEqual(t, len(got.Body), 100)
	}
}
