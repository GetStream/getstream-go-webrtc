package netdelay

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testRTT = 100 * time.Millisecond

// requireRTT checks a measured round trip is testRTT within 5%.
func requireRTT(t *testing.T, got time.Duration, what string) {
	t.Helper()
	require.InDelta(t, float64(testRTT), float64(got), float64(testRTT)*0.05,
		"%s took %v, want %v ± 5%%", what, got, testRTT)
}

func tcpEchoServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return ln.Addr().String()
}

func TestTCPConnectAndEchoTakeOneRTTEach(t *testing.T) {
	t.Parallel()
	addr := tcpEchoServer(t)

	start := time.Now()
	conn, err := Dialer(testRTT, nil)(context.Background(), "tcp", addr)
	require.NoError(t, err)
	defer conn.Close()
	requireRTT(t, time.Since(start), "connect")

	for i := range 3 {
		start = time.Now()
		_, err = conn.Write([]byte("ping"))
		require.NoError(t, err)
		buf := make([]byte, 4)
		_, err = io.ReadFull(conn, buf)
		require.NoError(t, err)
		requireRTT(t, time.Since(start), "echo")
		require.Equal(t, "ping", string(buf), "round %d", i)
	}
}

// pion-ice dials TURN over TCP with DialTCP, not Dial.
func TestNetDialTCPConnectAndEchoTakeOneRTTEach(t *testing.T) {
	t.Parallel()
	addr, err := net.ResolveTCPAddr("tcp4", tcpEchoServer(t))
	require.NoError(t, err)
	n, err := NewNet(testRTT)
	require.NoError(t, err)

	start := time.Now()
	conn, err := n.DialTCP("tcp4", nil, addr)
	require.NoError(t, err)
	defer conn.Close()
	requireRTT(t, time.Since(start), "connect")

	start = time.Now()
	_, err = conn.Write([]byte("ping"))
	require.NoError(t, err)
	buf := make([]byte, 4)
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	requireRTT(t, time.Since(start), "echo")
	require.Equal(t, "ping", string(buf))
}

func TestTCPKeepsOrderAcrossManyWrites(t *testing.T) {
	t.Parallel()
	conn, err := Dialer(testRTT, nil)(context.Background(), "tcp", tcpEchoServer(t))
	require.NoError(t, err)
	defer conn.Close()

	const n = 500
	go func() {
		for i := range n {
			var b [4]byte
			binary.BigEndian.PutUint32(b[:], uint32(i))
			_, _ = conn.Write(b[:])
		}
	}()
	for i := range n {
		var b [4]byte
		_, err := io.ReadFull(conn, b[:])
		require.NoError(t, err)
		require.Equal(t, uint32(i), binary.BigEndian.Uint32(b[:]))
	}
}

func TestReadDeadlineInterruptsAHeldRead(t *testing.T) {
	t.Parallel()
	conn, err := Dialer(testRTT, nil)(context.Background(), "tcp", tcpEchoServer(t))
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(10*time.Millisecond)))
	_, err = conn.Read(make([]byte, 1))
	require.ErrorIs(t, err, os.ErrDeadlineExceeded)
	var netErr net.Error
	require.ErrorAs(t, err, &netErr)
	require.True(t, netErr.Timeout())

	// The data sent before the deadline is not lost by it.
	require.NoError(t, conn.SetReadDeadline(time.Time{}))
	_, err = conn.Write([]byte("x"))
	require.NoError(t, err)
	b := make([]byte, 1)
	_, err = io.ReadFull(conn, b)
	require.NoError(t, err)
	require.Equal(t, "x", string(b))
}

func TestUDPEchoTakesOneRTT(t *testing.T) {
	t.Parallel()
	server, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer server.Close()
	go func() {
		buf := make([]byte, 1500)
		for {
			n, addr, err := server.ReadFrom(buf)
			if err != nil {
				return
			}
			_, _ = server.WriteTo(buf[:n], addr)
		}
	}()

	n, err := NewNet(testRTT)
	require.NoError(t, err)
	client, err := n.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer client.Close()

	for i := range 3 {
		start := time.Now()
		_, err = client.WriteTo([]byte{byte(i)}, server.LocalAddr())
		require.NoError(t, err)
		buf := make([]byte, 1500)
		got, from, err := client.ReadFrom(buf)
		require.NoError(t, err)
		requireRTT(t, time.Since(start), "udp echo")
		require.Equal(t, []byte{byte(i)}, buf[:got])
		require.Equal(t, server.LocalAddr().String(), from.String())
	}
}

func TestUDPKeepsOrder(t *testing.T) {
	t.Parallel()
	server, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer server.Close()

	client, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	delayed := NewPacketConn(client, testRTT)
	defer delayed.Close()

	const n = 200
	for i := range n {
		_, err := delayed.WriteTo([]byte{byte(i)}, server.LocalAddr())
		require.NoError(t, err)
	}
	buf := make([]byte, 16)
	for i := range n {
		require.NoError(t, server.SetReadDeadline(time.Now().Add(5*time.Second)))
		got, _, err := server.ReadFrom(buf)
		require.NoError(t, err)
		require.Equal(t, []byte{byte(i)}, buf[:got])
	}
}
