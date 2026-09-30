// Package wsdial opens client websockets the way net/http opens connections: through a
// replaceable dial function, and reporting DNS, TCP, TLS, request and first-byte times
// to the net/http/httptrace.ClientTrace in the context.
package wsdial

import (
	"context"
	"crypto/tls"
	"net"
	"net/http/httptrace"
	"sync/atomic"

	"github.com/gobwas/ws"
)

// DialFunc opens a network connection, like net.Dialer.DialContext.
type DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error)

// Dial opens a websocket to urlstr. A nil dial uses a plain net.Dialer; a nil tlsConfig
// the system roots. Like net/http, it leaves reporting DNS and connect times to dial.
func Dial(ctx context.Context, urlstr string, dial DialFunc, tlsConfig *tls.Config) (net.Conn, error) {
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	trace := httptrace.ContextClientTrace(ctx)
	if trace == nil {
		trace = &httptrace.ClientTrace{}
	}
	d := ws.Dialer{
		// The dial function reports DNS and connect to the trace, as it does for net/http.
		NetDial: dial,
		TLSClient: func(conn net.Conn, hostname string) net.Conn {
			cfg := &tls.Config{}
			if tlsConfig != nil {
				cfg = tlsConfig.Clone()
			}
			if cfg.ServerName == "" {
				cfg.ServerName = hostname
			}
			tc := tls.Client(conn, cfg)
			if trace.TLSHandshakeStart != nil {
				trace.TLSHandshakeStart()
			}
			// A failed handshake is reported again by the upgrade's first write.
			err := tc.HandshakeContext(ctx)
			if trace.TLSHandshakeDone != nil {
				trace.TLSHandshakeDone(tc.ConnectionState(), err)
			}
			return tc
		},
		WrapConn: func(conn net.Conn) net.Conn {
			if trace.GotConn != nil {
				trace.GotConn(httptrace.GotConnInfo{Conn: conn})
			}
			return &tracedConn{Conn: conn, trace: trace}
		},
	}
	conn, _, _, err := d.Dial(ctx, urlstr)
	return conn, err
}

// tracedConn reports the upgrade request's write and the response's first byte.
type tracedConn struct {
	net.Conn
	trace *httptrace.ClientTrace
	wrote atomic.Bool
	read  atomic.Bool
}

func (c *tracedConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if !c.wrote.Load() && c.wrote.CompareAndSwap(false, true) && c.trace.WroteRequest != nil {
		c.trace.WroteRequest(httptrace.WroteRequestInfo{Err: err})
	}
	return n, err
}

func (c *tracedConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 && !c.read.Load() && c.read.CompareAndSwap(false, true) && c.trace.GotFirstResponseByte != nil {
		c.trace.GotFirstResponseByte()
	}
	return n, err
}
