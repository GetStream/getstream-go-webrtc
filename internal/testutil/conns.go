package testutil

import (
	"net"
	"sync"
	"sync/atomic"
)

// ConnCounter is a listener that counts the connections it accepts and those still open.
type ConnCounter struct {
	net.Listener
	accepted, open atomic.Int64
}

// CountConns wraps l.
func CountConns(l net.Listener) *ConnCounter { return &ConnCounter{Listener: l} }

func (l *ConnCounter) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.accepted.Add(1)
	l.open.Add(1)
	return &countedConn{Conn: conn, l: l}, nil
}

// Accepted is how many connections were accepted.
func (l *ConnCounter) Accepted() int64 { return l.accepted.Load() }

// Open is how many accepted connections are not closed yet.
func (l *ConnCounter) Open() int64 { return l.open.Load() }

type countedConn struct {
	net.Conn
	l    *ConnCounter
	once sync.Once
}

func (c *countedConn) Close() error {
	c.once.Do(func() { c.l.open.Add(-1) })
	return c.Conn.Close()
}
