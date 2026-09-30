// Package netdelay makes a local connection behave like one across a network with a given
// round-trip time: every write is held for half the RTT before it is sent and every read
// is held for half the RTT after it arrived, and a TCP connect takes one RTT. Only this
// side of a connection is wrapped, so a request and its response together pay one RTT.
// Packets are never reordered within a connection.
//
// It is for tests and benches only.
package netdelay

import (
	"context"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/pion/transport/v4/deadline"
)

// DialFunc opens a network connection, like net.Dialer.DialContext.
type DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error)

// Dialer wraps base, a nil base being a plain net.Dialer, so its connections are delayed
// by rtt. It reports the whole delayed connect to the context's httptrace.ClientTrace; base
// does not see that trace, or it would report the undelayed one.
func Dialer(rtt time.Duration, base DialFunc) DialFunc {
	if base == nil {
		base = (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		trace := httptrace.ContextClientTrace(ctx)
		if trace != nil && trace.ConnectStart != nil {
			trace.ConnectStart(network, addr)
		}
		conn, err := dial(ctx, rtt, base, network, addr)
		if trace != nil && trace.ConnectDone != nil {
			trace.ConnectDone(network, addr, err)
		}
		return conn, err
	}
}

func dial(ctx context.Context, rtt time.Duration, base DialFunc, network, addr string) (net.Conn, error) {
	untraced, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	conn, err := base(untraced, network, addr)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	if isTCP(network) {
		// The SYN out and the SYN-ACK back.
		t := time.NewTimer(rtt)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			_ = conn.Close()
			return nil, ctx.Err()
		}
	}
	return NewConn(conn, rtt), nil
}

// HTTPTransport is http.DefaultTransport with its connections delayed by rtt.
func HTTPTransport(rtt time.Duration) *http.Transport {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DialContext = Dialer(rtt, nil)
	return tr
}

func isTCP(network string) bool {
	switch network {
	case "tcp", "tcp4", "tcp6":
		return true
	}
	return false
}

// packet is one write or read held until due.
type packet struct {
	b    []byte
	addr net.Addr
	due  time.Time
	err  error
}

// queueSize bounds the packets held in each direction; a full queue blocks the writer or
// the socket reader, like a full socket buffer.
const queueSize = 1024

// delayer holds the two queues of one connection.
type delayer struct {
	half time.Duration

	out    chan packet
	in     chan packet
	closed chan struct{}
	once   sync.Once

	readDeadline  *deadline.Deadline
	writeDeadline *deadline.Deadline

	// rmu serializes readers; head is the packet being handed out.
	rmu  sync.Mutex
	head *packet

	errMu    sync.Mutex
	writeErr error
}

func newDelayer(rtt time.Duration) *delayer {
	return &delayer{
		half:          rtt / 2,
		out:           make(chan packet, queueSize),
		in:            make(chan packet, queueSize),
		closed:        make(chan struct{}),
		readDeadline:  deadline.New(),
		writeDeadline: deadline.New(),
	}
}

// enqueueWrite holds a copy of b for half the RTT.
func (d *delayer) enqueueWrite(b []byte, addr net.Addr) (int, error) {
	if err := d.lastWriteErr(); err != nil {
		return 0, err
	}
	select {
	case <-d.writeDeadline.Done():
		return 0, os.ErrDeadlineExceeded
	default:
	}
	p := packet{b: append([]byte(nil), b...), addr: addr, due: time.Now().Add(d.half)}
	select {
	case d.out <- p:
		return len(b), nil
	case <-d.closed:
		return 0, net.ErrClosed
	case <-d.writeDeadline.Done():
		return 0, os.ErrDeadlineExceeded
	}
}

// writeLoop sends each held write when it is due, in order. After Close it still sends
// what was already written, then calls closeConn.
func (d *delayer) writeLoop(send func(packet) error, closeConn func()) {
	defer closeConn()
	for {
		select {
		case p := <-d.out:
			sleepUntil(p.due)
			if err := send(p); err != nil {
				d.setWriteErr(err)
				return
			}
		case <-d.closed:
			for {
				select {
				case p := <-d.out:
					sleepUntil(p.due)
					if send(p) != nil {
						return
					}
				default:
					return
				}
			}
		}
	}
}

// readLoop reads from the socket until it fails, stamping each read with when it is due.
func (d *delayer) readLoop(recv func([]byte) (int, net.Addr, error), bufSize int) {
	buf := make([]byte, bufSize)
	for {
		n, addr, err := recv(buf)
		due := time.Now().Add(d.half)
		if n > 0 {
			select {
			case d.in <- packet{b: append([]byte(nil), buf[:n]...), addr: addr, due: due}:
			case <-d.closed:
				return
			}
		}
		if err != nil {
			select {
			case d.in <- packet{err: err, due: due}:
			case <-d.closed:
			}
			return
		}
	}
}

// next waits for the next read to be due. With whole set, the packet is handed out in
// one piece (datagrams); otherwise the caller consumes head and clears it when empty.
func (d *delayer) next() (*packet, error) {
	if d.head == nil {
		select {
		case p := <-d.in:
			d.head = &p
		case <-d.readDeadline.Done():
			return nil, os.ErrDeadlineExceeded
		case <-d.closed:
			return nil, net.ErrClosed
		}
	}
	if wait := time.Until(d.head.due) - spin; wait > 0 {
		t := time.NewTimer(wait)
		select {
		case <-t.C:
		case <-d.readDeadline.Done():
			t.Stop()
			return nil, os.ErrDeadlineExceeded
		case <-d.closed:
			t.Stop()
			return nil, net.ErrClosed
		}
	}
	spinUntil(d.head.due)
	if d.head.err != nil {
		return nil, d.head.err
	}
	return d.head, nil
}

func (d *delayer) close() bool {
	first := false
	d.once.Do(func() {
		first = true
		close(d.closed)
	})
	return first
}

func (d *delayer) setDeadline(t time.Time) {
	d.readDeadline.Set(t)
	d.writeDeadline.Set(t)
}

func (d *delayer) setWriteErr(err error) {
	d.errMu.Lock()
	defer d.errMu.Unlock()
	if d.writeErr == nil {
		d.writeErr = err
	}
}

func (d *delayer) lastWriteErr() error {
	d.errMu.Lock()
	defer d.errMu.Unlock()
	return d.writeErr
}

// spin is how early a sleep ends so the rest of the wait can be spun off: timers
// overshoot by a millisecond or two, macOS's especially, and every delayed packet would
// otherwise add that to the RTT.
const spin = 2 * time.Millisecond

func sleepUntil(t time.Time) {
	if wait := time.Until(t) - spin; wait > 0 {
		time.Sleep(wait)
	}
	spinUntil(t)
}

func spinUntil(t time.Time) {
	for time.Now().Before(t) {
		runtime.Gosched()
	}
}

// Conn is a stream connection with delayed reads and writes.
type Conn struct {
	net.Conn
	d *delayer
}

// NewConn wraps conn so it behaves as if it crossed a network with round-trip time rtt.
func NewConn(conn net.Conn, rtt time.Duration) *Conn {
	c := &Conn{Conn: conn, d: newDelayer(rtt)}
	go c.d.writeLoop(func(p packet) error {
		_, err := conn.Write(p.b)
		return err
	}, func() { _ = conn.Close() })
	go c.d.readLoop(func(b []byte) (int, net.Addr, error) {
		n, err := conn.Read(b)
		return n, nil, err
	}, 32<<10)
	return c
}

// Read returns data at least half an RTT after it arrived.
func (c *Conn) Read(b []byte) (int, error) {
	c.d.rmu.Lock()
	defer c.d.rmu.Unlock()
	p, err := c.d.next()
	if err != nil {
		return 0, err
	}
	n := copy(b, p.b)
	p.b = p.b[n:]
	if len(p.b) == 0 {
		c.d.head = nil
	}
	return n, nil
}

// Write holds b for half an RTT, then sends it. It returns at once.
func (c *Conn) Write(b []byte) (int, error) {
	return c.d.enqueueWrite(b, nil)
}

// Close stops reads at once; writes already made are still sent, then the connection
// closes.
func (c *Conn) Close() error {
	if !c.d.close() {
		return net.ErrClosed
	}
	return nil
}

func (c *Conn) SetDeadline(t time.Time) error {
	c.d.setDeadline(t)
	return nil
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	c.d.readDeadline.Set(t)
	return nil
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.d.writeDeadline.Set(t)
	return nil
}
