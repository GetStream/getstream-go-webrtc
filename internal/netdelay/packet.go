package netdelay

import (
	"errors"
	"net"
	"time"

	"github.com/pion/transport/v4"
	"github.com/pion/transport/v4/stdnet"
)

// PacketConn is a datagram socket with delayed reads and writes.
type PacketConn struct {
	conn net.PacketConn
	d    *delayer
}

// NewPacketConn wraps conn so it behaves as if it crossed a network with round-trip time
// rtt.
func NewPacketConn(conn net.PacketConn, rtt time.Duration) *PacketConn {
	c := &PacketConn{conn: conn, d: newDelayer(rtt)}
	connected, _ := conn.(interface{ Write([]byte) (int, error) })
	go c.d.writeLoop(func(p packet) error {
		var err error
		if p.addr == nil && connected != nil {
			_, err = connected.Write(p.b)
		} else {
			_, err = conn.WriteTo(p.b, p.addr)
		}
		// A datagram that cannot be sent is lost, like on a network; only a closed socket
		// ends the loop.
		if errors.Is(err, net.ErrClosed) {
			return err
		}
		return nil
	}, func() { _ = conn.Close() })
	go c.d.readLoop(conn.ReadFrom, 64<<10)
	return c
}

// ReadFrom returns a datagram at least half an RTT after it arrived. A datagram longer
// than p is truncated.
func (c *PacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	c.d.rmu.Lock()
	defer c.d.rmu.Unlock()
	pkt, err := c.d.next()
	if err != nil {
		return 0, nil, err
	}
	c.d.head = nil
	return copy(p, pkt.b), pkt.addr, nil
}

// WriteTo holds p for half an RTT, then sends it. It returns at once.
func (c *PacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	return c.d.enqueueWrite(p, addr)
}

// Close stops reads at once; datagrams already written are still sent.
func (c *PacketConn) Close() error {
	if !c.d.close() {
		return net.ErrClosed
	}
	return nil
}

func (c *PacketConn) LocalAddr() net.Addr { return c.conn.LocalAddr() }

func (c *PacketConn) SetDeadline(t time.Time) error {
	c.d.setDeadline(t)
	return nil
}

func (c *PacketConn) SetReadDeadline(t time.Time) error {
	c.d.readDeadline.Set(t)
	return nil
}

func (c *PacketConn) SetWriteDeadline(t time.Time) error {
	c.d.writeDeadline.Set(t)
	return nil
}

// UDPConn is a PacketConn over a pion transport.UDPConn, for ICE.
type UDPConn struct {
	*PacketConn
	udp transport.UDPConn
}

var _ transport.UDPConn = (*UDPConn)(nil)

// NewUDPConn wraps conn so it behaves as if it crossed a network with round-trip time rtt.
func NewUDPConn(conn transport.UDPConn, rtt time.Duration) *UDPConn {
	return &UDPConn{PacketConn: NewPacketConn(conn, rtt), udp: conn}
}

func (c *UDPConn) RemoteAddr() net.Addr           { return c.udp.RemoteAddr() }
func (c *UDPConn) SetReadBuffer(bytes int) error  { return c.udp.SetReadBuffer(bytes) }
func (c *UDPConn) SetWriteBuffer(bytes int) error { return c.udp.SetWriteBuffer(bytes) }

func (c *UDPConn) Read(b []byte) (int, error) {
	n, _, err := c.ReadFrom(b)
	return n, err
}

func (c *UDPConn) ReadFromUDP(b []byte) (int, *net.UDPAddr, error) {
	n, addr, err := c.ReadFrom(b)
	udpAddr, _ := addr.(*net.UDPAddr)
	return n, udpAddr, err
}

// ReadMsgUDP drops the out-of-band data.
func (c *UDPConn) ReadMsgUDP(b, _ []byte) (n, oobn, flags int, addr *net.UDPAddr, err error) {
	n, addr, err = c.ReadFromUDP(b)
	return n, 0, 0, addr, err
}

func (c *UDPConn) Write(b []byte) (int, error) {
	return c.d.enqueueWrite(b, nil)
}

func (c *UDPConn) WriteToUDP(b []byte, addr *net.UDPAddr) (int, error) {
	return c.WriteTo(b, addr)
}

// WriteMsgUDP drops the out-of-band data.
func (c *UDPConn) WriteMsgUDP(b, _ []byte, addr *net.UDPAddr) (n, oobn int, err error) {
	if addr == nil {
		n, err = c.Write(b)
	} else {
		n, err = c.WriteTo(b, addr)
	}
	return n, 0, err
}

// Net is the host network for pion with every UDP socket and dialed connection delayed.
// TCP listeners are not wrapped: the SDK gathers UDP candidates only.
type Net struct {
	transport.Net
	rtt time.Duration
}

// NewNet returns the host network with its sockets delayed by rtt.
func NewNet(rtt time.Duration) (*Net, error) {
	std, err := stdnet.NewNet()
	if err != nil {
		return nil, err
	}
	return &Net{Net: std, rtt: rtt}, nil
}

func (n *Net) ListenUDP(network string, laddr *net.UDPAddr) (transport.UDPConn, error) {
	conn, err := n.Net.ListenUDP(network, laddr)
	if err != nil {
		return nil, err
	}
	return NewUDPConn(conn, n.rtt), nil
}

func (n *Net) ListenPacket(network, address string) (net.PacketConn, error) {
	conn, err := n.Net.ListenPacket(network, address)
	if err != nil {
		return nil, err
	}
	return NewPacketConn(conn, n.rtt), nil
}

func (n *Net) DialUDP(network string, laddr, raddr *net.UDPAddr) (transport.UDPConn, error) {
	conn, err := n.Net.DialUDP(network, laddr, raddr)
	if err != nil {
		return nil, err
	}
	return NewUDPConn(conn, n.rtt), nil
}

func (n *Net) Dial(network, address string) (net.Conn, error) {
	conn, err := n.Net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	if isTCP(network) {
		time.Sleep(n.rtt)
	}
	return NewConn(conn, n.rtt), nil
}
