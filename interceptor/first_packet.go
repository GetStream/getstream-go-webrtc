package interceptor

import (
	"sync/atomic"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
)

// FirstPacketFactory reports when a peer connection sends its first RTP packet and when
// it receives its first one. After that it costs one atomic load per packet.
type FirstPacketFactory struct {
	onFirstWrite func(time.Time)
	onFirstRead  func(time.Time)
}

// NewFirstPacketFactory returns a factory whose interceptors call onFirstWrite after the
// first RTP packet is sent and onFirstRead after the first one is read. A write pion drops
// because the transport is not ready yet does not count as sent. Either may be
// nil. The callbacks run on the packet path and must not block.
func NewFirstPacketFactory(onFirstWrite, onFirstRead func(time.Time)) *FirstPacketFactory {
	return &FirstPacketFactory{onFirstWrite: onFirstWrite, onFirstRead: onFirstRead}
}

// NewInterceptor implements interceptor.Factory.
func (f *FirstPacketFactory) NewInterceptor(string) (interceptor.Interceptor, error) {
	return &firstPacket{onFirstWrite: f.onFirstWrite, onFirstRead: f.onFirstRead}, nil
}

type firstPacket struct {
	interceptor.NoOp

	onFirstWrite func(time.Time)
	onFirstRead  func(time.Time)
	wrote        atomic.Bool
	read         atomic.Bool
}

// BindLocalStream reports the first packet written on any outgoing stream.
func (i *firstPacket) BindLocalStream(_ *interceptor.StreamInfo, writer interceptor.RTPWriter) interceptor.RTPWriter {
	if i.onFirstWrite == nil {
		return writer
	}
	return interceptor.RTPWriterFunc(func(header *rtp.Header, payload []byte, attributes interceptor.Attributes) (int, error) {
		n, err := writer.Write(header, payload, attributes)
		// Before DTLS finishes pion accepts packets and drops them, reporting zero bytes
		// written, so only a write that sent something counts.
		if err == nil && n > 0 && !i.wrote.Load() && i.wrote.CompareAndSwap(false, true) {
			i.onFirstWrite(time.Now())
		}
		return n, err
	})
}

// BindRemoteStream reports the first packet read on any incoming stream.
func (i *firstPacket) BindRemoteStream(_ *interceptor.StreamInfo, reader interceptor.RTPReader) interceptor.RTPReader {
	if i.onFirstRead == nil {
		return reader
	}
	return interceptor.RTPReaderFunc(func(b []byte, attributes interceptor.Attributes) (int, interceptor.Attributes, error) {
		n, attrs, err := reader.Read(b, attributes)
		if err == nil && !i.read.Load() && i.read.CompareAndSwap(false, true) {
			i.onFirstRead(time.Now())
		}
		return n, attrs, err
	})
}
