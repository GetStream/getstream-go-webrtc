// Package websocket wraps a gobwas/ws connection in a typed, codec-driven
// read/write API.
package websocket

type Encoder[R, W any] interface {
	Encode(*W) ([]byte, error)
}

type Decoder[R, W any] interface {
	Decode([]byte) (*R, error)
}

type Codec[R, W any] interface {
	Encoder[R, W]
	Decoder[R, W]
}

func NewCodec[R, W any](encode func(*W) ([]byte, error), decode func([]byte) (*R, error)) Codec[R, W] {
	return &encoder[R, W]{encode: encode, decode: decode}
}

type encoder[R, W any] struct {
	encode func(*W) ([]byte, error)
	decode func([]byte) (*R, error)
}

func (e *encoder[R, W]) Encode(msg *W) ([]byte, error) {
	return e.encode(msg)
}

func (e *encoder[R, W]) Decode(data []byte) (*R, error) {
	return e.decode(data)
}
