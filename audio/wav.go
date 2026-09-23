package audio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
)

// WAV format tags from the RIFF specification.
const (
	wavFormatPCM       = 1
	wavFormatFloat     = 3
	wavFormatExtension = 0xFFFE
)

// ErrNotWAV is returned when the input is not a RIFF/WAVE stream at all, as
// opposed to a WAVE file this package cannot represent.
var ErrNotWAV = errors.New("audio: not a RIFF/WAVE stream")

// WriteWAV writes the buffer as a WAV file.
//
// s16 buffers are written as integer PCM and f32 buffers as IEEE float, so a
// round trip through [ReadWAV] preserves the samples exactly. Everything is
// buffered in memory first, because the RIFF header carries sizes that are only
// known once the payload is complete.
func WriteWAV(w io.Writer, p PCM) error {
	if err := validate(p.Format, p.SampleRate, p.Channels); err != nil {
		return err
	}

	formatTag := uint16(wavFormatPCM)
	if p.Format == FormatF32 {
		formatTag = wavFormatFloat
	}
	bitsPerSample := uint16(p.Format.BytesPerSample() * 8)
	blockAlign := uint16(p.Channels * p.Format.BytesPerSample())
	data := p.Bytes()

	var buf bytes.Buffer
	buf.Grow(len(data) + 64)

	// "fmt " chunk.
	fmtChunk := make([]byte, 16)
	binary.LittleEndian.PutUint16(fmtChunk[0:], formatTag)
	binary.LittleEndian.PutUint16(fmtChunk[2:], uint16(p.Channels))
	binary.LittleEndian.PutUint32(fmtChunk[4:], uint32(p.SampleRate))
	binary.LittleEndian.PutUint32(fmtChunk[8:], uint32(p.SampleRate)*uint32(blockAlign))
	binary.LittleEndian.PutUint16(fmtChunk[12:], blockAlign)
	binary.LittleEndian.PutUint16(fmtChunk[14:], bitsPerSample)

	// A float WAV needs a "fact" chunk to be strictly conformant.
	var factChunk []byte
	if p.Format == FormatF32 {
		factChunk = make([]byte, 4)
		binary.LittleEndian.PutUint32(factChunk, uint32(p.Len()))
	}

	body := 4 // "WAVE"
	body += 8 + len(fmtChunk)
	if factChunk != nil {
		body += 8 + len(factChunk)
	}
	body += 8 + len(data) + len(data)%2

	buf.WriteString("RIFF")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(body))
	buf.WriteString("WAVE")

	writeChunk(&buf, "fmt ", fmtChunk)
	if factChunk != nil {
		writeChunk(&buf, "fact", factChunk)
	}
	writeChunk(&buf, "data", data)

	_, err := w.Write(buf.Bytes())
	return err
}

func writeChunk(buf *bytes.Buffer, id string, payload []byte) {
	buf.WriteString(id)
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(payload)))
	buf.Write(payload)
	if len(payload)%2 == 1 {
		buf.WriteByte(0) // RIFF chunks are word-aligned.
	}
}

// ReadWAV reads a WAV file.
//
// Integer PCM at 8, 16, 24 or 32 bits becomes an s16 buffer and IEEE float
// becomes an f32 buffer, so any common WAV a TTS engine or a fixture produces
// can be read without the caller checking the bit depth first. Chunks other
// than "fmt " and "data" are skipped.
func ReadWAV(r io.Reader) (PCM, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return PCM{}, err
	}
	if len(data) < 12 || string(data[0:4]) != "RIFF" || string(data[8:12]) != "WAVE" {
		return PCM{}, ErrNotWAV
	}

	var (
		formatTag             uint16
		channels, sampleRate  int
		bitsPerSample         int
		payload               []byte
		haveFormat, havePayld bool
	)

	for pos := 12; pos+8 <= len(data); {
		id := string(data[pos : pos+4])
		size := int(binary.LittleEndian.Uint32(data[pos+4 : pos+8]))
		pos += 8
		if size < 0 || pos+size > len(data) {
			// Tolerate a truncated final chunk: some encoders write a
			// placeholder size when streaming.
			size = len(data) - pos
		}
		chunk := data[pos : pos+size]

		switch id {
		case "fmt ":
			if len(chunk) < 16 {
				return PCM{}, fmt.Errorf("audio: wav fmt chunk is %d bytes, want at least 16", len(chunk))
			}
			formatTag = binary.LittleEndian.Uint16(chunk[0:])
			channels = int(binary.LittleEndian.Uint16(chunk[2:]))
			sampleRate = int(binary.LittleEndian.Uint32(chunk[4:]))
			bitsPerSample = int(binary.LittleEndian.Uint16(chunk[14:]))
			if formatTag == wavFormatExtension && len(chunk) >= 26 {
				// WAVE_FORMAT_EXTENSIBLE stores the real tag first in the GUID.
				formatTag = binary.LittleEndian.Uint16(chunk[24:])
			}
			haveFormat = true
		case "data":
			payload = chunk
			havePayld = true
		}

		pos += size + size%2
	}

	if !haveFormat {
		return PCM{}, errors.New("audio: wav file has no fmt chunk")
	}
	if !havePayld {
		return PCM{}, errors.New("audio: wav file has no data chunk")
	}
	if err := validate(FormatS16, sampleRate, channels); err != nil {
		return PCM{}, err
	}

	switch {
	case formatTag == wavFormatFloat && bitsPerSample == 32:
		return FromBytes(payload, FormatF32, sampleRate, channels)
	case formatTag == wavFormatFloat && bitsPerSample == 64:
		return wavFromFloat64(payload, sampleRate, channels)
	case formatTag == wavFormatPCM:
		return wavFromInt(payload, bitsPerSample, sampleRate, channels)
	default:
		return PCM{}, fmt.Errorf("audio: unsupported wav encoding (format tag %d, %d-bit)", formatTag, bitsPerSample)
	}
}

// wavFromInt widens or narrows any integer bit depth to s16.
func wavFromInt(payload []byte, bits, sampleRate, channels int) (PCM, error) {
	width := bits / 8
	if width < 1 || width > 4 || bits%8 != 0 {
		return PCM{}, fmt.Errorf("audio: unsupported wav bit depth %d", bits)
	}
	if width == 2 {
		return FromBytes(payload, FormatS16, sampleRate, channels)
	}

	frameSize := width * channels
	payload = payload[:len(payload)-len(payload)%frameSize]
	out := make([]int16, len(payload)/width)

	for i := range out {
		b := payload[i*width : (i+1)*width]
		switch width {
		case 1:
			// 8-bit WAV is unsigned with a bias of 128.
			out[i] = int16(int(b[0])-128) << 8
		case 3:
			v := int32(b[0]) | int32(b[1])<<8 | int32(int8(b[2]))<<16
			out[i] = int16(v >> 8)
		case 4:
			v := int32(binary.LittleEndian.Uint32(b))
			out[i] = int16(v >> 16)
		}
	}
	return FromInt16(out, sampleRate, channels), nil
}

func wavFromFloat64(payload []byte, sampleRate, channels int) (PCM, error) {
	frameSize := 8 * channels
	payload = payload[:len(payload)-len(payload)%frameSize]
	out := make([]float32, len(payload)/8)
	for i := range out {
		bits := binary.LittleEndian.Uint64(payload[i*8:])
		out[i] = float32(math.Float64frombits(bits))
	}
	return FromFloat32(out, sampleRate, channels), nil
}

// WriteWAVFile writes the buffer to a WAV file at path, replacing it if it
// already exists.
func WriteWAVFile(path string, p PCM) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := WriteWAV(f, p); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// ReadWAVFile reads the WAV file at path.
func ReadWAVFile(path string) (PCM, error) {
	f, err := os.Open(path)
	if err != nil {
		return PCM{}, err
	}
	defer f.Close()
	return ReadWAV(f)
}
