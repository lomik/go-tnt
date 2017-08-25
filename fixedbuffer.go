package tnt

import (
	"encoding/binary"
	"io"
)

// FixedBuffer is a byte buffer with fixed length in opposite to bytes.Buffer.
// It is used to avoid allocations and redundant error checks when data length is known "a priori".
// FixedBuffer is linked with internal pool.
type FixedBuffer struct {
	buf []byte
	pos int
}

func NewFixedBuffer(n int) *FixedBuffer {
	return &FixedBuffer{buf: make([]byte, n)}
}

func (fb *FixedBuffer) Write(p []byte) (int, error) {
	if len(p) > len(fb.buf)-fb.pos {
		return 0, io.ErrShortBuffer
	}
	copy(fb.buf[fb.pos:fb.pos+len(p)], p)
	fb.pos += len(p)
	return 0, nil
}

func (fb *FixedBuffer) WriteUint32(v uint32) error {
	if len(fb.buf)-fb.pos < 4 {
		return io.ErrShortBuffer
	}
	binary.LittleEndian.PutUint32(fb.buf[fb.pos:fb.pos+4], v)
	fb.pos += 4
	return nil
}

func (fb *FixedBuffer) Bytes() []byte {
	return fb.buf
}
