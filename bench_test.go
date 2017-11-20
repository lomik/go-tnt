package tnt

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

func BenchmarkSelectPack(b *testing.B) {
	for n := 0; n < b.N; n++ {
		query := &Select{
			Values: Tuple{PackInt(11), PackInt(12)},
			Space:  10,
			Offset: 13,
			Limit:  14,
			Index:  15,
		}
		query.Pack(0, 0)
	}
}

func BenchmarkPackInt(b *testing.B) {
	value := uint32(4294866796)
	for n := 0; n < b.N; n++ {
		PackInt(value)
	}
}

func BenchmarkPackIntAlt1(b *testing.B) {
	value := uint32(4294866796)
	for n := 0; n < b.N; n++ {
		body := new(bytes.Buffer)
		binary.Write(body, binary.LittleEndian, value)
		body.Bytes()
	}
}

func BenchmarkUnpackBody(b *testing.B) {
	body := []uint8{0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0xa, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x4, 0xa3, 0x51, 0x53, 0x71, 0x4, 0x2, 0x0, 0x0, 0x0}
	for n := 0; n < b.N; n++ {
		UnpackBody(body)
	}
}

func BenchmarkReadHeader(b *testing.B) {
	header := make([]byte, 12)
	headerLen := len(header)
	var bodyLen uint32
	var requestID uint32

	for n := 0; n < b.N; n++ {
		buf := bytes.NewBuffer([]uint8{0x11, 0x0, 0x0, 0x0, 0x2c, 0x0, 0x0, 0x0, 0x5, 0x0, 0x0, 0x0})
		io.ReadAtLeast(buf, header, headerLen)
		bodyLen = UnpackInt(header[4:8])
		requestID = UnpackInt(header[8:12])
		if bodyLen != 44 || requestID != 5 {
			b.FailNow()
		}
	}
}

func BenchmarkReadHeaderAlt1(b *testing.B) {
	header := make([]int32, 3)

	for n := 0; n < b.N; n++ {
		buf := bytes.NewBuffer([]uint8{0x11, 0x0, 0x0, 0x0, 0x2c, 0x0, 0x0, 0x0, 0x5, 0x0, 0x0, 0x0})
		binary.Read(buf, binary.LittleEndian, &header)
		if header[1] != 44 || header[2] != 5 {
			b.FailNow()
		}
	}
}
