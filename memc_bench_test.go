package tnt

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

var expectedConcatResult = []byte{0x3f, 0x9d, 0xaa, 0x56, 0x0, 0x0, 0x0, 0x0, 0x94, 0x88, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0}

func BenchmarkConcatBytes1(b *testing.B) {
	assert := assert.New(b)
	b1 := PackInt(1454021951)
	b2 := PackInt(0)
	b3 := PackLong(100500)

	r := append(b1[:], b2[:]...)
	r = append(r[:], b3[:]...)
	assert.Equal(expectedConcatResult, r)

	for n := 0; n < b.N; n++ {
		r := append(b1[:], b2[:]...)
		r = append(r[:], b3[:]...)
	}
}

func BenchmarkConcatBytes2(b *testing.B) {
	assert := assert.New(b)
	b1 := PackInt(1454021951)
	b2 := PackInt(0)
	b3 := PackLong(100500)

	var buf bytes.Buffer
	buf.Write(b1)
	buf.Write(b2)
	buf.Write(b3)
	r := buf.Bytes()
	assert.Equal(expectedConcatResult, r)

	for n := 0; n < b.N; n++ {
		var buf bytes.Buffer
		buf.Write(b1)
		buf.Write(b2)
		buf.Write(b3)
		r = buf.Bytes()
	}
}

func BenchmarkConcatBytes3(b *testing.B) {
	assert := assert.New(b)
	b1 := PackInt(1454021951)
	b2 := PackInt(0)
	b3 := PackLong(100500)

	r := []byte(string(b1) + string(b2) + string(b3))
	assert.Equal(expectedConcatResult, r)

	for n := 0; n < b.N; n++ {
		r = []byte(string(b1) + string(b2) + string(b3))
	}
}

func BenchmarkConcatBytes4(b *testing.B) {
	assert := assert.New(b)
	b1 := PackInt(1454021951)
	b2 := PackInt(0)
	b3 := PackLong(100500)

	r := []byte{
		b1[0], b1[1], b1[2], b1[3],
		b2[0], b2[1], b2[2], b2[3],
		b3[0], b3[1], b3[2], b3[3],
		b3[4], b3[5], b3[6], b3[7],
	}
	assert.Equal(expectedConcatResult, r)

	for n := 0; n < b.N; n++ {
		r = []byte{
			b1[0], b1[1], b1[2], b1[3],
			b2[0], b2[1], b2[2], b2[3],
			b3[0], b3[1], b3[2], b3[3],
			b3[4], b3[5], b3[6], b3[7],
		}
	}
}

type memcacheMetaInfo struct {
	expires uint32
	flags   uint32
	cas     uint64
}

func BenchmarkConcatBytes5(b *testing.B) {
	// from Gunstvin's tarantool.space.go:memToTuple
	assert := assert.New(b)
	metaInfo := struct {
		expires uint32
		flags   uint32
		cas     uint64
	}{
		expires: 1454021951,
		flags:   0,
		cas:     100500,
	}

	var metaInfoBuf = bytes.NewBuffer(nil)
	binary.Write(metaInfoBuf, binary.LittleEndian, metaInfo)
	r := metaInfoBuf.Bytes()

	assert.Equal(expectedConcatResult, r)

	for n := 0; n < b.N; n++ {
		var metaInfoBuf = bytes.NewBuffer(nil)
		binary.Write(metaInfoBuf, binary.LittleEndian, metaInfo)
		r = metaInfoBuf.Bytes()
	}
}
