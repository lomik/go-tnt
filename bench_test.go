package tnt

import (
	"bytes"
	"encoding/binary"
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
		query.Pack(0)
	}
}

func BenchmarkPackInt(b *testing.B) {
	value := uint32(4294866796)
	for n := 0; n < b.N; n++ {
		PackInt(value)
	}
}

func BenchmarkPackInt1(b *testing.B) {
	value := uint32(4294866796)
	for n := 0; n < b.N; n++ {
		body := new(bytes.Buffer)
		binary.Write(body, binary.LittleEndian, value)
		body.Bytes()
	}
}

func BenchmarkSelect(b *testing.B) {
	b.SkipNow()

	conn, err := Connect("192.168.99.100:2001")
	defer conn.Close()

	if err != nil {
		b.FailNow()
	}

	for n := 0; n < b.N; n++ {
		conn.Execute(&Select{
			Value: PackInt(0),
			Space: 10,
		})
	}

}
