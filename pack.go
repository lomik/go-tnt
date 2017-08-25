package tnt

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

var packedInt0 = PackInt(0)
var packedInt1 = PackInt(1)

func packLittle(value uint, bytes int) []byte {
	b := value
	result := make([]byte, bytes)
	for i := 0; i < bytes; i++ {
		result[i] = uint8(b & 0xFF)
		b >>= 8
	}
	return result
}

func PackLittle(value uint, bytes int) []byte {
	return packLittle(value, bytes)
}

func packBig(value int, bytes int) []byte {
	b := value
	result := make([]byte, bytes)
	for i := bytes - 1; i >= 0; i-- {
		result[i] = uint8(b & 0xFF)
		b >>= 8
	}
	return result
}

func PackBig(value int, bytes int) []byte {
	return packBig(value, bytes)
}

func PackB(value uint8) []byte {
	return []byte{value}
}

// PackInt is alias for PackL
func PackInt(value uint32) []byte {
	return packLittle(uint(value), 4)
}

// PackLong is alias for PackQ
func PackLong(value uint64) []byte {
	return packLittle(uint(value), 8)
}

func PackDouble(value float64) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.LittleEndian, value)
	return buffer.Bytes()
}

func base128len(length int) int {
	switch {
	case length < (1 << 7):
		return 1 + length
	case length < (1 << 14):
		return 2 + length
	case length < (1 << 21):
		return 3 + length
	case length < (1 << 28):
		return 4 + length
	default:
		return 5 + length
	}
}

// PackIntBase128 is port from python pack_int_base128
func PackIntBase128(value uint32) []byte {
	if value < (1 << 7) {
		return []byte{
			uint8(value),
		}
	}

	if value < (1 << 14) {
		return []byte{
			uint8((value>>7)&0xff | 0x80),
			uint8(value & 0x7F),
		}
	}

	if value < (1 << 21) {
		return []byte{
			uint8((value>>14)&0xff | 0x80),
			uint8((value>>7)&0xff | 0x80),
			uint8(value & 0x7F),
		}
	}

	if value < (1 << 28) {
		return []byte{
			uint8((value>>21)&0xff | 0x80),
			uint8((value>>14)&0xff | 0x80),
			uint8((value>>7)&0xff | 0x80),
			uint8(value & 0x7F),
		}
	}

	return []byte{
		uint8((value>>28)&0xff | 0x80),
		uint8((value>>21)&0xff | 0x80),
		uint8((value>>14)&0xff | 0x80),
		uint8((value>>7)&0xff | 0x80),
		uint8(value & 0x7F),
	}
}

func packFieldStr(value []byte) []byte {
	valueLenPacked := PackIntBase128(uint32(len(value)))

	var buffer bytes.Buffer
	buffer.Write(valueLenPacked)
	buffer.Write(value)

	return buffer.Bytes()
}

func packFieldInt(value uint32) []byte {
	var buffer bytes.Buffer
	buffer.Write(PackB(4))
	buffer.Write(PackInt(value))

	return buffer.Bytes()
}

func packTuple(value Tuple) []byte {
	var buffer bytes.Buffer

	fields := len(value)

	buffer.Write(PackInt(uint32(fields)))

	for i := 0; i < fields; i++ {
		buffer.Write(packFieldStr(value[i]))
	}

	return buffer.Bytes()
}

func interfaceToUint32(t interface{}) (uint32, error) {
	switch t := t.(type) {
	default:
		return 0, fmt.Errorf("unexpected type %T\n", t) // %T prints whatever type t has
	case int:
		return uint32(t), nil
	case int64:
		return uint32(t), nil
	case uint:
		return uint32(t), nil
	case uint64:
		return uint32(t), nil
	case int32:
		return uint32(t), nil
	case uint32:
		return t, nil
	}
}

func (q *Select) Pack(requestID uint32, defaultSpace uint32) ([]byte, error) {
	length := q.ByteLength()

	buf := NewFixedBuffer(length + 12)

	buf.WriteUint32(requestTypeSelect)
	buf.WriteUint32(uint32(length))
	buf.WriteUint32(requestID)

	limit := q.Limit
	if limit == 0 {
		limit = 0xffffffff
	}

	if q.Space != nil {
		i, err := interfaceToUint32(q.Space)
		if err != nil {
			return nil, err
		}
		buf.WriteUint32(i)
	} else {
		buf.WriteUint32(defaultSpace)
	}

	buf.WriteUint32(q.Index)
	buf.WriteUint32(q.Offset)
	buf.WriteUint32(limit)

	switch {
	case q.Value != nil:
		buf.WriteUint32(1) // count
		buf.WriteUint32(1) // fields
		vlp := PackIntBase128(uint32(len(q.Value)))
		buf.Write(vlp)
		buf.Write(q.Value)
	case q.Values != nil:
		cnt := len(q.Values)
		buf.WriteUint32(uint32(cnt))
		for i := 0; i < cnt; i++ {
			buf.WriteUint32(1) // fields
			vlp := PackIntBase128(uint32(len(q.Values[i])))
			buf.Write(vlp)
			buf.Write(q.Values[i])
		}
	case q.Tuples != nil:
		cnt := len(q.Tuples)
		buf.WriteUint32(uint32(cnt))
		for i := 0; i < cnt; i++ {
			tuple := q.Tuples[i]
			fields := len(tuple)
			buf.WriteUint32(uint32(fields))
			for j := 0; j < fields; j++ {
				vlp := PackIntBase128(uint32(len(tuple[j])))
				buf.Write(vlp)
				buf.Write(tuple[j])
			}
		}
	default:
		buf.WriteUint32(0) // count
	}

	return buf.Bytes(), nil
}

func (q *Insert) Pack(requestID uint32, defaultSpace uint32) ([]byte, error) {
	var bodyBuffer bytes.Buffer
	var buffer bytes.Buffer

	if q.Space != nil {
		i, err := interfaceToUint32(q.Space)
		if err != nil {
			return nil, err
		}
		bodyBuffer.Write(PackInt(i))
	} else {
		bodyBuffer.Write(PackInt(defaultSpace))
	}

	if q.ReturnTuple {
		bodyBuffer.Write(packedInt1)
	} else {
		bodyBuffer.Write(packedInt0)
	}
	bodyBuffer.Write(packTuple(q.Tuple))

	buffer.Write(PackInt(requestTypeInsert))
	buffer.Write(PackInt(uint32(bodyBuffer.Len())))
	buffer.Write(PackInt(requestID))
	buffer.Write(bodyBuffer.Bytes())

	return buffer.Bytes(), nil

}

func (q *Delete) Pack(requestID uint32, defaultSpace uint32) ([]byte, error) {
	var bodyBuffer bytes.Buffer
	var buffer bytes.Buffer

	if q.Space != nil {
		i, err := interfaceToUint32(q.Space)
		if err != nil {
			return nil, err
		}
		bodyBuffer.Write(PackInt(i))
	} else {
		bodyBuffer.Write(PackInt(defaultSpace))
	}

	if q.ReturnTuple {
		bodyBuffer.Write(packedInt1)
	} else {
		bodyBuffer.Write(packedInt0)
	}

	if q.Value != nil {
		bodyBuffer.Write(packTuple(Tuple{q.Value}))
	} else if q.Values != nil {
		cnt := len(q.Values)
		for i := 0; i < cnt; i++ {
			bodyBuffer.Write(packTuple(Tuple{q.Values[i]}))
		}
	}

	buffer.Write(PackInt(requestTypeDelete))
	buffer.Write(PackInt(uint32(bodyBuffer.Len())))
	buffer.Write(PackInt(requestID))
	buffer.Write(bodyBuffer.Bytes())

	return buffer.Bytes(), nil

}

func (q *Call) Pack(requestID uint32, defaultSpace uint32) ([]byte, error) {
	var bodyBuffer bytes.Buffer
	var buffer bytes.Buffer

	if q.ReturnTuple {
		bodyBuffer.Write(packedInt1)
	} else {
		bodyBuffer.Write(packedInt0)
	}
	bodyBuffer.Write(packFieldStr(q.Name))
	bodyBuffer.Write(packTuple(q.Tuple))

	buffer.Write(PackInt(requestTypeCall))
	buffer.Write(PackInt(uint32(bodyBuffer.Len())))
	buffer.Write(PackInt(requestID))
	buffer.Write(bodyBuffer.Bytes())

	return buffer.Bytes(), nil

}
