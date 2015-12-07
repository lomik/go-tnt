package tnt

import "bytes"

var packedL0 []byte = PackL(0)
var packedL1 []byte = PackL(1)

func packLittle(value uint, bytes int) []byte {
	b := value
	result := make([]byte, bytes)
	for i := 0; i < bytes; i++ {
		result[i] = uint8(b & 0xFF)
		b >>= 8
	}
	return result
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

func PackB(value uint8) []byte {
	return []byte{value}
}

func PackL(value uint32) []byte {
	return packLittle(uint(value), 4)
}

func PackQ(value uint64) []byte {
	return packLittle(uint(value), 8)
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
	buffer.Write([]byte(value))

	return buffer.Bytes()
}

func packFieldInt(value uint32) []byte {
	var buffer bytes.Buffer
	buffer.Write(PackB(4))
	buffer.Write(PackL(value))

	return buffer.Bytes()
}

func packTuple(value Tuple) []byte {
	var buffer bytes.Buffer

	fields := len(value)

	buffer.Write(PackL(uint32(fields)))

	for i := 0; i < fields; i++ {
		buffer.Write(packFieldStr(value[i]))
	}

	return buffer.Bytes()
}

func (q *Select) Pack() []byte {
	var bodyBuffer bytes.Buffer
	var buffer bytes.Buffer

	limit := q.Limit
	if limit == 0 {
		limit = 0xffffffff
	}

	bodyBuffer.Write(PackL(q.Space))
	bodyBuffer.Write(PackL(q.Index))
	bodyBuffer.Write(PackL(q.Offset))
	bodyBuffer.Write(PackL(limit))

	if q.Value != nil {
		bodyBuffer.Write(PackL(1))
		bodyBuffer.Write(packTuple(Tuple{q.Value}))
	} else if q.Values != nil {
		cnt := len(q.Values)
		bodyBuffer.Write(PackL(uint32(cnt)))
		for i := 0; i < cnt; i++ {
			bodyBuffer.Write(packTuple(Tuple{q.Values[i]}))
		}
	} else if q.Tuples != nil {
		cnt := len(q.Tuples)
		bodyBuffer.Write(PackL(uint32(cnt)))
		for i := 0; i < cnt; i++ {
			bodyBuffer.Write(packTuple(q.Tuples[i]))
		}
	} else {
		bodyBuffer.Write(packedL0)
	}

	buffer.Write(PackL(requestTypeSelect))
	buffer.Write(PackL(uint32(bodyBuffer.Len())))
	buffer.Write(packedL0)
	buffer.Write(bodyBuffer.Bytes())

	return buffer.Bytes()
}

func (q *Insert) Pack() []byte {
	var bodyBuffer bytes.Buffer
	var buffer bytes.Buffer

	bodyBuffer.Write(PackL(q.Space))
	if q.ReturnTuple {
		bodyBuffer.Write(packedL1)
	} else {
		bodyBuffer.Write(packedL0)
	}
	bodyBuffer.Write(packTuple(q.Tuple))

	buffer.Write(PackL(requestTypeInsert))
	buffer.Write(PackL(uint32(bodyBuffer.Len())))
	buffer.Write(packedL0)
	buffer.Write(bodyBuffer.Bytes())

	return buffer.Bytes()

}
