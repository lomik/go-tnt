package tnt

import "bytes"

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

// PackInt is alias for PackL
func PackInt(value uint32) []byte {
	return packLittle(uint(value), 4)
}

// PackLong is alias for PackQ
func PackLong(value uint64) []byte {
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

func (q *Select) Pack(requestID uint32) []byte {
	var bodyBuffer bytes.Buffer
	var buffer bytes.Buffer

	limit := q.Limit
	if limit == 0 {
		limit = 0xffffffff
	}

	bodyBuffer.Write(PackInt(q.Space))
	bodyBuffer.Write(PackInt(q.Index))
	bodyBuffer.Write(PackInt(q.Offset))
	bodyBuffer.Write(PackInt(limit))

	if q.Value != nil {
		bodyBuffer.Write(PackInt(1))
		bodyBuffer.Write(packTuple(Tuple{q.Value}))
	} else if q.Values != nil {
		cnt := len(q.Values)
		bodyBuffer.Write(PackInt(uint32(cnt)))
		for i := 0; i < cnt; i++ {
			bodyBuffer.Write(packTuple(Tuple{q.Values[i]}))
		}
	} else if q.Tuples != nil {
		cnt := len(q.Tuples)
		bodyBuffer.Write(PackInt(uint32(cnt)))
		for i := 0; i < cnt; i++ {
			bodyBuffer.Write(packTuple(q.Tuples[i]))
		}
	} else {
		bodyBuffer.Write(packedInt0)
	}

	buffer.Write(PackInt(requestTypeSelect))
	buffer.Write(PackInt(uint32(bodyBuffer.Len())))
	buffer.Write(PackInt(requestID))
	buffer.Write(bodyBuffer.Bytes())

	return buffer.Bytes()
}

func (q *Insert) Pack(requestID uint32) []byte {
	var bodyBuffer bytes.Buffer
	var buffer bytes.Buffer

	bodyBuffer.Write(PackInt(q.Space))
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

	return buffer.Bytes()

}
