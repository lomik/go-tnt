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

func PackIntBase128ToSlice(value uint32, data []byte) int {
	if value < (1 << 7) {
		data[0] = uint8(value)
		return 1
	}

	if value < (1 << 14) {
		data[0] = uint8((value>>7)&0xff | 0x80)
		data[1] = uint8(value & 0x7F)
		return 2
	}

	if value < (1 << 21) {
		data[0] = uint8((value>>14)&0xff | 0x80)
		data[1] = uint8((value>>7)&0xff | 0x80)
		data[2] = uint8(value & 0x7F)
		return 3
	}

	if value < (1 << 28) {
		data[0] = uint8((value>>21)&0xff | 0x80)
		data[1] = uint8((value>>14)&0xff | 0x80)
		data[2] = uint8((value>>7)&0xff | 0x80)
		data[3] = uint8(value & 0x7F)
		return 4
	}

	data[0] = uint8((value>>28)&0xff | 0x80)
	data[1] = uint8((value>>21)&0xff | 0x80)
	data[2] = uint8((value>>14)&0xff | 0x80)
	data[3] = uint8((value>>7)&0xff | 0x80)
	data[4] = uint8(value & 0x7F)
	return 5
}

func packFieldStr(value []byte, out []byte) int {
	l := PackIntBase128ToSlice(uint32(len(value)), out)
	copy(out[l:], value)
	return l + len(value)
}

func packTuple(value Tuple) []byte {
	fields := len(value)
	bodyLen := 4

	for i := 0; i < fields; i++ {
		bodyLen += base128len(len(value[i]))
	}

	data := make([]byte, bodyLen)
	binary.LittleEndian.PutUint32(data, uint32(fields))
	offset := 4
	for i := 0; i < fields; i++ {
		offset += packFieldStr(value[i], data[offset: ])
	}
	return data
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
	bodyLen := 0
	switch {
	case q.Value != nil:
		bodyLen = 8 + base128len(len(q.Value))
	case q.Values != nil:
		bodyLen = 4
		for i := 0; i < len(q.Values); i++ {
			bodyLen += 4 + base128len(len(q.Values[i]))
		}
	case q.Tuples != nil:
		bodyLen = 4
		for i := 0; i < len(q.Tuples); i++ {
			bodyLen += 4
			for j := 0; j < len(q.Tuples[i]); j++ {
				bodyLen += base128len(len(q.Tuples[i][j]))
			}
		}
	default:
		bodyLen = 4
	}
	data := make([]byte, bodyLen + 28)

	binary.LittleEndian.PutUint32(data, requestTypeSelect)
	binary.LittleEndian.PutUint32(data[4:], uint32(bodyLen) + 16)
	binary.LittleEndian.PutUint32(data[8:], requestID)
	if q.Space != nil {
		i, err := interfaceToUint32(q.Space)
		if err != nil {
			return nil, err
		}
		binary.LittleEndian.PutUint32(data[12:], i)
	} else {
		binary.LittleEndian.PutUint32(data[12:], defaultSpace)
	}

	limit := q.Limit
	if limit == 0 {
		limit = 0xffffffff
	}
	binary.LittleEndian.PutUint32(data[16:], q.Index)
	binary.LittleEndian.PutUint32(data[20:], q.Offset)
	binary.LittleEndian.PutUint32(data[24:], limit)

	switch {
	case q.Value != nil:
		binary.LittleEndian.PutUint32(data[28:], 1) // count
		binary.LittleEndian.PutUint32(data[32:], 1) // fields
		l := PackIntBase128ToSlice(uint32(len(q.Value)), data[36:])
		copy(data[36 + l:], q.Value)
	case q.Values != nil:
		cnt := len(q.Values)
		binary.LittleEndian.PutUint32(data[28:], uint32(cnt)) // count
		offset := 32
		for i := 0; i < cnt; i++ {
			binary.LittleEndian.PutUint32(data[offset:], 1) // fields
			length := len(q.Values[i])
			l := PackIntBase128ToSlice(uint32(length), data[offset + 4:])
			copy(data[offset + 4 + l:], q.Values[i])
			offset += 4 + l + length
		}
	case q.Tuples != nil:
		cnt := len(q.Tuples)
		binary.LittleEndian.PutUint32(data[28:], uint32(cnt)) // count
		offset := 32
		for i := 0; i < cnt; i++ {
			tuple := q.Tuples[i]
			fields := len(tuple)
			binary.LittleEndian.PutUint32(data[offset:], uint32(fields))
			offset += 4
			for j := 0; j < fields; j++ {
				length := len(tuple[j])
				l := PackIntBase128ToSlice(uint32(length), data[offset:])
				copy(data[offset + l:], tuple[j])
				offset += l + length
			}
		}
	default:
		binary.LittleEndian.PutUint32(data[28:], 0) // count
	}

	return data, nil
}

func (q *Insert) Pack(requestID uint32, defaultSpace uint32) ([]byte, error) {
	tuple := packTuple(q.Tuple)
	bodyLen := len(tuple)
	data := make([]byte, bodyLen + 20)

	binary.LittleEndian.PutUint32(data, requestTypeInsert)
	binary.LittleEndian.PutUint32(data[4:], uint32(bodyLen) + 8)
	binary.LittleEndian.PutUint32(data[8:], requestID)
	if q.Space != nil {
		i, err := interfaceToUint32(q.Space)
		if err != nil {
			return nil, err
		}
		binary.LittleEndian.PutUint32(data[12:], i)
	} else {
		binary.LittleEndian.PutUint32(data[12:], defaultSpace)
	}
	if q.ReturnTuple {
		copy(data[16:], packedInt1)
	} else {
		copy(data[16:], packedInt0)
	}
	copy(data[20:], tuple)

	return data, nil
}

func (q *Update) Pack(requestID uint32, defaultSpace uint32) ([]byte, error) {
	tuple := packTuple(q.Tuple)
	bodyLen := len(tuple)
	opLen := 0
	if len(q.Ops) != 0 {
		opLen += 4
		for i := 0; i < len(q.Ops); i++ {
			l := len(q.Ops[i].Value)
			opLen += 5 + base128len(l)
		}
	}
	data := make([]byte, bodyLen + 20 + opLen)

	binary.LittleEndian.PutUint32(data, requestTypeUpdate)
	binary.LittleEndian.PutUint32(data[4:], uint32(bodyLen) + uint32(opLen) + 8)
	binary.LittleEndian.PutUint32(data[8:], requestID)
	if q.Space != nil {
		i, err := interfaceToUint32(q.Space)
		if err != nil {
			return nil, err
		}
		binary.LittleEndian.PutUint32(data[12:], i)
	} else {
		binary.LittleEndian.PutUint32(data[12:], defaultSpace)
	}
	if q.ReturnTuple {
		copy(data[16:], packedInt1)
	} else {
		copy(data[16:], packedInt0)
	}
	copy(data[20:], tuple)
	if len(q.Ops) != 0 {
		cnt := len(q.Ops)
		offset := 20 + bodyLen
		binary.LittleEndian.PutUint32(data[offset:], uint32(cnt))
		offset += 4
		for i := 0; i < cnt; i++ {
			op := q.Ops[i]
			binary.LittleEndian.PutUint32(data[offset:], op.Field)
			data[offset + 4] = byte(op.OpCode)
			l := PackIntBase128ToSlice(uint32(len(op.Value)), data[offset + 5:])
			copy(data[offset + 5 + l:], op.Value)
			offset += 5 + l + len(op.Value)
		}
	}

	return data, nil
}

func (q *Delete) Pack(requestID uint32, defaultSpace uint32) ([]byte, error) {
	tuple := packTuple(q.Tuple)
	bodyLen := len(tuple)
	data := make([]byte, bodyLen + 20)

	binary.LittleEndian.PutUint32(data, requestTypeDelete)
	binary.LittleEndian.PutUint32(data[4:], uint32(bodyLen) + 8)
	binary.LittleEndian.PutUint32(data[8:], requestID)
	if q.Space != nil {
		i, err := interfaceToUint32(q.Space)
		if err != nil {
			return nil, err
		}
		binary.LittleEndian.PutUint32(data[12:], i)
	} else {
		binary.LittleEndian.PutUint32(data[12:], defaultSpace)
	}
	if q.ReturnTuple {
		copy(data[16:], packedInt1)
	} else {
		copy(data[16:], packedInt0)
	}
	copy(data[20:], tuple)

	return data, nil
}

func (q *Call) Pack(requestID uint32, defaultSpace uint32) ([]byte, error) {
	name := base128len(len(q.Name))
	tuple := packTuple(q.Tuple)
	bodyLen := name + len(tuple)
	data := make([]byte, bodyLen + 16)

	binary.LittleEndian.PutUint32(data, requestTypeCall)
	binary.LittleEndian.PutUint32(data[4:], uint32(bodyLen) + 4)
	binary.LittleEndian.PutUint32(data[8:], requestID)
	if q.ReturnTuple {
		copy(data[12:], packedInt1)
	} else {
		copy(data[12:], packedInt0)
	}
	packFieldStr(q.Name, data[16:])
	copy(data[16 + name:], tuple)

	return data, nil
}