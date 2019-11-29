package tnt

import (
	"bytes"
	"encoding/binary"
	"errors"
)

func UnpackInt(p []byte) uint32 {
	result := uint32(0)
	for i := uint(0); i < 4; i++ {
		result |= uint32(p[i]) << (8 * i)
	}
	return result
}

func UnpackLong(p []byte) uint64 {
	result := uint64(0)
	for i := uint(0); i < 8; i++ {
		result |= uint64(p[i]) << (8 * i)
	}
	return result
}

func UnpackDouble(p []byte) float64 {
	var res float64
	buf := bytes.NewReader(p)
	binary.Read(buf, binary.LittleEndian, &res)
	return res
}

func unpackIntBase128(p []byte) (uint32, int, error) {
	length := len(p)
	b := uint32(0)

	var i int
	for ; ; i++ {
		if i >= length || i > 11 {
			return 0, 0, errors.New("Error varint unpack")
		}
		if (p[i] & 0x80) != 0 {
			b = (b << 7) | uint32(p[i]^0x80)
		} else {
			b = (b << 7) | uint32(p[i])
			break
		}
	}

	return b, i + 1, nil
}

func unpackTuple(p []byte) (Tuple, error) {
	rawLength := len(p)
	fieldsCount := int(UnpackInt(p[:4]))

	tuple := make(Tuple, fieldsCount)

	offset := 4

	for i := 0; i < fieldsCount; i++ { // @TODO: improve validation
		if offset >= rawLength {
			return nil, errors.New("Unpack tuple error")
		}

		dataLength, varintLength, err := unpackIntBase128(p[offset:])
		if err != nil {
			return nil, err
		}

		offset += varintLength

		tuple[i] = p[offset : offset+int(dataLength)]
		offset += int(dataLength)
	}

	return tuple, nil
}

func UnpackBody(body []byte) (*Response, error) {
	var err error

	returnCode := UnpackInt(body[:4])

	// completionStatus := returnCode % 0x100
	returnCode = returnCode / 0x100

	response := &Response{}

	if returnCode != 0 {
		errorMsg := body[4:]
		if len(errorMsg) > 0 && errorMsg[len(errorMsg)-1] == 0x0 {
			errorMsg = errorMsg[:len(errorMsg)-1]
		}
		response.Error = NewQueryError(string(errorMsg))
		return response, nil
	}

	var rowCount uint32
	if len(body) >= 8 {
	rowCount = UnpackInt(body[4:8])
	}
	data := make([]Tuple, rowCount)

	if rowCount > 0 {
		bodyLen := len(body)
		offset := 8
		var i int

		for i = 0; offset < bodyLen; i++ { // @TODO: impove overflow validation
			tupleSize := int(UnpackInt(body[offset:offset+4]) + 4)
			tupleData := body[offset+4 : offset+4+tupleSize]

			data[i], err = unpackTuple(tupleData)
			if err != nil {
				return nil, err
			}
			offset += tupleSize + 4
		}

		response.Data = data[:i]
	} else {
		response.Data = []Tuple{}
	}

	return response, nil
}
