package tnt

import "bytes"

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

func PackString(value string) []byte {
	valueLenPacked := PackIntBase128(uint32(len(value)))

	var buffer bytes.Buffer
	buffer.Write(valueLenPacked)
	buffer.Write([]byte(value))

	return buffer.Bytes()
}
