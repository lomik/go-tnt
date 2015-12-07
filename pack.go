package tnt

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
