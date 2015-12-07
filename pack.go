package tnt

func PackB(value int) []byte {
	return []byte{uint8(value % 0x100)}
}
