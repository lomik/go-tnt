package tnt

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnpackInt(t *testing.T) {
	assert := assert.New(t)

	for value := range values(32) {
		packed := PackInt(uint32(value))
		assert.Equal(
			value,
			UnpackInt(packed),
		)
	}
}

func TestUnpackIntBase128(t *testing.T) {
	assert := assert.New(t)

	for value := range values(32) {
		packed := PackIntBase128(uint32(value))

		result, bytes, err := unpackIntBase128(packed)
		assert.Equal(value, result)
		assert.NoError(err)
		assert.Equal(len(packed), bytes)
	}
}

func TestUnpackTuple(t *testing.T) {
	assert := assert.New(t)

	raw := []byte("\x0c\x00\x00\x00\x08.u?MX\x00\x00\x00\x042\xc3iP\x042\xc3iP\x08\x08\x00~\x93\r1\x00\x00\x042\xc3iP\x08 \x00\x1e\xf2\xea*\x00\x00\x042\xc3iP\x08X\x00ji8*\x00\x00\x042\xc3iP\x08x\x00G9Q.\x00\x00\x042\xc3iP\x08\xd0\x00\x98\xbe..\x00\x00")

	tuple, err := unpackTuple(raw)

	assert.NoError(err)
	assert.Equal(Tuple{
		[]byte(".u?MX\x00\x00\x00"),
		[]byte("2\xc3iP"),
		[]byte("2\xc3iP"),
		[]byte("\x08\x00~\x93\r1\x00\x00"),
		[]byte("2\xc3iP"),
		[]byte(" \x00\x1e\xf2\xea*\x00\x00"),
		[]byte("2\xc3iP"),
		[]byte("X\x00ji8*\x00\x00"),
		[]byte("2\xc3iP"),
		[]byte("x\x00G9Q.\x00\x00"),
		[]byte("2\xc3iP"),
		[]byte("\xd0\x00\x98\xbe..\x00\x00"),
	},
		tuple,
	)
}

func TestUnpackBodyWithError(t *testing.T) {
	assert := assert.New(t)

	body := []uint8{0x2, 0x39, 0x0, 0x0, 0x53, 0x70, 0x61, 0x63, 0x65, 0x20, 0x30, 0x20, 0x64, 0x6f, 0x65, 0x73, 0x20, 0x6e, 0x6f, 0x74, 0x20, 0x65, 0x78, 0x69, 0x73, 0x74, 0x0}

	response, err := UnpackBody(body)
	assert.NoError(err)
	assert.Nil(response.Data)
	assert.Error(response.Error)
	assert.Equal("Space 0 does not exist", response.Error.Error())
}

func TestUnpackBodyEmpty(t *testing.T) {
	assert := assert.New(t)

	body := []uint8{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}

	response, err := UnpackBody(body)
	assert.NoError(err)
	assert.NotNil(response.Data)
	assert.Empty(response.Data)
	assert.NoError(response.Error)
}
