package tnt

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// func TestInsert(t *testing.T) {
// 	assert := assert.New(t)

// 	conn, err := Connect("127.0.0.1:2001")
// 	assert.NoError(err)
// 	defer conn.Close()

// 	conn.Execute(&Insert{
// 		Tuple: Tuple{},
// 	})
// }

func TestSelect(t *testing.T) {
	assert := assert.New(t)

	conn, err := Connect("192.168.99.100:2001")
	assert.NoError(err)
	defer conn.Close()

	data, err := conn.Execute(&Select{
		Value: PackInt(0),
		Space: 0,
	})
	assert.Nil(data)
	assert.Error(err)
	assert.Equal("Space 0 does not exist", err.Error())
}
