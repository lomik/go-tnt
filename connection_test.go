package tnt

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelect(t *testing.T) {
	assert := assert.New(t)

	conn, err := Connect("192.168.99.100:2001", nil)
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

func TestInsert(t *testing.T) {
	assert := assert.New(t)

	conn, err := Connect("192.168.99.100:2001", nil)
	assert.NoError(err)
	defer conn.Close()

	value1 := uint32(rand.Int31())
	value2 := uint32(rand.Int31())
	value3 := uint32(rand.Int31())
	value4 := uint32(rand.Int31())

	conn.Execute(&Insert{
		Space: 1,
		Tuple: Tuple{
			PackInt(value1),
			PackInt(value3),
		},
	})

	conn.Execute(&Insert{
		Space: 1,
		Tuple: Tuple{
			PackInt(value1),
			PackInt(value4),
		},
	})

	conn.Execute(&Insert{
		Space: 1,
		Tuple: Tuple{
			PackInt(value2),
			PackInt(value4),
		},
	})

	// select 1

	data, err := conn.Execute(&Select{
		Value: PackInt(value1),
		Space: 1,
	})

	assert.NoError(err)
	assert.Equal(
		[]Tuple{
			Tuple{
				PackInt(value1),
				PackInt(value4),
			},
		},
		data,
	)

	// select 2
	data, err = conn.Execute(&Select{
		Value: PackInt(value4),
		Space: 1,
		Index: 1,
	})

	assert.NoError(err)
	assert.Equal(2, len(data))
	assert.Equal(Field(PackInt(value4)), data[0][1])
	assert.Equal(Field(PackInt(value4)), data[1][1])
}
