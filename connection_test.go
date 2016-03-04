package tnt

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelect(t *testing.T) {
	assert := assert.New(t)

	primaryPort, tearDown, err := setUp()
	assert.NoError(err)
	defer tearDown()

	conn, err := Connect(fmt.Sprintf("127.0.0.1:%d", primaryPort), nil)
	if !assert.NoError(err) {
		return
	}
	defer conn.Close()

	data, err := conn.Execute(&Select{
		Value: PackInt(0),
		Space: 15,
	})
	assert.Nil(data)
	assert.Error(err)
	assert.Equal("Space 15 does not exist", err.Error())
}

func TestInsert(t *testing.T) {
	assert := assert.New(t)

	primaryPort, tearDown, err := setUp()
	assert.NoError(err)
	defer tearDown()

	conn, err := Connect(fmt.Sprintf("127.0.0.1:%d", primaryPort), nil)
	if !assert.NoError(err) {
		return
	}
	defer conn.Close()

	value1 := uint32(rand.Int31())
	value2 := uint32(rand.Int31())
	value3 := uint32(rand.Int31())
	value4 := uint32(rand.Int31())

	conn.Execute(&Insert{
		Tuple: Tuple{
			PackInt(value1),
			PackInt(value3),
		},
	})

	conn.Execute(&Insert{
		Tuple: Tuple{
			PackInt(value1),
			PackInt(value4),
		},
	})

	conn.Execute(&Insert{
		Tuple: Tuple{
			PackInt(value2),
			PackInt(value4),
		},
	})

	// select 1

	data, err := conn.Execute(&Select{
		Value: PackInt(value1),
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
		Index: 1,
	})

	assert.NoError(err)
	assert.Equal(2, len(data))
	assert.Equal(Bytes(PackInt(value4)), data[0][1])
	assert.Equal(Bytes(PackInt(value4)), data[1][1])
}

func TestDefaultSpace(t *testing.T) {
	assert := assert.New(t)

	primaryPort, tearDown, err := setUp()
	assert.NoError(err)
	defer tearDown()

	conn, err := Connect(fmt.Sprintf("127.0.0.1:%d/24", primaryPort), nil)
	if !assert.NoError(err) {
		return
	}
	defer conn.Close()

	data, err := conn.Execute(&Select{
		Value: PackInt(0),
	})
	assert.Nil(data)
	assert.Error(err)
	assert.Equal("Space 24 does not exist", err.Error())
}

func TestDefaultSpace2(t *testing.T) {
	assert := assert.New(t)

	primaryPort, tearDown, err := setUp()
	assert.NoError(err)
	defer tearDown()

	conn, err := Connect(fmt.Sprintf("127.0.0.1:%d/24", primaryPort), &Options{
		DefaultSpace: 48,
	})
	if !assert.NoError(err) {
		return
	}
	defer conn.Close()

	data, err := conn.Execute(&Select{
		Value: PackInt(0),
	})
	assert.Nil(data)
	assert.Error(err)
	assert.Equal("Space 48 does not exist", err.Error())
}
