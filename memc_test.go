package tnt

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMem(t *testing.T) {
	assert := assert.New(t)

	conn, err := Connect("192.168.99.100:2001", nil)
	if !assert.NoError(err) {
		return
	}
	defer conn.Close()

	key := fmt.Sprintf("key_%d", time.Now().Unix())

	// get empty
	data, err := conn.MemGet(key)
	assert.NoError(err)
	assert.Nil(data)

	// set
	err = conn.MemSet(key, []byte("hello"), uint32(time.Now().Add(time.Duration(time.Hour)).Unix()))
	assert.NoError(err)

	data, err = conn.MemGet(key)
	assert.NoError(err)
	assert.Equal([]byte("hello"), data)

	// delete
	err = conn.MemDelete(key)
	assert.NoError(err)

	data, err = conn.MemGet(key)
	assert.NoError(err)
	assert.Nil(data)
}
