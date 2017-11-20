package tnt

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMem(t *testing.T) {
	assert := assert.New(t)

	primaryPort, tearDown := setUp(t)
	if t.Skipped() {
		return
	}
	defer tearDown()

	conn, err := Connect(fmt.Sprintf("127.0.0.1:%d", primaryPort), nil)
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
