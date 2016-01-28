package tnt

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClose(t *testing.T) {
	assert := assert.New(t)
	raddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	assert.NoError(err)

	listener, err := net.ListenTCP("tcp", raddr)
	if !assert.NoError(err) {
		return
	}
	defer listener.Close()

	// go func() {
	// 	time.Sleep(time.Second)
	// 	// 	log.Fatal(1)
	// 	pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	// }()

	// pp.Println(listener.Addr().String())

	conn, err := Connect(listener.Addr().String(), nil)
	if !assert.NoError(err) {
		return
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		data, err := conn.Execute(&Select{
			Value: PackInt(0),
			Space: 0,
		})

		assert.Empty(data)
		assert.Error(err)
		assert.Equal("Connection closed", err.Error())

		wg.Done()
	}()

	time.Sleep(10 * time.Millisecond)
	conn.Close()

	wg.Wait()
}

func TestCloseExecute(t *testing.T) {
	assert := assert.New(t)
	raddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	assert.NoError(err)

	listener, err := net.ListenTCP("tcp", raddr)
	if !assert.NoError(err) {
		return
	}

	conn, err := Connect(listener.Addr().String(), nil)
	if !assert.NoError(err) {
		return
	}

	go func() {
		time.Sleep(time.Duration(100 * time.Millisecond))
		listener.Close()
	}()

	data, err := conn.Execute(&Select{
		Value: PackInt(1),
		Space: 1,
	})

	assert.Nil(data)
	assert.Error(err)
	assert.True(err.(Error).Connection())

	time.Sleep(100 * time.Millisecond)

	// execute on closed connection
	data, err = conn.Execute(&Select{
		Value: PackInt(1),
		Space: 1,
	})

	assert.Nil(data)
	assert.Error(err)
	assert.True(err.(Error).Connection())
}
