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
	assert.NoError(err)

	// go func() {
	// 	time.Sleep(time.Second)
	// 	// 	log.Fatal(1)
	// 	pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	// }()

	// pp.Println(listener.Addr().String())

	conn, err := Connect(listener.Addr().String(), nil)
	assert.NoError(err)

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
	// listener.Accept()
	// pp.Println("close")
	conn.Close()

	// pp.Println("wait")
	wg.Wait()

	// pp.Println("finish")

	// assert.Nil(data)
	// assert.Error(err)
	// assert.Equal("Space 0 does not exist", err.Error())
}
