package tnt

import (
	"testing"
	"time"

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
	return
	assert := assert.New(t)

	conn, err := Connect("192.168.99.100:2001")
	assert.NoError(err)
	defer conn.Close()

	conn.Execute(&Select{
		Value: PackInt(0),
		Space: 10,
	})

	<-time.After(time.Second)
	// log.Fatal("")
}
