package tnt

import (
	"math"
	"net"
)

func Connect(addr string) (connection *Connection, err error) {
	raddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return
	}

	connection = &Connection{
		addr:        raddr,
		requests:    make(map[int]*request),
		requestChan: make(chan *request, 1024),
	} //, conn, 0, make(map[int32]chan *Response), make(chan *Pack)}

	// conn, err := net.DialTCP("tcp", nil, raddr)
	// if err != nil {
	// 	return
	// }
	// connection = &Connection{addr, conn, 0, make(map[int32]chan *Response), make(chan *Pack)}

	// go connection.read()
	// go connection.write()

	return
}

func (conn *Connection) nextID() uint32 {
	if conn.requestID == math.MaxUint32 {
		conn.requestID = 0
	}
	conn.requestID++
	return conn.requestID
}

// func (conn *Connection) router() {
// 	requestChan := conn.requestChan
// 	select {
// 	case request := <-requestChan:
// 		// pass
// 	}
// }
