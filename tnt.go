package tnt

import "sync"

type Field []byte
type Tuple []Field

const requestTypeCall = 22
const requestTypeDelete = 21
const requestTypeInsert = 13
const requestTypeSelect = 17
const requestTypeUpdate = 19

type Query interface {
	Pack(requestID uint32) []byte
}

type request struct {
	query     Query
	raw       []byte
	replyChan chan *Response
}

type Select struct {
	// Scalar
	// This request is looking for one single record
	Value Field

	// List of scalars
	// This request is looking for several records using single-valued index
	// Ex: select(space_no, index_no, [1, 2, 3])
	// Transform a list of scalar values to a list of tuples
	Values []Field

	// List of tuples
	// This request is looking for serveral records using composite index
	Tuples []Tuple

	Space  uint32
	Index  uint32
	Limit  uint32 // 0x0 == 0xffffffff
	Offset uint32
}

type Insert struct {
	Tuple       Tuple
	Space       uint32
	ReturnTuple bool
}

type Response struct {
	Data      []Tuple
	Error     error
	requestID uint32
}

type Options struct {
}

type Connection struct {
	addr        string
	requestID   uint32
	requests    map[uint32]*request
	requestChan chan *request
	closeExit   sync.Once
	exit        chan bool
	closed      chan bool
}

func (conn *Connection) Execute(q Query) ([]Tuple, error) {
	request := &request{
		query:     q,
		replyChan: make(chan *Response, 1),
	}

	conn.requestChan <- request

	response := <-request.replyChan
	return response.Data, response.Error
}

func (conn *Connection) Close() {
	conn.closeExit.Do(func() {
		close(conn.requestChan)
		close(conn.exit)
	})
	<-conn.closed
}
