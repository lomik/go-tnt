package tnt

import (
	"net"
	"sync"
	"time"
)

type Bytes []byte
type Tuple []Bytes

const requestTypeCall = 22
const requestTypeDelete = 21
const requestTypeInsert = 13
const requestTypeSelect = 17
const requestTypeUpdate = 19

type Query interface {
	Pack(requestID uint32, defaultSpace uint32) []byte
}

type request struct {
	query     Query
	raw       []byte
	replyChan chan *Response
}

type Select struct {
	// Scalar
	// This request is looking for one single record
	Value Bytes

	// List of scalars
	// This request is looking for several records using single-valued index
	// Ex: select(space_no, index_no, [1, 2, 3])
	// Transform a list of scalar values to a list of tuples
	Values []Bytes

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

type Delete struct {
	// Scalar
	// This request is looking for one single record
	Value Bytes

	// List of scalars
	// This request is looking for several records using single-valued index
	// Ex: select(space_no, index_no, [1, 2, 3])
	// Transform a list of scalar values to a list of tuples
	Values []Bytes

	Space       uint32
	ReturnTuple bool

	// Index  uint32
}

var _ Query = (*Select)(nil)
var _ Query = (*Insert)(nil)
var _ Query = (*Delete)(nil)

type Response struct {
	Data      []Tuple
	Error     error
	requestID uint32
}

type Options struct {
	ConnectTimeout time.Duration
	QueryTimeout   time.Duration
	MemcacheSpace  uint32
	DefaultSpace   uint32
}

type QueryOptions struct {
	Timeout time.Duration
}

type Connection struct {
	addr        string
	requestID   uint32
	requests    map[uint32]*request
	requestChan chan *request
	closeOnce   sync.Once
	exit        chan bool
	closed      chan bool
	tcpConn     net.Conn
	memcacheCas uint64
	// options
	queryTimeout  time.Duration
	memcacheSpace uint32
	defaultSpace  uint32
}

func (conn *Connection) ExecuteOptions(q Query, opts *QueryOptions) (result []Tuple, err error) {
	request := &request{
		query:     q,
		replyChan: make(chan *Response, 1),
	}

	// make options
	if opts == nil {
		opts = &QueryOptions{}
	}

	if opts.Timeout.Nanoseconds() == 0 {
		opts.Timeout = conn.queryTimeout
	}

	// set execute deadline
	deadline := time.After(opts.Timeout)

	select {
	case conn.requestChan <- request:
		// pass
	case <-deadline:
		return nil, NewConnectionError("Request send timeout")
	case <-conn.exit:
		return nil, ConnectionClosedError()
	}

	var response *Response
	select {
	case response = <-request.replyChan:
		// pass
	case <-deadline:
		return nil, NewConnectionError("Response read timeout")
	case <-conn.exit:
		return nil, ConnectionClosedError()
	}

	result = response.Data
	err = response.Error
	return
}

func (conn *Connection) Execute(q Query) (result []Tuple, err error) {
	return conn.ExecuteOptions(q, nil)
}

func (conn *Connection) Close() {
	conn.stop()
	<-conn.closed
}
