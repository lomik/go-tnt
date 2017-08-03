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
	Pack(requestID uint32, defaultSpace uint32) ([]byte, error)
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

	Space  interface{}
	Index  uint32
	Limit  uint32 // 0x0 == 0xffffffff
	Offset uint32
}

type Insert struct {
	Tuple       Tuple
	Space       interface{}
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

	Space       interface{}
	ReturnTuple bool

	// Index  uint32
}

type Call struct {
	Name        Bytes
	Tuple       Tuple
	ReturnTuple bool
}

var _ Query = (*Select)(nil)
var _ Query = (*Insert)(nil)
var _ Query = (*Delete)(nil)
var _ Query = (*Call)(nil)

type Response struct {
	Data      []Tuple
	Error     error
	requestID uint32
}

type Options struct {
	ConnectTimeout time.Duration
	QueryTimeout   time.Duration
	MemcacheSpace  interface{}
	DefaultSpace   interface{}
}

type QueryOptions struct {
	Timeout time.Duration
}

type Connection struct {
	addr        string
	requestID   uint32
	requests    *requestMap
	requestChan chan *request
	closeOnce   sync.Once
	exit        chan bool
	closed      chan bool
	tcpConn     net.Conn
	memcacheCas uint64
	// options
	queryTimeout  time.Duration
	memcacheSpace interface{}
	defaultSpace  uint32
}

func (conn *Connection) ExecuteOptions(q Query, opts *QueryOptions) (result []Tuple, err error) {
	request := &request{
		query:     q,
		replyChan: make(chan *Response, 1),
	}
	err = conn.newRequest(request)
	if err != nil {
		return
	}

	var timeout time.Duration
	if opts != nil && opts.Timeout > 0 {
		timeout = opts.Timeout
	} else {
		timeout = conn.queryTimeout
	}

	// set execute deadline
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	select {
	case conn.requestChan <- request:
		// pass
	case <-deadline.C:
		return nil, ErrRequestTimeout
	case <-conn.exit:
		return nil, ErrConnectionClosed
	}

	select {
	case response := <-request.replyChan:
		return response.Data, response.Error
	case <-deadline.C:
		return nil, ErrResponseTimeout
	case <-conn.exit:
		return nil, ErrConnectionClosed
	}
}

func (conn *Connection) Execute(q Query) (result []Tuple, err error) {
	return conn.ExecuteOptions(q, nil)
}

func (conn *Connection) Close() {
	conn.stop()
	<-conn.closed
}

func (conn *Connection) IsClosed() bool {
	select {
	case <-conn.exit:
		return true
	default:
		return false
	}
}
