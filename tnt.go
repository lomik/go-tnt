package tnt

import (
	"context"
	"net"
	"sync"
	"time"
)

type Bytes []byte
type Tuple []Bytes

const (
	requestTypeInsert = 13
	requestTypeSelect = 17
	requestTypeUpdate = 19
	requestTypeDelete = 21
	requestTypeCall   = 22
)

type Query interface {
	Pack(requestID uint32, defaultSpace uint32) ([]byte, error)
}

var requestsPool sync.Pool

type request struct {
	raw       []byte
	replyChan chan *Response
}

type Select struct {
	// Value is a Scalar.
	// Request with Value is looking for one single record.
	Value Bytes

	// Values is a List of scalars.
	// Request with Values is looking for several records using single-valued index.
	// Ex: select(space_no, index_no, [1, 2, 3])
	// Transform a list of scalar values to a list of tuples.
	Values []Bytes

	// Tuples is a List of tuples.
	// Request with Tuples is looking for serveral records using composite index
	Tuples []Tuple

	Space interface{}
	Index uint32
	// Limit selected records.
	// Limit equal to 0x0 means 0xffffffff
	Limit  uint32
	Offset uint32
}

func (q *Select) ByteLength() (length int) {
	length = 20
	switch {
	case q.Value != nil:
		length += 4 + base128len(len(q.Value))
	case q.Values != nil:
		cnt := len(q.Values)
		for i := 0; i < cnt; i++ {
			length += 4 + base128len(len(q.Values[i]))
		}
	case q.Tuples != nil:
		cnt := len(q.Tuples)
		for i := 0; i < cnt; i++ {
			fields := len(q.Tuples[i])
			length += 4
			for j := 0; j < fields; j++ {
				length += base128len(len(q.Tuples[i][j]))
			}
		}
	}
	return
}

type Insert struct {
	Tuple       Tuple
	Space       interface{}
	ReturnTuple bool
}

type OpCode uint8

const (
	opSet OpCode = iota
	opAdd
	opAnd
	opXor
	opOr
	opSplice
	opDelete
	opInsert
)

func OpSet(field uint32, value Bytes) Operator {
	return Operator{field, opSet, value}
}

func OpDelete(field uint32, value Bytes) Operator {
	return Operator{field, opDelete, value}
}

func OpInsert(field uint32, value Bytes) Operator {
	return Operator{field, opInsert, value}
}

type Operator struct {
	Field  uint32
	OpCode OpCode
	Value  Bytes
}

type Update struct {
	Tuple       Tuple
	Space       interface{}
	Ops         []Operator
	ReturnTuple bool
}

type Delete struct {
	Tuple Tuple

	Space       interface{}
	ReturnTuple bool
}

type Call struct {
	Name        Bytes
	Tuple       Tuple
	ReturnTuple bool
}

var _ Query = (*Select)(nil)
var _ Query = (*Insert)(nil)
var _ Query = (*Update)(nil)
var _ Query = (*Delete)(nil)
var _ Query = (*Call)(nil)

type Response struct {
	Data  []Tuple
	Error error
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

type IConnection interface {
	MemGet(key string) ([]byte, error)
	MemSet(key string, value []byte, expires uint32) error
	MemDelete(key string) error
	Exec(ctx context.Context, q Query) (result []Tuple, err error)
	ExecuteOptions(q Query, opts *QueryOptions) (result []Tuple, err error)
	Execute(q Query) (result []Tuple, err error)
	Close()
	IsClosed() bool
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

// Connection implements IConnection
var _ IConnection = &Connection{}

// Exec does the q query with context.
func (conn *Connection) Exec(ctx context.Context, q Query) (result []Tuple, err error) {
	var opts *QueryOptions
	if deadline, ok := ctx.Deadline(); ok {
		opts = &QueryOptions{Timeout: time.Until(deadline)}
	}
	return conn.ExecuteOptions(q, opts)
}

func (conn *Connection) ExecuteOptions(q Query, opts *QueryOptions) (result []Tuple, err error) {
	reqID, request, err := conn.newRequest(q)
	if err != nil {
		return
	}

	if old := conn.requests.Put(reqID, request); old != nil {
		// ouroboros has happened
		old.replyChan <- &Response{Error: ErrShredOldRequests}
	}

	var timeout time.Duration
	if opts != nil && opts.Timeout > 0 {
		timeout = opts.Timeout
	} else {
		timeout = conn.queryTimeout
	}

	// set execute deadline
	deadline := acquireTimer(timeout)
	defer releaseTimer(deadline)

	select {
	case conn.requestChan <- request:
		// pass
	case <-deadline.C:
		// delete request from map to avoid leakage
		if request := conn.requests.Pop(reqID); request != nil {
			conn.releaseRequest(request)
		}
		return nil, ErrRequestTimeout
	case <-conn.exit:
		return nil, ErrConnectionClosed
	}

	select {
	case response := <-request.replyChan:
		conn.releaseRequest(request)
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
