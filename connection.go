package tnt

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

func Connect(addr string, opts *Options) (connection *Connection, err error) {
	connection = &Connection{
		addr:        addr,
		requests:    make(map[uint32]*request),
		requestChan: make(chan *request, 16),
		exit:        make(chan bool),
		closed:      make(chan bool),
	}

	if opts == nil {
		opts = &Options{}
	}

	if opts.ConnectTimeout.Nanoseconds() == 0 {
		opts.ConnectTimeout = time.Duration(time.Second)
	}

	if opts.QueryTimeout.Nanoseconds() == 0 {
		opts.QueryTimeout = time.Duration(time.Second)
	}

	if opts.MemcacheSpace == nil {
		opts.MemcacheSpace = uint32(23)
	}

	var defaultSpace uint32

	splittedAddr := strings.Split(addr, "/")
	remoteAddr := splittedAddr[0]
	if len(splittedAddr) > 1 {
		i, err := strconv.Atoi(splittedAddr[1])
		if err != nil {
			return nil, fmt.Errorf("Wrong space: %s", splittedAddr[1])
		}
		defaultSpace = uint32(i)
	}

	if opts.DefaultSpace != nil {
		i, err := interfaceToUint32(opts.DefaultSpace)
		if err != nil {
			return nil, fmt.Errorf("Wrong space: %#v", opts.DefaultSpace)
		}
		defaultSpace = uint32(i)
	}

	connection.memcacheSpace = opts.MemcacheSpace
	connection.queryTimeout = opts.QueryTimeout
	connection.defaultSpace = defaultSpace

	connection.tcpConn, err = net.DialTimeout("tcp", remoteAddr, opts.ConnectTimeout)
	if err != nil {
		return nil, err
	}

	go connection.worker(connection.tcpConn)

	return
}

func (conn *Connection) nextID() uint32 {
	if conn.requestID == math.MaxUint32 {
		conn.requestID = 0
	}
	conn.requestID++
	return conn.requestID
}

func (conn *Connection) newRequest(r *request) error {
	var err error

	requestID := conn.nextID()
	old, exists := conn.requests[requestID]
	if exists {
		old.replyChan <- &Response{
			Error: NewConnectionError("Shred old requests"), // wtf?
		}
		close(old.replyChan)
		delete(conn.requests, requestID)
	}

	r.raw, err = r.query.Pack(requestID, conn.defaultSpace)
	if err != nil {
		r.replyChan <- &Response{
			Error: &QueryError{
				error: err,
			},
		}
		return err
	}

	conn.requests[requestID] = r
	return nil
}

func (conn *Connection) handleReply(res *Response) {
	request, exists := conn.requests[res.requestID]
	if exists {
		request.replyChan <- res
		close(request.replyChan)
		delete(conn.requests, res.requestID)
	}
}

func (conn *Connection) stop() {
	conn.closeOnce.Do(func() {
		// debug.PrintStack()
		close(conn.exit)
		conn.tcpConn.Close()
	})
}

func (conn *Connection) worker(tcpConn net.Conn) {

	var wg sync.WaitGroup

	readChan := make(chan *Response, 256)
	writeChan := make(chan *request, 256)

	wg.Add(3)

	go func() {
		conn.router(readChan, writeChan, conn.exit)
		conn.stop()
		wg.Done()
		// pp.Println("router")
	}()

	go func() {
		writer(tcpConn, writeChan, conn.exit)
		conn.stop()
		wg.Done()
		// pp.Println("writer")
	}()

	go func() {
		reader(tcpConn, readChan)
		conn.stop()
		wg.Done()
		// pp.Println("reader")
	}()

	wg.Wait()

	// send error reply to all pending requests
	for requestID, req := range conn.requests {
		req.replyChan <- &Response{
			Error: ConnectionClosedError(),
		}
		close(req.replyChan)
		delete(conn.requests, requestID)
	}

	var req *request

FETCH_INPUT:
	// and to all requests in input queue
	for {
		select {
		case req = <-conn.requestChan:
			// pass
		default: // all fetched
			break FETCH_INPUT
		}
		req.replyChan <- &Response{
			Error: ConnectionClosedError(),
		}
		close(req.replyChan)
	}

	close(conn.closed)
}

func (conn *Connection) router(readChan chan *Response, writeChan chan *request, stopChan chan bool) {
	// close(readChan) for stop router
	var err error
	requestChan := conn.requestChan

	readChanThreshold := cap(readChan) / 10

ROUTER_LOOP:
	for {
		// force read reply
		if len(readChan) > readChanThreshold {
			requestChan = nil
		} else {
			requestChan = conn.requestChan
		}

		select {
		case r, ok := <-requestChan:
			if !ok {
				break ROUTER_LOOP
			}

			err = conn.newRequest(r)
			if err == nil {
				select {
				case writeChan <- r:
					// pass
				case <-stopChan:
					break ROUTER_LOOP
				}
			}
		case <-stopChan:
			break ROUTER_LOOP
		case res, ok := <-readChan:
			if !ok {
				break ROUTER_LOOP
			}
			conn.handleReply(res)
		}
	}
}

func writer(tcpConn net.Conn, writeChan chan *request, stopChan chan bool) {
	var err error
	var n int
	w := bufio.NewWriter(tcpConn)

WRITER_LOOP:
	for {
		select {
		case request, ok := <-writeChan:
			if !ok {
				break WRITER_LOOP
			}
			n, err = w.Write(request.raw)
			// @TODO: handle error
			if err != nil || n != len(request.raw) {
				break WRITER_LOOP
			}
		case <-stopChan:
			break WRITER_LOOP
		default:
			if err = w.Flush(); err != nil {
				break WRITER_LOOP
			}

			// same without flush
			select {
			case request, ok := <-writeChan:
				if !ok {
					break WRITER_LOOP
				}
				n, err = w.Write(request.raw)
				if err != nil || n != len(request.raw) {
					break WRITER_LOOP
				}
			case <-stopChan:
				break WRITER_LOOP
			}
		}
	}
	if err != nil {
		// @TODO
		// pp.Println(err)
	}
}

func reader(tcpConn net.Conn, readChan chan *Response) {
	// var msgLen uint32
	// var err error
	header := make([]byte, 12)
	headerLen := len(header)

	var bodyLen uint32
	var requestID uint32
	var response *Response

	var err error
	r := bufio.NewReaderSize(tcpConn, 128*1024)

READER_LOOP:
	for {
		_, err = io.ReadAtLeast(r, header, headerLen)
		// @TODO: log error
		if err != nil {
			break READER_LOOP
		}

		bodyLen = UnpackInt(header[4:8])
		requestID = UnpackInt(header[8:12])

		body := make([]byte, bodyLen)

		_, err = io.ReadAtLeast(r, body, int(bodyLen))
		// @TODO: log error
		if err != nil {
			break READER_LOOP
		}

		response, err = UnpackBody(body)
		// @TODO: log error
		if err != nil {
			break READER_LOOP
		}
		response.requestID = requestID

		readChan <- response
	}
}
