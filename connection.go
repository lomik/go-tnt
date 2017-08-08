package tnt

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func Connect(addr string, opts *Options) (connection *Connection, err error) {
	connection = &Connection{
		addr:        addr,
		requests:    newRequestMap(),
		requestChan: make(chan *request, 1024),
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
	return atomic.AddUint32(&conn.requestID, 1)
}

func (conn *Connection) newRequest(q Query) (reqID uint32, r *request, err error) {

	r = &request{query: q, replyChan: make(chan *Response, 1)}
	reqID = conn.nextID()

	r.raw, err = r.query.Pack(reqID, conn.defaultSpace)
	if err != nil {
		return 0, nil, &QueryError{error: err}
	}

	return reqID, r, nil
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
	wg.Add(2)

	go func() {
		writer(tcpConn, conn.requestChan, conn.exit)
		conn.stop()
		wg.Done()
	}()

	go func() {
		conn.reader(tcpConn)
		conn.stop()
		wg.Done()
	}()

	wg.Wait()

	// send error reply to all pending requests
	conn.requests.CleanUp(func(req *request) {
		req.replyChan <- &Response{
			Error: ErrConnectionClosed,
		}
		close(req.replyChan)
	})

	close(conn.closed)
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

func (conn *Connection) reader(tcpConn net.Conn) {
	// var msgLen uint32
	// var err error
	header := make([]byte, 12)
	headerLen := len(header)

	var bodyLen uint32
	var requestID uint32
	var response *Response
	var req *request

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

		req = conn.requests.Pop(requestID)
		if req != nil {
			req.replyChan <- response
			close(req.replyChan)
		}
	}
}
