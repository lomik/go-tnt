package tnt

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"
)

func Connect(addr string) (connection *Connection, err error) {
	raddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return
	}

	connection = &Connection{
		addr:        raddr,
		requests:    make(map[uint32]*request),
		requestChan: make(chan *request, 16),
		exit:        make(chan bool),
		closed:      make(chan bool),
	}

	go connection.worker()

	return
}

func (conn *Connection) nextID() uint32 {
	if conn.requestID == math.MaxUint32 {
		conn.requestID = 0
	}
	conn.requestID++
	return conn.requestID
}

func (conn *Connection) newRequest(r *request) {
	requestID := conn.nextID()
	old, exists := conn.requests[requestID]
	if exists {
		old.replyChan <- &Response{
			Error: fmt.Errorf("Shred old requests"),
		}
		close(old.replyChan)
		delete(conn.requests, requestID)
	}

	r.raw = r.query.Pack(requestID)
	conn.requests[requestID] = r
}

func (conn *Connection) handleReply(res *Response) {
	request, exists := conn.requests[res.requestID]
	if exists {
		request.replyChan <- res
		close(request.replyChan)
		delete(conn.requests, res.requestID)
	}
}

func (conn *Connection) worker() {
	// @TODO: Send all waiting requests?

WORKER_LOOP:
	for {
		tcpConn, err := net.DialTCP("tcp", nil, conn.addr)
		if err != nil {
			time.Sleep(time.Second)
			// @TODO: log err
			continue
		}

		var wg sync.WaitGroup

		readChan := make(chan *Response, 256)
		writeChan := make(chan *request, 256)

		stopChan := make(chan bool)
		var stopOnce sync.Once

		stop := func() {
			stopOnce.Do(func() {
				close(stopChan)
			})
		}

		wg.Add(4)

		go func() {
			select {
			case <-conn.exit:
				tcpConn.Close()
			case <-stopChan:
				// break
			}
			stop()
			wg.Done()
		}()

		go func() {
			conn.router(readChan, writeChan, stopChan)
			stop()
			wg.Done()
		}()

		go func() {
			writer(tcpConn, writeChan, stopChan)
			stop()
			wg.Done()
		}()

		go func() {
			reader(tcpConn, readChan)
			stop()
			wg.Done()
		}()

		wg.Wait()

		select {
		case <-conn.exit:
			break WORKER_LOOP
		default:
		}
	}

	close(conn.closed)
}

func (conn *Connection) router(readChan chan *Response, writeChan chan *request, stopChan chan bool) {
	// close(readChan) for stop router
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
		case r := <-requestChan:
			conn.newRequest(r)
			select {
			case writeChan <- r:
				// pass
			case <-stopChan:
				break ROUTER_LOOP
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

func writer(tcpConn *net.TCPConn, writeChan chan *request, stopChan chan bool) {
WRITER_LOOP:
	for {
		select {
		case request, ok := <-writeChan:
			if !ok {
				break WRITER_LOOP
			}
			_, err := tcpConn.Write(request.raw)
			// @TODO: handle error
			if err != nil {
				break WRITER_LOOP
			}
		case <-stopChan:
			break WRITER_LOOP
		}
	}
}

func reader(tcpConn *net.TCPConn, readChan chan *Response) {
	reader := bufio.NewReader(tcpConn)
	// var msgLen uint32
	// var err error
	header := make([]byte, 12)
	headerLen := len(header)

	var bodyLen uint32
	var requestID uint32
	var response *Response

	var err error

READER_LOOP:
	for {
		_, err = io.ReadAtLeast(reader, header, headerLen)
		// @TODO: log error
		if err != nil {
			break READER_LOOP
		}

		bodyLen = UnpackInt(header[4:8])
		requestID = UnpackInt(header[8:12])

		body := make([]byte, bodyLen)

		_, err = io.ReadAtLeast(reader, body, int(bodyLen))
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
