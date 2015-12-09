package tnt

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"sync"
	"time"

	"github.com/k0kubun/pp"
)

func Connect(addr string) (connection *Connection, err error) {
	raddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return
	}

	connection = &Connection{
		addr:        raddr,
		requests:    make(map[uint32]*request),
		requestChan: make(chan *request, 1024),
		exit:        make(chan bool),
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
		log.Printf("connect")
		tcpConn, err := net.DialTCP("tcp", nil, conn.addr)
		log.Printf("connection error: %s", err)
		pp.Println("connection error", err)
		if err != nil {
			time.Sleep(time.Second)
			// @TODO: log err
			continue
		}

		var wg sync.WaitGroup

		readChan := make(chan *Response, 1024)
		writeChan := make(chan *request, 1024)
		finished := make(chan bool)

		wg.Add(4)

		go func() {
			select {
			case <-conn.exit:
				tcpConn.Close()
			case <-finished:
				// break
			}

			wg.Done()
		}()

		go func() {
			conn.router(writeChan, readChan)
			close(finished)
			wg.Done()
		}()

		go func() {
			writer(tcpConn, writeChan)
			wg.Done()
		}()

		go func() {
			reader(tcpConn, readChan)
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

func (conn *Connection) router(writeChan chan *request, readChan chan *Response) {
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
			writeChan <- r
		case res, ok := <-readChan:
			if !ok {
				break ROUTER_LOOP
			}
			conn.handleReply(res)
		}
	}
}

func writer(tcpConn *net.TCPConn, writeChan chan *request) {
WRITER_LOOP:
	for {
		request := <-writeChan
		_, err := tcpConn.Write(request.raw)
		// @TODO: handle error
		if err != nil {
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

		pp.Println(header, int(bodyLen), body, requestID)

	}
}
