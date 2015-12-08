package tnt

import (
	"fmt"
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
		addr:      raddr,
		requests:  make(map[uint32]*request),
		queryChan: make(chan Query, 1024),
		exit:      make(chan bool),
	}

	return
}

func (conn *Connection) nextID() uint32 {
	if conn.requestID == math.MaxUint32 {
		conn.requestID = 0
	}
	conn.requestID++
	return conn.requestID
}

func (conn *Connection) newQuery(q Query) *request {
	r := &request{
		query:     q,
		replyChan: make(chan *Response, 1),
	}
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
	return r
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
	newQueryChan := conn.queryChan

	readChanThreshold := cap(readChan) / 10

ROUTER_LOOP:
	for {
		// force read reply
		if len(readChan) > readChanThreshold {
			newQueryChan = nil
		} else {
			newQueryChan = conn.queryChan
		}

		select {
		case q := <-newQueryChan:
			r := conn.newQuery(q)
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

}

func reader(tcpConn *net.TCPConn, readChan chan *Response) {

}
