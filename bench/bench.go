package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync/atomic"
	"time"

	"gitlab.corp.mail.ru/rb/go/helper/tnt"
	"gitlab.corp.mail.ru/rb/go/logging"
)

import _ "net/http/pprof"

func main() {
	// var packSelectSimple = flag.Int("pack-select-simple", 0, "pack_select(10, values, offset=13, limit=14, index=15)")
	var pprof = flag.String("pprof", ":6060", "Pprof listen address")
	var server = flag.String("server", "127.0.0.1:2001", "Pprof listen address")
	var threads = flag.Int("threads", 1, "Threads count")
	flag.Parse()

	go func() {
		err := http.ListenAndServe(*pprof, nil)
		if err != nil {
			logging.Error(err.Error())
		}
	}()

	connector := tnt.New(*server, &tnt.Options{
		ConnectTimeout: 100 * time.Millisecond,
	})

	value1 := uint32(rand.Int31())
	value2 := uint32(rand.Int31())

	conn, err := connector.Connect()
	if err == nil {
		conn.Execute(&tnt.Insert{
			Space: "1",
			Tuple: tnt.Tuple{
				tnt.PackInt(value1),
				tnt.PackInt(value2),
			},
		})
	}

	var successCount int64
	var errorCount int64
	var connectErrorCount int64

	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			<-ticker.C
			s := atomic.LoadInt64(&successCount)
			atomic.AddInt64(&successCount, -s)
			e := atomic.LoadInt64(&errorCount)
			atomic.AddInt64(&errorCount, -e)
			ce := atomic.LoadInt64(&connectErrorCount)
			atomic.AddInt64(&connectErrorCount, -ce)
			fmt.Printf("success %d/s, execute error %d/s, connect error %d/s\n", s, e, ce)
		}
	}()

	worker := func() {
		var conn *tnt.Connection
		var err error
		for {
			if conn == nil {
				conn, err = connector.Connect()
				if err != nil {
					atomic.AddInt64(&connectErrorCount, 1)
					continue
				}
			}
			data, err := conn.Execute(&tnt.Select{
				Value: tnt.PackInt(value1),
				Space: "1",
			})
			if err != nil {
				atomic.AddInt64(&errorCount, 1)
				conn = nil
			} else {
				if len(data) != 1 {
					log.Fatal("len(data) != 1")
				}
				atomic.AddInt64(&successCount, 1)
			}
		}
	}

	for i := 0; i < (*threads - 1); i++ {
		go worker()
	}
	worker()
}
