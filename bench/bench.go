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
	flag.Parse()

	go func() {
		err := http.ListenAndServe(*pprof, nil)
		if err != nil {
			logging.Error(err.Error())
		}
	}()

	conn, err := tnt.Connect(*server, nil)
	if err != nil {
		log.Fatal(err)
	}

	value1 := uint32(rand.Int31())
	value2 := uint32(rand.Int31())

	conn.Execute(&tnt.Insert{
		Space: 1,
		Tuple: tnt.Tuple{
			tnt.PackInt(value1),
			tnt.PackInt(value2),
		},
	})

	var counter int64
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			<-ticker.C
			val := atomic.LoadInt64(&counter)
			atomic.AddInt64(&counter, -val)
			fmt.Printf("select %d/s\n", val)
		}
	}()

	worker := func() {
conn, err := tnt.Connect(*server, nil)
    if err != nil {
        log.Fatal(err)
    }
		for {
			data, err := conn.Execute(&tnt.Select{
				Value: tnt.PackInt(value1),
				Space: 1,
			})
			if err != nil {
				log.Fatal(err)
			}
			if len(data) != 1 {
				log.Fatal("len(data) != 1")
			}
			atomic.AddInt64(&counter, 1)
		}
	}

    go worker()
    go worker()
    go worker()
    go worker()
	worker()
}
