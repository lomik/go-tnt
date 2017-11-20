package tnt

import (
	"sync"
	"time"
)

var timerPool sync.Pool

func acquireTimer(d time.Duration) *time.Timer {
	v := timerPool.Get()
	if v == nil {
		return time.NewTimer(d)
	}
	tm := v.(*time.Timer)
	if tm.Reset(d) {
		panic("timerPool: got active timer")
	}
	return tm
}

func releaseTimer(tm *time.Timer) {
	if !tm.Stop() {
		return
	}
	timerPool.Put(tm)
}
