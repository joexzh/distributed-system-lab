package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func TimeoutCall(timeout time.Duration, f func(chan<- interface{})) interface{} {
	done := make(chan interface{}, 1)

	go f(done)
	select {
	case <-time.After(timeout):
		return nil
	case val := <-done:
		return val
	}
}
