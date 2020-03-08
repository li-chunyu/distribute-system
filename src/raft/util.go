package raft

import "log"
import "time"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func GetNowTime() int64 {
	return time.Now().UnixNano() / 1e6
}