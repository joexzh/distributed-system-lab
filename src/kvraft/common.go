package kvraft

import "log"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"

	OpGet    = "Get"
	OpPut    = "Put"
	OpAppend = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int
	Serial   int64
}

type PutAppendReply struct {
	Err    Err
	Leader int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int
	Serial   int64
}

type GetReply struct {
	Err    Err
	Value  string
	Leader int
}
