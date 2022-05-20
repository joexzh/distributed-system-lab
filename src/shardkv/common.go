package shardkv

import "log"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OK                     = "OK"
	ErrNoKey               = "ErrNoKey"
	ErrWrongGroup          = "ErrWrongGroup"
	ErrWrongLeader         = "ErrWrongLeader"
	ErrDuplicateRequest    = "ErrDuplicateRequest"
	ErrTransferShardUnDone = "ErrTransferShardUnDone"

	NoOp               = 0
	OpGet              = 1
	OpPut              = 2
	OpAppend           = 3
	OpUpdateConfig     = 4
	OpShardDone        = 5
	OpUpdateShardStore = 6
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
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
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int
	Serial   int64
}

type GetReply struct {
	Err   Err
	Value string
}
