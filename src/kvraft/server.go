package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Op       string
	ClientId int
	Serial   int64
}

type ProcessReply struct {
	Value string
	Err   Err
}

type RequestId struct {
	ClientId int
	Serial   int64
}

type RequestChan struct {
	Index int
	C     chan ProcessReply
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// flow control
	applyTimeout     time.Duration
	requestsReplyMap sync.Map

	// data
	data             map[string]string
	index            int
	term             int
	lastClientSerial map[int]int64 // key: clientId, value: serial number
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.RLock()
	if kv.lastClientSerial[args.ClientId] >= args.Serial {
		value := kv.data[args.Key]
		reply.Value = value
		reply.Err = OK
		DPrintf("KVServer %d Get serial skip, client %d, args.Serial %d, Serial %d", kv.me, args.ClientId, args.Serial, kv.lastClientSerial[args.ClientId])
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	op := Op{Key: args.Key, Op: OpGet, ClientId: args.ClientId, Serial: args.Serial}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.Leader = kv.rf.GetVoteFor()
		// DPrintf("KVServer %d Get wrong leader, newLeader %d, client %d, args.Serial %d, Serial %d", kv.me, leader, args.ClientId, args.Serial, kv.lastClientSerial[args.ClientId])
		return
	}
	requestId := RequestId{ClientId: args.ClientId, Serial: args.Serial}
	processCh := make(chan ProcessReply, 1)
	requestChan := RequestChan{Index: index, C: processCh}
	kv.requestsReplyMap.Store(requestId, requestChan)
	DPrintf("KVServer %d Get rf.Start, index %d, args.ClientId %d, args.Serial %d", kv.me, index, args.ClientId, args.Serial)

	select {
	case <-time.After(kv.applyTimeout):
		reply.Err = ErrWrongLeader
		voteFor := kv.rf.GetVoteFor()
		if kv.me != voteFor {
			reply.Leader = voteFor
		}
	case processReply := <-processCh:
		reply.Value = processReply.Value
		reply.Err = processReply.Err
	}
	kv.requestsReplyMap.Delete(requestId)
	// DPrintf("KVServer %d Get process done, args.Client %d, args.Serial %d, reply.Err %s, reply.Value %s", kv.me, args.ClientId, args.Serial, reply.Err, reply.Value)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.RLock()
	if kv.lastClientSerial[args.ClientId] >= args.Serial {
		DPrintf("KVServer %d PutAppend Serial skip, client %d, args.Serial %d, Serial %d", kv.me, args.ClientId, args.Serial, kv.lastClientSerial[args.ClientId])
		kv.mu.RUnlock()
		reply.Err = OK
		return
	}
	kv.mu.RUnlock()

	op := Op{Key: args.Key, Value: args.Value, Op: args.Op, ClientId: args.ClientId, Serial: args.Serial}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.Leader = kv.rf.GetVoteFor()
		// DPrintf("KVServer %d PutAppend wrong leader, newLeader %d, client %d, args.Serial %d, Serial %d", kv.me, leader, args.ClientId, args.Serial, kv.lastClientSerial[args.ClientId])
		return
	}
	requestId := RequestId{ClientId: args.ClientId, Serial: args.Serial}
	processCh := make(chan ProcessReply, 1)
	requestChan := RequestChan{Index: index, C: processCh}
	kv.requestsReplyMap.Store(requestId, requestChan)
	DPrintf("KVServer %d PutAppend kv.Start, index %d, args.ClientId %d, args.Serial %d", kv.me, index, args.ClientId, args.Serial)

	select {
	case <-time.After(kv.applyTimeout):
		reply.Err = ErrWrongLeader
		voteFor := kv.rf.GetVoteFor()
		if kv.me != voteFor {
			reply.Leader = voteFor
		}
	case processReply := <-processCh:
		if processReply.Err == ErrWrongLeader {
			reply.Err = ErrWrongLeader
			reply.Leader = -1
		} else {
			reply.Err = OK
		}
	}
	kv.requestsReplyMap.Delete(requestId)
	// kv.mu.RLock()
	// DPrintf("KVServer %d PutAppend process done, args %+v, reply %+v", kv.me, args, reply)
	// kv.mu.RUnlock()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyCommand() {
	for !kv.killed() {
		select {
		case applyMsg, ok := <-kv.applyCh:
			if !ok { // killed
				return
			}
			if applyMsg.CommandValid {
				op := applyMsg.Command.(Op)

				requestId := RequestId{ClientId: op.ClientId, Serial: op.Serial}
				rc, requestOk := kv.requestsReplyMap.Load(requestId)
				var requestChan RequestChan
				if requestOk {
					requestChan = rc.(RequestChan)
				}
				kv.mu.Lock()
				kv.index = applyMsg.CommandIndex

				processReply := ProcessReply{}
				// DPrintf("KVServer %d before apply, op %s, op.ClientId %d, op.Serial %d, currentSerial %d, command.index %d, op.Key %s, op.Value %s, currentValue %s", kv.me, op.Op, op.ClientId, op.Serial, kv.lastClientSerial[op.ClientId], applyMsg.CommandIndex, op.Key, op.Value, kv.data[op.Key])
				switch {
				case op.Op == OpGet:
					val, ok := kv.data[op.Key]
					if !ok {
						processReply.Err = ErrNoKey
					}
					processReply.Value = val
				case op.Serial > kv.lastClientSerial[op.ClientId]:
					if op.Op == OpPut {
						kv.data[op.Key] = op.Value
					} else if op.Op == OpAppend {
						kv.data[op.Key] = kv.data[op.Key] + op.Value
					}
				}
				if requestOk {
					if applyMsg.CommandIndex == requestChan.Index && op.Serial > kv.lastClientSerial[op.ClientId] {
						if processReply.Err == "" {
							processReply.Err = OK
						}
					} else {
						processReply.Err = ErrWrongLeader
					}
				}
				if op.Serial > kv.lastClientSerial[op.ClientId] {
					kv.lastClientSerial[op.ClientId] = op.Serial
				}
				kv.mu.Unlock()

				if requestOk {
					requestChan.C <- processReply
				}
			}
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.lastClientSerial = make(map[int]int64)
	kv.applyTimeout = time.Second
	// todo restore snapshot

	go kv.applyCommand()

	return kv
}
