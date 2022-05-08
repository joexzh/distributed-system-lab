package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
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

type RequestInfo struct {
	Index int
	C     chan ProcessReply
}

type KvSnapshot struct {
	Store        map[string]string
	ClientSerial map[int]int64
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister

	// flow control
	applyTimeout     time.Duration
	requestsReplyMap sync.Map

	index int
	// fields should be snapshot
	Store        map[string]string
	ClientSerial map[int]int64 // key: clientId, value: serial number
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.RLock()
	if kv.ClientSerial[args.ClientId] >= args.Serial {
		value, ok := kv.Store[args.Key]
		reply.Value = value
		reply.Err = OK
		if !ok {
			reply.Err = ErrNoKey
		}
		DPrintf("KVServer %d Get serial skip, client %d, args.Serial %d, Serial %d", kv.me, args.ClientId, args.Serial, kv.ClientSerial[args.ClientId])
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	op := Op{Key: args.Key, Op: OpGet, ClientId: args.ClientId, Serial: args.Serial}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.Leader = kv.rf.GetVoteFor()
		// DPrintf("KVServer %d Get wrong leader, newLeader %d, client %d, args.Serial %d, Serial %d", kv.me, leader, args.ClientId, args.Serial, kv.ClientSerial[args.ClientId])
		return
	}
	requestId := RequestId{ClientId: args.ClientId, Serial: args.Serial}
	processCh := make(chan ProcessReply, 1)
	requestInfo := RequestInfo{Index: index, C: processCh}
	kv.requestsReplyMap.Store(requestId, requestInfo)
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
		if reply.Err == ErrWrongLeader {
			reply.Leader = kv.rf.GetVoteFor()
		}
	}
	kv.requestsReplyMap.Delete(requestId)
	// DPrintf("KVServer %d Get process done, args.Client %d, args.Serial %d, reply.Err %s, reply.Value %s", kv.me, args.ClientId, args.Serial, reply.Err, reply.Value)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.RLock()
	if kv.ClientSerial[args.ClientId] >= args.Serial {
		DPrintf("KVServer %d PutAppend Serial skip, client %d, args.Serial %d, Serial %d", kv.me, args.ClientId, args.Serial, kv.ClientSerial[args.ClientId])
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
		DPrintf("KVServer %d PutAppend wrong leader, client %d, args.Serial %d, reply.Leader %d", kv.me, args.ClientId, args.Serial, reply.Leader)
		return
	}
	requestId := RequestId{ClientId: args.ClientId, Serial: args.Serial}
	processCh := make(chan ProcessReply, 1)
	requestInfo := RequestInfo{Index: index, C: processCh}
	kv.requestsReplyMap.Store(requestId, requestInfo)
	DPrintf("KVServer %d PutAppend kv.Start, index %d, args.ClientId %d, args.Serial %d", kv.me, index, args.ClientId, args.Serial)

	select {
	case <-time.After(kv.applyTimeout):
		reply.Err = ErrWrongLeader
		voteFor := kv.rf.GetVoteFor()
		if kv.me != voteFor {
			reply.Leader = voteFor
		}
	case processReply := <-processCh:
		reply.Err = processReply.Err
		if reply.Err == ErrWrongLeader {
			reply.Leader = kv.rf.GetVoteFor()
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

func (kv *KVServer) apply() {
	// kv.applyCh is closed, this loop breaks
	for applyMsg := range kv.applyCh {
		switch {
		case applyMsg.CommandValid:
			op := applyMsg.Command.(Op)
			requestId := RequestId{ClientId: op.ClientId, Serial: op.Serial}
			requestObj, requestOk := kv.requestsReplyMap.Load(requestId)
			var requestInfo RequestInfo
			if requestOk {
				requestInfo = requestObj.(RequestInfo)
			}
			kv.mu.Lock()
			kv.index = applyMsg.CommandIndex

			processReply := ProcessReply{Value: ErrWrongLeader}
			// DPrintf("KVServer %d before apply, op %s, op.ClientId %d, op.Serial %d, currentSerial %d, command.index %d, op.Key %s, op.Value %s, currentValue %s", kv.me, op.Op, op.ClientId, op.Serial, kv.ClientSerial[op.ClientId], apply.CommandIndex, op.Key, op.Value, kv.Store[op.Key])
			if op.Serial > kv.ClientSerial[op.ClientId] {
				switch {
				case op.Op == OpGet:
					if !requestOk {
						break
					}
					val, ok := kv.Store[op.Key]
					if !ok {
						processReply.Err = ErrNoKey
					}
					processReply.Value = val
				case op.Op == OpPut:
					kv.Store[op.Key] = op.Value
				case op.Op == OpAppend:
					kv.Store[op.Key] = kv.Store[op.Key] + op.Value
				}
				if requestOk && applyMsg.CommandIndex == requestInfo.Index {
					if processReply.Err == "" {
						processReply.Err = OK
					}
				}
				kv.ClientSerial[op.ClientId] = op.Serial
				DPrintf("KVServer %d apply command: client %d, new serial %d, old serial %d, requestIndex %d, commandIndex %d, command %v, reply.Err %s",
					kv.me, op.ClientId, op.Serial, kv.ClientSerial[op.ClientId], requestInfo.Index, applyMsg.CommandIndex, applyMsg.Command, processReply.Err)
			}

			// check should snapshot
			if kv.maxraftstate > -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				DPrintf("KVServer %d apply command: maxraftstate %d exceeded, call Snapshot(), client %d, serial %d", kv.me, kv.maxraftstate, op.ClientId, op.Serial)
				data := kv.encodeSnapshot()
				kv.rf.Snapshot(kv.index, data)
			}
			kv.mu.Unlock()

			if requestOk {
				requestInfo.C <- processReply
			}
		case applyMsg.SnapshotValid:
			kv.mu.Lock()
			if applyMsg.SnapshotIndex >= kv.index {
				if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
					DPrintf("KVServer %d apply snapshot: switching snapshot, snapshotIndex %d, snapshotTerm %d, kv.index %d", kv.me, applyMsg.SnapshotIndex, applyMsg.SnapshotTerm, kv.index)
					kv.switch2Snapshot(applyMsg.SnapshotIndex, applyMsg.Snapshot)
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Store)
	e.Encode(kv.ClientSerial)
	return w.Bytes()
}

func (kv *KVServer) decodeSnapshot(data []byte) KvSnapshot {
	snapshot := KvSnapshot{
		Store:        map[string]string{},
		ClientSerial: map[int]int64{},
	}
	if len(data) == 0 {
		return snapshot
	}

	b := bytes.NewBuffer(data)
	d := labgob.NewDecoder(b)
	if d.Decode(&snapshot.Store) != nil {
		snapshot.Store = map[string]string{}
	}
	if d.Decode(&snapshot.ClientSerial) != nil {
		snapshot.ClientSerial = map[int]int64{}
	}
	return snapshot
}

func (kv *KVServer) switch2Snapshot(lastIncludedIndex int, data []byte) {
	kvss := kv.decodeSnapshot(data)
	kv.Store = kvss.Store
	kv.ClientSerial = kvss.ClientSerial
	if lastIncludedIndex > 0 {
		kv.index = lastIncludedIndex
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should Store snapshots through the underlying Raft
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
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.applyTimeout = time.Second
	// restore snapshot
	ss := kv.rf.LoadSnapshot()
	kv.switch2Snapshot(ss.LastIncludedIndex, ss.Data)

	go kv.apply()

	return kv
}
