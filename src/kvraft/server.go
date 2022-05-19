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

type processResult struct {
	value string
	err   Err
}

type requestId struct {
	clientId int
	serial   int64
}

type requestInfo struct {
	replied bool
	index   int
	c       chan processResult
}

type kvSnapshot struct {
	Store        map[string]string
	ClientSerial map[int]int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister

	// flow control
	applyTimeout time.Duration
	requests     map[requestId]requestInfo

	index int
	// fields should be snapshot
	store        map[string]string
	clientSerial map[int]int64 // key: clientId, value: serial number
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Leader = -1
	kv.mu.Lock()
	if kv.clientSerial[args.ClientId] >= args.Serial {
		value, ok := kv.store[args.Key]
		reply.Value = value
		reply.Err = OK
		if !ok {
			reply.Err = ErrNoKey
		}
		DPrintf("KVServer %d Get: serial skip, client %d, args.serial %d, serial %d", kv.me, args.ClientId, args.Serial, kv.clientSerial[args.ClientId])
		kv.mu.Unlock()
		return
	}
	rId := requestId{clientId: args.ClientId, serial: args.Serial}
	_, ok := kv.requests[rId]
	if ok {
		kv.mu.Unlock()
		reply.Err = ErrDuplicateRequest
		DPrintf("KVServer %d me Get: duplicate rId %v", kv.me, rId)
		return
	}
	op := Op{Key: args.Key, Op: OpGet, ClientId: args.ClientId, Serial: args.Serial}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		reply.Leader = kv.rf.GetLeader()
		DPrintf("KVServer %d Get: kv.rf.Start(op) wrong leader, newLeader %d, client %d, args.serial %d", kv.me, reply.Leader, args.ClientId, args.Serial)
		return
	}
	processCh := make(chan processResult, 1)
	rInfo := requestInfo{index: index, c: processCh}
	kv.requests[rId] = rInfo
	kv.mu.Unlock()
	DPrintf("KVServer %d Get accept, index %d, args.Key %s, args.clientId %d, args.serial %d", kv.me, index, args.Key, args.ClientId, args.Serial)

	timer := time.NewTimer(kv.applyTimeout)
	defer timer.Stop()
	select {
	case <-timer.C:
		reply.Err = ErrWrongLeader
		leader := kv.rf.GetLeader()
		if kv.me != leader {
			reply.Leader = leader
		}
	case processReply := <-processCh:
		reply.Value = processReply.value
		reply.Err = processReply.err
	}
	kv.mu.Lock()
	delete(kv.requests, rId)
	kv.mu.Unlock()
	// DPrintf("KVServer %d Get process done, args.Client %d, args.serial %d, reply.err %s, reply.value %s", kv.me, args.clientId, args.serial, reply.err, reply.value)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Leader = -1
	kv.mu.Lock()
	if kv.clientSerial[args.ClientId] >= args.Serial {
		DPrintf("KVServer %d PutAppend: serial skip, client %d, args.serial %d, serial %d", kv.me, args.ClientId, args.Serial, kv.clientSerial[args.ClientId])
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	rId := requestId{clientId: args.ClientId, serial: args.Serial}
	_, ok := kv.requests[rId]
	if ok {
		kv.mu.Unlock()
		reply.Err = ErrDuplicateRequest
		DPrintf("KVServer %d me PutAppend: duplicate rId %v", kv.me, rId)
		return
	}

	op := Op{Key: args.Key, Value: args.Value, Op: args.Op, ClientId: args.ClientId, Serial: args.Serial}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		reply.Leader = kv.rf.GetLeader()
		DPrintf("KVServer %d PutAppend: kv.rf.Start(op) wrong leader, client %d, args.serial %d, reply.Leader %d", kv.me, args.ClientId, args.Serial, reply.Leader)
		return
	}

	processCh := make(chan processResult, 1)
	rInfo := requestInfo{index: index, c: processCh}
	kv.requests[rId] = rInfo
	kv.mu.Unlock()
	DPrintf("KVServer %d accept %s, index %d, args.Key %s, args.value %s, args.clientId %d, args.serial %d", kv.me, args.Op, index, args.Key, args.Value, args.ClientId, args.Serial)

	timer := time.NewTimer(kv.applyTimeout)
	defer timer.Stop()
	select {
	case <-timer.C:
		reply.Err = ErrWrongLeader
		voteFor := kv.rf.GetLeader()
		if kv.me != voteFor {
			reply.Leader = voteFor
		}
	case processReply := <-processCh:
		reply.Err = processReply.err
	}
	kv.mu.Lock()
	delete(kv.requests, rId)
	kv.mu.Unlock()
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

// replyInvalidRequests reply wrong leader to previous index requests, except for the one matches applyMsg requestId
func (kv *KVServer) replyInvalidRequests(index int, currentRid requestId) {
	invalidRids := make([]requestId, 0)
	for rId := range kv.requests {
		if kv.requests[rId].index <= index && rId != currentRid && kv.requests[rId].replied == false {
			invalidRids = append(invalidRids, rId)
		}
	}
	for i := range invalidRids {
		rInfo := kv.requests[invalidRids[i]]
		rInfo.replied = true
		rInfo.c <- processResult{err: ErrWrongLeader}
		kv.requests[invalidRids[i]] = rInfo
	}
}

func (kv *KVServer) apply() {
	// kv.applyCh is closed, this loop breaks
	for applyMsg := range kv.applyCh {
		switch {
		case applyMsg.CommandValid:
			kv.index = applyMsg.CommandIndex
			op, ok := applyMsg.Command.(Op)
			if !ok {
				continue
			}
			kv.mu.Lock()
			rId := requestId{clientId: op.ClientId, serial: op.Serial}
			kv.replyInvalidRequests(applyMsg.CommandIndex, rId)

			rInfo, requestOk := kv.requests[rId]
			processReply := processResult{err: OK}
			if op.Op == OpGet {
				if requestOk {
					val, ok := kv.store[op.Key]
					if !ok {
						processReply.err = ErrNoKey
					}
					processReply.value = val
				}
			}
			if op.Serial > kv.clientSerial[op.ClientId] {
				switch {
				case op.Op == OpPut:
					kv.store[op.Key] = op.Value
				case op.Op == OpAppend:
					kv.store[op.Key] = kv.store[op.Key] + op.Value
				}
				DPrintf("KVServer %d apply command: client %d, new serial %d, old serial %d, requestIndex %d, commandIndex %d, command %v, reply.err %s",
					kv.me, op.ClientId, op.Serial, kv.clientSerial[op.ClientId], rInfo.index, applyMsg.CommandIndex, applyMsg.Command, processReply.err)
				kv.clientSerial[op.ClientId] = op.Serial
			}
			if requestOk {
				rInfo.replied = true
				rInfo.c <- processReply
				kv.requests[rId] = rInfo
			}

			// check should snapshot
			if kv.maxraftstate > -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				DPrintf("KVServer %d apply command: maxraftstate %d exceeded, call Snapshot(), client %d, serial %d", kv.me, kv.maxraftstate, op.ClientId, op.Serial)
				data := kv.encodeSnapshot()
				kv.rf.Snapshot(kv.index, data)
			}
			kv.mu.Unlock()
		case applyMsg.SnapshotValid:
			kv.mu.Lock()
			if applyMsg.SnapshotIndex >= kv.index {
				if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
					kv.switch2Snapshot(applyMsg.SnapshotIndex, applyMsg.Snapshot)
					DPrintf("KVServer %d apply snapshot: switching snapshot, snapshotIndex %d, snapshotTerm %d, kv.index %d", kv.me, applyMsg.SnapshotIndex, applyMsg.SnapshotTerm, kv.index)
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) encodeSnapshot() []byte {
	snapshot := kvSnapshot{
		Store:        kv.store,
		ClientSerial: kv.clientSerial,
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(snapshot)
	return w.Bytes()
}

func (kv *KVServer) decodeSnapshot(data []byte) kvSnapshot {
	snapshot := kvSnapshot{
		Store:        map[string]string{},
		ClientSerial: map[int]int64{},
	}
	if len(data) == 0 {
		return snapshot
	}

	b := bytes.NewBuffer(data)
	d := labgob.NewDecoder(b)
	if err := d.Decode(&snapshot); err != nil {
		panic(err)
	}

	return snapshot
}

func (kv *KVServer) switch2Snapshot(lastIncludedIndex int, data []byte) {
	kvss := kv.decodeSnapshot(data)
	kv.store = kvss.Store
	kv.clientSerial = kvss.ClientSerial
	kv.index = lastIncludedIndex
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
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.applyTimeout = time.Second
	kv.requests = map[requestId]requestInfo{}
	// restore snapshot
	ss := kv.rf.LoadSnapshot()
	kv.switch2Snapshot(ss.LastIncludedIndex, ss.Data)

	go kv.apply()

	return kv
}
