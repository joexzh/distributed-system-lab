package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op       int
	OpClient *OpClient
	OpConfig *OpConfig
}

type OpClient struct {
	ClientId int
	Serial   int64
	Key      string
	Value    string
}

type OpConfig struct {
	ConfigNum    int                 // for TransferTable and UpdateShardStore
	Shard        int                 // for TransferTable and UpdateShardStore
	Config       *shardctrler.Config // for UpdateConfig
	ShardStore   map[string]string   // for UpdateShardStore
	ClientSerial map[int]int64       // for UpdateShardStore
}

type clientRequestId struct {
	clientId int
	serial   int64
}

type clientRequestInfo struct {
	replied bool
	index   int
	c       chan processResult
}

type processResult struct {
	value string
	err   Err
}

type transferDoneRequestId struct {
	configNum int
	shard     int
}

type transferDoneRequest struct {
	replied bool
	index   int
	c       chan transferDoneResult
}

type transferDoneResult struct {
	wrongLeader bool
	success     bool
}

type kvSnapshot struct {
	Config        shardctrler.Config
	PrevConfig    shardctrler.Config
	ClientSerial  map[int]int64
	TransferTable map[int]transferStatus
	ShardStore    map[int]map[string]string
}

const (
	transferStatusSame = 1
	transferStatusNew  = 2
	transferStatusLose = 3
)

type transferStatus struct {
	DataDone  bool
	ReplyDone bool
	Type      int
}

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister            *raft.Persister
	timeout              time.Duration
	killCh               chan struct{}
	clientRequests       map[clientRequestId]clientRequestInfo
	transferDoneRequests map[transferDoneRequestId]transferDoneRequest
	index                int
	term                 int // detect term change, for sending blank no-op entry
	mck                  *shardctrler.Clerk

	// persist data
	shardStore   map[int]map[string]string
	clientSerial map[int]int64
	// for config change
	transferTable map[int]transferStatus
	config        shardctrler.Config
	prevConfig    shardctrler.Config
}

func (kv *ShardKV) shardStoreValue(key string) (v string, ok bool) {
	shard := key2shard(key)
	store, ok := kv.shardStore[shard]
	if !ok {
		return "", false
	}
	v, ok = store[key]
	return
}

func (kv *ShardKV) setShardStoreValue(key, value string) {
	shard := key2shard(key)
	_, ok := kv.shardStore[shard]
	if !ok {
		kv.shardStore[shard] = map[string]string{}
	}
	kv.shardStore[shard][key] = value
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Op: OpGet,
		OpClient: &OpClient{
			ClientId: args.ClientId,
			Serial:   args.Serial,
			Key:      args.Key,
		},
	}
	generalReply := kv.operate(op)
	reply.Err = generalReply.Err
	reply.Value = generalReply.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	op := Op{
		OpClient: &OpClient{
			ClientId: args.ClientId,
			Serial:   args.Serial,
			Key:      args.Key,
			Value:    args.Value,
		},
	}
	if args.Op == "Put" {
		op.Op = OpPut
	}
	if args.Op == "Append" {
		op.Op = OpAppend
	}
	generalReply := kv.operate(op)
	reply.Err = generalReply.Err
}

func (kv *ShardKV) operate(op Op) (generalReply GetReply) {
	kv.mu.Lock()
	rightGroup, shardDataDone := kv.validateKey(op.OpClient.Key)
	if !rightGroup || !shardDataDone {
		kv.mu.Unlock()
		if !rightGroup {
			generalReply.Err = ErrWrongGroup
		} else {
			generalReply.Err = ErrTransferShardUnDone
		}
		return
	}
	if kv.clientSerial[op.OpClient.ClientId] >= op.OpClient.Serial {
		generalReply.Err = OK
		if op.Op == OpGet {
			v, ok := kv.shardStoreValue(op.OpClient.Key)
			if !ok {
				generalReply.Err = ErrNoKey
			}
			generalReply.Value = v
		}
		kv.mu.Unlock()
		return
	}
	rId := clientRequestId{clientId: op.OpClient.ClientId, serial: op.OpClient.Serial}
	_, ok := kv.clientRequests[rId]
	if ok {
		kv.mu.Unlock()
		generalReply.Err = ErrDuplicateRequest
		return
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		generalReply.Err = ErrWrongLeader
		return
	}
	resultCh := make(chan processResult, 1)
	kv.clientRequests[rId] = clientRequestInfo{index: index, c: resultCh}
	kv.mu.Unlock()

	timer := time.NewTimer(kv.timeout)
	defer timer.Stop()
	select {
	case <-timer.C:
		generalReply.Err = ErrWrongLeader
	case result := <-resultCh:
		generalReply.Err = result.err
		generalReply.Value = result.value
	}
	kv.mu.Lock()
	delete(kv.clientRequests, rId)
	kv.mu.Unlock()
	return
}

func (kv *ShardKV) validateKey(key string) (rightGroup, shardDataDone bool) {
	shard := key2shard(key)
	status, statusOk := kv.transferTable[shard]
	rightGroup = kv.config.Shards[shard] == kv.gid
	shardDataDone = statusOk && status.DataDone && (status.Type == transferStatusNew || status.Type == transferStatusSame)
	return
}

type TransferShardArgs struct {
	ConfigNum int
	GID       int
	Shard     int
}

type TransferShardReply struct {
	ShardStore   map[string]string
	ClientSerial map[int]int64
	Success      bool
}

// TransferShard transfer shard data along with the clientSerial data to prevent duplicate apply.
// for real system, client serial simply to be timeStamp as each key version.
// when client send request, assign a "TrueTime" timeStamp as request version.
func (kv *ShardKV) TransferShard(args *TransferShardArgs, reply *TransferShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("ShardKV me %d gid %d receive TransferShard, meConfigNum %d, meShardStore %+v, meTransferTable %+v, args %+v",
		kv.me, kv.gid, kv.config.Num, kv.shardStore, kv.transferTable, args)
	reply.Success = false
	if kv.gid != args.GID || kv.config.Num != args.ConfigNum {
		return
	}
	status, statusOk := kv.transferTable[args.Shard]
	if !statusOk || status.Type != transferStatusLose {
		return
	}

	// copy shard
	store := kv.shardStore[args.Shard]
	reply.ShardStore = map[string]string{}
	for k, v := range store {
		reply.ShardStore[k] = v
	}
	// copy clientSerial to prevent duplicate apply
	reply.ClientSerial = map[int]int64{}
	for id, serial := range kv.clientSerial {
		reply.ClientSerial[id] = serial
	}
	reply.Success = true
}

type TransferShardDoneArgs struct {
	ConfigNum int
	GID       int
	Shard     int
}
type TransferShardDoneReply struct {
	WrongLeader bool
	Success     bool
}

func (kv *ShardKV) TransferShardDone(args *TransferShardDoneArgs, reply *TransferShardDoneReply) {
	kv.mu.Lock()

	reply.Success = false
	if kv.gid != args.GID || args.ConfigNum < kv.config.Num || args.Shard >= len(kv.prevConfig.Shards) {
		kv.mu.Unlock()
		return
	}
	status, statusOk := kv.transferTable[args.Shard]
	if kv.config.Num == args.ConfigNum && (!statusOk || status.Type != transferStatusLose) {
		kv.mu.Unlock()
		return
	}
	if (kv.config.Num == args.ConfigNum && status.ReplyDone) || kv.config.Num > args.ConfigNum {
		kv.mu.Unlock()
		reply.Success = true
		return
	}
	requestId := transferDoneRequestId{configNum: args.ConfigNum, shard: args.Shard}
	_, ok := kv.transferDoneRequests[requestId]
	if ok {
		kv.mu.Unlock()
		return
	}
	op := Op{Op: OpShardDone, OpConfig: &OpConfig{ConfigNum: args.ConfigNum, Shard: args.Shard}}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.WrongLeader = true
		return
	}
	resultCh := make(chan transferDoneResult, 1)
	kv.transferDoneRequests[requestId] = transferDoneRequest{index: index, c: resultCh}
	kv.mu.Unlock()

	timer := time.NewTimer(kv.timeout)
	defer timer.Stop()
	select {
	case <-timer.C:
		reply.WrongLeader = true
	case ret := <-resultCh:
		reply.WrongLeader = ret.wrongLeader
		reply.Success = ret.success
	}
	kv.mu.Lock()
	delete(kv.transferDoneRequests, requestId)
	kv.mu.Unlock()
}

func (kv *ShardKV) queryConfig() {
	term, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}
	if term > kv.term {
		// if detected new term, add a blank no-op log entry to make sure raft can move forward
		kv.term = term
		kv.rf.Start(Op{Op: NoOp})
	}
	kv.mu.RLock()
	newDone, loseDone := kv.checkConfigDone()
	if !newDone || !loseDone {
		DPrintf("ShardKV me %d gid %d checkConfigDone() false, configNum %d, transferTable %+v", kv.me, kv.gid, kv.config.Num, kv.transferTable)
		if !newDone {
			transferShardArgsList, transferDoneArgsList := kv.args4Rpc()
			prevConfig := kv.prevConfig
			kv.mu.RUnlock()
			kv.sendRPCs(transferShardArgsList, transferDoneArgsList, &prevConfig)
			return
		}
		kv.mu.RUnlock()
		return
	}
	DPrintf("ShardKV me %d gid %d checkConfigDone() true, configNum %d", kv.me, kv.gid, kv.config.Num)
	nextNum := kv.config.Num + 1
	kv.mu.RUnlock()

	nextConfig := kv.mck.Query(nextNum)

	kv.mu.RLock()
	if nextConfig.Num != kv.config.Num+1 {
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	// start new config
	DPrintf("ShardKV me %d gid %d get config: %+v", kv.me, kv.gid, nextConfig)
	op := Op{Op: OpUpdateConfig, OpConfig: &OpConfig{Config: &nextConfig}}
	kv.rf.Start(op)
}

func (kv *ShardKV) startPollConfig() {
	kv.queryConfig()
	for {
		select {
		case <-kv.killCh:
			DPrintf("ShardKV me %d gid %d exit StartPollConfig", kv.me, kv.gid)
			return
		case <-time.After(100 * time.Millisecond):
			kv.queryConfig()
		}
	}
}

func (kv *ShardKV) checkConfigDone() (newDone, loseDone bool) {
	newDone, loseDone = true, true
	if kv.config.Num < 2 || len(kv.transferTable) == 0 {
		return true, true
	}
	for shard := range kv.transferTable {
		if kv.transferTable[shard].Type == transferStatusNew && (!kv.transferTable[shard].DataDone || !kv.transferTable[shard].ReplyDone) {
			newDone = false
			continue
		}
		if kv.transferTable[shard].Type == transferStatusLose && !kv.transferTable[shard].ReplyDone {
			loseDone = false
		}
	}
	return
}

func (kv *ShardKV) args4Rpc() ([]TransferShardArgs, []TransferShardDoneArgs) {
	var shardArgs []TransferShardArgs
	var shardDoneArgs []TransferShardDoneArgs
	for shard := range kv.transferTable {
		// new shard from gid previous owned
		if !kv.transferTable[shard].DataDone && kv.transferTable[shard].Type == transferStatusNew {
			args := TransferShardArgs{
				ConfigNum: kv.config.Num,
				GID:       kv.prevConfig.Shards[shard],
				Shard:     shard,
			}
			shardArgs = append(shardArgs, args)
			continue
		}
		// new shard applied, not yet confirm to gid previous owned
		if !kv.transferTable[shard].ReplyDone && kv.transferTable[shard].DataDone && kv.transferTable[shard].Type == transferStatusNew {
			args := TransferShardDoneArgs{
				ConfigNum: kv.config.Num,
				GID:       kv.prevConfig.Shards[shard],
				Shard:     shard,
			}
			shardDoneArgs = append(shardDoneArgs, args)
		}
	}
	return shardArgs, shardDoneArgs
}

func (kv *ShardKV) sendRPCs(shardArgsList []TransferShardArgs, doneArgsList []TransferShardDoneArgs, prevConfig *shardctrler.Config) {
	wg := sync.WaitGroup{}
	for i := range shardArgsList {
		wg.Add(1)
		go func(args TransferShardArgs) {
			defer wg.Done()
			reply := kv.sendTransferShard(&args, prevConfig)
			if !reply.Success {
				return
			}
			op := Op{Op: OpUpdateShardStore, OpConfig: &OpConfig{ConfigNum: args.ConfigNum, Shard: args.Shard,
				ShardStore: reply.ShardStore, ClientSerial: reply.ClientSerial}}
			kv.rf.Start(op)
		}(shardArgsList[i])
	}
	for i := range doneArgsList {
		wg.Add(1)
		go func(args TransferShardDoneArgs) {
			defer wg.Done()
			reply := kv.sendTransferShardDone(&args, prevConfig)
			if !reply.Success {
				return
			}
			op := Op{Op: OpShardDone, OpConfig: &OpConfig{ConfigNum: args.ConfigNum, Shard: args.Shard}}
			kv.rf.Start(op)
		}(doneArgsList[i])
	}
	wg.Wait()
}

func (kv *ShardKV) sendTransferShard(args *TransferShardArgs, prevConfig *shardctrler.Config) TransferShardReply {
	sNames := prevConfig.Groups[args.GID]
	for i := range sNames {
		clientEnd := kv.make_end(sNames[i])
		var reply TransferShardReply
		ok := clientEnd.Call("ShardKV.TransferShard", args, &reply)
		if !ok {
			continue
		}
		if reply.Success {
			return reply
		}
	}
	return TransferShardReply{Success: false}
}

func (kv *ShardKV) sendTransferShardDone(args *TransferShardDoneArgs, prevConfig *shardctrler.Config) TransferShardDoneReply {
	sNames := prevConfig.Groups[args.GID]
	for i := range sNames {
		clientEnd := kv.make_end(sNames[i])
		var reply TransferShardDoneReply
		ok := clientEnd.Call("ShardKV.TransferShardDone", args, &reply)
		if !ok {
			continue
		}
		if reply.Success {
			return reply
		}
	}
	return TransferShardDoneReply{Success: false}
}

func (kv *ShardKV) updateConfig(newConfig shardctrler.Config) {
	tb := map[int]transferStatus{}
	if newConfig.Num == 1 {
		for shard, gid := range newConfig.Shards {
			if gid == kv.gid {
				tb[shard] = transferStatus{DataDone: true, ReplyDone: true, Type: transferStatusSame}
			}
		}
	} else {
		for shard, gid := range kv.config.Shards {
			if gid == kv.gid {
				tb[shard] = transferStatus{DataDone: true, ReplyDone: false, Type: transferStatusLose}
			}
		}
		for shard, gid := range newConfig.Shards {
			if gid == kv.gid {
				status, statusOk := tb[shard]
				if statusOk {
					status.ReplyDone = true
					status.Type = transferStatusSame
					tb[shard] = status
					continue
				}
				tb[shard] = transferStatus{DataDone: false, ReplyDone: false, Type: transferStatusNew}
			}
		}
	}

	kv.prevConfig = kv.config
	kv.config = newConfig
	kv.transferTable = tb
	DPrintf("ShardKV me %d gid %d updated config %+v, transferTable %+v, previousConfig %+v", kv.me, kv.gid, kv.config, kv.transferTable, kv.prevConfig)
}

func (kv *ShardKV) applyUpdateConfig(op Op) {
	if kv.config.Num+1 != op.OpConfig.Config.Num {
		return
	}

	newDone, loseDone := kv.checkConfigDone()
	if newDone && loseDone {
		kv.updateConfig(*op.OpConfig.Config)
	}
}

func (kv *ShardKV) replyInvalidTransferDoneRequests(index int, currentRid transferDoneRequestId) {
	var invalidRids []transferDoneRequestId
	for rId := range kv.transferDoneRequests {
		if kv.transferDoneRequests[rId].index <= index && rId != currentRid && kv.transferDoneRequests[rId].replied == false {
			invalidRids = append(invalidRids, rId)
		}
	}
	for i := range invalidRids {
		request := kv.transferDoneRequests[invalidRids[i]]
		request.replied = true
		request.c <- transferDoneResult{wrongLeader: true, success: false}
		kv.transferDoneRequests[invalidRids[i]] = request
	}
}

func (kv *ShardKV) applyShardDone(index int, op Op) {
	shard := op.OpConfig.Shard
	rId := transferDoneRequestId{configNum: op.OpConfig.ConfigNum, shard: shard}
	kv.replyInvalidTransferDoneRequests(index, rId)

	request, requestOk := kv.transferDoneRequests[rId]
	status, statusOk := kv.transferTable[shard]
	result := transferDoneResult{success: false}
	if kv.config.Num == op.OpConfig.ConfigNum && statusOk && status.DataDone && !status.ReplyDone {
		status.ReplyDone = true
		kv.transferTable[shard] = status
		result.success = true
		// if success, delete lose shard
		if status.Type == transferStatusLose {
			delete(kv.shardStore, shard)
		}
		DPrintf("ShardKV me %d gid %d applied ShardDone, configNum %d, shard %d, status %+v", kv.me, kv.gid, kv.config.Num, shard, status)
	}
	if requestOk && request.replied == false {
		request.replied = true
		request.c <- result
		kv.transferDoneRequests[rId] = request
	}
}

func (kv *ShardKV) applyUpdateShardStore(op Op) {
	shard := op.OpConfig.Shard
	if shard >= len(kv.config.Shards) || kv.config.Num != op.OpConfig.ConfigNum || kv.config.Shards[shard] != kv.gid {
		return
	}
	status, statusOk := kv.transferTable[shard]
	if statusOk && !status.DataDone && !status.ReplyDone && status.Type == transferStatusNew {
		// copy shard
		// if me is in old config, me will reject client write to shard according to wrong group,
		// if me is in the latest config same as client, me will reject client write to shard according to transferStatus[shard].DataDone,
		// so it's safe to overwrite shard data
		kv.shardStore[shard] = map[string]string{}
		for k, v := range op.OpConfig.ShardStore {
			kv.shardStore[shard][k] = v
		}
		// update client serial > than me, to prevent duplicate apply
		for id, serial := range op.OpConfig.ClientSerial {
			if serial > kv.clientSerial[id] {
				kv.clientSerial[id] = serial
			}
		}
		status.DataDone = true
		kv.transferTable[shard] = status
		DPrintf("ShardKV me %d gid %d applied UpdateShard, shard %d, config %d map %+v",
			kv.me, kv.gid, shard, op.OpConfig.ConfigNum, op.OpConfig.ShardStore)
	}
}

// replyInvalidRequests reply wrong leader to previous index clientRequests, except for the one matches applyMsg clientRequestId
func (kv *ShardKV) replyInvalidRequests(index int, currentRid clientRequestId) {
	var invalidRids []clientRequestId
	for rId := range kv.clientRequests {
		if kv.clientRequests[rId].index <= index && rId != currentRid && kv.clientRequests[rId].replied == false {
			invalidRids = append(invalidRids, rId)
		}
	}
	for i := range invalidRids {
		request := kv.clientRequests[invalidRids[i]]
		request.replied = true
		request.c <- processResult{err: ErrWrongLeader}
		kv.clientRequests[invalidRids[i]] = request
	}
}

func (kv *ShardKV) applyGetPutAppend(index int, op Op) {
	rId := clientRequestId{clientId: op.OpClient.ClientId, serial: op.OpClient.Serial}
	kv.replyInvalidRequests(index, rId)
	request, requestOk := kv.clientRequests[rId]

	processReply := processResult{err: OK}
	rightGroup, shardDataDone := kv.validateKey(op.OpClient.Key)
	if !rightGroup || !shardDataDone {
		if requestOk && request.replied == false {
			if !rightGroup {
				processReply.err = ErrWrongGroup
			} else {
				processReply.err = ErrTransferShardUnDone
			}
			request.replied = true
			request.c <- processReply
			kv.clientRequests[rId] = request
		}
		return
	}
	if op.Op == OpGet {
		if requestOk {
			v, ok := kv.shardStoreValue(op.OpClient.Key)
			if !ok {
				processReply.err = ErrNoKey
			}
			processReply.value = v
		}
	}
	if op.OpClient.Serial > kv.clientSerial[op.OpClient.ClientId] {
		switch {
		case op.Op == OpPut:
			kv.setShardStoreValue(op.OpClient.Key, op.OpClient.Value)
		case op.Op == OpAppend:
			value, _ := kv.shardStoreValue(op.OpClient.Key)
			value = value + op.OpClient.Value
			kv.setShardStoreValue(op.OpClient.Key, value)
		}
		DPrintf("ShardKV me %d gid %d startApply command: client %d, new serial %d, old serial %d, requestIndex %d, commandIndex %d, command %+v, reply.err %s",
			kv.me, kv.gid, op.OpClient.ClientId, op.OpClient.Serial, kv.clientSerial[op.OpClient.ClientId], request.index, index, *op.OpClient, processReply.err)
		kv.clientSerial[op.OpClient.ClientId] = op.OpClient.Serial
	}

	if requestOk && request.replied == false {
		request.replied = true
		request.c <- processReply
		kv.clientRequests[rId] = request
	}
}

func (kv *ShardKV) startApply() {
	for applyMsg := range kv.applyCh {
		switch {
		case applyMsg.CommandValid:
			kv.index = applyMsg.CommandIndex
			op, ok := applyMsg.Command.(Op)
			if !ok {
				DPrintf("ShardKV me %d gid %d WARNING: command cannot convert to Op! This should never happen!!!", kv.me, kv.gid)
				continue
			}
			DPrintf("ShardKV me %d gid %d receive command, op %d", kv.me, kv.gid, op.Op)
			kv.mu.Lock()
			switch op.Op {
			case OpUpdateConfig:
				kv.applyUpdateConfig(op)
			case OpShardDone:
				kv.applyShardDone(applyMsg.CommandIndex, op)
			case OpUpdateShardStore:
				kv.applyUpdateShardStore(op)
			case OpGet, OpPut, OpAppend:
				kv.applyGetPutAppend(applyMsg.CommandIndex, op)
			}
			// if state size to big, snapshot it
			if kv.maxraftstate > -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				DPrintf("ShardKV me %d gid %d startApply command: maxraftstate %d exceeded, call Snapshot(), index %d",
					kv.me, kv.gid, kv.maxraftstate, kv.index)
				data := kv.encodeSnapshot()
				kv.rf.Snapshot(kv.index, data)
			}
			kv.mu.Unlock()
		case applyMsg.SnapshotValid:
			kv.mu.Lock()
			if applyMsg.SnapshotIndex >= kv.index {
				if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
					kv.switch2Snapshot(applyMsg.SnapshotIndex, applyMsg.Snapshot)
					DPrintf("ShardKV me %d gid %d startApply snapshot: switching snapshot, snapshotIndex %d, snapshotTerm %d, kv.index %d",
						kv.me, kv.gid, applyMsg.SnapshotIndex, applyMsg.SnapshotTerm, kv.index)
				}
			}
			kv.mu.Unlock()
		}
	}
	DPrintf("ShardKV me %d gid %d exit startApply()", kv.me, kv.gid)
}

func (kv *ShardKV) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	snapshot := kvSnapshot{
		Config:        kv.config,
		PrevConfig:    kv.prevConfig,
		ClientSerial:  kv.clientSerial,
		TransferTable: kv.transferTable,
		ShardStore:    kv.shardStore,
	}
	e.Encode(snapshot)
	return w.Bytes()
}

func (kv *ShardKV) decodeSnapshot(data []byte) kvSnapshot {
	snapshot := kvSnapshot{}
	if len(data) == 0 {
		snapshot.ShardStore = map[int]map[string]string{}
		snapshot.ClientSerial = map[int]int64{}
		snapshot.TransferTable = map[int]transferStatus{}
		return snapshot
	}

	b := bytes.NewBuffer(data)
	d := labgob.NewDecoder(b)
	if err := d.Decode(&snapshot); err != nil {
		panic(err)
	}
	return snapshot
}

func (kv *ShardKV) switch2Snapshot(lastIncludedIndex int, data []byte) {
	snapshot := kv.decodeSnapshot(data)
	kv.shardStore = snapshot.ShardStore
	kv.clientSerial = snapshot.ClientSerial
	kv.config = snapshot.Config
	kv.prevConfig = snapshot.PrevConfig
	kv.transferTable = snapshot.TransferTable
	kv.index = lastIncludedIndex
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	DPrintf("ShardKV me %d gid %d start Kill()", kv.me, kv.gid)
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.killCh)
	DPrintf("ShardKV me %d gid %d Kill() success", kv.me, kv.gid)
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.timeout = time.Second
	kv.killCh = make(chan struct{})
	kv.shardStore = map[int]map[string]string{}
	kv.clientRequests = map[clientRequestId]clientRequestInfo{}
	kv.transferDoneRequests = map[transferDoneRequestId]transferDoneRequest{}
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	// Your initialization code here.
	// Use something like this to talk to the shardctrler:

	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	rfSnapshot := kv.rf.LoadSnapshot()
	kv.switch2Snapshot(rfSnapshot.LastIncludedIndex, rfSnapshot.Data)

	go kv.startApply()
	go kv.startPollConfig()

	DPrintf("ShardKV me %d gid %d join, snapshot index %d, rf.Log %+v, config %+v, transferTable %+v", kv.me, kv.gid, kv.index, kv.rf.Log, kv.config, kv.transferTable)

	return kv
}
