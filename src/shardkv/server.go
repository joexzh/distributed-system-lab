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
	Op       string
	OpClient *OpStore
	OpConfig *OpConfig
}

type OpStore struct {
	Key      string
	Value    string
	ClientId int
	Serial   int64
}

type OpConfig struct {
	Config       *shardctrler.Config
	ConfigNum    int               // for UpdateConfig and ShardsDone and UpdateShardStore
	Shard        int               // for ShardsDone and UpdateShardStore
	ShardStore   map[string]string // for UpdateShardStore
	ClientSerial map[int]int64     // for UpdateShardStore
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
	ShardStore   map[int]map[string]string
	ClientSerial map[int]int64
	Config       shardctrler.Config
	PrevConfig   shardctrler.Config
	ShardsDone   map[int]shardDone
}

const (
	shardDoneTypeSame = iota
	shardDoneTypeNew
	shardDoneTypeLose
)

type shardDone struct {
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
	persister      *raft.Persister
	shardReady     map[int]struct{}
	timeout        time.Duration
	killCh         chan struct{}
	clientRequests map[clientRequestId]clientRequestInfo
	index          int
	mck            *shardctrler.Clerk

	transferDoneRequests map[transferDoneRequestId]transferDoneRequest
	config               shardctrler.Config
	prevConfig           shardctrler.Config
	// persist data
	shardStore   map[int]map[string]string
	clientSerial map[int]int64
	// for config change
	shardsDone map[int]shardDone
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
		OpClient: &OpStore{
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
		Op: args.Op,
		OpClient: &OpStore{
			ClientId: args.ClientId,
			Serial:   args.Serial,
			Key:      args.Key,
			Value:    args.Value,
		},
	}
	generalReply := kv.operate(op)
	reply.Err = generalReply.Err
}

func (kv *ShardKV) operate(op Op) (generalReply GetReply) {
	kv.mu.Lock()
	rightGroup, sd := kv.validateKey(op.OpClient.Key)
	if !rightGroup || !sd {
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

func (kv *ShardKV) validateKey(key string) (rightGroup, shardDone bool) {
	shard := key2shard(key)
	sd, shardDoneOk := kv.shardsDone[shard]
	rightGroup = kv.config.Shards[shard] == kv.gid
	shardDone = shardDoneOk && sd.DataDone && (sd.Type == shardDoneTypeNew || sd.Type == shardDoneTypeSame)
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

	DPrintf("ShardKV me %d gid %d receive TransferShard, meConfigNum %d, meShardStore %+v, meShardDone %+v, args %+v",
		kv.me, kv.gid, kv.config.Num, kv.shardStore, kv.shardsDone, args)
	reply.Success = false
	if kv.gid != args.GID || kv.config.Num != args.ConfigNum {
		return
	}
	if sd, ok := kv.shardsDone[args.Shard]; !ok || (ok && sd.ReplyDone) {
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
	sd, shardDoneOk := kv.shardsDone[args.Shard]
	if !shardDoneOk || sd.Type != shardDoneTypeLose {
		kv.mu.Unlock()
		return
	}
	if (kv.config.Num == args.ConfigNum && sd.ReplyDone) || kv.config.Num > args.ConfigNum {
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
	result := make(chan transferDoneResult, 1)
	kv.transferDoneRequests[requestId] = transferDoneRequest{index: index, c: result}
	kv.mu.Unlock()

	timer := time.NewTimer(kv.timeout)
	defer timer.Stop()
	select {
	case <-timer.C:
		reply.WrongLeader = true
	case ret := <-result:
		reply.WrongLeader = ret.wrongLeader
		reply.Success = ret.success
	}
	kv.mu.Lock()
	delete(kv.transferDoneRequests, requestId)
	kv.mu.Unlock()
}

func (kv *ShardKV) queryConfig() {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}
	kv.mu.RLock()
	if !kv.checkConfigDone() {
		transferShardArgsList, transferDoneArgsList := kv.args4Rpc()
		kv.mu.RUnlock()
		kv.sendRPCs(transferShardArgsList, transferDoneArgsList)
		return
	}
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
			return
		case <-time.After(100 * time.Millisecond):
			kv.queryConfig()
		}
	}
}

func (kv *ShardKV) args4Rpc() ([]TransferShardArgs, []TransferShardDoneArgs) {
	var shardArgs []TransferShardArgs
	var shardDoneArgs []TransferShardDoneArgs
	for shard := range kv.shardsDone {
		// new shard from gid previous owned
		if !kv.shardsDone[shard].DataDone {
			args := TransferShardArgs{
				ConfigNum: kv.config.Num,
				GID:       kv.prevConfig.Shards[shard],
				Shard:     shard,
			}
			shardArgs = append(shardArgs, args)
			continue
		}
		// new shard applied, not yet confirm to gid previous owned
		if !kv.shardsDone[shard].ReplyDone && kv.shardsDone[shard].Type == shardDoneTypeNew {
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

func (kv *ShardKV) sendRPCs(shardArgsList []TransferShardArgs, doneArgsList []TransferShardDoneArgs) {
	for i := range shardArgsList {
		go func(args TransferShardArgs) {
			reply := kv.sendTransferShard(&args)
			if !reply.Success {
				return
			}
			op := Op{Op: OpUpdateShardStore, OpConfig: &OpConfig{ConfigNum: args.ConfigNum, Shard: args.Shard,
				ShardStore: reply.ShardStore, ClientSerial: reply.ClientSerial}}
			kv.rf.Start(op)
		}(shardArgsList[i])
	}
	for i := range doneArgsList {
		go func(args TransferShardDoneArgs) {
			reply := kv.sendTransferShardDone(&args)
			if !reply.Success {
				return
			}
			op := Op{Op: OpShardDone, OpConfig: &OpConfig{ConfigNum: args.ConfigNum, Shard: args.Shard}}
			kv.rf.Start(op)
		}(doneArgsList[i])
	}
}

func (kv *ShardKV) sendTransferShard(args *TransferShardArgs) TransferShardReply {
	kv.mu.RLock()
	if kv.config.Num != args.ConfigNum {
		kv.mu.RUnlock()
		return TransferShardReply{Success: false}
	}
	sNames := kv.prevConfig.Groups[args.GID]
	kv.mu.RUnlock()
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

func (kv *ShardKV) sendTransferShardDone(args *TransferShardDoneArgs) TransferShardDoneReply {
	kv.mu.RLock()
	if kv.config.Num != args.ConfigNum {
		kv.mu.RUnlock()
		return TransferShardDoneReply{Success: false}
	}
	sNames := kv.prevConfig.Groups[args.GID]
	kv.mu.RUnlock()
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
	shardsDone := map[int]shardDone{}
	if newConfig.Num == 1 {
		for shard, gid := range newConfig.Shards {
			if gid == kv.gid {
				shardsDone[shard] = shardDone{DataDone: true, ReplyDone: true, Type: shardDoneTypeSame}
			}
		}
	} else {
		for shard, gid := range kv.config.Shards {
			if gid == kv.gid {
				shardsDone[shard] = shardDone{DataDone: true, ReplyDone: false, Type: shardDoneTypeLose}
			}
		}
		for shard, gid := range newConfig.Shards {
			if gid == kv.gid {
				sd, shardDoneOk := shardsDone[shard]
				if shardDoneOk {
					sd.ReplyDone = true
					sd.Type = shardDoneTypeSame
					shardsDone[shard] = sd
					continue
				}
				shardsDone[shard] = shardDone{DataDone: false, ReplyDone: false, Type: shardDoneTypeNew}
			}
		}
	}

	kv.prevConfig = kv.config
	kv.config = newConfig
	kv.shardsDone = shardsDone
	DPrintf("ShardKV me %d gid %d updated config %+v, shardsDone %+v", kv.me, kv.gid, kv.config, kv.shardsDone)
}

func (kv *ShardKV) checkConfigDone() bool {
	if len(kv.shardsDone) == 0 {
		return true
	}
	for shard := range kv.shardsDone {
		if !kv.shardsDone[shard].DataDone || !kv.shardsDone[shard].ReplyDone {
			return false
		}
	}
	return true
}

func (kv *ShardKV) applyUpdateConfig(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Num+1 != op.OpConfig.Config.Num {
		return
	}

	if kv.checkConfigDone() {
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
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := op.OpConfig.Shard
	rId := transferDoneRequestId{configNum: op.OpConfig.ConfigNum, shard: shard}
	kv.replyInvalidTransferDoneRequests(index, rId)

	request, requestOk := kv.transferDoneRequests[rId]
	sd, sdOk := kv.shardsDone[shard]
	result := transferDoneResult{success: false}
	if kv.config.Num == op.OpConfig.ConfigNum && sdOk && sd.DataDone && !sd.ReplyDone {
		sd.ReplyDone = true
		kv.shardsDone[shard] = sd
	}
	if kv.config.Num == op.OpConfig.ConfigNum && sdOk && sd.ReplyDone {
		result.success = true
		// if success, delete lose shard
		if sd.Type == shardDoneTypeLose {
			delete(kv.shardStore, shard)
		}
	}

	if requestOk {
		request.replied = true
		request.c <- result
		kv.transferDoneRequests[rId] = request
	}
}

func (kv *ShardKV) applyUpdateShardStore(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := op.OpConfig.Shard
	if shard >= len(kv.config.Shards) || kv.config.Num != op.OpConfig.ConfigNum || kv.config.Shards[shard] != kv.gid {
		return
	}
	sd, shardDoneOk := kv.shardsDone[shard]
	if shardDoneOk && !sd.DataDone {
		// copy shard
		// if me is in old config, me will reject client write to shard according to wrong group,
		// if me is in the latest config same as client, me will reject client write to shard according to shardDone[shard].DataDone,
		// so it's safe to overwrite shard data
		kv.shardStore[shard] = map[string]string{}
		for k, v := range op.OpConfig.ShardStore {
			kv.shardStore[shard][k] = v
		}
		sd.DataDone = true
		kv.shardsDone[shard] = sd
		// update client serial > than me, to prevent duplicate apply
		for id, serial := range op.OpConfig.ClientSerial {
			if serial > kv.clientSerial[id] {
				kv.clientSerial[id] = serial
			}
		}
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
		rInfo := kv.clientRequests[invalidRids[i]]
		rInfo.replied = true
		rInfo.c <- processResult{err: ErrWrongLeader}
		kv.clientRequests[invalidRids[i]] = rInfo
	}
}

func (kv *ShardKV) applyGetPutAppend(index int, op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	rId := clientRequestId{clientId: op.OpClient.ClientId, serial: op.OpClient.Serial}
	kv.replyInvalidRequests(index, rId)
	rInfo, requestOk := kv.clientRequests[rId]

	processReply := processResult{err: OK}
	rightGroup, sd := kv.validateKey(op.OpClient.Key)
	if !rightGroup || !sd {
		if requestOk {
			if !rightGroup {
				processReply.err = ErrWrongGroup
			} else {
				processReply.err = ErrTransferShardUnDone
			}
			rInfo.replied = true
			rInfo.c <- processReply
			kv.clientRequests[rId] = rInfo
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
		DPrintf("ShardKV gid %d, me %d startApply command: client %d, new serial %d, old serial %d, requestIndex %d, commandIndex %d, command %+v, reply.err %s",
			kv.gid, kv.me, op.OpClient.ClientId, op.OpClient.Serial, kv.clientSerial[op.OpClient.ClientId], rInfo.index, index, op, processReply.err)
		kv.clientSerial[op.OpClient.ClientId] = op.OpClient.Serial
	}

	// check should snapshot
	if kv.maxraftstate > -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
		DPrintf("ShardKV gid %d, me %d startApply command: maxraftstate %d exceeded, call Snapshot(), client %d, serial %d",
			kv.gid, kv.me, kv.maxraftstate, op.OpClient.ClientId, op.OpClient.Serial)
		data := kv.encodeSnapshot()
		kv.rf.Snapshot(kv.index, data)
	}

	if requestOk {
		rInfo.replied = true
		rInfo.c <- processReply
		kv.clientRequests[rId] = rInfo
	}
}

func (kv *ShardKV) startApply() {
	for applyMsg := range kv.applyCh {
		switch {
		case applyMsg.CommandValid:
			kv.index = applyMsg.CommandIndex
			op, ok := applyMsg.Command.(Op)
			if !ok {
				continue
			}
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
		case applyMsg.SnapshotValid:
			kv.mu.Lock()
			if applyMsg.SnapshotIndex >= kv.index {
				if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
					kv.switch2Snapshot(applyMsg.SnapshotIndex, applyMsg.Snapshot)
					DPrintf("ShardKV gid %d, me %d startApply snapshot: switching snapshot, snapshotIndex %d, snapshotTerm %d, kv.index %d", kv.gid, kv.me, applyMsg.SnapshotIndex, applyMsg.SnapshotTerm, kv.index)
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	snapshot := kvSnapshot{
		ShardStore:   kv.shardStore,
		ClientSerial: kv.clientSerial,
		Config:       kv.config,
		PrevConfig:   kv.prevConfig,
		ShardsDone:   kv.shardsDone,
	}
	e.Encode(snapshot)
	return w.Bytes()
}

func (kv *ShardKV) decodeSnapshot(data []byte) kvSnapshot {
	snapshot := kvSnapshot{}
	if len(data) == 0 {
		snapshot.ShardStore = map[int]map[string]string{}
		snapshot.ClientSerial = map[int]int64{}
		snapshot.ShardsDone = map[int]shardDone{}
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
	kv.shardsDone = snapshot.ShardsDone
	kv.index = lastIncludedIndex
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.killCh)
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

	DPrintf("ShardKV me %d, gid %d join", kv.me, kv.gid)

	return kv
}
