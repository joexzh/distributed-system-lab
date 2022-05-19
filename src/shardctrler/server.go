package shardctrler

import (
	"6.824/raft"
	"bytes"
	"sort"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	clientSerial map[int]int64
	requests     map[requestId]requestInfo
	index        int
	timeout      time.Duration
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	configs []Config // indexed by config num
}

type scSnapshot struct {
	ClientSerial map[int]int64
	Configs      []Config
}

type Op struct {
	// Your data here.
	ClientId         int
	Serial           int64
	Op               string
	GIDServerMap     map[int][]string // used for Join and Leave GIDs
	ShardOrConfigNum int              // used for Move: shard and Query: number
	GID              int              // used for Move: GID
}

type processResult struct {
	wrongLeader bool
	config      Config
	err         Err
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

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		ClientId:     args.ClientId,
		Serial:       args.Serial,
		Op:           OpJoin,
		GIDServerMap: args.Servers,
	}
	generalReply := sc.operate(op)
	reply.Err = generalReply.Err
	reply.WrongLeader = generalReply.WrongLeader
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	gidMap := make(map[int][]string, len(args.GIDs))
	for i := range args.GIDs {
		gidMap[args.GIDs[i]] = nil
	}

	op := Op{
		ClientId:     args.ClientId,
		Serial:       args.Serial,
		Op:           OpLeave,
		GIDServerMap: gidMap,
	}
	generalReply := sc.operate(op)
	reply.Err = generalReply.Err
	reply.WrongLeader = generalReply.WrongLeader
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		ClientId:         args.ClientId,
		Serial:           args.Serial,
		Op:               OpMove,
		GIDServerMap:     nil,
		ShardOrConfigNum: args.Shard,
		GID:              args.GID,
	}
	generalReply := sc.operate(op)
	reply.Err = generalReply.Err
	reply.WrongLeader = generalReply.WrongLeader
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		ClientId:         args.ClientId,
		Serial:           args.Serial,
		Op:               OpQuery,
		GIDServerMap:     nil,
		ShardOrConfigNum: args.Num,
	}
	generalReply := sc.operate(op)
	reply.Err = generalReply.Err
	reply.WrongLeader = generalReply.WrongLeader
	reply.Config = generalReply.Config
}

func (sc *ShardCtrler) operate(op Op) (generalReply QueryReply) {
	sc.mu.Lock()
	if sc.clientSerial[op.ClientId] >= op.Serial {
		generalReply.Err = OK
		if op.Op == OpQuery {
			c, ok := sc.queryConfig(op.ShardOrConfigNum)
			if !ok {
				generalReply.Err = ErrConfigNotFound
			}
			generalReply.Config = c
		}
		sc.mu.Unlock()
		return
	}
	rId := requestId{clientId: op.ClientId, serial: op.Serial}
	_, ok := sc.requests[rId]
	if ok {
		sc.mu.Unlock()
		generalReply.Err = ErrDuplicateRequest
		return
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		sc.mu.Unlock()
		generalReply.WrongLeader = true
		return
	}

	processCh := make(chan processResult, 1)
	sc.requests[rId] = requestInfo{index: index, c: processCh}
	sc.mu.Unlock()

	timer := time.NewTimer(sc.timeout)
	defer timer.Stop()
	select {
	case <-timer.C:
		generalReply.Err = ErrTimeout
		generalReply.WrongLeader = true
	case result := <-processCh:
		generalReply.Err = result.err
		generalReply.Config = result.config
	}
	sc.mu.Lock()
	delete(sc.requests, rId)
	sc.mu.Unlock()
	return
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) queryConfig(num int) (Config, bool) {
	configsLen := len(sc.configs)
	if num == -1 {
		return sc.configs[configsLen-1], true
	}
	for i := configsLen - 1; i > -1; i-- {
		if num >= sc.configs[i].Num {
			return sc.configs[i], true
		}
	}
	return Config{}, false
}

func (sc *ShardCtrler) processJoinLeave(joinServers map[int][]string, leaveServers map[int][]string) {
	lastConfig := sc.configs[len(sc.configs)-1]

	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Groups: make(map[int][]string, len(lastConfig.Groups)),
	}
	// copy group
	for gid, v := range lastConfig.Groups {
		newConfig.Groups[gid] = v
	}
	// add join group
	for gid := range joinServers {
		if gid == 0 {
			continue
		}
		newConfig.Groups[gid] = joinServers[gid]
	}
	// delete leave group
	for gid := range leaveServers {
		delete(newConfig.Groups, gid)
	}
	reBalance(&newConfig)
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) processMove(shard, gid int) Err {
	lastConfig := sc.configs[len(sc.configs)-1]

	if shard < 0 || shard > len(lastConfig.Shards)-1 {
		return ErrShardNotFound
	}
	_, ok := lastConfig.Groups[gid]
	if !ok {
		return ErrGIDNotFound
	}

	newShards := lastConfig.Shards
	newShards[shard] = gid
	newGroups := make(map[int][]string, len(lastConfig.Groups))
	// copy group
	for gid, v := range lastConfig.Groups {
		newGroups[gid] = v
	}
	newCfg := Config{
		Num:    lastConfig.Num + 1,
		Shards: newShards,
		Groups: newGroups,
	}
	sc.configs = append(sc.configs, newCfg)
	return OK
}

// replyInvalidRequests reply wrong leader to previous index requests, except for the one matches applyMsg requestId
func (sc *ShardCtrler) replyInvalidRequests(index int, currentRid requestId) {
	invalidRids := make([]requestId, 0)
	for rId := range sc.requests {
		if sc.requests[rId].index <= index && rId != currentRid && sc.requests[rId].replied == false {
			invalidRids = append(invalidRids, rId)
		}
	}
	for i := range invalidRids {
		rInfo := sc.requests[invalidRids[i]]
		rInfo.replied = true
		rInfo.c <- processResult{wrongLeader: true}
		sc.requests[invalidRids[i]] = rInfo
	}
}

func (sc *ShardCtrler) apply() {
	// kv.applyCh is closed, this loop breaks
	for applyMsg := range sc.applyCh {
		switch {
		case applyMsg.CommandValid:

			sc.index = applyMsg.CommandIndex
			op, ok := applyMsg.Command.(Op)
			if !ok {
				continue
			}
			sc.mu.Lock()
			rId := requestId{clientId: op.ClientId, serial: op.Serial}
			sc.replyInvalidRequests(applyMsg.CommandIndex, rId)
			reqeust, requestOk := sc.requests[rId]
			processReply := processResult{err: OK}
			if op.Op == OpQuery {
				if requestOk {
					c, ok := sc.queryConfig(op.ShardOrConfigNum)
					if !ok {
						processReply.err = ErrConfigNotFound
					}
					processReply.config = c
				}
			}
			if op.Serial > sc.clientSerial[op.ClientId] {
				switch {
				case op.Op == OpJoin:
					sc.processJoinLeave(op.GIDServerMap, nil)
				case op.Op == OpLeave:
					sc.processJoinLeave(nil, op.GIDServerMap)
				case op.Op == OpMove:
					processReply.err = sc.processMove(op.ShardOrConfigNum, op.GID)
				}
				sc.clientSerial[op.ClientId] = op.Serial
			}
			if requestOk && reqeust.replied == false {
				reqeust.replied = true
				reqeust.c <- processReply
				sc.requests[rId] = reqeust
			}
			// check should snapshot
			if sc.maxraftstate > -1 && sc.persister.RaftStateSize() >= sc.maxraftstate {
				DPrintf("ShardCtrler %d apply command: maxraftstate %d exceeded, call Snapshot(), client %d, serial %d", sc.me, sc.maxraftstate, op.ClientId, op.Serial)
				data := sc.encodeSnapshot()
				sc.rf.Snapshot(sc.index, data)
			}
			sc.mu.Unlock()
		case applyMsg.SnapshotValid:
			sc.mu.Lock()
			if applyMsg.SnapshotIndex >= sc.index {
				if sc.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
					sc.switch2Snapshot(applyMsg.SnapshotIndex, applyMsg.Snapshot)
					DPrintf("ShardCtrler %d apply snapshot: switching snapshot, snapshotIndex %d, snapshotTerm %d, kv.index %d", sc.me, applyMsg.SnapshotIndex, applyMsg.SnapshotTerm, sc.index)
				}
			}
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) encodeSnapshot() []byte {
	snapshotS := scSnapshot{
		ClientSerial: sc.clientSerial,
		Configs:      sc.configs,
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(snapshotS)
	return w.Bytes()
}

func (sc *ShardCtrler) decodeSnapshot(data []byte) scSnapshot {
	snapshotS := scSnapshot{}
	if len(data) == 0 {
		snapshotS.Configs = make([]Config, 1)
		snapshotS.Configs[0].Groups = map[int][]string{}
		snapshotS.ClientSerial = map[int]int64{}
		return snapshotS
	}

	b := bytes.NewBuffer(data)
	d := labgob.NewDecoder(b)
	if err := d.Decode(&snapshotS); err != nil {
		panic(err)
	}
	return snapshotS
}

func (sc *ShardCtrler) switch2Snapshot(lastIncludedIndex int, data []byte) {
	snapshotS := sc.decodeSnapshot(data)
	sc.clientSerial = snapshotS.ClientSerial
	sc.configs = snapshotS.Configs
	sc.index = lastIncludedIndex
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.persister = persister
	sc.requests = map[requestId]requestInfo{}
	sc.timeout = time.Second
	sc.maxraftstate = 10000
	ss := sc.rf.LoadSnapshot()
	sc.switch2Snapshot(ss.LastIncludedIndex, ss.Data)

	go sc.apply()

	return sc
}

func reBalance(config *Config) {
	if len(config.Groups) == 0 {
		for i := range config.Shards {
			config.Shards[i] = 0
		}
		return
	}
	gids := make([]int, 0, len(config.Groups))
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	// assign shard to group
	for shard := range config.Shards {
		groupIndex := shard % len(config.Groups)
		config.Shards[shard] = gids[groupIndex]
	}
}
