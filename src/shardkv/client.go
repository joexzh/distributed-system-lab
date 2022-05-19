package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.824/labrpc"
	"sync"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	mu     sync.RWMutex
	id     int
	serial int64
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.id = int(nrand())
	ck.config = ck.sm.Query(-1)
	DPrintf("MakeClerk %d", ck.id)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.ClientId = ck.id
	args.Serial = atomic.AddInt64(&ck.serial, 1)
	shard := key2shard(key)

	for {
		// shard := key2shard(key)
		ck.mu.RLock()
		gid := ck.config.Shards[shard]
		servers, ok := ck.config.Groups[gid]
		ck.mu.RUnlock()
		if ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				DPrintf("Clerk start Get: %+v", args)
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					DPrintf("Clerk result Get success: id %d, serial %d, err %s, key %s, value %s", ck.id, args.Serial, reply.Err, args.Key, reply.Value)
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					DPrintf("Clerk result Get fail: id %d, serial %d, err %s, key %s, value %s", ck.id, args.Serial, reply.Err, args.Key, reply.Value)
					break
				}
				// ... not ok, or ErrWrongLeader
				if ok {
					DPrintf("Clerk result Get fail: id %d, serial %d, err %s, key %s, value %s", ck.id, args.Serial, reply.Err, args.Key, reply.Value)
				} else {
					DPrintf("Clerk result Get fail: id %d, serial %d, not ok, key %s, value %s", ck.id, args.Serial, args.Key, reply.Value)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.

		newCfg := ck.sm.Query(-1)
		ck.mu.Lock()
		ck.config = newCfg
		ck.mu.Unlock()
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.id
	args.Serial = atomic.AddInt64(&ck.serial, 1)
	shard := key2shard(key)

	for {
		// shard := key2shard(key)
		ck.mu.RLock()
		gid := ck.config.Shards[shard]
		servers, ok := ck.config.Groups[gid]
		ck.mu.RUnlock()
		if ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				DPrintf("Clerk start %s: %+v", op, args)
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					DPrintf("Clerk result %s success: id %d, serial %d, err %s, key %s, value %s", op, ck.id, args.Serial, reply.Err, args.Key, args.Value)
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					DPrintf("Clerk result %s fail: id %d, serial %d, err %s, key %s, value %s", op, ck.id, args.Serial, reply.Err, args.Key, args.Value)
					break
				}
				// ... not ok, or ErrWrongLeader
				if ok {
					DPrintf("Clerk result %s fail: id %d, serial %d, err %s, key %s, value %s", op, ck.id, args.Serial, reply.Err, args.Key, args.Value)
				} else {
					DPrintf("Clerk result %s fail: id %d, serial %d, not ok, key %s, value %s", op, ck.id, args.Serial, args.Key, args.Value)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		newCfg := ck.sm.Query(-1)
		ck.mu.Lock()
		ck.config = newCfg
		ck.mu.Unlock()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
