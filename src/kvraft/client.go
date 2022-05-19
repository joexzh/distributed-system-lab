package kvraft

import (
	"6.824/labrpc"
	"sync"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu     sync.Mutex
	id     int
	Serial int64
	leader int32

	rpcTimeout time.Duration
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = int(nrand())
	ck.rpcTimeout = 1500 * time.Millisecond
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	serial := atomic.AddInt64(&ck.Serial, 1)
	for {
		DPrintf("Clerk start Get: client %d, serial %d, key %s", ck.id, serial, key)
		args := GetArgs{
			Key:      key,
			ClientId: ck.id,
			Serial:   serial,
		}
		for i := 0; i < len(ck.servers); i++ {
			leader := atomic.LoadInt32(&ck.leader)
			reply := ck.sendGetWithTimeout(int(leader), &args)
			if reply != nil {
				if reply.Err == OK {
					DPrintf("clerk result Get: client %d, serial %d, ok, reply.err %s, leader %d, key %s, value %s",
						ck.id, serial, reply.Err, leader, key, reply.Value)
					return reply.Value
				}
				if reply.Err == ErrNoKey {
					DPrintf("clerk result Get: client %d, serial %d, ok, reply.err %s, leader %d, key %s, value %s",
						ck.id, serial, reply.Err, leader, key, reply.Value)
					return ""
				}
			}
			// wrong leader or not ok
			DPrintf("clerk result Get: client %d, serial %d, NOT OK or WrongLeader, leader %d, key %s",
				ck.id, serial, leader, key)
			newLeader := -1
			if reply != nil {
				newLeader = reply.Leader
			}
			ck.changeLeader(leader, int32(newLeader))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	serial := atomic.AddInt64(&ck.Serial, 1)
	for {
		DPrintf("Clerk start %s: client %d, serial %d, key %s, value %s", op, ck.id, serial, key, value)
		args := PutAppendArgs{
			Key:      key,
			Value:    value,
			Op:       op,
			ClientId: ck.id,
			Serial:   serial,
		}
		for i := 0; i < len(ck.servers); i++ {
			leader := atomic.LoadInt32(&ck.leader)
			reply := ck.sendPutAppendWithTimeout(int(leader), &args)
			if reply != nil && reply.Err == OK {
				DPrintf("clerk result %s: client %d, serial %d, ok, reply.err %s, leader %d, key %s, value %s",
					op, ck.id, serial, reply.Err, leader, key, value)
				return
			}
			// not ok or wrong leader
			DPrintf("clerk result %s: client %d, serial %d, NOT OK or WrongLeader, leader %d, key %s, value %s",
				op, ck.id, serial, leader, key, value)
			newLeader := -1
			if reply != nil {
				newLeader = reply.Leader
			}
			ck.changeLeader(leader, int32(newLeader))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}

func (ck *Clerk) sendGetWithTimeout(server int, args *GetArgs) *GetReply {
	// reply := &GetReply{}
	// ok := ck.sendGet(server, args, reply)
	// if ok {
	// 	return reply
	// }
	// return nil

	done := make(chan *GetReply, 1)

	go func() {
		reply := &GetReply{}
		ok := ck.sendGet(server, args, reply)
		if !ok {
			done <- nil
			return
		}
		done <- reply
	}()

	timer := time.NewTimer(ck.rpcTimeout)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case reply := <-done:
		return reply
	}
}

func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply) bool {
	return ck.servers[server].Call("KVServer.Get", args, reply)
}

func (ck *Clerk) sendPutAppendWithTimeout(server int, args *PutAppendArgs) *PutAppendReply {
	done := make(chan *PutAppendReply, 1)

	go func() {
		reply := &PutAppendReply{}
		ok := ck.sendPutAppend(server, args, reply)
		if !ok {
			done <- nil
			return
		}
		done <- reply
	}()

	timer := time.NewTimer(ck.rpcTimeout)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case reply := <-done:
		return reply
	}
}

func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	return ck.servers[server].Call("KVServer.PutAppend", args, reply)
}

// changeLeader, simply add 1.
func (ck *Clerk) changeLeader(oldLeader, newLeader int32) {
	// todo looks like the tester don't support assign leader
	// if leader < 0 {
	// 	ck.leader = (ck.leader + 1) % len(ck.servers)
	// 	return
	// }
	// ck.leader = leader
	atomic.CompareAndSwapInt32(&ck.leader, oldLeader, (oldLeader+1)%int32(len(ck.servers)))
}
