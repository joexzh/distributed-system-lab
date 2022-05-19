package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"sync/atomic"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	id     int
	serial int64
	leader int32
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
	// Your code here.
	ck.id = int(nrand())
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.ClientId = ck.id
	args.Serial = atomic.AddInt64(&ck.serial, 1)
	args.Num = num
	for {
		// try each known server.
		for range ck.servers {
			leader := atomic.LoadInt32(&ck.leader)
			var reply QueryReply
			ok := ck.servers[leader].Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
			ck.changeLeader(leader, -1)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.ClientId = ck.id
	args.Serial = atomic.AddInt64(&ck.serial, 1)
	args.Servers = servers

	for {
		// try each known server.
		for range ck.servers {
			leader := atomic.LoadInt32(&ck.leader)
			var reply JoinReply
			ok := ck.servers[leader].Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
			ck.changeLeader(leader, -1)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.ClientId = ck.id
	args.Serial = atomic.AddInt64(&ck.serial, 1)
	args.GIDs = gids

	for {
		// try each known server.
		for range ck.servers {
			leader := atomic.LoadInt32(&ck.leader)
			var reply LeaveReply
			ok := ck.servers[leader].Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
			ck.changeLeader(leader, -1)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.ClientId = ck.id
	args.Serial = atomic.AddInt64(&ck.serial, 1)
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for range ck.servers {
			leader := atomic.LoadInt32(&ck.leader)
			var reply MoveReply
			ok := ck.servers[leader].Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
			ck.changeLeader(leader, -1)
		}
	}
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
