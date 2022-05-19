# 6.824 lab 1-4 -  my implementation

http://nil.csail.mit.edu/6.824/2021/index.html

    For education purpose only.

Passed all test including lab 4 challenges.

I think the `raft.go` implementation is good enough.

## ShardKV implementation

### each group holds:

* current config
* previous config
* its own shard transfer table, for example:

| shard | dataDone | replyDone | Type |
|-------|----------|-----------|------|
| 1     | true     | false     | Lose |
| 3     | false    | false     | new  |

When detect config change, re-generate the table

* if I (the group) lose a shard, add record to table as:
  * `shard` - the shard I own in previous config, but lose in current config
  * `dataDone` - default `true`, I have the shard data, true
  * `replyDone`- default `false`, wait for other group to confirm that he has the shard data for current config
  * Type - lose
* if I own a new shard that not in previous config, add record to table as:
  * `shard` - the shard now I own, but not in previous config
  * `dataDone` - default `false`, I don't have the new shard data
  * `replyDone` - default `false`, after I get the new shard data, have I successfully told the other group 
  * Type - new
* if I own the same shard as previous config, add record to table, this record will not change in current config,
    just for clarify
  * `shard`
  * `dataDone` - default `true`
  * `replyDone` - default `true`
  * `type` - `same`


### Configs are updated one by one, monotonically.

When all `dataDone=true` and `replyDone=true` in table, we can consider the config is DONE.

Consider one shard as DONE if it's `dataDone=true` and `replyDone=true` and `type=(new or same)`.

Only when a config is DONE, can go forward to fetch for new configs every 100ms.

Only allow changes to shard when the shard is DONE.

Wait forever for other group to reply for shard `type=lose`.

If type=lose get marked `replyDone=true`, then it's ok to delete the shard data, 
because I (the group holds the table) can guarantee that other group has the shard data. (raft linearizabiliy)

`type=new` is positive, `type=lose` is passive.

      
### RPCs from one group to another, leader to leader at best try

Add two RPC `TransferShard` and `TransferShardDone`.

#### `TransferShard`: ask for shard data

```go 
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

func (kv *ShardKV) TransferShard(args *TransferShardArgs, reply *TransferShardReply)
```

gid servers are from previous config.

If I get the reply data, overwrite the shard data and the clientSerial (version), 
mark `dataDone=true` for the shard.

If failed, retry forever.

#### `TransferShardDone`: tell other group that I have done for `TransferShard`

```go
type TransferShardDoneArgs struct {
	ConfigNum int
	GID       int
	Shard     int
}

type TransferShardDoneReply struct {
	WrongLeader bool
	Success     bool
}

func (kv *ShardKV) TransferShardDone(args *TransferShardDoneArgs, reply *TransferShardDoneReply)
```

gid servers are from previous config.

If success mark shard `replyDone=true` in table. 
If I send the request, then shard type should be `new`.
If I receive the request, then shard type should be `lose`. 

If failed, retry forever.