package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"math/rand"
	"sort"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	follower = iota
	leader
	candidate
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	CurrentTerm int
	VoteFor     int // if no vote, set to -1
	Log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// for leader
	nextIndex           []int
	matchIndex          []int
	stopAppendEntriesCh chan struct{}

	state             int
	applyCh           chan ApplyMsg
	majority          int
	heartbeatInterval time.Duration
	rpcTimeout        time.Duration
	timeoutResetCh    chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term = rf.CurrentTerm
	isleader = rf.state == leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	b := bytes.NewBuffer(data)
	d := labgob.NewDecoder(b)
	if d.Decode(&rf.CurrentTerm) != nil {
		rf.CurrentTerm = 0
	}
	if d.Decode(&rf.VoteFor) != nil {
		rf.VoteFor = -1
	}
	if d.Decode(&rf.Log) != nil || len(rf.Log) == 0 {
		rf.Log = []LogEntry{
			{Term: 0, Command: nil},
		}
	}
	DPrintf("me %d readPersist, CurrentTerm %d, VoteFor %d, log %+v", rf.me, rf.CurrentTerm, rf.VoteFor, rf.Log)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) resetTimeout() {
	rf.timeoutResetCh <- struct{}{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	ConflictTerm  int
	ConflictIndex int
	Success       bool
}

func (rf *Raft) sendCommits(startIndex, endIndex int, from int) {
	DPrintf("me %d start batch sendCommits", rf.me)
	for endIndex >= startIndex {
		commitLogTerm := rf.Log[startIndex].Term
		DPrintf("me %d as state %d AppendEntries committed %d:%d from %d", rf.me, rf.state, commitLogTerm, startIndex, from)

		rf.applyCh <- ApplyMsg{
			Command:      rf.Log[startIndex].Command,
			CommandIndex: startIndex,
			CommandValid: true,
		}
		startIndex++
	}
}

// AppendEntries reply
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// DPrintf("me %d AppendEntries receive from %d, currentTerm %d, %+v", rf.me, args.LeaderId, rf.currentTerm, *args)
	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		reply.Success = false
		DPrintf("me %d reject AppendEntries from %d without reset timer, currentTerm %d, %+v", rf.me, args.LeaderId, reply.Term, *args)
		rf.mu.Unlock()
		return
	}

	persist := false
	// switch to follower, continue to check log entries
	if args.Term > rf.CurrentTerm || (args.Term == rf.CurrentTerm && rf.state == candidate) {
		DPrintf("me %d AppendEntries receive higher term %d > %d from %d, return to follower, state=%v", rf.me, args.Term, reply.Term, args.LeaderId, rf.state)
		rf.convertToFollower()
		rf.CurrentTerm = args.Term
		rf.VoteFor = args.LeaderId
		persist = true
	}

	// check if contains prevLogIndex
	if args.PrevLogIndex > len(rf.Log)-1 || rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		endIndex := len(rf.Log) - 1
		if args.PrevLogIndex < endIndex {
			endIndex = args.PrevLogIndex
		}
		conflictTerm := rf.Log[endIndex].Term
		if conflictTerm == args.PrevLogTerm {
			reply.ConflictTerm = conflictTerm
			reply.ConflictIndex = endIndex
		} else {
			reply.ConflictTerm = conflictTerm
			reply.ConflictIndex = 0
			for i := endIndex; i > -1; i-- {
				if rf.Log[i].Term < conflictTerm {
					reply.ConflictIndex = i + 1
					break
				}
				reply.ConflictIndex = i
			}
		}

	} else {
		// no conflict, apply leader log entries
		reply.Success = true
		for i := 0; i < len(args.Entries); i++ {
			if args.PrevLogIndex+1+i > len(rf.Log)-1 {
				rf.Log = append(rf.Log, args.Entries[i:]...)
				break
			}
			if rf.Log[args.PrevLogIndex+1+i].Term != args.Entries[i].Term {
				rf.Log = append(rf.Log[:args.PrevLogIndex+1+i], args.Entries[i:]...)
				break
			}
		}
		if len(args.Entries) > 0 {
			persist = true
		}

		// commit
		nextCommit := rf.commitIndex + 1
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < len(rf.Log)-1 {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = len(rf.Log) - 1
			}
		}
		rf.sendCommits(nextCommit, rf.commitIndex, args.LeaderId)
	}
	if persist {
		rf.persist()
	}
	rf.mu.Unlock()
	rf.resetTimeout()
}

// appendEntries for leader
func (rf *Raft) appendEntries() {
	doneCh := make(chan *bool, len(rf.peers))
	success, fail := true, false

	for node := range rf.peers {
		if rf.me == node {
			go func(server int) {
				rf.mu.Lock()
				rf.nextIndex[server] = len(rf.Log)
				rf.matchIndex[server] = len(rf.Log) - 1
				rf.mu.Unlock()
				doneCh <- &success
			}(node)
			continue
		}
		go func(node int) {
			var (
				prevLogIndex int
				prevLogTerm  int
				entries      []LogEntry
			)

			rf.mu.RLock()
			if rf.nextIndex[node] >= len(rf.Log) {
				prevLogIndex = len(rf.Log) - 1
				entries = nil
			} else {
				prevLogIndex = rf.nextIndex[node] - 1
				entries = rf.Log[rf.nextIndex[node]:]
			}
			prevLogTerm = rf.Log[prevLogIndex].Term

			args := &AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.RUnlock()

			timeoutReply := rf.sendAppendEntriesWithTimeout(node, args)

			rf.mu.Lock()
			if rf.state != leader {
				rf.mu.Unlock()
				return
			}
			switch {
			case timeoutReply == nil:
				doneCh <- &fail
			case timeoutReply.Success:
				rf.nextIndex[node] += len(entries)
				rf.matchIndex[node] = args.PrevLogIndex + len(entries)

				doneCh <- &success
			case timeoutReply.Term > rf.CurrentTerm:
				DPrintf("me %d appendEntries response a higher term %d > %d from %d, return to follower, state=%d", rf.me, timeoutReply.Term, rf.CurrentTerm, node, rf.state)
				rf.CurrentTerm = timeoutReply.Term
				rf.convertToFollower()
				rf.VoteFor = -1
				rf.persist()

				doneCh <- nil
			case timeoutReply.Term <= rf.CurrentTerm:
				// decrease nextIndex
				if rf.Log[timeoutReply.ConflictIndex].Term == timeoutReply.ConflictTerm {
					rf.nextIndex[node] = timeoutReply.ConflictIndex
				} else {
					rf.nextIndex[node] = timeoutReply.ConflictIndex - 1
				}
				if rf.nextIndex[node] < 1 {
					rf.nextIndex[node] = 1
				}

				doneCh <- &fail
			}
			rf.mu.Unlock()
		}(node)
	}

	// check if commitIndex is changed
	successCount := 0
	for ret := range doneCh {
		if ret == nil {
			break
		}

		if *ret == success {
			successCount++
		}

		rf.mu.Lock()
		// pager's figure 2, leader's commit rule
		if successCount >= rf.majority {
			matchIndices := make([]int, 0, len(rf.peers))
			for i := range rf.peers {
				if rf.matchIndex[i] > rf.commitIndex {
					matchIndices = append(matchIndices, rf.matchIndex[i])
				}
			}
			if len(matchIndices) >= rf.majority {
				sort.Ints(matchIndices)
				n := matchIndices[len(matchIndices)-rf.majority]
				if rf.Log[n].Term == rf.CurrentTerm {

					nextCommit := rf.commitIndex + 1
					rf.commitIndex = n

					rf.sendCommits(nextCommit, rf.commitIndex, rf.me)

					rf.mu.Unlock()
					return
				}
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startAppendEntries() {
	go rf.appendEntries()
	for !rf.killed() {
		select {
		case <-rf.stopAppendEntriesCh:
			return
		case <-time.After(rf.heartbeatInterval):
			go rf.appendEntries()
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesWithTimeout(server int, args *AppendEntriesArgs) *AppendEntriesReply {
	result := TimeoutCall(rf.rpcTimeout, func(done chan<- interface{}) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, args, reply)
		if !ok {
			done <- nil
		}
		done <- reply
	})
	if result != nil {
		return result.(*AppendEntriesReply)
	}
	return nil
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) lastLogTermIndex() (lastLogTerm int, lastLogIndex int) {
	lastLogIndex = len(rf.Log) - 1
	if lastLogIndex > -1 {
		lastLogTerm = rf.Log[lastLogIndex].Term
	}
	return
}

// RequestVote replay RPC
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()

	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	persist := false
	if args.Term > rf.CurrentTerm {
		rf.convertToFollower()
		rf.VoteFor = -1
		rf.CurrentTerm = args.Term
		persist = true
	}

	// check candidate is valid to vote for
	if args.Term > rf.CurrentTerm || (rf.VoteFor == -1 || rf.VoteFor == args.CandidateId) {
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			reply.VoteGranted = true
			DPrintf("me %d vote to %d, return to follower, change term form %d to %d , state=%d", rf.me, args.CandidateId, rf.CurrentTerm, args.Term, rf.state)
			rf.convertToFollower()
			rf.CurrentTerm = args.Term
			rf.VoteFor = args.CandidateId
			persist = true
			rf.resetTimeout()
		}
	} else {
		DPrintf("me %d didn't vote to %d, args.Term %d, my term %d, voteFor %d, args.LastLog %d:%d, myLastLog %d:%d",
			rf.me, args.CandidateId, args.Term, rf.CurrentTerm, rf.VoteFor, args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex)
		reply.VoteGranted = false
	}
	if persist {
		rf.persist()
	}
	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendRequestVoteWithTimeout(server int, args *RequestVoteArgs) *RequestVoteReply {
	result := TimeoutCall(rf.rpcTimeout, func(done chan<- interface{}) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(server, args, reply)
		if !ok {
			done <- nil
		}
		done <- reply
	})
	if result != nil {
		return result.(*RequestVoteReply)
	}
	return nil
}

func (rf *Raft) requestVote() {
	voteCount := 1
	rf.mu.Lock()
	rf.state = candidate
	rf.CurrentTerm++
	rf.VoteFor = rf.me
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	args := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	DPrintf("me %d starts a new election, term %d", rf.me, rf.CurrentTerm)
	rf.persist()
	rf.mu.Unlock()
	rf.resetTimeout()

	replyCh := make(chan *RequestVoteReply, len(rf.peers))
	for node := range rf.peers {
		if node == rf.me {
			go func() { replyCh <- nil }()
			continue
		}

		go func(node int) {
			reply := rf.sendRequestVoteWithTimeout(node, args)
			replyCh <- reply
		}(node)
	}

	node := 0
	for reply := range replyCh {
		rf.mu.Lock()
		switch {
		case reply == nil:
			break
		case reply.VoteGranted:
			voteCount++
			if voteCount >= rf.majority {
				rf.becomeLeader()
				rf.persist()
				rf.mu.Unlock()
				return
			}
		case !reply.VoteGranted:
			if reply.Term > rf.CurrentTerm {
				DPrintf("me %d requestVote response higher term %d > %d from %d, return to follower, state=%d", rf.me, reply.Term, rf.CurrentTerm, node, rf.state)
				rf.CurrentTerm = reply.Term
				rf.convertToFollower()
				rf.VoteFor = -1
				rf.persist()
				rf.mu.Unlock()
				return
			}
		}

		if rf.state == candidate && voteCount+(len(rf.peers)-node-1) < rf.majority {
			DPrintf("me %d requestVote not enough votes, return to follower, state=%d", rf.me, rf.state)
			rf.convertToFollower()
			rf.VoteFor = -1
			rf.persist()

			rf.mu.Unlock()
			// rf.resetTimeout()
			return
		}

		rf.mu.Unlock()
		node++
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.CurrentTerm
	isLeader = rf.state == leader
	index = len(rf.Log)
	if isLeader {
		rf.Log = append(rf.Log, LogEntry{
			Term:    term,
			Command: command,
		})
		rf.persist()
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		select {
		case <-rf.timeoutResetCh:
		case <-time.After(time.Duration(rand.Intn(150)+150)*time.Millisecond + rf.rpcTimeout):
			rf.mu.RLock()
			if rf.state == leader {
				rf.mu.RUnlock()
				continue
			}
			rf.mu.RUnlock()
			go rf.requestVote()
		}
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = leader
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.Log)
	}
	rf.matchIndex = make([]int, len(rf.peers))

	DPrintf("me %d become a leader, term %d, logs %+v", rf.me, rf.CurrentTerm, rf.Log)
	go rf.startAppendEntries()
}

func (rf *Raft) convertToFollower() {
	if rf.state == leader || rf.state == candidate {
		if rf.state == leader {
			rf.stopAppendEntriesCh <- struct{}{}
		}
		DPrintf("me %d convert to follower, stopAppendEntriesCh %v", rf.me, rf.state == leader)
		rf.state = follower
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.Log = []LogEntry{{0, nil}}
	rf.applyCh = applyCh
	rf.majority = len(peers)/2 + 1
	rf.stopAppendEntriesCh = make(chan struct{}, 1)
	rf.rpcTimeout = time.Second
	rf.timeoutResetCh = make(chan struct{}, 1)
	rf.heartbeatInterval = time.Millisecond * 100

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if rf.state == leader {
		go rf.startAppendEntries()
	}
	// start ticker goroutine to start elections
	go rf.ticker()

	DPrintf("me %d join, log %+v", me, rf.Log)
	return rf
}
