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
	"6.824/labrpc"
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
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

const (
	follower = iota
	leader
	candidate
)

type LogEntry struct {
	Term    int
	Command interface{}
}

type State struct {
	CurrentTerm int
	VoteFor     int
	Log         []LogEntry
}

type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

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
	CurrentTerm             int
	VoteFor                 int // if no vote, set to -1
	Log                     []LogEntry
	snapshotTerm            int
	snapshotIndex           int
	snapshotDataOnlyForLoad []byte

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// for leader
	nextIndex  []int
	matchIndex []int

	// only for flow control
	state             int
	applyCh           chan ApplyMsg
	applyChMu         sync.Mutex
	majority          int
	heartbeatInterval time.Duration
	heartbeatWaitFlag int32
	electionWaitFlag  int32
	rpcTimeout        time.Duration
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

func (rf *Raft) GetVoteFor() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.VoteFor
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

	if len(data) == 0 { // bootstrap without any state?
		return
	}

	b := bytes.NewBuffer(data)
	d := labgob.NewDecoder(b)
	if d.Decode(&rf.CurrentTerm) != nil {
		rf.CurrentTerm = 0
	}
	if d.Decode(&rf.VoteFor) != nil {
		rf.VoteFor = -1
	}
	if d.Decode(&rf.Log) != nil {
		rf.Log = nil
	}
}

func (rf *Raft) persistSnapshot(state State, snapshot *Snapshot) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(state.CurrentTerm)
	e.Encode(state.VoteFor)
	e.Encode(state.Log)
	stateData := w.Bytes()

	w = new(bytes.Buffer)
	e = labgob.NewEncoder(w)
	e.Encode(snapshot.LastIncludedTerm)
	e.Encode(snapshot.LastIncludedIndex)
	e.Encode(snapshot.Data)
	ssData := w.Bytes()

	rf.persister.SaveStateAndSnapshot(stateData, ssData)
}

func (rf *Raft) readSnapshot(data []byte) *Snapshot {
	var snapshot Snapshot

	if len(data) == 0 { // bootstrap without any state?
		snapshot.LastIncludedIndex = -1
		snapshot.LastIncludedTerm = -1
		return &snapshot
	}

	b := bytes.NewBuffer(data)
	d := labgob.NewDecoder(b)
	if d.Decode(&snapshot.LastIncludedTerm) != nil {
		snapshot.LastIncludedTerm = -1
	}
	if d.Decode(&snapshot.LastIncludedIndex) != nil {
		snapshot.LastIncludedIndex = -1
	}
	if d.Decode(&snapshot.Data) != nil {
		snapshot.Data = nil
	}
	return &snapshot
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot for follower reply.
// Don't implement offset, send the entire snapshot at once.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.CurrentTerm

	if rf.CurrentTerm > args.Term {
		DPrintf("me %d InstallSnapshot reject, myTerm %d, args.Term %d, args.LeaderId %d", rf.me, rf.CurrentTerm, args.Term, args.LeaderId)
		rf.mu.Unlock()
		return
	}
	rf.resetElectionTimeout()
	if args.Term > rf.CurrentTerm || (args.Term == rf.CurrentTerm && rf.state == candidate) {
		DPrintf("me %d InstallSnapshot receive higher term %d > %d from %d, return to follower, state=%v", rf.me, args.Term, reply.Term, args.LeaderId, rf.state)
		rf.convertToFollower()
		rf.CurrentTerm = args.Term
		rf.VoteFor = args.LeaderId
		rf.persist()
	}

	snapshot := Snapshot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
	}
	var snapshotMsg ApplyMsg
	if args.Done {
		snapshotMsg = ApplyMsg{
			SnapshotValid: true,
			SnapshotTerm:  snapshot.LastIncludedTerm,
			SnapshotIndex: snapshot.LastIncludedIndex,
			Snapshot:      snapshot.Data,
		}
		// DPrintf("me %d InstallSnapshot ask server to apply snapshot, old index %d, old term %d, new index %d, new term %d",
		// 	rf.me, rf.snapshotIndex, rf.snapshotTerm, snapshot.LastIncludedIndex, snapshot.LastIncludedTerm)
	}
	rf.mu.Unlock()

	if snapshotMsg.SnapshotValid {
		rf.applyChMu.Lock()
		defer rf.applyChMu.Unlock()
		if rf.killed() {
			return
		}
		rf.applyCh <- snapshotMsg
	}
}

// installSnapshot for leader request.
// don't care rpcTimeout since install snapshot can consume time.
// don't care resend since appendEntries will auto-detect nextIndex[i] is discarded.
func (rf *Raft) installSnapshot(server int, term int) {
	DPrintf("me %d installSnapshot to %d", rf.me, server)
	snapshot := rf.readSnapshot(rf.persister.ReadSnapshot())
	args := &InstallSnapshotArgs{
		Term:              term,
		LeaderId:          rf.me,
		LastIncludedIndex: snapshot.LastIncludedIndex,
		LastIncludedTerm:  snapshot.LastIncludedTerm,
		Offset:            0,
		Data:              snapshot.Data,
		Done:              true,
	}
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if term != rf.CurrentTerm {
			return
		}

		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.convertToFollower()
			rf.VoteFor = -1
		} else {
			rf.nextIndex[server] = rf.snapshotIndex + 1
		}
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastApplied > lastIncludedIndex || rf.snapshotIndex >= lastIncludedIndex {
		DPrintf("me %d CondInstallSnapshot refuse, lastIncludedTerm %d, lastIncludedIndex %d, rf.lastApplied %d, rf.snapshotIndex %d",
			rf.me, lastIncludedTerm, lastIncludedIndex, rf.lastApplied, rf.snapshotIndex)
		return false
	}

	rf.trimLog(lastIncludedIndex, lastIncludedTerm)
	state := State{
		CurrentTerm: rf.CurrentTerm,
		VoteFor:     rf.VoteFor,
		Log:         rf.Log,
	}
	ss := &Snapshot{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              snapshot,
	}
	rf.persistSnapshot(state, ss)
	rf.updateAfterInstalledSnapshot()
	DPrintf("me %d CondInstallSnapshot approve, lastIncludedTerm %d, lastIncludedIndex %d, rf.lastApplied %d, rf.snapshotIndex %d, log %+v",
		rf.me, lastIncludedTerm, lastIncludedIndex, rf.lastApplied, rf.snapshotIndex, rf.Log)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.snapshotIndex >= index {
		return
	}

	pos, ok := rf.positionOf(index)
	if !ok {
		return
	}
	lastIncludedTerm := rf.Log[pos].Term
	rf.trimLog(index, lastIncludedTerm)
	state := State{
		CurrentTerm: rf.CurrentTerm,
		VoteFor:     rf.VoteFor,
		Log:         rf.Log,
	}
	ss := &Snapshot{
		LastIncludedIndex: index,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              snapshot,
	}
	rf.persistSnapshot(state, ss)
	DPrintf("me %d Snapshot, snapshotIndex %d, snapshotTerm %d, commitIndex %d, log %+v", rf.me, rf.snapshotIndex, rf.snapshotTerm, rf.commitIndex, rf.Log)
}

func (rf *Raft) trimLog(lastIncludedIndex, lastIncludedTerm int) {
	nextPos, ok := rf.positionOf(lastIncludedIndex + 1)
	if !ok {
		rf.Log = nil
	} else {
		retainedLog := rf.Log[nextPos:]
		rf.Log = make([]LogEntry, len(retainedLog))
		copy(rf.Log, retainedLog)
	}

	rf.snapshotTerm = lastIncludedTerm
	rf.snapshotIndex = lastIncludedIndex
}

func (rf *Raft) updateAfterInstalledSnapshot() {
	if rf.commitIndex < rf.snapshotIndex {
		rf.commitIndex = rf.snapshotIndex
	}
	if rf.lastApplied < rf.snapshotIndex {
		rf.lastApplied = rf.snapshotIndex
	}
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

func (rf *Raft) sendCommit2ApplyCh() {
	rf.applyChMu.Lock()
	defer rf.applyChMu.Unlock()
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	applyMsgs := make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		appliedPos, ok := rf.positionOf(rf.lastApplied)
		if !ok {
			continue
		}
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command:      rf.Log[appliedPos].Command,
		}
		applyMsgs = append(applyMsgs, applyMsg)
	}
	rf.mu.Unlock()

	if len(applyMsgs) > 0 {
		for i := range applyMsgs {
			rf.applyCh <- applyMsgs[i]
			DPrintf("me %d applied commit applyMsg, index %d, command %v", rf.me, applyMsgs[i].CommandIndex, applyMsgs[i].Command)
		}
	}
}

// AppendEntries reply
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	// DPrintf("me %d AppendEntries receive from %d, currentTerm %d, %+v", rf.me, args.LeaderId, rf.CurrentTerm, *args)
	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		reply.Success = false
		DPrintf("me %d reject AppendEntries from server %d, myCurrentTerm %d, args.Term %d", rf.me, args.LeaderId, reply.Term, args.Term)
		rf.mu.Unlock()
		return
	}

	rf.resetElectionTimeout()
	persist := false
	commit := false
	// switch to follower, continue to check log entries
	if args.Term > rf.CurrentTerm || rf.state == candidate {
		DPrintf("me %d AppendEntries receive higher term %d > %d from %d, return to follower, state=%v", rf.me, args.Term, reply.Term, args.LeaderId, rf.state)
		rf.convertToFollower()
		rf.CurrentTerm = args.Term
		rf.VoteFor = args.LeaderId
		persist = true
	}

	_, lastLogIndex := rf.lastLogTermIndex(len(rf.Log))
	argsPrevPos, argsPrevOk := rf.positionOf(args.PrevLogIndex)
	termEqual := true
	if argsPrevOk {
		if rf.Log[argsPrevPos].Term != args.PrevLogTerm {
			termEqual = false
		}
	} else {
		if argsPrevPos < -1 || argsPrevPos > len(rf.Log)-1 || (argsPrevPos == -1 && rf.snapshotTerm != args.PrevLogTerm) {
			termEqual = false
		}
	}
	// check if contains prevLogIndex
	if args.PrevLogIndex > lastLogIndex || !termEqual {
		// conflict
		reply.Success = false
		if len(rf.Log) == 0 {
			reply.ConflictIndex = rf.snapshotIndex
			reply.ConflictTerm = rf.snapshotTerm
		} else {
			endPos := len(rf.Log) - 1
			if argsPrevPos >= 0 && argsPrevPos < endPos {
				endPos = argsPrevPos
			}
			conflictTerm, conflictIndex := rf.lastLogTermIndex(endPos + 1)
			reply.ConflictTerm = conflictTerm
			if endPos == 0 && rf.snapshotIndex == -1 {
				reply.ConflictIndex = 0
			} else if conflictTerm == args.PrevLogTerm {
				reply.ConflictIndex = conflictIndex
			} else {
				for i := endPos; i >= -1; i-- {
					if i == -1 {
						reply.ConflictIndex = rf.snapshotIndex
						reply.ConflictTerm = rf.snapshotTerm
						break
					}
					if rf.Log[i].Term < conflictTerm {
						term, index := rf.lastLogTermIndex(i + 1)
						reply.ConflictIndex = index
						reply.ConflictTerm = term
						break
					}
				}
			}
		}
		DPrintf("me %d AppendEntries conflict from leader %d, myLastLogIndex %d, termEqual %v, snapshotIndex %d, snapshotTerm %d, args.PrevLogTerm %d, args.PrevLogIndex %d, reply.ConflictIndex %d, reply.ConflictTerm %d",
			rf.me, args.LeaderId, lastLogIndex, termEqual, rf.snapshotIndex, rf.snapshotTerm, args.PrevLogTerm, args.PrevLogIndex, reply.ConflictIndex, reply.ConflictTerm)

	} else {
		// no conflict, append leader log entries
		reply.Success = true
		_, lastLogIndex = rf.lastLogTermIndex(len(rf.Log))
		for i := 0; i < len(args.Entries); i++ {
			if args.PrevLogIndex+1+i > lastLogIndex {
				rf.Log = append(rf.Log, args.Entries[i:]...)
				break
			}
			argsPrevPos, _ = rf.positionOf(args.PrevLogIndex + 1 + i)
			if rf.Log[argsPrevPos].Term != args.Entries[i].Term {
				rf.Log = append(rf.Log[:argsPrevPos], args.Entries[i:]...)
				break
			}
		}
		DPrintf("me %d AppendEntries append from leader %d, args.PrevLogIndex %d, args.PrevLogTerm %d, args.LeaderCommit %d, args.Entries %+v", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)
		if len(args.Entries) > 0 {
			persist = true
		}

		// commit
		_, lastLogIndex = rf.lastLogTermIndex(len(rf.Log))
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < lastLogIndex {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = lastLogIndex
			}
		}
		if rf.commitIndex > rf.lastApplied {
			commit = true
		}
	}
	if persist {
		rf.persist()
	}
	rf.mu.Unlock()

	if commit {
		rf.sendCommit2ApplyCh()
	}
}

// appendEntries for leader
func (rf *Raft) appendEntries() {
	rf.mu.RLock()
	isLeader := rf.state == leader
	term := rf.CurrentTerm
	rf.mu.RUnlock()
	if !isLeader {
		return
	}

	for node := range rf.peers {
		if rf.me == node {
			rf.mu.Lock()
			_, lastLogIndex := rf.lastLogTermIndex(len(rf.Log))
			rf.nextIndex[node] = lastLogIndex + 1
			rf.matchIndex[node] = lastLogIndex
			rf.mu.Unlock()
			continue
		}
		go func(node int) {
			var (
				prevLogIndex int
				prevLogTerm  int
				entries      []LogEntry
			)

			rf.mu.RLock()
			_, lastLogIndex := rf.lastLogTermIndex(len(rf.Log))
			if rf.nextIndex[node] > lastLogIndex {
				prevLogIndex = lastLogIndex
				entries = nil
			} else {
				if rf.nextIndex[node] <= rf.snapshotIndex {
					// send installSnapshot
					rf.mu.RUnlock()
					rf.installSnapshot(node, term)
					return
				}
				prevLogIndex = rf.nextIndex[node] - 1
				nextIndexPos, _ := rf.positionOf(rf.nextIndex[node])
				entries = make([]LogEntry, len(rf.Log[nextIndexPos:]))
				copy(entries, rf.Log[nextIndexPos:])
			}
			prevIndexPos, ok := rf.positionOf(prevLogIndex)
			if ok {
				prevLogTerm = rf.Log[prevIndexPos].Term
			} else {
				prevLogTerm = rf.snapshotTerm
			}

			nextIndex := rf.nextIndex[node]
			args := &AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.RUnlock()

			reply := rf.sendAppendEntriesWithTimeout(node, args)

			rf.mu.Lock()

			// when term change, me no longer the leader of old term, ignore reply from old term
			if term != rf.CurrentTerm {
				rf.mu.Unlock()
				return
			}

			switch {
			case reply == nil:
			case reply.Success:
				rf.nextIndex[node] = nextIndex + len(entries)
				rf.matchIndex[node] = prevLogIndex + len(entries)

				// pager's figure 2, leader's commit rule
				if rf.matchIndex[node] > rf.commitIndex {
					matchIndices := make([]int, 0, len(rf.peers))
					for i := range rf.peers {
						if rf.matchIndex[i] > rf.commitIndex {
							matchIndices = append(matchIndices, rf.matchIndex[i])
						}
					}
					if len(matchIndices) >= rf.majority {
						sort.Ints(matchIndices)
						n := matchIndices[len(matchIndices)-rf.majority]
						pos, ok := rf.positionOf(n)
						if ok && rf.Log[pos].Term == rf.CurrentTerm {
							rf.commitIndex = n
							if rf.commitIndex > rf.lastApplied {
								rf.mu.Unlock()
								rf.sendCommit2ApplyCh()
								return
							}
						}
					}
				}
			case reply.Term > rf.CurrentTerm:
				DPrintf("me %d appendEntries response a higher term %d > %d from %d, return to follower, state=%d", rf.me, reply.Term, rf.CurrentTerm, node, rf.state)
				rf.CurrentTerm = reply.Term
				rf.convertToFollower()
				rf.VoteFor = -1
				rf.persist()
				rf.resetElectionTimeout()

			case reply.Term <= term:
				// decrease nextIndex
				conflictPos, ok := rf.positionOf(reply.ConflictIndex)
				if ok {
					if rf.Log[conflictPos].Term == reply.ConflictTerm {
						rf.nextIndex[node] = reply.ConflictIndex + 1
					} else {
						rf.nextIndex[node] = reply.ConflictIndex
					}
				} else {
					if rf.snapshotIndex > 0 {
						rf.nextIndex[node] = rf.snapshotIndex
					} else {
						rf.nextIndex[node] = 1
					}
				}
			}
			rf.mu.Unlock()
		}(node)
	}
}

func (rf *Raft) startAppendEntries() {
	rf.appendEntries()
	for !rf.killed() {

		time.Sleep(rf.heartbeatInterval)

		rf.mu.RLock()
		isLeader := rf.state == leader
		rf.mu.RUnlock()

		if !isLeader {
			return
		}

		if atomic.LoadInt32(&rf.heartbeatWaitFlag) == 0 {
			go rf.appendEntries()
		}
		atomic.StoreInt32(&rf.heartbeatWaitFlag, 0)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesWithTimeout(server int, args *AppendEntriesArgs) *AppendEntriesReply {
	done := make(chan *AppendEntriesReply, 1)
	reply := &AppendEntriesReply{}

	go func() {
		// todo this will block until rpc call return, lead to goroutine leak, we should use some better lib which supports timeout natively
		ok := rf.sendAppendEntries(server, args, reply)
		if !ok {
			done <- nil
			return
		}
		done <- reply
	}()
	timer := time.NewTimer(rf.rpcTimeout)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case val := <-done:
		return val
	}
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

// RequestVote replay RPC
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		return
	}

	persist := false
	if args.Term > rf.CurrentTerm {
		rf.convertToFollower()
		rf.VoteFor = -1
		rf.CurrentTerm = args.Term
		persist = true
	}

	lastLogTerm, lastLogIndex := rf.lastLogTermIndex(len(rf.Log))
	// check candidate is valid to vote for
	if args.Term > rf.CurrentTerm || (rf.VoteFor == -1 || rf.VoteFor == args.CandidateId) {

		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			reply.VoteGranted = true
			DPrintf("me %d vote to candidate %d, return to follower, args.Term %d, args.LastLogIndex %d, args.LastLogTerm %d, myLastIndex %d, myLastTerm %d, state=%d",
				rf.me, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm, lastLogIndex, lastLogTerm, rf.state)
			rf.convertToFollower()
			rf.CurrentTerm = args.Term
			rf.VoteFor = args.CandidateId
			persist = true
			rf.resetElectionTimeout()
		}
	} else {
		DPrintf("me %d didn't vote to %d, args.Term %d, my term %d, voteFor %d, args.LastLog %d:%d, myLastLog %d:%d",
			rf.me, args.CandidateId, args.Term, rf.CurrentTerm, rf.VoteFor, args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex)
		reply.VoteGranted = false
	}
	if persist {
		rf.persist()
	}
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
	done := make(chan *RequestVoteReply, 1)
	reply := &RequestVoteReply{}
	go func() {
		// todo this will block until rpc call return, lead to goroutine leak, we should use some better lib which supports timeout natively
		ok := rf.sendRequestVote(server, args, reply)
		if !ok {
			done <- nil
			return
		}
		done <- reply
	}()
	timer := time.NewTimer(rf.rpcTimeout)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case val := <-done:
		return val
	}
}

func (rf *Raft) requestVote() {
	processed, voteCount, exit := 1, 1, false

	rf.mu.Lock()
	rf.state = candidate
	rf.CurrentTerm++
	term := rf.CurrentTerm
	rf.VoteFor = rf.me
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex(len(rf.Log))
	args := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	DPrintf("me %d starts a new election, term %d", rf.me, rf.CurrentTerm)
	rf.persist()
	rf.mu.Unlock()
	rf.resetElectionTimeout()

	for node := range rf.peers {
		if node == rf.me {
			continue
		}

		go func(node int) {
			reply := rf.sendRequestVoteWithTimeout(node, args)
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// when multiple candidates start vote request, me vote for a higher term candidate, me then returns to follower and term increased.
			// so ignore the all the later vote responses me started this term
			if exit || rf.CurrentTerm != term {
				return
			}
			processed++

			switch {
			case reply == nil:
			case reply.VoteGranted:
				voteCount++
				// todo
				DPrintf("me %d get voted, voteCount %d", rf.me, voteCount)
				if voteCount >= rf.majority {
					rf.becomeLeader()
					rf.persist()
					exit = true
					return
				}
			case !reply.VoteGranted:
				if reply.Term > rf.CurrentTerm {
					DPrintf("me %d requestVote response higher term %d > %d from %d, return to follower, state=%d", rf.me, reply.Term, rf.CurrentTerm, node, rf.state)
					rf.CurrentTerm = reply.Term
					rf.convertToFollower()
					rf.VoteFor = -1
					rf.persist()
					exit = true
					rf.resetElectionTimeout()
					return
				}
			}

			if rf.state == candidate && voteCount+(len(rf.peers)-processed) < rf.majority {
				DPrintf("me %d requestVote not enough votes, return to follower, state=%d", rf.me, rf.state)
				rf.convertToFollower()
				rf.VoteFor = -1
				rf.persist()
				exit = true
				// rf.resetElectionTimeout()
			}
		}(node)
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
	_, lastLogIndex := rf.lastLogTermIndex(len(rf.Log))
	index = lastLogIndex + 1
	if isLeader {
		rf.Log = append(rf.Log, LogEntry{
			Term:    term,
			Command: command,
		})
		rf.persist()
		go rf.appendEntries()
		atomic.StoreInt32(&rf.heartbeatWaitFlag, 1)
		// DPrintf("me %d Start, index %d, term %d, command %v", rf.me, index, term, command)
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
	rf.applyChMu.Lock()
	defer rf.applyChMu.Unlock()

	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.applyCh)
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
		time.Sleep(time.Duration(rand.Intn(150)+150)*time.Millisecond + rf.rpcTimeout)

		if atomic.LoadInt32(&rf.electionWaitFlag) == 0 {
			rf.mu.RLock()
			if rf.state == leader {
				rf.mu.RUnlock()
				continue
			}
			rf.mu.RUnlock()
			go rf.requestVote()
		}

		atomic.StoreInt32(&rf.electionWaitFlag, 0)
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = leader
	rf.nextIndex = make([]int, len(rf.peers))
	_, lastLogIndex := rf.lastLogTermIndex(len(rf.Log))
	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))

	DPrintf("me %d become a leader, term %d, snapshotIndex %d, snapshotTerm %d, logs %+v", rf.me, rf.CurrentTerm, rf.snapshotIndex, rf.snapshotTerm, rf.Log)
	go rf.startAppendEntries()
}

func (rf *Raft) convertToFollower() {
	if rf.state == leader || rf.state == candidate {
		DPrintf("me %d convert to follower, is prevLeader %v", rf.me, rf.state == leader)
		rf.state = follower
	}
}

func (rf *Raft) lastLogTermIndex(logLength int) (lastLogTerm int, lastLogIndex int) {
	lastLogIndex = logLength + rf.snapshotIndex
	if len(rf.Log) == 0 {
		lastLogTerm = rf.snapshotTerm
	} else {
		lastLogTerm = rf.Log[logLength-1].Term
	}
	return
}

// positionOf return the position of index in Log. only works when index > snapshotIndex
func (rf *Raft) positionOf(index int) (int, bool) {
	pos := index - rf.snapshotIndex - 1
	if pos < 0 || pos > len(rf.Log)-1 {
		return pos, false
	}
	return pos, true
}

func (rf *Raft) resetElectionTimeout() {
	atomic.StoreInt32(&rf.electionWaitFlag, 1)
}

func (rf *Raft) LoadSnapshot() Snapshot {
	data := rf.snapshotDataOnlyForLoad
	rf.snapshotDataOnlyForLoad = nil
	return Snapshot{
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm:  rf.snapshotTerm,
		Data:              data,
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
	rf.applyCh = applyCh
	rf.majority = len(peers)/2 + 1
	rf.rpcTimeout = time.Second
	rf.heartbeatInterval = time.Millisecond * 100

	// initialize from state persisted before a crash
	ss := rf.readSnapshot(persister.ReadSnapshot())
	rf.snapshotIndex = ss.LastIncludedIndex
	rf.snapshotTerm = ss.LastIncludedTerm
	rf.snapshotDataOnlyForLoad = ss.Data
	rf.updateAfterInstalledSnapshot()
	rf.readPersist(persister.ReadRaftState())
	if rf.snapshotIndex == -1 && len(rf.Log) == 0 {
		rf.Log = []LogEntry{{Term: 0, Command: nil}}
	}

	if rf.state == leader {
		go rf.startAppendEntries()
	}
	// start ticker goroutine to start elections
	go rf.ticker()

	DPrintf("me %d join, snapshotIndex %d, snapshotTerm %d, log %+v", me, rf.snapshotIndex, rf.snapshotTerm, rf.Log)
	return rf
}
