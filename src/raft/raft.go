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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// 3B
	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A
	currentTerm       int
	voteFor           int
	state             State
	electionTimeout   time.Duration
	electionResetTime time.Time
	lastHeartbeatTime time.Time

	replicateCond    []*sync.Cond
	replicatePending []bool

	// 2B
	log         []LogEntry
	commitIndex int
	lastApplied int
	// leader 独有
	nextIndex  []int
	matchIndex []int

	// 3B
	lastIncludedIndex int
	lastIncludedTerm  int
	// applyCh           chan ApplyMsg

	hasSnapshot   bool
	snapshotIndex int
	snapshotTerm  int
	snapshot      []byte
}

const HeartbeatInterval = 100 * time.Millisecond

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

func (rf *Raft) stateString() string {
	switch rf.state {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).

	data := rf.encodeState()
	rf.persister.SaveRaftState(data)
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	return w.Bytes()
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var voteFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		DPrintf("[S%d] readPersist decode error", rf.me)
		panic("readPersist decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		DPrintf("[S%d] readPersist currentTerm=%d voteFor=%d logLen=%d",
			rf.me, rf.currentTerm, rf.voteFor, len(rf.log))
	}
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
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// 此时需要将日志中index之前的日志都丢弃掉，并且更新lastIncludedIndex和lastIncludedTerm，同时还要停止apply goroutine
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}

	term := rf.logTerm(index)
	sliceIdx := rf.toSliceIndex(index)
	suffix := append([]LogEntry(nil), rf.log[sliceIdx+1:]...)

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term

	newLog := make([]LogEntry, 1)
	newLog[0] = LogEntry{
		Term:    term,
		Command: nil,
	}
	rf.log = append(newLog, suffix...)

	// 持久化
	state := rf.encodeState()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
	return
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	needPersist := false
	if args.Term > rf.currentTerm {
		DPrintf("[S%d T%d %s] RequestVote higher term from S%d term=%d -> follower",
			rf.me, rf.currentTerm, rf.stateString(), args.CandidateID, args.Term)
		rf.becomeFollower(args.Term)
		// rf.persist()
		needPersist = true
		reply.Term = rf.currentTerm
	}

	if (rf.voteFor == -1 || rf.voteFor == args.CandidateID) && rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.voteFor = args.CandidateID
		// rf.persist()
		needPersist = true
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.resetTime()
		DPrintf("[S%d T%d %s] granted vote to S%d", rf.me, rf.currentTerm, rf.stateString(), args.CandidateID)
	} else {
		DPrintf("[S%d T%d %s] denied vote to S%d voteFor=%d argsTerm=%d",
			rf.me, rf.currentTerm, rf.stateString(), args.CandidateID, rf.voteFor, args.Term)
	}

	if needPersist {
		rf.persist()
	}
}

func (rf *Raft) isUpToDate(lastLogIndex, lastLogTerm int) bool {
	lastIndex := rf.lastLogIndex()
	lastTerm := rf.lastLogTerm()

	if lastTerm > lastLogTerm || (lastTerm == lastLogTerm && lastIndex > lastLogIndex) {
		return false
	}
	return true
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	if rf.state == Leader || time.Since(rf.electionResetTime) < rf.electionTimeout {
		rf.mu.Unlock()
		return
	}
	rf.becomeCandidate()
	rf.persist()
	termStarted := rf.currentTerm
	var votes int64 = 1
	DPrintf("[S%d T%d %s] startElection timeout=%v", rf.me, rf.currentTerm, rf.stateString(), rf.electionTimeout)

	args := &RequestVoteArgs{
		Term:         termStarted,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int, args *RequestVoteArgs) {
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(server, args, reply) {
				return
			}

			rf.mu.Lock()

			if reply.Term > rf.currentTerm {
				DPrintf("[S%d T%d %s] vote reply from S%d has higher term=%d -> follower",
					rf.me, rf.currentTerm, rf.stateString(), server, reply.Term)
				rf.becomeFollower(reply.Term)
				rf.persist()
				rf.resetTime()
				rf.mu.Unlock()
				return
			}

			if rf.state != Candidate || rf.currentTerm != termStarted {
				rf.mu.Unlock()
				return
			}

			if reply.VoteGranted {
				newVotes := atomic.AddInt64(&votes, 1)
				DPrintf("[S%d T%d %s] got vote from S%d total=%d",
					rf.me, rf.currentTerm, rf.stateString(), server, newVotes)
				if newVotes > int64(len(rf.peers)/2) {
					rf.becomeLeader()
					DPrintf("[S%d T%d %s] became leader", rf.me, rf.currentTerm, rf.stateString())
					rf.mu.Unlock()

					// 立即发送一次心跳
					for i := range rf.peers {
						if i != rf.me {
							rf.notifyReplicate(i)
						}
					}
					return
				}
			}
			rf.mu.Unlock()
		}(i, args)
	}
}

// AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries_new(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = rf.lastLogIndex() + 1

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term >= rf.currentTerm {
		termChanged := args.Term > rf.currentTerm
		rf.becomeFollower(args.Term)
		if termChanged {
			rf.persist()
		}
	}
	rf.resetTime()
	reply.Term = rf.currentTerm

	// 这种情况可能发生在Follower已经做了snapshot，日志被压缩掉了，而Leader还没有做snapshot，仍然保留着之前的日志
	if args.PrevLogIndex < rf.lastIncludedIndex {
		if args.PrevLogIndex+len(args.Entries) <= rf.lastIncludedIndex {
			reply.Success = true
			return
		}

		skip := rf.lastIncludedIndex - args.PrevLogIndex
		args.PrevLogIndex = rf.lastIncludedIndex
		args.PrevLogTerm = rf.lastIncludedTerm
		args.Entries = args.Entries[skip:]
	}

	// 正常情况下的日志不一致，PrevLogIndex超过了当前日志的长度
	if !rf.validLogIndex(args.PrevLogIndex) {
		return
	}

	if rf.logTerm(args.PrevLogIndex) != args.PrevLogTerm {
		term := rf.logTerm(args.PrevLogIndex)
		reply.ConflictTerm = term

		i := args.PrevLogIndex
		for i > rf.lastIncludedIndex && rf.logTerm(i-1) == term {
			i--
		}
		reply.ConflictIndex = i
		return
	}

	// 追加日志
	insertIdx := rf.toSliceIndex(args.PrevLogIndex + 1)
	i := 0
	for ; i < len(args.Entries); i++ {
		if insertIdx+i >= len(rf.log) || rf.log[insertIdx+i].Term != args.Entries[i].Term {
			break
		}
	}

	if i < len(args.Entries) {
		rf.log = rf.log[:insertIdx+i]
		rf.log = append(rf.log, args.Entries[i:]...)
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
	}
	reply.Success = true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	if args.Term >= rf.currentTerm {
		if rf.state != Follower || args.Term > rf.currentTerm {
			DPrintf("[S%d T%d %s] AppendEntries from S%d term=%d -> follower",
				rf.me, rf.currentTerm, rf.stateString(), args.LeaderID, args.Term)
		}

		termChanged := (args.Term > rf.currentTerm)
		rf.becomeFollower(args.Term)
		if termChanged {
			rf.persist()
		}
		rf.resetTime()
	}

	if args.PrevLogIndex >= len(rf.log) {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		DPrintf("[S%d T%d] reject AE from S%d: prevIdx=%d beyond logLen=%d",
			rf.me, rf.currentTerm, args.LeaderID, args.PrevLogIndex, len(rf.log))
		rf.mu.Unlock()
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term

		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.log[i].Term != reply.ConflictTerm {
				reply.ConflictIndex = i + 1
				break
			}
		}
		// if reply.ConflictIndex == 0 {
		// 	reply.ConflictIndex = rf.commitIndex + 1
		// }

		DPrintf("[S%d T%d] reject AE from S%d: prev mismatch idx=%d localTerm=%d reqTerm=%d conflictIdx=%d",
			rf.me, rf.currentTerm, args.LeaderID, args.PrevLogIndex,
			rf.log[args.PrevLogIndex].Term, args.PrevLogTerm, reply.ConflictIndex)
		rf.mu.Unlock()
		return
	}

	// Append new entries
	// rf.log = rf.log[:args.PrevLogIndex+1]
	// rf.log = append(rf.log, args.Entries...)

	// 只有遇到同index不同term的日志时才需要删除，否则说明之前已经有过相同的日志了，不需要删除
	prevCommit := rf.commitIndex
	insertIdx := args.PrevLogIndex + 1
	i := 0
	for ; i < len(args.Entries); i++ {
		if insertIdx+i >= len(rf.log) {
			break
		}
		if rf.log[insertIdx+i].Term != args.Entries[i].Term {
			break
		}
	}
	if i < len(args.Entries) {
		rf.log = rf.log[:insertIdx+i]
		rf.log = append(rf.log, args.Entries[i:]...)
		rf.persist()
		DPrintf("[S%d T%d] append %d new entries from S%d at idx=%d",
			rf.me, rf.currentTerm, len(args.Entries)-i, args.LeaderID, insertIdx+i)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		if rf.commitIndex != prevCommit {
			DPrintf("[S%d T%d] commitIndex %d->%d from leader %d",
				rf.me, rf.currentTerm, prevCommit, rf.commitIndex, args.LeaderID)
		}
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	// rf.electionResetTime = time.Now()
	// rf.electionTimeout = randElectionTimeout()
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries_new", args, reply)
	return ok
}

func (rf *Raft) notifyReplicate(server int) {
	rf.mu.Lock()
	rf.replicatePending[server] = true
	rf.replicateCond[server].Signal()
	rf.mu.Unlock()
}

func (rf *Raft) startReplicateRound_new(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	nextIndex := rf.nextIndex[server]
	prevIndex := nextIndex - 1
	if nextIndex <= rf.lastIncludedIndex {
		// 此时发送InstallSnapshot RPC
		rf.mu.Unlock()
		rf.StartInstallSnapshot(server)
		return
	}

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  rf.logTerm(prevIndex),
		LeaderCommit: rf.commitIndex,
	}
	if nextIndex <= rf.lastLogIndex() {
		idx := rf.toSliceIndex(prevIndex)
		entries := make([]LogEntry, len(rf.log)-idx-1)
		copy(entries, rf.log[idx+1:])
		args.Entries = entries
	} else {
		args.Entries = nil
	}
	rf.mu.Unlock()

	go func(server int, prevIdx int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		if !rf.sendAppendEntries(server, args, reply) {
			if len(args.Entries) > 0 {
				DPrintf("[S%d T%d] replicate -> S%d RPC failed", rf.me, args.Term, server)
			}

			time.Sleep(20 * time.Millisecond)
			rf.mu.Lock()
			if rf.state == Leader && rf.currentTerm == args.Term {
				rf.replicatePending[server] = true
				rf.replicateCond[server].Signal()
			}
			rf.mu.Unlock()
			return
		}

		sentMatch := prevIdx + len(args.Entries)
		sentNext := sentMatch + 1

		rf.mu.Lock()
		defer rf.mu.Unlock()

		// 收到更高任期的回复，说明自己此时不是leader了，应该变为follower
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			rf.resetTime()
			rf.persist()
			return
		}

		// 出现这种情况的原因：
		// 1. 在leader因收到更高任期的回复从而降级成为follower后，早期发送的AppendEntries RPC才陆续收到回复
		// 2. 在等待RPC回复时，此节点可能收到了更高任期的AppendEntries/RequestVote RPC，从而降级成为follower
		if rf.state != Leader || rf.currentTerm != args.Term {
			return
		}

		if reply.Success {
			if rf.nextIndex[server] < sentNext {
				rf.nextIndex[server] = sentNext
				rf.matchIndex[server] = sentMatch
				if len(args.Entries) > 0 {
					DPrintf("[S%d T%d] replicate ack from S%d match=%d next=%d",
						rf.me, rf.currentTerm, server, rf.matchIndex[server], rf.nextIndex[server])
				}

				isUpdate := rf.updateCommitIndex_new()
				if isUpdate {
					// 立即通知所有follower进行复制，以便尽快提交日志
					for i := range rf.peers {
						if i != rf.me {
							rf.replicatePending[i] = true
							rf.replicateCond[i].Signal()
						}
					}
				}
			}
			return
		}

		if reply.ConflictIndex <= 0 {
			panic("unexpected ConflictIndex in AppendEntriesReply")
		}

		oldNext := rf.nextIndex[server]
		newNext := reply.ConflictIndex

		// 先看ConflictTerm，再看ConflictIndex
		if reply.ConflictTerm != -1 {
			last := -1
			for i := rf.lastLogIndex(); i > rf.lastIncludedIndex; i-- {
				if rf.logTerm(i) == reply.ConflictTerm {
					last = i
					break
				}
				if rf.logTerm(i) < reply.ConflictTerm {
					break
				}
			}

			if last != -1 {
				newNext = last + 1
			}
		}

		// if newNext < rf.commitIndex+1 {
		// 	newNext = rf.commitIndex + 1
		// }
		if newNext < rf.nextIndex[server] {
			rf.nextIndex[server] = newNext
		}
		DPrintf("[S%d T%d] replicate reject by S%d: nextIndex %d->%d",
			rf.me, rf.currentTerm, server, oldNext, rf.nextIndex[server])
		rf.replicatePending[server] = true
		rf.replicateCond[server].Signal()
	}(server, prevIndex, args)

}

func (rf *Raft) startReplicateRound(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	idx := rf.nextIndex[server] - 1
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: idx,
		PrevLogTerm:  rf.log[idx].Term,
		LeaderCommit: rf.commitIndex,
	}
	entries := make([]LogEntry, len(rf.log)-idx-1)
	copy(entries, rf.log[idx+1:])
	args.Entries = entries
	if len(args.Entries) > 0 {
		DPrintf("[S%d T%d] replicate -> S%d prevIdx=%d entries=%d",
			rf.me, rf.currentTerm, server, args.PrevLogIndex, len(args.Entries))
	}
	rf.mu.Unlock()

	// 1. 异步RPC版本

	go func(server int, prevIdx int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		if !rf.sendAppendEntries(server, args, reply) {
			if len(args.Entries) > 0 {
				DPrintf("[S%d T%d] replicate -> S%d RPC failed", rf.me, args.Term, server)
			}
			time.Sleep(25 * time.Millisecond)

			rf.mu.Lock()
			if rf.state == Leader && rf.currentTerm == args.Term {
				rf.replicatePending[server] = true
			}
			rf.mu.Unlock()
			return
		}

		sentMatch := prevIdx + len(args.Entries)
		sentNext := sentMatch + 1

		rf.mu.Lock()
		defer rf.mu.Unlock()

		// 收到更高任期的回复，说明自己此时不是leader了，应该变为follower
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			rf.resetTime()
			rf.persist()
			return
		}

		// 出现这种情况的原因：
		// 1. 在leader因收到更高任期的回复从而降级成为follower后，早期发送的AppendEntries RPC才陆续收到回复
		// 2. 在等待RPC回复时，此节点可能收到了更高任期的AppendEntries/RequestVote RPC，从而降级成为follower
		if rf.state != Leader || rf.currentTerm != args.Term {
			return
		}

		if reply.Success {
			if rf.nextIndex[server] < sentNext {
				rf.nextIndex[server] = sentNext
				rf.matchIndex[server] = sentMatch
				if len(args.Entries) > 0 {
					DPrintf("[S%d T%d] replicate ack from S%d match=%d next=%d",
						rf.me, rf.currentTerm, server, rf.matchIndex[server], rf.nextIndex[server])
				}

				isUpdate := rf.updateCommitIndex()
				if isUpdate {
					// 立即通知所有follower进行复制，以便尽快提交日志
					for i := range rf.peers {
						if i != rf.me {
							rf.replicatePending[i] = true
							rf.replicateCond[i].Signal()
						}
					}
				}
			}
			return
		}

		if reply.ConflictIndex <= 0 {
			panic("unexpected ConflictIndex in AppendEntriesReply")
		}

		oldNext := rf.nextIndex[server]
		newNext := reply.ConflictIndex

		// 先看ConflictTerm，再看ConflictIndex
		if reply.ConflictTerm != -1 {
			last := -1
			for i := len(rf.log) - 1; i >= 1; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					last = i
					break
				}
				if rf.log[i].Term < reply.ConflictTerm {
					break
				}
			}

			if last != -1 {
				newNext = last + 1
			}
		}

		// if newNext < rf.commitIndex+1 {
		// 	newNext = rf.commitIndex + 1
		// }
		if newNext < rf.nextIndex[server] {
			rf.nextIndex[server] = newNext
		}
		DPrintf("[S%d T%d] replicate reject by S%d: nextIndex %d->%d",
			rf.me, rf.currentTerm, server, oldNext, rf.nextIndex[server])
		rf.replicatePending[server] = true
		rf.replicateCond[server].Signal()
	}(server, idx, args)

	/*
			2. 同步RPC版本
			这里将异步发送RPC改成了同步发送，是因为在leader上同时进行多轮复制可能会导致日志不一致的情况更难调试，同步发送RPC保证了每个时刻只有一个RPC在进行
			并且异步发送RPC受到阻塞时，多发送的RPC也会被阻塞，没有太大意义

		reply := &AppendEntriesReply{}
		if !rf.sendAppendEntries(server, args, reply) {
			if len(args.Entries) > 0 {
				DPrintf("[S%d T%d] replicate -> S%d RPC failed", rf.me, args.Term, server)
			}

			time.Sleep(25 * time.Millisecond)

			rf.mu.Lock()
			if rf.state == Leader && rf.currentTerm == args.Term {
				rf.replicatePending[server] = true
			}
			rf.mu.Unlock()
			return
		}

		sentMatch := idx + len(args.Entries)
		sentNext := sentMatch + 1

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			rf.resetTime()
			rf.persist()
			return
		}

		if rf.state != Leader || rf.currentTerm != args.Term {
			return
		}

		if reply.Success {
			if rf.nextIndex[server] < sentNext {
				rf.nextIndex[server] = sentNext
				rf.matchIndex[server] = sentMatch
				if len(args.Entries) > 0 {
					DPrintf("[S%d T%d] replicate ack from S%d match=%d next=%d",
						rf.me, rf.currentTerm, server, rf.matchIndex[server], rf.nextIndex[server])
				}

				isUpdate := rf.updateCommitIndex()
				if isUpdate {
					// 立即通知所有follower进行复制，以便尽快提交日志
					for i := range rf.peers {
						if i != rf.me {
							rf.replicatePending[i] = true
							rf.replicateCond[i].Signal()
						}
					}
				}
			}
			return
		}

		if reply.ConflictIndex <= 0 {
			panic("unexpected ConflictIndex in AppendEntriesReply")
		}


			// oldNext := rf.nextIndex[server]
			// if reply.ConflictIndex < rf.nextIndex[server] {
			// 	rf.nextIndex[server] = reply.ConflictIndex
			// }
			// DPrintf("[S%d T%d] replicate reject by S%d: nextIndex %d->%d",
			// 	rf.me, rf.currentTerm, server, oldNext, rf.nextIndex[server])
			// rf.replicatePending[server] = true


		oldNext := rf.nextIndex[server]
		newNext := reply.ConflictIndex

		// 先看ConflictTerm，再看ConflictNext
		if reply.ConflictTerm != -1 {
			last := -1
			for i := len(rf.log) - 1; i >= 1; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					last = i
					break
				}
				if rf.log[i].Term < reply.ConflictTerm {
					break
				}
			}

			if last != -1 {
				newNext = last + 1
			}
		}

		// if newNext < rf.commitIndex+1 {
		// 	newNext = rf.commitIndex + 1
		// }
		if newNext < rf.nextIndex[server] {
			rf.nextIndex[server] = newNext
		}
		DPrintf("[S%d T%d] replicate reject by S%d: nextIndex %d->%d",
			rf.me, rf.currentTerm, server, oldNext, rf.nextIndex[server])
		rf.replicatePending[server] = true
		rf.replicateCond[server].Signal()
	*/
}

func (rf *Raft) replicator(server int) {
	for !rf.killed() {
		rf.mu.Lock()
		for !rf.replicatePending[server] && !rf.killed() {
			rf.replicateCond[server].Wait()
		}

		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		rf.replicatePending[server] = false
		rf.mu.Unlock()
		rf.startReplicateRound_new(server)

		// idx := rf.nextIndex[server] - 1
		// args := &AppendEntriesArgs{
		// 	Term:         rf.currentTerm,
		// 	LeaderID:     rf.me,
		// 	PrevLogIndex: idx,
		// 	PrevLogTerm:  rf.log[idx].Term,
		// 	LeaderCommit: rf.commitIndex,
		// }
		// entries := make([]LogEntry, len(rf.log)-idx-1)
		// copy(entries, rf.log[idx+1:])
		// args.Entries = entries
		// DPrintf("[S%d T%d %s] send AppendEntries to S%d prevIdx=%d entries=%d",
		// 	rf.me, rf.currentTerm, rf.stateString(), server, args.PrevLogIndex, len(args.Entries))
		// rf.mu.Unlock()

		// reply := &AppendEntriesReply{}
		// if !rf.sendAppendEntries(server, args, reply) {
		// 	DPrintf("[S%d T%d %s] AppendEntries to S%d failed",
		// 		rf.me, args.Term, rf.stateString(), server)
		// 	continue
		// }

		// rf.mu.Lock()
		// if reply.Term > rf.currentTerm {
		// 	rf.becomeFollower(reply.Term)
		// 	rf.mu.Unlock()
		// 	continue
		// }

		// if rf.state != Leader || rf.currentTerm != args.Term {
		// 	rf.mu.Unlock()
		// 	continue
		// }

		// if reply.Success {
		// 	rf.nextIndex[server] = idx + len(args.Entries) + 1
		// 	rf.matchIndex[server] = rf.nextIndex[server] - 1
		// 	rf.updateCommitIndex()
		// } else {
		// 	rf.nextIndex[server] = reply.ConflictIndex
		// 	rf.mu.Unlock()
		// 	rf.notifyReplicate(server)
		// }
		// rf.mu.Unlock()
	}
}

func (rf *Raft) updateCommitIndex_new() bool {
	oldCommit := rf.commitIndex
	for i := rf.lastLogIndex(); i > rf.commitIndex; i-- {
		if rf.logTerm(i) != rf.currentTerm {
			break
		}

		count := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = i
			DPrintf("[S%d T%d] leader commitIndex %d->%d",
				rf.me, rf.currentTerm, oldCommit, rf.commitIndex)
			return true
		}
	}
	return false
}

func (rf *Raft) updateCommitIndex() bool {
	oldCommit := rf.commitIndex
	for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
		if rf.log[i].Term != rf.currentTerm {
			continue
		}

		count := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = i
			DPrintf("[S%d T%d] leader commitIndex %d->%d",
				rf.me, rf.currentTerm, oldCommit, rf.commitIndex)
			return true
		}
	}
	return false
}

// InstallSnapshot RPC
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	if args.LastIncludedIndex <= rf.lastIncludedIndex || args.LastIncludedIndex <= rf.commitIndex || args.LastIncludedIndex <= rf.lastApplied {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	if args.Term >= rf.currentTerm {
		termChanged := args.Term > rf.currentTerm
		rf.becomeFollower(args.Term)
		if termChanged {
			rf.persist()
		}
		rf.resetTime()
	}

	// 这种情况会发生在：
	// 1. 旧的InstallSnapshot RPC到达现在的Follower时，说明之前的日志已经被压缩掉了，不需要再安装快照了
	// 2. Leader重复发送
	// 3. 旧Leader发送的InstallSnapshot RPC在网络中延迟很久才到达Follower，此时Leader已经做了新的快照了，旧的快照已经过时了

	// 安装快照
	if !rf.validLogIndex(args.LastIncludedIndex) || rf.logTerm(args.LastIncludedIndex) != args.LastIncludedTerm {
		// 此时应该丢弃Follower的日志，直接用快照覆盖掉
		rf.log = []LogEntry{
			{
				Term:    args.LastIncludedTerm,
				Command: nil,
			},
		}
	} else {
		newLog := make([]LogEntry, 1)
		newLog[0] = LogEntry{
			Term:    args.LastIncludedTerm,
			Command: nil,
		}
		suffix := append([]LogEntry(nil), rf.log[rf.toSliceIndex(args.LastIncludedIndex)+1:]...)
		rf.log = append(newLog, suffix...)
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), args.Data)

	// 更新状态机
	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}

	// msg := ApplyMsg{
	// 	CommandValid:  false,
	// 	SnapshotValid: true,
	// 	Snapshot:      args.Data,
	// 	SnapshotIndex: args.LastIncludedIndex,
	// }

	rf.hasSnapshot = true
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.snapshot = args.Data
	rf.mu.Unlock()
	// rf.applyCh <- msg
}

func (rf *Raft) StartInstallSnapshot(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	go func(server int, args *InstallSnapshotArgs) {
		reply := &InstallSnapshotReply{}
		if !rf.sendInstallSnapshot(server, args, reply) {
			time.Sleep(20 * time.Millisecond)
			rf.mu.Lock()
			if rf.state == Leader && rf.currentTerm == args.Term {
				rf.replicatePending[server] = true
				rf.replicateCond[server].Signal()
			}
			rf.mu.Unlock()
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != Leader || rf.currentTerm != args.Term {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			rf.resetTime()
			rf.persist()
			return
		}
		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.replicatePending[server] = true
		rf.replicateCond[server].Signal()
	}(server, args)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	// defer rf.mu.Unlock()

	if rf.state != Leader {
		isLeader = false
		rf.mu.Unlock()
		return index, term, isLeader
	}

	index = rf.lastLogIndex() + 1
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	rf.persist()
	DPrintf("[S%d T%d] Start append idx=%d", rf.me, term, index)
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me {
			rf.notifyReplicate(i)
		}
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := range rf.replicateCond {
		rf.replicateCond[i].Broadcast()
	}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		state:             Follower,
		voteFor:           -1,
		currentTerm:       0,
		electionResetTime: time.Now(),
		electionTimeout:   randElectionTimeout(),

		log: []LogEntry{
			{Term: 0, Command: nil},
		},
		commitIndex: 0,
		lastApplied: 0,

		replicateCond:    make([]*sync.Cond, len(peers)),
		replicatePending: make([]bool, len(peers)),
	}

	// Your initialization code here (2A, 2B, 2C).
	for i := range peers {
		rf.replicateCond[i] = sync.NewCond(&rf.mu)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex
	rf.log[0] = LogEntry{
		Term:    rf.lastIncludedTerm,
		Command: nil,
	}

	for i := range peers {
		if i != me {
			go rf.replicator(i)
		}
	}
	go rf.ticker()
	go rf.applier(applyCh)

	return rf
}

func randElectionTimeout() time.Duration {
	return time.Duration(400+rand.Intn(400)) * time.Millisecond
}

func (rf *Raft) resetTime() {
	rf.electionResetTime = time.Now()
	rf.electionTimeout = randElectionTimeout()
}

func (rf *Raft) becomeFollower(term int) {
	prevState := rf.state
	prevTerm := rf.currentTerm
	prevVoteFor := rf.voteFor
	rf.state = Follower
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.voteFor = -1
	}
	// rf.electionResetTime = time.Now()
	// rf.electionTimeout = randElectionTimeout()
	if prevState != rf.state || prevTerm != rf.currentTerm || prevVoteFor != rf.voteFor {
		DPrintf("[S%d] state %v->%v term %d->%d voteFor=%d",
			rf.me, prevState, rf.state, prevTerm, rf.currentTerm, rf.voteFor)
	}
}

func (rf *Raft) becomeCandidate() {
	prevState := rf.state
	prevTerm := rf.currentTerm
	rf.state = Candidate
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.resetTime()
	DPrintf("[S%d] state %v->%v term %d->%d voteFor=%d",
		rf.me, prevState, rf.state, prevTerm, rf.currentTerm, rf.voteFor)
}

func (rf *Raft) becomeLeader() {
	prevState := rf.state
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = rf.lastLogIndex()
	rf.lastHeartbeatTime = time.Now()
	DPrintf("[S%d] state %v->%v term %d", rf.me, prevState, rf.state, rf.currentTerm)
	// 此处需要立即发送一次心跳
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(5 * time.Millisecond)

		rf.mu.Lock()
		if rf.state != Leader && time.Since(rf.electionResetTime) >= rf.electionTimeout {
			DPrintf("[S%d T%d %s] election timeout elapsed=%v timeout=%v",
				rf.me, rf.currentTerm, rf.stateString(), time.Since(rf.electionResetTime), rf.electionTimeout)
			rf.mu.Unlock()
			rf.startElection()
			continue
		}

		if rf.state == Leader && time.Since(rf.lastHeartbeatTime) >= HeartbeatInterval {
			rf.lastHeartbeatTime = time.Now()
			rf.mu.Unlock()

			for i := range rf.peers {
				if i != rf.me {
					rf.notifyReplicate(i)
				}
			}
			continue
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) applier(applyCh chan ApplyMsg) {
	for !rf.killed() {
		time.Sleep(1 * time.Millisecond)
		rf.mu.Lock()

		entries := make([]ApplyMsg, 0)
		if rf.hasSnapshot {
			entries=append(entries,ApplyMsg{
				CommandValid:false,
				SnapshotValid:true,
				Snapshot:rf.snapshot,
				SnapshotIndex:rf.snapshotIndex,
				SnapshotTerm:rf.snapshotTerm,
			})
		}

		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.lastLogIndex() {
			rf.lastApplied++
			entries = append(entries, ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.toSliceIndex(rf.lastApplied)].Command,
				CommandIndex: rf.lastApplied,
			})
		}
		rf.mu.Unlock()
		for _, msg := range entries {
			applyCh <- msg
		}
	}
}

func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) toSliceIndex(idx int) int {
	return idx - rf.lastIncludedIndex
}

func (rf *Raft) validLogIndex(idx int) bool {
	return idx >= rf.lastIncludedIndex && idx <= rf.lastLogIndex()
}

func (rf *Raft) logTerm(idx int) int {
	if idx == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	return rf.log[rf.toSliceIndex(idx)].Term
}
