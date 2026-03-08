package raft

// //
// // this is an outline of the API that raft must expose to
// // the service (or tester). see comments below for
// // each of these functions for more details.
// //
// // rf = Make(...)
// //   create a new Raft server.
// // rf.Start(command interface{}) (index, term, isleader)
// //   start agreement on a new log entry
// // rf.GetState() (term, isLeader)
// //   ask a Raft for its current term, and whether it thinks it is leader
// // ApplyMsg
// //   each time a new entry is committed to the log, each Raft peer
// //   should send an ApplyMsg to the service (or tester)
// //   in the same server.
// //

// import (
// 	"bytes"
// 	"math/rand"
// 	"sort"
// 	"sync"
// 	"sync/atomic"
// 	"time"

// 	"../labgob"
// 	"../labrpc"
// )

// // as each Raft peer becomes aware that successive log entries are
// // committed, the peer should send an ApplyMsg to the service (or
// // tester) on the same server, via the applyCh passed to Make(). set
// // CommandValid to true to indicate that the ApplyMsg contains a newly
// // committed log entry.
// //
// // in Lab 3 you'll want to send other kinds of messages (e.g.,
// // snapshots) on the applyCh; at that point you can add fields to
// // ApplyMsg, but set CommandValid to false for these other uses.
// type ApplyMsg struct {
// 	CommandValid bool
// 	Command      interface{}
// 	CommandIndex int
// }

// //
// // A Go object implementing a single Raft peer.
// //

// // Raft 节点状态
// type State int

// const (
// 	Follower State = iota
// 	Candidate
// 	Leader
// )

// // 日志条目结构
// type LogEntry struct {
// 	Term    int         // leader 接收到该条目时的任期
// 	Command interface{} // 客户端命令
// }

// type Raft struct {
// 	mu        sync.Mutex          // Lock to protect shared access to this peer's state
// 	peers     []*labrpc.ClientEnd // RPC end points of all peers
// 	persister *Persister          // Object to hold this peer's persisted state
// 	me        int                 // this peer's index into peers[]
// 	dead      int32               // set by Kill()

// 	// Your data here (2A, 2B, 2C).
// 	// Look at the paper's Figure 2 for a description of what
// 	// state a Raft server must maintain.

// 	// 2A
// 	role              State         // 节点状态
// 	currentTerm       int           // 当前任期
// 	voteFor           int           // 投票给哪个节点
// 	electionResetTime time.Time     // 用来判断是否超时触发选举（在收到心跳信号/投票成功时更新）
// 	electionTimeout   time.Duration // 选举超时时间
// 	lastHeartbeatTime time.Time     // leader上次发送心跳的时间

// 	// 2B
// 	log         []LogEntry // 日志条目
// 	commitIndex int        // 已提交的最高的日志条目的索引
// 	lastApplied int        // 已经被应用到状态机的最高的日志条目的索引
// 	nextIndex   []int      // leader 为每个 follower 维护的下一个日志条目的索引
// 	matchIndex  []int      // leader 为每个 follower 维护的已经复制到 follower 的最高日志条目的索引
// 	// 对每个 follower 限制同一时刻只有一个 AppendEntries 在飞，避免并发请求堆积。
// 	appendInFlight []bool
// }

// func (rf *Raft) ticker() {
// 	heartbeatSent := time.Duration(100) * time.Millisecond // 心跳间隔时间100ms
// 	for !rf.killed() {
// 		time.Sleep(10 * time.Millisecond) // 每10ms检查一次状态

// 		rf.mu.Lock()
// 		role := rf.role
// 		now := time.Now()

// 		if role != Leader && now.Sub(rf.electionResetTime) >= rf.electionTimeout {
// 			rf.mu.Unlock()
// 			rf.startElection()
// 			continue
// 		}
// 		if now.Sub(rf.lastHeartbeatTime) < heartbeatSent {
// 			rf.mu.Unlock()
// 			continue
// 		}

// 		rf.lastHeartbeatTime = now
// 		rf.mu.Unlock()
// 		if role == Leader {
// 			rf.broadcastAppendEntries()
// 		}
// 	}
// }

// // return currentTerm and whether this server
// // believes it is the leader.
// func (rf *Raft) GetState() (int, bool) {

// 	var term int
// 	var isleader bool
// 	// Your code here (2A).
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	term = rf.currentTerm
// 	isleader = (rf.role == Leader)
// 	return term, isleader
// }

// // save Raft's persistent state to stable storage,
// // where it can later be retrieved after a crash and restart.
// // see paper's Figure 2 for a description of what should be persistent.

// // persistent state: currentTerm, voteFor, log[]
// func (rf *Raft) persist() {
// 	w := new(bytes.Buffer)
// 	e := labgob.NewEncoder(w)
// 	e.Encode(rf.currentTerm)
// 	e.Encode(rf.voteFor)
// 	e.Encode(rf.log)

// 	data := w.Bytes()
// 	rf.persister.SaveRaftState(data)

// 	// Your code here (2C).
// 	// Example:
// 	// w := new(bytes.Buffer)
// 	// e := labgob.NewEncoder(w)
// 	// e.Encode(rf.xxx)
// 	// e.Encode(rf.yyy)
// 	// data := w.Bytes()
// 	// rf.persister.SaveRaftState(data)
// }

// // restore previously persisted state.
// func (rf *Raft) readPersist(data []byte) {
// 	if data == nil || len(data) < 1 { // bootstrap without any state?
// 		return
// 	}

// 	r := bytes.NewBuffer(data)
// 	d := labgob.NewDecoder(r)

// 	var currentTerm int
// 	var voteFor int
// 	var log []LogEntry

// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	if d.Decode(&currentTerm) != nil ||
// 		d.Decode(&voteFor) != nil ||
// 		d.Decode(&log) != nil {
// 		// 反序列化失败，说明数据可能被损坏了，直接返回不恢复状态
// 		panic("Failed to decode persisted state")
// 	} else {
// 		rf.currentTerm = currentTerm
// 		rf.voteFor = voteFor
// 		rf.log = log
// 	}
// 	// Your code here (2C).
// 	// Example:
// 	// r := bytes.NewBuffer(data)
// 	// d := labgob.NewDecoder(r)
// 	// var xxx
// 	// var yyy
// 	// if d.Decode(&xxx) != nil ||
// 	//    d.Decode(&yyy) != nil {
// 	//   error...
// 	// } else {
// 	//   rf.xxx = xxx
// 	//   rf.yyy = yyy
// 	// }
// }

// // example RequestVote RPC arguments structure.
// // field names must start with capital letters!
// type RequestVoteArgs struct {
// 	// Your data here (2A, 2B).
// 	Term         int // candidate当前任期
// 	CandidateID  int // candidate的ID
// 	LastLogIndex int // candidate最后一条日志的索引
// 	LastLogTerm  int // candidate最后一条日志的任期
// }

// // example RequestVote RPC reply structure.
// // field names must start with capital letters!
// type RequestVoteReply struct {
// 	// Your data here (2A).
// 	Term        int  // 当前任期号，用于候选人更新自己的任期
// 	VoteGranted bool // true表示候选人赢得了此张选票
// }

// // example RequestVote RPC handler.
// func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
// 	// Your code here (2A, 2B).
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	// 1. 如果候选人的任期号小于自己的任期号，拒绝投票
// 	if args.Term < rf.currentTerm {
// 		reply.Term = rf.currentTerm
// 		reply.VoteGranted = false
// 		return
// 	}
// 	// 2. 如果候选人的任期号大于自己的任期号，更新自己的任期号并切换到Follower状态
// 	if args.Term > rf.currentTerm {
// 		rf.becomeFollower(args.Term)
// 	}

// 	// 3. 如果自己还没有投票或者已经投票给了这个候选人，并且候选人的日志至少和自己一样新，投票给他
// 	if (rf.voteFor == -1 || rf.voteFor == args.CandidateID) && rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
// 		rf.voteFor = args.CandidateID
// 		rf.persist()
// 		rf.electionResetTime = time.Now() // 投票成功，重置选举计时器
// 		reply.Term = rf.currentTerm
// 		reply.VoteGranted = true
// 		return
// 	}

// 	// 4. 否则拒绝投票
// 	reply.Term = rf.currentTerm
// 	reply.VoteGranted = false
// }

// func (rf *Raft) isUpToDate(lastLogIndex, lastLogTerm int) bool {
// 	// 先比较任期号，任期号大的日志更新
// 	if lastLogTerm < rf.lastLogTerm() {
// 		return false
// 	}
// 	// 任期号相同，比较日志索引，索引大的日志更新（2A先当作永远满足）
// 	if (lastLogTerm == rf.lastLogTerm()) && (lastLogIndex < rf.lastLogIndex()) {
// 		return false
// 	}
// 	return true
// }

// // startElection目标：
// // 1. 切换到Candidate状态发起新一轮选举
// // 2. 并发请求投票（获得多数票的成为leader，发现更大任期时自己退回Follower）
// func (rf *Raft) startElection() {
// 	rf.mu.Lock()
// 	if rf.role == Leader || time.Since(rf.electionResetTime) < rf.electionTimeout {
// 		rf.mu.Unlock()
// 		return
// 	}

// 	rf.becomeCandidate()
// 	termStarted := rf.currentTerm              // 用于后续丢弃过时的回复
// 	rf.electionTimeout = randElectionTimeout() // 重新生成随机选举超时时间
// 	votes := 1

// 	// 并发请求投票
// 	args := &RequestVoteArgs{
// 		Term:         termStarted,
// 		CandidateID:  rf.me,
// 		LastLogIndex: rf.lastLogIndex(),
// 		LastLogTerm:  rf.lastLogTerm(),
// 	}
// 	rf.mu.Unlock()

// 	for i := range rf.peers {
// 		if i == rf.me {
// 			continue
// 		}

// 		go func(server int, args *RequestVoteArgs) {
// 			rf.mu.Lock()
// 			if rf.role != Candidate || rf.currentTerm != termStarted {
// 				rf.mu.Unlock()
// 				return
// 			}
// 			rf.mu.Unlock()

// 			reply := &RequestVoteReply{}
// 			ok := rf.sendRequestVote(server, args, reply)
// 			if !ok {
// 				return
// 			}

// 			rf.mu.Lock()
// 			// defer rf.mu.Unlock()

// 			// 先处理更高任期：自己立刻降级。
// 			if reply.Term > rf.currentTerm {
// 				rf.becomeFollower(reply.Term)
// 				rf.mu.Unlock()
// 				return
// 			}

// 			// 只处理本轮选举、且自己仍是Candidate的回包。
// 			if rf.role != Candidate || rf.currentTerm != termStarted {
// 				rf.mu.Unlock()
// 				return
// 			}

// 			// 丢弃非本任期回包，避免旧回包污染当前计票。
// 			if reply.Term != termStarted {
// 				rf.mu.Unlock()
// 				return
// 			}

// 			if reply.VoteGranted {
// 				votes++
// 				if votes > len(rf.peers)/2 {
// 					rf.becomeLeader()
// 					rf.mu.Unlock()
// 					rf.broadcastAppendEntries() // 成为leader后立即发送心跳

// 					return
// 				}
// 			}
// 			rf.mu.Unlock()
// 		}(i, args)
// 	}
// }

// // example code to send a RequestVote RPC to a server.
// // server is the index of the target server in rf.peers[].
// // expects RPC arguments in args.
// // fills in *reply with RPC reply, so caller should
// // pass &reply.
// // the types of the args and reply passed to Call() must be
// // the same as the types of the arguments declared in the
// // handler function (including whether they are pointers).
// //
// // The labrpc package simulates a lossy network, in which servers
// // may be unreachable, and in which requests and replies may be lost.
// // Call() sends a request and waits for a reply. If a reply arrives
// // within a timeout interval, Call() returns true; otherwise
// // Call() returns false. Thus Call() may not return for a while.
// // A false return can be caused by a dead server, a live server that
// // can't be reached, a lost request, or a lost reply.
// //
// // Call() is guaranteed to return (perhaps after a delay) *except* if the
// // handler function on the server side does not return.  Thus there
// // is no need to implement your own timeouts around Call().
// //
// // look at the comments in ../labrpc/labrpc.go for more details.
// //
// // if you're having trouble getting RPC to work, check that you've
// // capitalized all field names in structs passed over RPC, and
// // that the caller passes the address of the reply struct with &, not
// // the struct itself.
// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

// // 写AppendEntries args/reply + handler（2A只实现心跳）
// type AppendEntriesArgs struct {
// 	Term         int        // leader 当前任期
// 	LeaderID     int        // leader 的 ID
// 	PrevLogTerm  int        // 紧邻在新条目之前的日志条目的任期
// 	PrevLogIndex int        // 紧邻在新条目之前的日志条目的索引
// 	Entries      []LogEntry // 需要被保存的日志条目（心跳时为空）
// 	LeaderCommit int        // leader 已经提交的最高日志条目的索引
// }

// type AppendEntriesReply struct {
// 	Term    int // follower当前任期
// 	Success bool

// 	// 优化：当回复失败时，返回冲突日志的相关信息，帮助leader快速回退nextIndex
// 	ConflictTerm     int // 冲突日志的任期
// 	MinConflictIndex int // 冲突日志的索引
// }

// func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	if args.Term < rf.currentTerm {
// 		reply.Term = rf.currentTerm
// 		reply.Success = false
// 		return
// 	}

// 	rf.becomeFollower(args.Term)

// 	// 日志复制核心逻辑
// 	// 1. 检查日志匹配性：如果日志中没有与PrevLogIndex和PrevLogTerm匹配的条目，回复失败
// 	if args.PrevLogIndex >= len(rf.log) {
// 		reply.ConflictTerm = -1
// 		reply.MinConflictIndex = len(rf.log)

// 		reply.Term = rf.currentTerm
// 		reply.Success = false
// 		return
// 	}
// 	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
// 		reply.ConflictTerm = rf.logTerm(args.PrevLogIndex)
// 		for i := args.PrevLogIndex; i >= 0; i-- {
// 			if rf.logTerm(i) != reply.ConflictTerm {
// 				reply.MinConflictIndex = i + 1
// 				break
// 			}
// 		}

// 		reply.Term = rf.currentTerm
// 		reply.Success = false
// 		return
// 	}

// 	// 2. 如果匹配成功，只在发现冲突时截断并追加（心跳不应删除后缀日志）
// 	logChanged := false
// 	insertAt := args.PrevLogIndex + 1
// 	for i := 0; i < len(args.Entries); i++ {
// 		pos := insertAt + i
// 		if pos >= len(rf.log) {
// 			rf.log = append(rf.log, args.Entries[i:]...)
// 			logChanged = true
// 			break
// 		}
// 		if rf.log[pos].Term != args.Entries[i].Term {
// 			rf.log = rf.log[:pos]
// 			rf.log = append(rf.log, args.Entries[i:]...)
// 			logChanged = true
// 			break
// 		}
// 	}
// 	if logChanged {
// 		rf.persist()
// 	}

// 	// 3. 更新commitIndex（需要判断是否有多数节点追加了日志）
// 	if args.LeaderCommit > rf.commitIndex {
// 		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
// 	}

// 	reply.Term = rf.currentTerm
// 	reply.Success = true
// }

// func (rf *Raft) broadcastAppendEntries() {
// 	type appendTask struct {
// 		server int
// 		term   int
// 		args   *AppendEntriesArgs
// 	}

// 	tasks := make([]appendTask, 0, len(rf.peers)-1)

// 	rf.mu.Lock()
// 	if rf.role != Leader {
// 		rf.mu.Unlock()
// 		return
// 	}
// 	term := rf.currentTerm

// 	for i := range rf.peers {
// 		if i == rf.me {
// 			continue
// 		}
// 		if rf.appendInFlight[i] {
// 			continue
// 		}
// 		rf.appendInFlight[i] = true

// 		next := rf.nextIndex[i]
// 		args := &AppendEntriesArgs{
// 			Term:         term,
// 			LeaderID:     rf.me,
// 			PrevLogIndex: next - 1,
// 			PrevLogTerm:  rf.log[next-1].Term,
// 			LeaderCommit: rf.commitIndex,
// 		}
// 		entries := make([]LogEntry, len(rf.log[next:]))
// 		copy(entries, rf.log[next:])
// 		args.Entries = entries

// 		tasks = append(tasks, appendTask{
// 			server: i,
// 			term:   term,
// 			args:   args,
// 		})
// 	}
// 	rf.mu.Unlock()

// 	for _, task := range tasks {
// 		go rf.sendAppendEntriesOne(task.server, task.term, task.args)
// 	}
// }

// func (rf *Raft) sendAppendEntriesOne(server int, term int, args *AppendEntriesArgs) {
// 	reply := &AppendEntriesReply{}
// 	ok := rf.sendAppendEntries(server, args, reply)

// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	rf.appendInFlight[server] = false

// 	if !ok {
// 		return
// 	}

// 	if reply.Term > rf.currentTerm {
// 		rf.becomeFollower(reply.Term)
// 		return
// 	}

// 	if rf.role != Leader || rf.currentTerm != term {
// 		return
// 	}

// 	if reply.Success {
// 		newNext := args.PrevLogIndex + len(args.Entries) + 1
// 		if newNext > rf.nextIndex[server] {
// 			rf.nextIndex[server] = newNext
// 		}
// 		newMatch := newNext - 1
// 		if newMatch > rf.matchIndex[server] {
// 			rf.matchIndex[server] = newMatch
// 		}
// 		rf.updateCommitIndex1() // 更新 commitIndex
// 		return
// 	}

// 	backoff := max(1, reply.MinConflictIndex)
// 	if backoff < rf.nextIndex[server] {
// 		rf.nextIndex[server] = backoff
// 	}
// }

// func (rf *Raft) updateCommitIndex() {
// 	// 判断是否有一个 N 满足 N > commitIndex，并且大多数节点的 matchIndex[i] >= N，并且 log[N].Term == currentTerm
// 	for N := rf.commitIndex + 1; N <= rf.lastLogIndex(); N++ {
// 		count := 1 // 包括 leader 自己
// 		for i := range rf.peers {
// 			if i != rf.me && rf.matchIndex[i] >= N {
// 				count++
// 			}
// 		}
// 		if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
// 			rf.commitIndex = N
// 		}
// 	}
// }

// func (rf *Raft) updateCommitIndex1() {
// 	// 新的更新 commitIndex 的思路：
// 	// 1. 先确定 leader 自己的日志中当期任期的第一条日志索引 baseIndex
// 	// 2. 在 [max(baseIndex,commitIndex+1),lastLogIndex] 范围内进行二分
// 	// for i:=rf.lastLogIndex();i>=1;i--{
// 	// 	if rf.log[i].Term == rf.currentTerm {
// 	// 		baseIndex = i
// 	// 	}else{
// 	// 		break
// 	// 	}
// 	// }

// 	matchIndexs := make([]int, len(rf.peers))
// 	for i := range rf.peers {
// 		matchIndexs[i] = rf.matchIndex[i]
// 	}
// 	sort.Ints(matchIndexs)
// 	targetIndex := matchIndexs[len(rf.peers)-len(rf.peers)/2-1] // 大多数节点的 matchIndex
// 	if targetIndex <= rf.commitIndex || rf.log[targetIndex].Term != rf.currentTerm {
// 		return
// 	}
// 	rf.commitIndex = targetIndex
// }

// func (rf *Raft) applier(applyCh chan ApplyMsg) {
// 	for rf.killed() == false {
// 		time.Sleep(1 * time.Millisecond)
// 		rf.mu.Lock()

// 		upper := min(rf.commitIndex, rf.lastLogIndex())
// 		if upper > rf.lastApplied {
// 			rf.lastApplied++
// 			applyMsg := ApplyMsg{
// 				CommandValid: true,
// 				Command:      rf.log[rf.lastApplied].Command,
// 				CommandIndex: rf.lastApplied,
// 			}
// 			rf.mu.Unlock()
// 			applyCh <- applyMsg
// 		} else {
// 			rf.mu.Unlock()
// 		}
// 	}
// }

// func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
// 	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
// 	return ok
// }

// // the service using Raft (e.g. a k/v server) wants to start
// // agreement on the next command to be appended to Raft's log. if this
// // server isn't the leader, returns false. otherwise start the
// // agreement and return immediately. there is no guarantee that this
// // command will ever be committed to the Raft log, since the leader
// // may fail or lose an election. even if the Raft instance has been killed,
// // this function should return gracefully.
// //
// // the first return value is the index that the command will appear at
// // if it's ever committed. the second return value is the current
// // term. the third return value is true if this server believes it is
// // the leader.
// func (rf *Raft) Start(command interface{}) (int, int, bool) {
// 	index := -1 // 这条命令在日志中的位置，上层用它去匹配 ApplyMsg 中的 CommandIndex
// 	term := -1
// 	isLeader := true

// 	// Your code here (2B).
// 	// 先判断节点是否是 leader，如果不是 leader 直接返回 false
// 	rf.mu.Lock()
// 	if rf.role != Leader {
// 		term = rf.currentTerm
// 		rf.mu.Unlock()
// 		isLeader = false
// 		return index, term, isLeader
// 	}

// 	// 是 leader 的话就将命令追加到日志中，并返回日志索引和当前任期
// 	term = rf.currentTerm
// 	entry := LogEntry{
// 		Term:    rf.currentTerm,
// 		Command: command,
// 	}
// 	rf.log = append(rf.log, entry)
// 	rf.persist()
// 	index = rf.lastLogIndex()       // 新条目的索引
// 	rf.matchIndex[rf.me] = index    // 更新 leader 自己的 matchIndex
// 	rf.nextIndex[rf.me] = index + 1 // 更新 leader 自己的 nextIndex
// 	rf.mu.Unlock()
// 	rf.broadcastAppendEntries() // 追加日志后立即发送 AppendEntries RPC

// 	return index, term, isLeader
// }

// // the tester doesn't halt goroutines created by Raft after each test,
// // but it does call the Kill() method. your code can use killed() to
// // check whether Kill() has been called. the use of atomic avoids the
// // need for a lock.
// //
// // the issue is that long-running goroutines use memory and may chew
// // up CPU time, perhaps causing later tests to fail and generating
// // confusing debug output. any goroutine with a long-running loop
// // should call killed() to check whether it should stop.
// func (rf *Raft) Kill() {
// 	atomic.StoreInt32(&rf.dead, 1)
// 	// Your code here, if desired.
// }

// func (rf *Raft) killed() bool {
// 	z := atomic.LoadInt32(&rf.dead)
// 	return z == 1
// }

// // the service or tester wants to create a Raft server. the ports
// // of all the Raft servers (including this one) are in peers[]. this
// // server's port is peers[me]. all the servers' peers[] arrays
// // have the same order. persister is a place for this server to
// // save its persistent state, and also initially holds the most
// // recent saved state, if any. applyCh is a channel on which the
// // tester or service expects Raft to send ApplyMsg messages.
// // Make() must return quickly, so it should start goroutines
// // for any long-running work.
// func Make(peers []*labrpc.ClientEnd, me int,
// 	persister *Persister, applyCh chan ApplyMsg) *Raft {
// 	// Your initialization code here (2A, 2B, 2C).
// 	rf := &Raft{
// 		peers:             peers,
// 		persister:         persister,
// 		me:                me,
// 		role:              Follower,
// 		currentTerm:       0,
// 		voteFor:           -1,
// 		electionResetTime: time.Now(),
// 		electionTimeout:   randElectionTimeout(),

// 		log: []LogEntry{
// 			// 2B日志索引从1开始，0位置放一个空日志条目
// 			{Term: 0},
// 		},
// 		nextIndex:      make([]int, len(peers)),
// 		matchIndex:     make([]int, len(peers)),
// 		appendInFlight: make([]bool, len(peers)),
// 	}
// 	// initialize from state persisted before a crash
// 	rf.readPersist(persister.ReadRaftState())
// 	go rf.ticker()
// 	go rf.applier(applyCh)

// 	return rf
// }

// // 生成随机选举超时时间，范围在300ms到600ms之间
// func randElectionTimeout() time.Duration {
// 	return time.Duration(300+rand.Intn(300)) * time.Millisecond
// }

// // 切换到Follower状态
// func (rf *Raft) becomeFollower(term int) {
// 	if term < rf.currentTerm {
// 		return
// 	}

// 	rf.role = Follower
// 	if term > rf.currentTerm {
// 		rf.currentTerm = term
// 		rf.voteFor = -1
// 		rf.persist()
// 	}

// 	rf.electionResetTime = time.Now()          // 切换到Follower时重置选举计时器
// 	rf.electionTimeout = randElectionTimeout() // 切换到Follower时重新生成随机选举超时时间
// }

// // 切换到Candidate状态
// func (rf *Raft) becomeCandidate() {
// 	rf.role = Candidate
// 	rf.currentTerm++
// 	rf.voteFor = rf.me
// 	rf.electionResetTime = time.Now()
// 	rf.persist()
// }

// // 切换到Leader状态
// func (rf *Raft) becomeLeader() {
// 	rf.role = Leader
// 	last := rf.lastLogIndex()

// 	for i := range rf.peers {
// 		rf.appendInFlight[i] = false
// 		rf.nextIndex[i] = last + 1
// 		if i == rf.me {
// 			rf.matchIndex[i] = last
// 		} else {
// 			rf.matchIndex[i] = 0
// 		}
// 	}
// 	rf.lastHeartbeatTime = time.Now()
// }

// // 日志辅助函数
// // lastLogIndex 返回日志中最后一个条目的索引，如果日志为空则返回-1
// func (rf *Raft) lastLogIndex() int {
// 	return len(rf.log) - 1
// }

// // lastLogTerm 返回日志中最后一个条目的任期，如果日志为空则返回-1
// func (rf *Raft) lastLogTerm() int {
// 	return rf.log[len(rf.log)-1].Term
// }

// // logTerm 返回日志中指定索引处条目的任期，如果索引无效则返回-1
// func (rf *Raft) logTerm(index int) int {
// 	if index >= len(rf.log) {
// 		return -1
// 	}
// 	return rf.log[index].Term
// }