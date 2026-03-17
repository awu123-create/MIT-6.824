package shardmaster

import (
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	// Your data here.

	configs []Config // indexed by config num

	// 去重
	lastRequest map[int64]LastOp      // key: ClientID
	notifyCh    map[int]chan OpResult // key: RequestID
}

type CommandType int

const (
	Join CommandType = iota
	Leave
	Move
	Query
)

type LastOp struct {
	RequestID int64    // 上一次请求的RequestID
	Result    OpResult // 上一次请求的结果
}

type OpResult struct {
	Err       Err    // Raft处理客户端命令时产生的错误
	Config    Config // Raft处理Query命令时返回的配置
	ClientID  int64  // 和RequestID一起唯一标识一个请求，用于去重
	RequestID int64
}

type Op struct {
	// Your data here.
	Type      CommandType
	ClientID  int64
	RequestID int64

	// Join(servers) -- add a set of groups (gid -> server-list mapping).
	Servers map[int][]string

	// Leave(gids) -- delete a set of groups.
	GIDs []int

	// Move(shard, gid) -- hand off one shard from current owner to gid.
	Shard int
	GID   int

	// Query(num) -> fetch Config # num, or latest config if num==-1.
	Num int
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	Op := Op{
		Type:      Join,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
		Servers:   args.Servers,
	}

	index, _, isLeader := sm.rf.Start(Op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sm.mu.Lock()
	ch, ok := sm.notifyCh[index]
	if !ok {
		ch = make(chan OpResult, 1)
		sm.notifyCh[index] = ch
	}
	sm.mu.Unlock()

	defer func() {
		sm.mu.Lock()
		delete(sm.notifyCh, index)
		sm.mu.Unlock()
	}()

	select {
	case result := <-ch:
		if result.ClientID == args.ClientID && result.RequestID == args.RequestID {
			reply.WrongLeader = false
			reply.Err = result.Err
		} else {
			reply.WrongLeader = true
		}
	case <-time.After(100 * time.Millisecond):
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	Op := Op{
		Type:      Leave,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
		GIDs:      args.GIDs,
	}

	index, _, isLeader := sm.rf.Start(Op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sm.mu.Lock()
	ch, ok := sm.notifyCh[index]
	if !ok {
		ch = make(chan OpResult, 1)
		sm.notifyCh[index] = ch
	}
	sm.mu.Unlock()

	defer func() {
		sm.mu.Lock()
		delete(sm.notifyCh, index)
		sm.mu.Unlock()
	}()

	select {
	case result := <-ch:
		if result.ClientID == args.ClientID && result.RequestID == args.RequestID {
			reply.WrongLeader = false
			reply.Err = result.Err
		} else {
			reply.WrongLeader = true
		}
	case <-time.After(100 * time.Millisecond):
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	Op := Op{
		Type:      Move,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
		Shard:     args.Shard,
		GID:       args.GID,
	}

	index, _, isLeader := sm.rf.Start(Op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sm.mu.Lock()
	ch, ok := sm.notifyCh[index]
	if !ok {
		ch = make(chan OpResult, 1)
		sm.notifyCh[index] = ch
	}
	sm.mu.Unlock()

	defer func() {
		sm.mu.Lock()
		delete(sm.notifyCh, index)
		sm.mu.Unlock()
	}()

	select {
	case result := <-ch:
		if result.ClientID == args.ClientID && result.RequestID == args.RequestID {
			reply.WrongLeader = false
			reply.Err = result.Err
		} else {
			reply.WrongLeader = true
		}
	case <-time.After(100 * time.Millisecond):
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	Op := Op{
		Type:      Query,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
		Num:       args.Num,
	}

	index, _, isLeader := sm.rf.Start(Op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sm.mu.Lock()
	ch, ok := sm.notifyCh[index]
	if !ok {
		ch = make(chan OpResult, 1)
		sm.notifyCh[index] = ch
	}
	sm.mu.Unlock()

	defer func() {
		sm.mu.Lock()
		delete(sm.notifyCh, index)
		sm.mu.Unlock()
	}()

	select {
	case result := <-ch:
		if result.ClientID == args.ClientID && result.RequestID == args.RequestID {
			reply.WrongLeader = false
			reply.Err = result.Err
			reply.Config = result.Config
		} else {
			reply.WrongLeader = true
		}
	case <-time.After(100 * time.Millisecond):
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) applier() {
	for !sm.killed() {
		select {
		case msg := <-sm.applyCh:
			sm.apply(msg)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (sm *ShardMaster) apply(msg raft.ApplyMsg) {
	op := msg.Command.(Op)

	sm.mu.Lock()
	lastOp, ok := sm.lastRequest[op.ClientID]
	ch, chExist := sm.notifyCh[msg.CommandIndex]
	sm.mu.Unlock()
	if ok && lastOp.RequestID >= op.RequestID {
		if chExist {
			ch <- lastOp.Result
		}
		return
	}

	result := OpResult{
		ClientID:  op.ClientID,
		RequestID: op.RequestID,
	}
	sm.mu.Lock()

	switch op.Type {
	case Query:
		if op.Num == -1 {
			result.Config = sm.configs[len(sm.configs)-1]
			result.Err = OK
		} else if op.Num < len(sm.configs) {
			result.Config = sm.configs[op.Num]
			result.Err = OK
		}
	case Join:
		// 先深拷贝当前配置
		idx := len(sm.configs) - 1
		newConfig := DeepCopy(sm.configs[idx])
		newConfig.Num++

		// 把新 group 加入 Groups
		for gid, servers := range op.Servers {
			newConfig.Groups[gid] = servers
		}

		// reBalancing
		newConfig.reBanlance()

		// 新配置 append 到 configs 中
		sm.configs = append(sm.configs, newConfig)
		result.Err = OK
	case Leave:
		idx := len(sm.configs) - 1
		newConfig := DeepCopy(sm.configs[idx])
		newConfig.Num++

		// 从 Groups 中删除指定 group
		for _, gid := range op.GIDs {
			delete(newConfig.Groups, gid)
		}

		// reBalancing
		newConfig.reBanlance()

		// 新配置 append 到 configs 中
		sm.configs = append(sm.configs, newConfig)
		result.Err = OK
	case Move:
		idx := len(sm.configs) - 1
		newConfig := DeepCopy(sm.configs[idx])
		newConfig.Num++

		// 把指定 shard 分配给指定 group
		newConfig.Shards[op.Shard] = op.GID

		// 新配置 append 到 configs 中
		sm.configs = append(sm.configs, newConfig)
		result.Err = OK
	}

	sm.lastRequest[op.ClientID] = LastOp{
		RequestID: op.RequestID,
		Result:    result,
	}
	sm.mu.Unlock()

	if chExist {
		ch <- result
	}
}

func DeepCopy(config Config) Config {
	newConfig := Config{
		Num:    config.Num,
		Shards: config.Shards,
		Groups: make(map[int][]string),
	}

	for gid, servers := range config.Groups {
		copied := make([]string, len(servers))
		copy(copied, servers)
		newConfig.Groups[gid] = copied
	}
	return newConfig
}

// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
	// Your code here, if desired.
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.lastRequest = make(map[int64]LastOp)
	sm.notifyCh = make(map[int]chan OpResult)

	go sm.applier()

	return sm
}
