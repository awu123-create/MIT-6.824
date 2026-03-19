package shardkv

// import "../shardmaster"
import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string
	Key       string
	Value     string
	ClientID  int64
	RequestID int64

	Config shardmaster.Config
}

type OpResult struct {
	Err       Err
	Value     string
	ClientID  int64
	RequestID int64
}

type LastOp struct {
	RequestID int64
	Result    OpResult
}

type ShardState int

const (
	Serving ShardState = iota
	BePulling
	BePushing
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead        int32
	lastRequest map[int64]LastOp
	notifyCh    map[int]chan OpResult

	mck                      *shardmaster.Clerk
	kvDB                     map[string]string
	lastAppliedSnapshotIndex int

	currentConfig shardmaster.Config
	lastConfig    shardmaster.Config
	shardState    map[int]ShardState
}

func (kv *ShardKV) canServe(key string) bool {
	shard := key2shard(key)

	return kv.currentConfig.Shards[shard] == kv.gid
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if !kv.canServe(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	Op := Op{
		Type:      "Get",
		Key:       args.Key,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}

	index, _, isLeader := kv.rf.Start(Op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch, ok := kv.notifyCh[index]
	if !ok {
		ch = make(chan OpResult, 1)
		kv.notifyCh[index] = ch
	}
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.notifyCh, index)
		kv.mu.Unlock()
	}()

	select {
	case result := <-ch:
		if result.ClientID == args.ClientID && result.RequestID == args.RequestID {
			reply.Err = result.Err
			reply.Value = result.Value
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(100 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if !kv.canServe(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	Op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}

	index, _, isLeader := kv.rf.Start(Op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch, ok := kv.notifyCh[index]
	if !ok {
		ch = make(chan OpResult, 1)
		kv.notifyCh[index] = ch
	}
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.notifyCh, index)
		kv.mu.Unlock()
	}()

	select {
	case result := <-ch:
		if result.ClientID == args.ClientID && result.RequestID == args.RequestID {
			reply.Err = result.Err
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(100 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			kv.apply(msg)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (kv *ShardKV) apply(msg raft.ApplyMsg) {
	if !msg.CommandValid && msg.SnapshotValid {
		kv.mu.Lock()
		if msg.SnapshotIndex <= kv.lastAppliedSnapshotIndex {
			kv.mu.Unlock()
			return
		}
		kv.lastAppliedSnapshotIndex = msg.SnapshotIndex
		kv.readSnapshot(msg.Snapshot)
		kv.mu.Unlock()
		return
	}

	op := msg.Command.(Op)

	kv.mu.Lock()
	lastOp, ok := kv.lastRequest[op.ClientID]
	ch, chExist := kv.notifyCh[msg.CommandIndex]
	kv.mu.Unlock()

	kv.mu.Lock()
	if !kv.canServe(op.Key) && (op.Type == "Get" || op.Type == "Put" || op.Type == "Append") {
		if chExist {
			ch <- OpResult{
				Err:       ErrWrongGroup,
				ClientID:  op.ClientID,
				RequestID: op.RequestID,
			}
		}
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	if ok && lastOp.RequestID >= op.RequestID && (op.Type == "Get" || op.Type == "Put" || op.Type == "Append") {
		if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
			kv.rf.Snapshot(msg.CommandIndex, kv.makeSnapshot())
		}

		if chExist {
			ch <- lastOp.Result
			return
		}
		return
	}

	switch op.Type {
	case "Get", "Put", "Append":
		result := kv.applyClientRequest(op)
		if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
			kv.rf.Snapshot(msg.CommandIndex, kv.makeSnapshot())
		}

		if chExist {
			ch <- result
		}

	case "Config":
		// 此时需要更新当前配置
		kv.mu.Lock()
		if op.Config.Num == kv.currentConfig.Num+1 {
			kv.currentConfig = op.Config
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) applyClientRequest(op Op) OpResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	result := OpResult{
		ClientID:  op.ClientID,
		RequestID: op.RequestID,
	}

	switch op.Type {
	case "Get":
		value, ok := kv.kvDB[op.Key]
		if ok {
			result.Err = OK
			result.Value = value
		} else {
			result.Err = ErrNoKey
		}
	case "Put":
		kv.kvDB[op.Key] = op.Value
		result.Err = OK
	case "Append":
		kv.kvDB[op.Key] += op.Value
		result.Err = OK
	}

	kv.lastRequest[op.ClientID] = LastOp{
		RequestID: op.RequestID,
		Result:    result,
	}

	return result
}

func (kv *ShardKV) ticker() {
	for !kv.killed() {
		time.Sleep(100 * time.Millisecond)

		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		kv.mu.Unlock()

		if !isLeader {
			continue
		}

		// 在当前配置下shard迁移完成后，才可以拉取新的配置
		// 拉取Config.Num+1的配置，避免不知道该如何迁移数据的情况发生
		kv.mu.Lock()
		num := kv.currentConfig.Num
		kv.mu.Unlock()

		newConfig := kv.mck.Query(num + 1)
		if newConfig.Num == num+1 {
			configOp := Op{
				Type:   "Config",
				Config: newConfig,
			}
			kv.rf.Start(configOp)
		}
	}
}

func (kv *ShardKV) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDB)
	e.Encode(kv.lastRequest)
	e.Encode(kv.currentConfig)

	return w.Bytes()
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvDB map[string]string
	var lastRequest map[int64]LastOp
	var currentConfig shardmaster.Config

	if d.Decode(&kvDB) != nil || d.Decode(&lastRequest) != nil || d.Decode(&currentConfig) != nil {
		panic("failed to read snapshot")
	} else {
		kv.kvDB = kvDB
		kv.lastRequest = lastRequest
		kv.currentConfig = currentConfig
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	kv.lastRequest = make(map[int64]LastOp)
	kv.notifyCh = make(map[int]chan OpResult)
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.kvDB = make(map[string]string)

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applier()
	go kv.ticker()

	return kv
}
