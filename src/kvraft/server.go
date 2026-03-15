package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	OpType    string // "Get", "Put" or "Append"
	Key       string
	Value     string
	ClientID  int64 // 每个客户端一个唯一ID
	RequestID int64 // 每个请求一个唯一ID，客户端递增
}

type LastOp struct {
	RequestID int64    // 上一次请求的RequestID
	Result    OpResult // 上一次请求的结果
}

type OpResult struct {
	Err       Err    // Raft处理客户端命令时产生的错误
	Value     string // 客户端Get请求读取的值
	ClientID  int64  // 和RequestID一起唯一标识一个请求，用于去重
	RequestID int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate             int // snapshot if log grows this big
	lastAppliedSnapshotIndex int

	// Your definitions here.
	kvDB        map[string]string     // kv存储
	lastRequest map[int64]LastOp      // 每个客户端的上一次请求结果，key是ClientID
	notifyChs   map[int]chan OpResult // 每个日志索引对应一个通知通道，Raft apply一个日志后，通过这个通道通知等待的请求处理结果
}

// 客户端通过Clerk发送请求，然后KVServer通过Raft共识算法达成日志一致，最后KVServer apply日志并执行对应的操作。每个请求都包含ClientID和RequestID，用于去重和返回结果。
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 1. 构造Op
	op := Op{
		OpType:    "Get",
		Key:       args.Key,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}

	// 2. 调用Raft的Start()方法将Op发送给Raft
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 3. 等待Raft apply日志并执行对应的操作，使用notifyChs进行通知
	kv.mu.Lock()
	ch, ok := kv.notifyChs[index]
	if !ok {
		ch = make(chan OpResult, 1)
		kv.notifyChs[index] = ch
	}
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.notifyChs, index)
		kv.mu.Unlock()
	}()

	select {
	case result := <-ch:
		// 进入此分支说明Raft已经apply了这个日志并执行了对应的操作，result是执行结果
		if result.ClientID == args.ClientID && result.RequestID == args.RequestID {
			reply.Err = result.Err
			reply.Value = result.Value
		} else {
			// 进入此分支有两种情况：
			// 1. 此请求之前被处理过了
			// 2. 服务器在调用Start方法时还是Leader，但当初那个 entry 还没提交，它所在的 log 位置后来可能被新 leader 的别的命令替换
			reply.Err = ErrWrongLeader
		}

	case <-time.After(100 * time.Millisecond):
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch, ok := kv.notifyChs[index]
	if !ok {
		ch = make(chan OpResult, 1)
		kv.notifyChs[index] = ch
	}
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.notifyChs, index)
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
		reply.Err = ErrTimeout
	}

}

// 生成snapshot
func (kv *KVServer) makeSnapshot() []byte {
	// snapshot中需要保存kvDB和lastRequest
	// kv.mu.Lock()
	// defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDB)
	e.Encode(kv.lastRequest)

	return w.Bytes()
}

// 读取snapshot
func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvDB map[string]string
	var lastRequest map[int64]LastOp

	if d.Decode(&kvDB) != nil || d.Decode(&lastRequest) != nil {
		panic("Failed to decode snapshot")
	} else {
		kv.kvDB = kvDB
		kv.lastRequest = lastRequest
	}
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			kv.apply(msg)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (kv *KVServer) apply(msg raft.ApplyMsg) {
	if !msg.CommandValid && msg.SnapshotValid {
		kv.mu.Lock()
		if msg.SnapshotIndex <= kv.lastAppliedSnapshotIndex {
			kv.mu.Unlock()
			return
		}
		kv.lastAppliedSnapshotIndex = msg.SnapshotIndex
		kv.readSnapshot(msg.Snapshot)
		kv.notifyChs = make(map[int]chan OpResult) // 读取snapshot后之前的notifyChs都失效了，需要重置
		kv.mu.Unlock()
		return
	}

	op := msg.Command.(Op)

	kv.mu.Lock()
	// 查重
	lastOp, ok := kv.lastRequest[op.ClientID]
	ch, chExist := kv.notifyChs[msg.CommandIndex]
	kv.mu.Unlock()
	if ok && op.RequestID <= lastOp.RequestID {
		// 已经处理过这个请求，直接返回上一次的结果
		kv.mu.Lock()
		if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
			snapshot := kv.makeSnapshot()
			kv.rf.Snapshot(msg.CommandIndex, snapshot)
		}

		kv.lastAppliedSnapshotIndex= msg.CommandIndex
		kv.mu.Unlock()

		if chExist {
			ch <- lastOp.Result
		}

		return
	}

	// 执行操作（修改kvDB、lastRequest）
	result := OpResult{
		ClientID:  op.ClientID,
		RequestID: op.RequestID,
	}
	kv.mu.Lock()
	switch op.OpType {
	case "Get":
		value, ok := kv.kvDB[op.Key]
		if ok {
			result.Err = OK
			result.Value = value
		} else {
			result.Err = ErrNoKey
		}
	case "Put", "Append":
		if op.OpType == "Append" {
			kv.kvDB[op.Key] += op.Value
		} else {
			kv.kvDB[op.Key] = op.Value
		}
		result.Err = OK
	}

	kv.lastRequest[op.ClientID] = LastOp{
		RequestID: op.RequestID,
		Result:    result,
	}

	if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
		snapshot := kv.makeSnapshot()
		kv.rf.Snapshot(msg.CommandIndex, snapshot)
	}

	// ch, ok := kv.notifyChs[msg.CommandIndex]
	kv.lastAppliedSnapshotIndex= msg.CommandIndex
	kv.mu.Unlock()
	// 通过notifyChs通知等待的请求处理结果
	if chExist {
		ch <- result
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvDB = make(map[string]string)
	kv.lastRequest = make(map[int64]LastOp)
	kv.notifyChs = make(map[int]chan OpResult)

	// You may need initialization code here.
	kv.readSnapshot(persister.ReadSnapshot())
	go kv.applier()

	return kv
}
