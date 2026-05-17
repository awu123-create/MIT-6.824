package shardkv

// import "../shardmaster"
import (
	"bytes"
	"fmt"
	"sort"
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

	Config      shardmaster.Config
	Data        map[int]map[string]string
	LastRequest map[int64]LastOp
	ConfigNum   int
	ShardIDs    []int
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

type ShardMeta struct {
	FromGID   int
	ToGID     int
	State     ShardState
	PendingGC bool
}

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

	mck *shardmaster.Clerk

	// shard -> (key -> value)
	kvDB                     map[int]map[string]string
	lastAppliedSnapshotIndex int

	// 当前配置和上一个配置，迁移数据时需要知道上一个配置的情况
	currentConfig shardmaster.Config
	lastConfig    shardmaster.Config
	shardState    map[int]ShardMeta

	// 减少重复RPC
	pullInFlight map[string]bool
	gcInFlight   map[string]bool
}

func (kv *ShardKV) canServe(key string) bool {
	// 现在这里每个shard都有单独的上下文，所以这个canServe是用来判断一个key对应的shard是否可以提供服务的
	shard := key2shard(key)
	meta, ok := kv.shardState[shard]
	if !ok {
		return false
	}

	return meta.State == Serving && kv.currentConfig.Shards[shard] == kv.gid
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if !kv.canServe(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	Op := Op{
		Type:      "Get",
		Key:       args.Key,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}

	index, _, isLeader := kv.rf.Start(Op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

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
	case <-time.After(200 * time.Millisecond):
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
		kv.mu.Unlock()
		return
	}

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
	case <-time.After(200 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) PullShard(args *PullShardArgs, reply *PullShardReply) {
	kv.mu.Lock()
	if args.ConfigNum != kv.currentConfig.Num {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	// 检查状态并浅拷贝引用，释放锁后再做深拷贝
	shardKeys := make([]int, 0, len(args.ShardIDs))
	for _, shard := range args.ShardIDs {
		meta := kv.shardState[shard]
		if meta.State != BePushing {
			reply.Err = ErrWrongGroup
			kv.mu.Unlock()
			return
		}
		shardKeys = append(shardKeys, shard)
	}

	// 在锁内读取 kvDB 的引用，但不在锁内遍历拷贝
	tempData := make(map[int]map[string]string)
	for _, shard := range shardKeys {
		src := kv.kvDB[shard]
		dst := make(map[string]string, len(src))
		for k, v := range src {
			dst[k] = v
		}
		tempData[shard] = dst
	}

	lastRequest := make(map[int64]LastOp)
	for clientID, lastOp := range kv.lastRequest {
		lastRequest[clientID] = lastOp
	}
	kv.mu.Unlock()

	reply.ShardData = tempData
	reply.LastRequest = lastRequest
	reply.Err = OK
}

func (kv *ShardKV) GC(args *GCArgs, reply *GCReply) {
	kv.mu.Lock()
	if args.ConfigNum > kv.currentConfig.Num {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	} else if args.ConfigNum < kv.currentConfig.Num {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	for _, shard := range args.ShardIDs {
		meta := kv.shardState[shard]
		if meta.State == BePulling {
			reply.Err = ErrWrongGroup
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	id := make([]int, len(args.ShardIDs))
	copy(id, args.ShardIDs)

	op := Op{
		Type:      "GC",
		ConfigNum: args.ConfigNum,
		ShardIDs:  id,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		_, stillLeader := kv.rf.GetState()
		if !stillLeader {
			reply.Err = ErrWrongLeader
			return
		}

		done := true
		kv.mu.Lock()
		for _, shard := range args.ShardIDs {
			meta := kv.shardState[shard]
			if meta.State != Serving {
				done = false
				break
			}
		}
		kv.mu.Unlock()

		if done {
			reply.Err = OK
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 超时了可能是leader发生了变化，也可能是网络问题等导致的
	reply.Err = ErrWrongLeader
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

	defer func() {
		kv.mu.Lock()
		shouldSnapshot := kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate
		var snapshot []byte
		if shouldSnapshot {
			snapshot = kv.makeSnapshot()
		}
		kv.lastAppliedSnapshotIndex = msg.CommandIndex
		kv.mu.Unlock()

		if shouldSnapshot {
			kv.rf.Snapshot(msg.CommandIndex, snapshot)
		}
	}()

	op := msg.Command.(Op)

	kv.mu.Lock()
	lastOp, ok := kv.lastRequest[op.ClientID]
	ch, chExist := kv.notifyCh[msg.CommandIndex]

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
		// 此时处理的是到达的重复请求，直接返回上次的结果
		if chExist {
			ch <- lastOp.Result
			return
		}
		return
	}

	switch op.Type {
	case "Get", "Put", "Append":
		result := kv.applyClientRequest(op, key2shard(op.Key))

		if chExist {
			ch <- result
		}

	case "Config":
		// 此时需要更新当前配置
		kv.mu.Lock()
		if op.Config.Num == kv.currentConfig.Num+1 {
			kv.lastConfig = kv.currentConfig
			kv.currentConfig = op.Config

			for i := 0; i < shardmaster.NShards; i++ {
				/*
					逐个计算shard的状态:
						1. 如果shard原来和现在都归我 -> Serving
						2. 如果shard原来归我，现在不归我 -> BePushing
						3. 如果shard原来不归我，现在归我 -> BePulling
						4. 如果shard现在归我，原来没人管 -> Serving
				*/
				meta := kv.shardState[i]
				if kv.lastConfig.Shards[i] == kv.gid && kv.currentConfig.Shards[i] == kv.gid {
					meta.State = Serving
					meta.FromGID = 0
					meta.ToGID = 0
				} else if kv.lastConfig.Shards[i] == kv.gid && kv.currentConfig.Shards[i] != kv.gid {
					meta.State = BePushing
					meta.FromGID = 0
					meta.ToGID = kv.currentConfig.Shards[i]
				} else if kv.lastConfig.Shards[i] == 0 && kv.currentConfig.Shards[i] == kv.gid {
					meta.State = Serving
					meta.FromGID = 0
					meta.ToGID = 0
				} else if kv.lastConfig.Shards[i] != kv.gid && kv.currentConfig.Shards[i] == kv.gid {
					meta.State = BePulling
					meta.FromGID = kv.lastConfig.Shards[i]
					meta.ToGID = 0
				}
				meta.PendingGC = false
				kv.shardState[i] = meta
			}
		}
		kv.mu.Unlock()

	case "Pull":
		kv.mu.Lock()
		if op.ConfigNum != kv.currentConfig.Num {
			kv.mu.Unlock()
			return
		}

		finished := make(map[int][]int)
		for shard, data := range op.Data {
			meta := kv.shardState[shard]
			if meta.State == BePulling {
				tempData := make(map[string]string)
				for k, v := range data {
					tempData[k] = v
				}
				kv.kvDB[shard] = tempData

				meta.State = Serving
				meta.PendingGC = true
				kv.shardState[shard] = meta

				finished[meta.FromGID] = append(finished[meta.FromGID], shard)
			}
		}

		for clientID, lastOp := range op.LastRequest {
			if lastOp.RequestID > kv.lastRequest[clientID].RequestID {
				kv.lastRequest[clientID] = lastOp
			}
		}

		for gid, shards := range finished {
			sort.Ints(shards)
			key := fmt.Sprintf("%d-%d-%v", kv.currentConfig.Num, gid, shards)
			delete(kv.pullInFlight, key)
		}
		kv.mu.Unlock()
	case "GC":
		// 需要修改shardState
		kv.mu.Lock()
		if op.ConfigNum != kv.currentConfig.Num {
			kv.mu.Unlock()
			return
		}

		for _, shard := range op.ShardIDs {
			meta := kv.shardState[shard]
			if meta.State == BePushing {
				meta.State = Serving
				meta.FromGID = 0
				meta.ToGID = 0
				meta.PendingGC = false
				kv.shardState[shard] = meta

				// 清空数据
				kv.kvDB[shard] = make(map[string]string)
			}
		}
		kv.mu.Unlock()

	case "FinishGC":
		kv.mu.Lock()
		if op.ConfigNum != kv.currentConfig.Num {
			kv.mu.Unlock()
			return
		}

		finished := make(map[int][]int)
		for _, shard := range op.ShardIDs {
			meta := kv.shardState[shard]
			if meta.State == Serving && meta.PendingGC {
				finished[meta.FromGID] = append(finished[meta.FromGID], shard)

				meta.PendingGC = false
				meta.FromGID = 0
				meta.ToGID = 0
				kv.shardState[shard] = meta
			}
		}

		for gid, shards := range finished {
			sort.Ints(shards)
			key := fmt.Sprintf("%d-%d-%v", kv.currentConfig.Num, gid, shards)
			delete(kv.gcInFlight, key)
		}
		kv.mu.Unlock()
	}

}

func (kv *ShardKV) applyClientRequest(op Op, shard int) OpResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	result := OpResult{
		ClientID:  op.ClientID,
		RequestID: op.RequestID,
	}

	switch op.Type {
	case "Get":
		value, ok := kv.kvDB[shard][op.Key]
		if ok {
			result.Err = OK
			result.Value = value
		} else {
			result.Err = ErrNoKey
		}
	case "Put":
		kv.kvDB[shard][op.Key] = op.Value
		result.Err = OK
	case "Append":
		kv.kvDB[shard][op.Key] += op.Value
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

		if !isLeader {
			kv.mu.Unlock()
			continue
		}

		// 在当前配置下shard迁移完成后，才可以拉取新的配置
		if !kv.migrationFinsished() {
			kv.mu.Unlock()
			continue
		}

		// 拉取Config.Num+1的配置，避免不知道该如何迁移数据的情况发生
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

func (kv *ShardKV) puller() {
	for !kv.killed() {
		time.Sleep(100 * time.Millisecond)

		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()

		if !isLeader {
			for key := range kv.pullInFlight {
				delete(kv.pullInFlight, key)
			}

			kv.mu.Unlock()
			continue
		}

		if kv.migrationFinsished() {
			kv.mu.Unlock()
			continue
		}

		// 拉取数据
		// 先计算出需要拉取数据的shard和对应的gid
		id := make(map[int][]int)
		for shard, meta := range kv.shardState {
			if meta.State == BePulling {
				gid := meta.FromGID
				if id[gid] == nil {
					id[gid] = make([]int, 0)
				}
				id[gid] = append(id[gid], shard)

				// key := fmt.Sprintf("%d-%d", kv.currentConfig.Num, shard)
				// kv.pullInFlight[key] = true
			}
		}

		currentConfig := kv.currentConfig
		lastConfig := kv.lastConfig
		kv.mu.Unlock()

		for gid, shards := range id {
			if len(shards) == 0 {
				continue
			}

			args := PullShardArgs{
				ConfigNum: currentConfig.Num,
				ShardIDs:  shards,
			}

			servers := lastConfig.Groups[gid]
			for _, server := range servers {
				reply := PullShardReply{}
				ok := kv.make_end(server).Call("ShardKV.PullShard", &args, &reply)
				if ok && reply.Err == OK {
					op := Op{
						Type:        "Pull",
						Data:        reply.ShardData,
						LastRequest: reply.LastRequest,
						ConfigNum:   currentConfig.Num,
					}

					sort.Ints(shards)
					key := fmt.Sprintf("%d-%d-%v", currentConfig.Num, gid, shards)

					kv.mu.Lock()
					if kv.pullInFlight[key] {
						kv.mu.Unlock()
						continue
					}
					kv.pullInFlight[key] = true
					kv.mu.Unlock()

					_, _, isLeader := kv.rf.Start(op)
					if !isLeader {
						kv.mu.Lock()
						delete(kv.pullInFlight, key)
						kv.mu.Unlock()
						continue
					}
				}
			}
		}
	}
}

func (kv *ShardKV) gcTicker() {
	for !kv.killed() {
		time.Sleep(200 * time.Millisecond)

		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			for key := range kv.gcInFlight {
				delete(kv.gcInFlight, key)
			}

			kv.mu.Unlock()
			continue
		}

		// 拉取数据完成后，通知旧组可以GC了
		gcSend := make(map[int][]int) // gid -> []shard
		for shard, meta := range kv.shardState {
			if meta.State == Serving && meta.PendingGC {
				gid := meta.FromGID
				if gcSend[gid] == nil {
					gcSend[gid] = make([]int, 0)
				}
				gcSend[gid] = append(gcSend[gid], shard)
			}
		}

		configNum := kv.currentConfig.Num
		lastConfig := kv.lastConfig
		kv.mu.Unlock()

		for gid, shards := range gcSend {
			shardIDs := make([]int, len(shards))
			copy(shardIDs, shards)

			args := GCArgs{
				ConfigNum: configNum,
				ShardIDs:  shardIDs,
			}
			servers := lastConfig.Groups[gid]

			for _, server := range servers {
				reply := GCReply{}
				ok := kv.make_end(server).Call("ShardKV.GC", &args, &reply)
				if ok && reply.Err == OK {
					op := Op{
						Type:      "FinishGC",
						ConfigNum: configNum,
						ShardIDs:  shardIDs,
					}

					sort.Ints(shardIDs)
					key := fmt.Sprintf("%d-%d-%v", configNum, gid, shardIDs)

					kv.mu.Lock()
					if kv.gcInFlight[key] {
						kv.mu.Unlock()
						continue
					}
					kv.gcInFlight[key] = true
					kv.mu.Unlock()

					_, _, isLeader := kv.rf.Start(op)
					if !isLeader {
						kv.mu.Lock()
						delete(kv.gcInFlight, key)
						kv.mu.Unlock()
						continue
					}
				}
			}
		}
	}
}

func (kv *ShardKV) migrationFinsished() bool {
	for i := 0; i < shardmaster.NShards; i++ {
		meta := kv.shardState[i]
		if meta.State == BePulling || meta.State == BePushing || meta.PendingGC {
			return false
		}
	}
	return true
}

func (kv *ShardKV) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDB)
	e.Encode(kv.lastRequest)
	e.Encode(kv.currentConfig)
	e.Encode(kv.lastConfig)
	e.Encode(kv.shardState)

	return w.Bytes()
}

func (kv *ShardKV) readSnapshot(data []byte) {
	// 拦截 nil 和空快照
	if len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvDB map[int]map[string]string
	var lastRequest map[int64]LastOp
	var currentConfig shardmaster.Config
	var lastConfig shardmaster.Config
	var shardState map[int]ShardMeta

	if d.Decode(&kvDB) != nil || d.Decode(&lastRequest) != nil || d.Decode(&currentConfig) != nil ||
		d.Decode(&lastConfig) != nil || d.Decode(&shardState) != nil {
		panic("failed to read snapshot")
	} else {
		kv.kvDB = kvDB
		kv.lastRequest = lastRequest
		kv.currentConfig = currentConfig
		kv.lastConfig = lastConfig
		kv.shardState = shardState
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
	kv.kvDB = make(map[int]map[string]string)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.kvDB[i] = make(map[string]string)
	}

	kv.shardState = make(map[int]ShardMeta)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.shardState[i] = ShardMeta{
			FromGID: 0,
			ToGID:   0,
			State:   Serving,
		}
	}

	kv.pullInFlight = make(map[string]bool)
	kv.gcInFlight = make(map[string]bool)

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.readSnapshot(persister.ReadSnapshot())

	go kv.applier()
	go kv.ticker()
	go kv.puller()
	go kv.gcTicker()

	return kv
}
