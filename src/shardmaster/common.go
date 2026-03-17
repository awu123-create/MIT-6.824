package shardmaster

import "sort"

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (c *Config) reBanlance() {
	// 计算每个group的shard数量
	groupShardCount := make(map[int]int)
	for _, gid := range c.Shards {
		groupShardCount[gid]++
	}

	if len(c.Groups) == 0 {
		for i := 0; i < NShards; i++ {
			c.Shards[i] = 0
		}
		return
	}

	targetCount := NShards / len(c.Groups)
	extraCount := NShards % len(c.Groups)

	// 先对gid进行排序
	gids := make([]int, 0)
	for gid := range c.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	// 重新分配shard，需要满足tester的minimal transfer
	// 前extraCount个group分配targetCount+1个shard，剩余的group分配targetCount个shard
	desiredCount := make(map[int]int)
	for i, gid := range gids {
		want := targetCount
		if i < extraCount {
			want++
		}
		desiredCount[gid] = want
	}

	// 对不在当前group中的gid，将它们的desiredCount设为0
	for gid := range groupShardCount {
		if _, ok := desiredCount[gid]; !ok {
			desiredCount[gid] = 0
		}
	}

	for _, gid := range gids {
		for groupShardCount[gid] < desiredCount[gid] {
			for j, curGid := range c.Shards {
				if curGid != gid && groupShardCount[curGid] > desiredCount[curGid] {
					c.Shards[j] = gid
					groupShardCount[gid]++
					groupShardCount[curGid]--
					break
				}
			}
		}
	}
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	ClientID  int64
	RequestID int64
	Servers   map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	ClientID  int64
	RequestID int64
	GIDs      []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	ClientID  int64
	RequestID int64
	Shard int
	GID   int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num       int // desired config number
	ClientID  int64
	RequestID int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
