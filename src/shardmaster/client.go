package shardmaster

//
// Shardmaster clerk.
//

import "../labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	clientID  int64
	requestID int64
	leaderID  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientID = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.requestID++
	args := &QueryArgs{
		ClientID:  ck.clientID,
		RequestID: ck.requestID,
		Num:       num,
	}
	// Your code here.

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}

		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.requestID++
	args := &JoinArgs{
		ClientID:  ck.clientID,
		RequestID: ck.requestID,
		Servers:   servers,
	}
	// Your code here.

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}

		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.requestID++
	args := &LeaveArgs{
		ClientID:  ck.clientID,
		RequestID: ck.requestID,
		GIDs:      gids,
	}
	// Your code here.

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}

		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.requestID++
	args := &MoveArgs{
		ClientID:  ck.clientID,
		RequestID: ck.requestID,
		Shard:     shard,
		GID:       gid,
	}
	// Your code here.

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}

		}
		time.Sleep(100 * time.Millisecond)
	}
}
