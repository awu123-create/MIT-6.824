package kvraft

import (
	"crypto/rand"
	"math/big"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderID  int   // 用于记录当前认为的Leader的ID，减少发送请求时重试的次数
	clientID  int64 // 唯一标识一个客户端
	requestID int64 // 每个请求的唯一ID，递增生成，配合clientID可以唯一标识一个请求，防止重复执行同一请求
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
	// You'll have to add code here.

	ck.clientID = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.requestID++ // 生成新的请求ID
	args := &GetArgs{
		Key:       key,
		ClientID:  ck.clientID,
		RequestID: ck.requestID,
	}

	// 首先尝试发送请求到当前认为的Leader服务器，如果请求失败或者返回错误，则轮询其他服务器，直到成功为止
	for {
		reply := &GetReply{}
		ok := ck.servers[ck.leaderID].Call("KVServer.Get", args, reply)
		if ok {
			switch reply.Err {
			case OK:
				return reply.Value
			case ErrNoKey:
				return ""
			default:

			}

		}

		// 请求失败或者返回错误，尝试下一个服务器
		ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
	}

	// You will have to modify this function.

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.requestID++
	args := &PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientID:  ck.clientID,
		RequestID: ck.requestID,
	}

	for {
		reply := &PutAppendReply{}
		ok := ck.servers[ck.leaderID].Call("KVServer.PutAppend", args, reply)
		if ok && reply.Err == OK {
			return
		}

		// 请求失败或者返回错误，尝试下一个服务器
		ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
