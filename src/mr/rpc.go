package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type RequestArgs struct {
}

type RequestReply struct {
	ID        int      // 任务ID
	Type      TaskType // 任务类型
	FileName  string   // map任务的输入文件
	NumMap    int      // map任务的总数
	NumReduce int      // reduce任务的总数
}

type ReportArgs struct {
	Type TaskType // 任务类型
	ID   int      // 任务ID
	Ok   bool     // 任务是否成功完成
}

type ReportReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
