package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"


type Master struct {
	// Your definitions here.
	mu sync.Mutex	// 保证RPC处理函数的互斥访问
	files []string	// 输入文件列表
	nReduce int		// reduce任务数量
	phase int		// 当前阶段：0-map阶段，1-reduce阶段，2-完成
	mapTasks []TaskMeta
	reduceTasks []TaskMeta
}

type State int

const(
	Idle State = iota
	InProgress
	Completed
)

type TaskMeta struct{
	state State
	startTime time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AssignTask(args *RequestArgs, reply *RequestReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	reply.NumReduce=m.nReduce
	reply.NumMap=len(m.files)

	// 回收超时任务
	const timeout=10*time.Second
	now:=time.Now()
	if m.phase==0{
		for i:=0;i<len(m.files);i++{
			if m.mapTasks[i].state==InProgress && now.Sub(m.mapTasks[i].startTime)>timeout{
				m.mapTasks[i].state=Idle
			}
		}
	}else if m.phase==1{
		for i:=0;i<m.nReduce;i++{
			if m.reduceTasks[i].state==InProgress && now.Sub(m.reduceTasks[i].startTime)>timeout{
				m.reduceTasks[i].state=Idle
			}
		}
	}

	// 分配任务
	allDone:=func(tasks []TaskMeta) bool{
		for i:=0;i<len(tasks);i++{
			if tasks[i].state!=Completed{
				return false
			}
		}
		return true
	}
	if m.phase==0{
		if allDone(m.mapTasks){
			m.phase=1
			m.reduceTasks=make([]TaskMeta,m.nReduce)
		}else{
			for i:=0;i<len(m.files);i++{
				if m.mapTasks[i].state==Idle{
					m.mapTasks[i].state=InProgress
					m.mapTasks[i].startTime=now
					reply.Type=MapTask
					reply.ID=i
					reply.FileName=m.files[i]
					return nil
				}
			}
			reply.Type=WaitTask
			return nil
		}
	}

	if m.phase==1{
		if allDone(m.reduceTasks){
			m.phase=2
			reply.Type=ExitTask
			return nil
		}else{
			for i:=0;i<m.nReduce;i++{
				if m.reduceTasks[i].state==Idle{
					m.reduceTasks[i].state=InProgress
					m.reduceTasks[i].startTime=now
					reply.Type=ReduceTask
					reply.ID=i
					return nil
				}
			}
			reply.Type=WaitTask
			return nil
		}
	}

	reply.Type=ExitTask
	return nil
}

func (m *Master) ReportTask(args *ReportArgs, reply *ReportReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !args.Ok{
		if args.Type==MapTask{
			if m.mapTasks[args.ID].state!=Completed{
				m.mapTasks[args.ID].state=Idle
			}
		}else{
			if m.reduceTasks[args.ID].state!=Completed{
				m.reduceTasks[args.ID].state=Idle
			}
		}
	}

	if args.Type==MapTask{
		m.mapTasks[args.ID].state=Completed
	}else if args.Type==ReduceTask{
		m.reduceTasks[args.ID].state=Completed
	}

	return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	// 加锁防止数据竞态
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()
	if m.phase==2{
		ret=true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.files=files
	m.nReduce=nReduce
	m.phase=0
	m.mapTasks=make([]TaskMeta,len(files))

	m.server()
	return &m
}
