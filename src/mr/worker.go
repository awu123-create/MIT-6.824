package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"

import "encoding/json"
import "sort"
import "time"
import "io"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		// 拉任务
		request:=RequestArgs{}
		reply:=RequestReply{}
		ok:=call("Master.AssignTask",&request,&reply)
		if !ok{
			time.Sleep(200*time.Millisecond)
			continue
		}

		// 执行任务
		if reply.Type==MapTask{
			err:=doMapTask(reply.ID,reply.FileName,reply.NumReduce,mapf)
			report(MapTask,reply.ID,err==nil)
		}else if reply.Type==ReduceTask{
			err:=doReduceTask(reply.ID,reply.NumMap,reducef)
			report(ReduceTask,reply.ID,err==nil)
		}else if reply.Type==WaitTask{
			// 等待一段时间
			time.Sleep(time.Second)
		}else{
			// 没有任务了，退出
			break
		}
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()
	
}

func report(taskType TaskType,taskId int,ok bool)  {
	args:=ReportArgs{
		Type:taskType,
		ID:taskId,
		Ok:ok,
	}
	reply:=ReportReply{}
	call("Master.ReportTask",&args,&reply)
}

func doMapTask(taskID int,filename string,nReduce int,mapf func(string, string) []KeyValue) error{
	file,err:=os.Open(filename)
	if err!=nil{
		return err
	}

	content, err := io.ReadAll(file)
	if err!=nil{
		return err
	}
	file.Close()
	kva:=mapf(filename,string(content))

	// 分桶
	buckets:=make([][]KeyValue,nReduce)
	for _,kv:=range kva{
		bucketID:=ihash(kv.Key)%nReduce
		buckets[bucketID]=append(buckets[bucketID],kv)
	}

	// 写入中间文件，合理形式为mr-X-Y，其中X为map任务编号，Y为reduce任务编号
	for i:=0;i<nReduce;i++{
		oname:=fmt.Sprintf("mr-%d-%d",taskID,i)
		tmp,err:=os.CreateTemp(".",fmt.Sprintf("mr-%d-%d-*",taskID,i))
		if err!=nil{
			return err
		}

		enc:=json.NewEncoder(tmp)
		for _,kv:=range buckets[i]{
			if err:=enc.Encode(&kv);err!=nil{
				tmp.Close()
				os.Remove(tmp.Name())
				return err
			}
		}

		if err:=tmp.Close();err!=nil{
			os.Remove(tmp.Name())
			return err
		}

		if err:=os.Rename(tmp.Name(),oname);err!=nil{
			 os.Remove(tmp.Name())
			return err
		}
	}

	return nil
}

func doReduceTask(taskID int,nMap int,reducef func(string, []string) string) error{
	intermediate:=make([]KeyValue,0)
	for i:=0;i<nMap;i++{
		filename:=fmt.Sprintf("mr-%d-%d",i,taskID)
		file,err:=os.Open(filename)
		if err!=nil{
			return err
		}

		dec:=json.NewDecoder(file)
		for{
			var kv KeyValue
			err:=dec.Decode(&kv)
			if err==io.EOF{
				break
			}
			if err!=nil{
				file.Close()
				return err
			}
			intermediate=append(intermediate,kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname:=fmt.Sprintf("mr-out-%d",taskID)
	tmp,err:=os.CreateTemp(".","mr-out-tmp-*")
	if err!=nil{
		return err
	}
	defer os.Remove(tmp.Name())

	i:=0
	for i<len(intermediate){
		j:=i+1
		for j<len(intermediate)&&intermediate[j].Key==intermediate[i].Key{
			j++
		}

		values:=make([]string,0)
		for k:=i;k<j;k++{
			values=append(values,intermediate[k].Value)
		}
		output:=reducef(intermediate[i].Key,values)
		fmt.Fprintf(tmp,"%v %v\n",intermediate[i].Key,output)
		i=j
	}

	if err:=tmp.Close();err!=nil{
		return err
	}
	return os.Rename(tmp.Name(),oname)
}
//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
