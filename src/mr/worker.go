package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	done := 0
	// Your worker implementation here.

	for done < 10 {

		// uncomment to send the Example RPC to the coordinator.
		task, _ := CallRequestTask()
		log.Printf("task %v: %s", task.TaskNumber, task.TaskData[0])
		results := mapf("", strings.Join(task.TaskData, " "))

		CallReturnTaskResults(task.TaskNumber, results)
		if CallDone() {
			done = 10
		}
	}

}

func CallDone() bool {
	args := DoneArgs{}
	reply := DoneReply{}
	ok := call("Coordinator.CheckDone", &args, &reply)
	if ok {
		return reply.IsDone
	} else {
		return true
	}
}

func CallRequestTask() (RequestTaskReply, error) {
	args := RequestTaskArgs{}
	args.Text = "test"
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		return reply, nil
	} else {
		return RequestTaskReply{}, fmt.Errorf("no strings returned")
	}
}

func CallReturnTaskResults(taskNumber int, results []KeyValue) {

	args := ReturnTaskResultsArgs{}
	args.Results = results
	args.TaskNumber = taskNumber
	reply := ReturnTaskResultsReply{}
	ok := call("Coordinator.ReturnTaskResults", &args, &reply)
	if ok {
		log.Printf("%s", reply.Value)
	} else {
		log.Printf("error")
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
