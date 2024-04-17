package mr

import (
	"bufio"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Task struct {
	TaskNumber   int
	TaskData     []string
	isComplete   bool
	isInProgress bool
}

type Coordinator struct {
	// Your definitions here.
	isComplete    bool
	completeTasks int
	tasks         []Task
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	for i, task := range c.tasks {
		log.Printf("check task %v", i)
		if !task.isComplete {
			log.Print("found task %v", i)
			reply.TaskNumber = task.TaskNumber
			reply.TaskData = task.TaskData
			task.isInProgress = true
			break
		}
	}
	return nil
}

func (c *Coordinator) ReturnTaskResults(args *ReturnTaskResultsArgs, reply *ReturnTaskResultsReply) error {
	for _, result := range args.Results {
		log.Printf("%s, %s", result.Key, result.Value)
	}
	for i, task := range c.tasks {
		if task.TaskNumber == args.TaskNumber {
			c.tasks[i].isComplete = true
			c.completeTasks += 1
			break
		}
	}
	return nil
}

func (c *Coordinator) CheckDone(args *DoneArgs, reply *DoneReply) error {
	if c.completeTasks+1 == len(c.tasks) {
		reply.IsDone = true
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	log.Printf("hello world")
	// Your code here.

	file, err := os.Open(files[0])
	if err != nil {
		log.Fatalf("failed to open file: %s", err)
	}
	defer file.Close() // Make sure to close the file later

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	// Check for errors during Scan
	if err := scanner.Err(); err != nil {
		log.Fatalf("error during scan: %s", err)
	}

	counter := 0
	for _, line := range lines {
		task := Task{}
		task.TaskData = append(task.TaskData, line)
		task.TaskNumber = counter
		task.isComplete = false
		c.tasks = append(c.tasks, task)
		counter += 1
	}
	c.completeTasks = 0
	for _, task := range c.tasks {
		log.Printf("%v, %s", task.TaskNumber, task.TaskData)
	}
	c.server()
	return &c
}
