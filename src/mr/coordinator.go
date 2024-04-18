package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Task struct {
	TaskType   string
	TaskNumber int
	TaskData   []string
	TaskStatus string
	isComplete bool
}

type Coordinator struct {
	// Your definitions here.
	isComplete          bool
	completeMapTasks    int
	completeReduceTasks int
	mapTasks            []Task
	reduceTasks         []Task

	allMapComplete    bool
	allReduceComplete bool
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
	if !c.allMapComplete {
		for i, task := range c.mapTasks {
			if !task.isComplete && task.TaskStatus != "In Progress" {
				log.Printf("found task %v", i)
				reply.TaskNumber = task.TaskNumber
				reply.TaskData = task.TaskData
				task.TaskStatus = "In Progress"
				reply.TaskType = "Map"
				break
			}
		}
	}
	if c.allMapComplete && !c.allReduceComplete {
		for i, task := range c.reduceTasks {
			if !task.isComplete && task.TaskStatus != "In Progress" {
				log.Printf("found reduce task %v", i)
				reply.TaskNumber = task.TaskNumber
				reply.TaskType = "Reduce"
				reply.TaskData = task.TaskData
				task.TaskStatus = "In Progress"
				break
			}
		}
	}
	return nil
}

func (c *Coordinator) ReturnTaskResults(args *ReturnTaskResultsArgs, reply *ReturnTaskResultsReply) error {
	log.Printf("return task %v", args)
	if args.TaskType == "Map" {
		for i, task := range c.mapTasks {
			if task.TaskNumber == args.TaskNumber {

				c.mapTasks[i].isComplete = true
				c.completeMapTasks += 1
				if c.completeMapTasks+1 == len(c.mapTasks) {
					c.allMapComplete = true
				}
				break
			}
		}
	}
	if args.TaskType == "Reduce" {
		for i, task := range c.reduceTasks {
			if task.TaskNumber == args.TaskNumber {
				c.reduceTasks[i].isComplete = true
				c.completeReduceTasks += 1
				if c.completeReduceTasks+1 == len(c.reduceTasks) {
					c.allReduceComplete = true
				}
				break
			}
		}
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
	if c.completeMapTasks+1 == len(c.mapTasks) {
		if c.completeReduceTasks+1 == len(c.reduceTasks) {
			ret = true
		}
	}

	// Your code here.

	return ret
}

func generateInputFiles(i int, file int) []string {
	var inputFiles []string

	for j := 0; j < file; j++ {
		inputFiles = append(inputFiles, fmt.Sprintf("mr-%d-%d", j, i))
	}

	return inputFiles
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	counter := 0
	for _, file := range files {
		task := Task{
			TaskType:   "Map",
			TaskData:   []string{file},
			TaskNumber: counter,
			TaskStatus: "Not Started",
			isComplete: false,
		}
		task.isComplete = false
		c.mapTasks = append(c.mapTasks, task)
		counter += 1
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, Task{
			TaskType:   "Reduce",
			TaskData:   generateInputFiles(i, len(files)-1),
			TaskNumber: i,
			TaskStatus: "Not Started",
			isComplete: false,
		})
	}
	c.completeMapTasks = 0
	c.completeReduceTasks = 0
	c.server()
	return &c
}
