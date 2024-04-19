package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
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
	completeMapTasks    int
	completeReduceTasks int
	mapTasks            []Task
	reduceTasks         []Task
	mutex               sync.Mutex

	allMapComplete    bool
	allReduceComplete bool
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.allMapComplete {
		for i, task := range c.mapTasks {
			if !task.isComplete && task.TaskStatus != "In Progress" {
				reply.TaskNumber = task.TaskNumber
				reply.TaskData = task.TaskData
				c.mapTasks[i].TaskStatus = "In Progress"
				reply.TaskType = "Map"
				break
			}
		}
	}
	if c.allMapComplete && !c.allReduceComplete {
		for i, task := range c.reduceTasks {
			if !task.isComplete && task.TaskStatus != "In Progress" {
				reply.TaskNumber = task.TaskNumber
				reply.TaskType = "Reduce"
				reply.TaskData = task.TaskData
				c.reduceTasks[i].TaskStatus = "In Progress"
				task.TaskStatus = "In Progress"
				break
			}
		}
	}
	if c.allMapComplete && c.allReduceComplete {
		reply.TaskType = "Sleep"
	}
	return nil
}

func (c *Coordinator) ReturnTaskResults(args *ReturnTaskResultsArgs, reply *ReturnTaskResultsReply) error {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if args.TaskType == "Map" {
		for i, task := range c.mapTasks {
			if task.TaskNumber == args.TaskNumber {

				c.mapTasks[i].isComplete = true
				c.mapTasks[i].TaskStatus = "Complete"
				c.completeMapTasks += 1
				if c.completeMapTasks == len(c.mapTasks) {
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
				c.reduceTasks[i].TaskStatus = "Complete"
				c.completeReduceTasks += 1
				if c.completeReduceTasks == len(c.reduceTasks) {
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

	// Your code here.

	return c.allMapComplete && c.allReduceComplete
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

	c.mutex = sync.Mutex{}
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
			TaskData:   generateInputFiles(i, len(files)),
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
