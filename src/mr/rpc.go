package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

type RequestTaskArgs struct {
	Text string
}

type RequestTaskReply struct {
	TaskType   string
	TaskNumber int
	TaskData   []string
}

type ReturnTaskResultsArgs struct {
	TaskNumber int
	TaskType   string
	Results    []string
}
type ReturnTaskResultsReply struct {
	Value string
}

type DoneArgs struct {
}

type DoneReply struct {
	IsDone bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
