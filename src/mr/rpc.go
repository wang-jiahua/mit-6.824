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

type Args struct {
	X int
}

type Reply struct {
	Y int
}

type Task struct {
	taskType   TaskType
	inputfiles []string
	status     Status
	id         int
}

type TaskType int

const (
	Map = iota
	Reduce
	Wait
	Exit
)

type Status int

const (
	Ready = iota
	Running
	Done
)

type Assign struct {
	TaskType   TaskType
	Inputfiles []string
	ReduceNum  int
	ID         int
}

type Report struct {
	TaskType    TaskType
	Outputfiles []string
	ID          int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
