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

const (
	taskTypeJobDone = iota - 1
	taskTypeEmpty
	taskTypeMap
	taskTypeReduce
)

const (
	taskStatusIdle = iota
	taskStatusBusy
	taskStatusFailed
	taskStatusComplete
)

type Task struct {
	InputFiles  []string
	OutputFiles map[int]string
	TaskType    int
	TaskNum     int
	Status      int
	NReduce     int
	Version     int
}

type EmptyArgs struct {
}

var emptyArgs = &EmptyArgs{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
