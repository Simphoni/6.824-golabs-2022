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

// Add your RPC definitions here.
type RequestArgs struct {
	Action   int // 0 - ask for work, 1 - done, 2 - get config
	TaskType int // 0 - Map, 1 - Reduce
	WorkerId int
}

type RequestReply struct {
	Action   int // 0 - idle, 1 - work assigned, 2 - exit
	TaskType int // 0 - Map, 1 - Reduce
	WorkerId int
	NumFiles int
	NumParts int
	FileName string
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
