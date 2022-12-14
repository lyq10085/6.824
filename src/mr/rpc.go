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

const (
	MAP = iota
	REDUCE
	WAIT
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
type AssignArgs struct {
	Tmp bool // todo
}

type AssignReply struct {
	Filename          string   // input file for map task
	Intermediatefiles []string // input intermedate file for reduce task
	Taskid            int
	NReduce           int //  key for reduce task
	MapOrReduceOrWait int // map 0; reduce 1; wait 2
}

type AccomplishArgs struct {
	MapOrReduce bool
	Taskid      int // task id
	// intermediate files ?
	Intermediatefiles []interfile
}

type AccomplishReply struct {
	Tmp bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	// fmt.Println(s)
	return s
}
