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

type TaskType string

const (
	MAP    TaskType = "MAP"
	REDUCE          = "REDUCE"
)

type TaskRequest struct {
	WorkerID string
}

type TaskAssignment struct {
	TaskID   int
	Type     TaskType
	Filename string
	NReduce  int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
