package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Empty empty structure
type Empty struct {}

// NewJobReply get filename to process
type NewJobReply struct {
	Filename string
	Type     string // "map" or "reduce"
	NReduce  int
	Id       int
}

// JobDoneRequest if job is done return job id
type JobDoneRequest struct {
	Id       int
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
