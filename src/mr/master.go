package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.

}

// Job defines map or reduce job
type Job struct {
	Filename string
	Type     string
	ID       int
}

// WantJob workers requests for a job
func (m *Master) WantJob(args *NewJobRequest, reply *NewJobReply) error {
	reply.Filename = "pg-grimm.txt"
	reply.Type = "map"
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.server()
	return &m
}
