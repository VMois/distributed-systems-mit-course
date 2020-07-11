package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

// Master master node internal state, logically represent a single MapReduce job
type Master struct {
	mux sync.Mutex

	mapTasksNum    int
	reduceTasksNum int
	Tasks          []Task
	NReduce        int
	ttl            int
}

// Task defines map or reduce operation
type Task struct {
	Filename string
	State    string // "idle", "in-progress" or "completed"; according to MapReduce paper
	Type     string // "map" or "reduce"
	Id       int
	ttl      int // time to live
}

// WantTask worker requests for a job
func (m *Master) WantTask(args *Empty, reply *NewJobReply) error {
	// default values if no Task to process
	reply.Filename = ""
	reply.Type = ""

	// TODO: maybe scope for Lock is too big, leaving it here for now
	m.mux.Lock()
	for i, task := range m.Tasks {
		if task.State == "idle" {
			reply.Filename = task.Filename
			reply.Type = task.Type
			reply.NReduce = m.NReduce
			if task.Type == "reduce" {
				reply.NReduce = task.Id
			}
			reply.Id = i
			m.Tasks[i].State = "in-progress"
			break
		}
	}
	m.mux.Unlock()
	return nil
}

// TaskDone worker calls this when map/reduce task is finished
func (m *Master) TaskDone(args *JobDoneRequest, reply *Empty) error {
	m.mux.Lock()
	m.Tasks[args.Id].State = "completed"

	if m.Tasks[args.Id].Type == "map" {
		m.mapTasksNum--
	}

	if m.Tasks[args.Id].Type == "reduce" {
		m.reduceTasksNum--
	}

	// all map tasks are finished, create reduce tasks
	if m.mapTasksNum == 0 && m.reduceTasksNum == m.NReduce {
		for i := 0; i < m.NReduce; i++ {
			m.Tasks = append(m.Tasks, Task{"dymmy.txt", "idle", "reduce", i, m.ttl})
		}
	}

	m.mux.Unlock()
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

	m.mux.Lock()
	isDone := m.mapTasksNum == 0 && m.reduceTasksNum == 0

	for i, task := range m.Tasks {
		if task.State == "in-progress" {
			m.Tasks[i].ttl--
		}

		if task.State == "in-progress" && task.ttl == 0 {
			m.Tasks[i].State = "idle"
			m.Tasks[i].ttl = m.ttl
			isDone = false
		}
	}

	m.mux.Unlock()

	return isDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.NReduce = nReduce
	m.mapTasksNum = len(files)
	m.reduceTasksNum = nReduce
	m.ttl = 10 // 10 seconds (Done is called every second)

	for i, filename := range files {
		m.Tasks = append(m.Tasks, Task{filename, "idle", "map", i, m.ttl})
	}

	m.server()
	return &m
}
