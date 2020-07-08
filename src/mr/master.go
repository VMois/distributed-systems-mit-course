package mr

import (
	"log"
	"net"
	"os"
	"fmt"
	"net/rpc"
	"net/http"
	"sync"
)

type Master struct {
	mux sync.Mutex

	Tasks []Task
	NReduce int
}

// Task defines map or reduce operation
type Task struct {
	Filename string
	State    string  // "idle", "in-progress" or "completed"; according to MapReduce paper
	Type     string  // "map" or "reduce"
	Id       int
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
			m.Tasks[i].State = "in-progess"
			break
		}
	}
	m.mux.Unlock()
	return nil
}

// TaskDone worker calls this when map/reduce task is finished
func (m *Master) TaskDone(args *JobDoneRequest, reply *Empty) error {
	mapDone := true

	m.mux.Lock()
	m.Tasks[args.Id].State = "completed"

	fmt.Printf("Finished: %s\n", m.Tasks[args.Id].Type)

	if m.Tasks[args.Id].Type == "map" {
		// check if last map is finished
		for _, task := range m.Tasks {
			if task.Type == "map" && task.State != "completed" {
				mapDone = false
			}
		}

		if mapDone {
			fmt.Printf("Map phase done. Start reduce\n")
			for i := 0; i < m.NReduce; i++ {
				m.Tasks = append(m.Tasks, Task{"dymmy.txt", "idle", "reduce", i})
			}
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
	finishedReduceTasks := m.NReduce

	m.mux.Lock()
	for _, task := range m.Tasks {
		if task.State == "completed" && task.Type == "reduce" {
			finishedReduceTasks--
		}
	}
	m.mux.Unlock()

	fmt.Printf("Unfinished reduce tasks: %d\n", finishedReduceTasks)

	return (finishedReduceTasks == 0)
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.NReduce = nReduce
	for i, filename := range files {
		m.Tasks = append(m.Tasks, Task{filename, "idle", "map", i})
	}

	m.server()
	return &m
}
