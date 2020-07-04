package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	request := NewJobRequest{}
	reply := NewJobReply{}
	for {
		call("Master.WantJob", &request, &reply)

		fmt.Printf("newJobReply.Filename %s\n", reply.Filename)
		fmt.Printf("newJobReply.Type %s\n", reply.Type)

		// empty Filename, no new job
		if reply.Filename == "" {
			time.Sleep(time.Second)
			continue
		}

		if reply.Type == "map" {
			file, err := os.Open(reply.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", reply.Filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			file.Close()

			kva := mapf(reply.Filename, string(content))
			reduceKeyNum := ihash(reply.Filename) % 10

			newFileName := fmt.Sprintf("mr-%d-%d.json", 1, reduceKeyNum)
			mapResultsFile, _ := os.OpenFile(newFileName, os.O_CREATE, os.ModePerm)
			defer mapResultsFile.Close()

			enc := json.NewEncoder(mapResultsFile)
			for _, kv := range kva {
				enc.Encode(&kv)
			}
		} else if reply.Type == "reduce" {

		}
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
