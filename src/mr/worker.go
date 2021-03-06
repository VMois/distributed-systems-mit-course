package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		empty := Empty{}
		request := JobDoneRequest{}
		reply := NewJobReply{}

		// if something goes wrong with connection, worker exits
		if !call("Master.WantTask", &empty, &reply) {
			break
		}

		// empty Filename, no Task
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

			var reduceKeyNum int
			var outputFileName string
			nameToFile := make(map[string]*os.File)

			for _, kv := range kva {
				reduceKeyNum = ihash(kv.Key) % reply.NReduce
				outputFileName = fmt.Sprintf("mr-%d-%d.json", reply.Id, reduceKeyNum)
				outputFile, exists := nameToFile[outputFileName]
				if !exists {
					outputFile, _ = os.Create(outputFileName)
					nameToFile[outputFileName] = outputFile
				}
				enc := json.NewEncoder(outputFile)
				enc.Encode(&kv)
			}

			// atomically move temp files
			for _, outputFile := range nameToFile {
				outputFile.Close()
			}
		} else if reply.Type == "reduce" {
			pattern := fmt.Sprintf("mr-*-%d.json", reply.NReduce)
			matches, err := filepath.Glob(pattern)

			if err != nil {
				fmt.Println(err)
			}

			var intermediate []KeyValue

			// read all intermediate data
			for _, filename := range matches {
				intermediateFile, _ := os.Open(filename)

				dec := json.NewDecoder(intermediateFile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				intermediateFile.Close()
			}

			sort.Sort(ByKey(intermediate))

			outputFileName := fmt.Sprintf("mr-out-%d", reply.NReduce)
			outputFile, _ := os.Create(outputFileName)
			defer outputFile.Close()

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			// remove intermediate files
			for _, filename := range matches {
				os.Remove(filename)
			}
		}

		request.Id = reply.Id
		call("Master.TaskDone", &request, &empty)
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

func moveFile(sourcePath, destPath string) error {
	inputFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("Couldn't open source file: %s", err)
	}
	outputFile, err := os.Create(destPath)
	if err != nil {
		inputFile.Close()
		return fmt.Errorf("Couldn't open dest file: %s", err)
	}
	defer outputFile.Close()
	_, err = io.Copy(outputFile, inputFile)
	inputFile.Close()
	if err != nil {
		return fmt.Errorf("Writing to output file failed: %s", err)
	}
	// The copy was successful, so now delete the original file
	err = os.Remove(sourcePath)
	if err != nil {
		return fmt.Errorf("Failed removing original file: %s", err)
	}
	return nil
}
