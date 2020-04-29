package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"

	"github.com/google/uuid"
)

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var workerID string

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerID = uuid.New().String()

	for {
		task, err := GetTask()
		if err != nil {
			log.Println("Got error response from master, quitting")
			break
		}
		log.Printf("Got task %v\n", task)
		if task.Type == MAP {
			runMapper(mapf, task)
		} else {
			runReducer(reducef, task)
		}
		time.Sleep(5 * time.Second)
	}

}

func runMapper(mapf func(string, string) []KeyValue, task *TaskAssignment) {
	log.Println("going to run mapper")

	taskID := task.TaskID
	filename := task.Filename
	log.Printf("Handling mapping task %d filename: %s\n", taskID, filename)
	content, err := readFile(filename)

	if err != nil {
		log.Printf("Couldn't read from %s\n", filename)
	}

	//call map function
	intermediatekeyValues := mapf(filename, content)

	log.Printf("Got intermediate values")

	sort.Sort(ByKey(intermediatekeyValues))

	encoders := make(map[int]*json.Encoder)

	//create intermediate files
	for i := 0; i < task.NReduce; i++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d.txt", taskID, i)
		emptyFile, err := os.Create(intermediateFileName)
		if err != nil {
			log.Printf("Couldn't create empty file %s\n", intermediateFileName)
		}
		enc := json.NewEncoder(emptyFile)
		encoders[i] = enc
	}

	//create numReduce files
	for _, kv := range intermediatekeyValues {
		err := encoders[ihash(kv.Key)%task.NReduce].Encode(&kv)
		if err != nil {
			log.Printf("Couldn't encode %v\n", &kv)
		}
	}
}

func runReducer(reducef func(string, []string) string, task *TaskAssignment) {
	log.Println("going to run reducer")

	taskID := task.TaskID
	filename := task.Filename
	//read file and run reduction
	log.Printf("now running reduce for taskId: %d and filename %s\n", taskID, filename)

}

// GetTask connects to master and gets task assignment */
func GetTask() (*TaskAssignment, error) {
	taskRequest := TaskRequest{}
	taskRequest.WorkerID = workerID
	taskAssignment := TaskAssignment{}
	if call("Master.AssignTask", &taskRequest, &taskAssignment) {
		log.Printf("got assignment file %v\n", taskAssignment.Filename)
		return &taskAssignment, nil
	}
	return nil, errors.New("Couldn't connect to master")

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
