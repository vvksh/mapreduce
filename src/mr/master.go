package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

// Master struct
type Master struct {
	files             []string
	nReduce           int
	mapAssignments    map[int]*TaskInfo
	reduceAssignments map[int]*TaskInfo
}

// TaskStatus : stages of tasks
type TaskStatus string

const (
	IDLE        TaskStatus = "IDLE"
	IN_PROGRESS            = "IN_PROGRESS"
	FAILED                 = "FAILED"
)

type TaskInfo struct {
	filename string
	workerID string
	status   TaskStatus
}

// AssignTask assigns task to worker
func (m *Master) AssignTask(taskRequest *TaskRequest, taskAssignment *TaskAssignment) error {
	//see if there are any idle map tasks available
	for taskID, taskInfo := range m.mapAssignments {
		log.Printf("looking for idle task %v\n", taskInfo)
		if taskInfo.status == IDLE {
			taskInfo.workerID = taskRequest.WorkerID
			taskInfo.status = IN_PROGRESS
			taskAssignment.TaskID = taskID
			taskAssignment.Filename = taskInfo.filename
			taskAssignment.Type = MAP
			taskAssignment.NReduce = 1
			log.Printf("task assignment: %v", taskAssignment)
			return nil
		}
	}
	return errors.New("No idle tasks to assign")
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
// MakeMaster create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.files = files
	m.nReduce = nReduce
	m.mapAssignments = make(map[int]*TaskInfo)
	for i, filename := range files {
		mapTaskInfo := TaskInfo{}
		mapTaskInfo.filename = filename
		mapTaskInfo.status = IDLE
		m.mapAssignments[i+1] = &mapTaskInfo
	}

	m.reduceAssignments = make(map[int]*TaskInfo)
	m.server()
	return &m
}
