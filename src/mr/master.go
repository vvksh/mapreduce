package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
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
	DONE                   = "DONE"
)

type TaskInfo struct {
	filenames []string
	workerID  string
	status    TaskStatus
}

var intRegex = regexp.MustCompile("[0-9]+")

// AssignTask assigns task to worker
func (m *Master) AssignTask(taskRequest *TaskRequest, taskAssignment *TaskAssignment) error {
	//see if there are any idle map tasks available
	for taskID, taskInfo := range m.mapAssignments {
		log.Printf("looking for idle MAP task %v\n", taskInfo)
		if taskInfo.status == IDLE {
			taskInfo.workerID = taskRequest.WorkerID
			taskInfo.status = IN_PROGRESS
			taskAssignment.TaskID = taskID
			taskAssignment.Filenames = taskInfo.filenames
			taskAssignment.Type = MAP
			taskAssignment.NReduce = m.nReduce
			log.Printf("task assignment: %v", taskAssignment)
			return nil
		}
	}

	// if no map assignements, look for any reduce tasks
	for taskID, taskInfo := range m.reduceAssignments {
		log.Printf("looking for idle REDUCE task %v\n", taskInfo)
		if taskInfo.status == IDLE {
			taskInfo.workerID = taskRequest.WorkerID
			taskInfo.status = IN_PROGRESS
			taskAssignment.TaskID = taskID
			taskAssignment.Filenames = taskInfo.filenames
			taskAssignment.Type = REDUCE
			taskAssignment.NReduce = m.nReduce
			log.Printf("task assignment: %v", taskAssignment)
			return nil
		}
	}

	return errors.New("No idle tasks to assign")
}

// TaskDone handles call from worker when worker is done with task
func (m *Master) TaskDone(taskDoneNotification *TaskDoneNotification, taskDoneAck *TaskDoneAck) error {
	// if map tasks, mark it done and create a reduce task
	if taskDoneNotification.Type == MAP {
		log.Printf("Received MAP DONE notification from worker")
		taskInfo := m.mapAssignments[taskDoneNotification.TaskID]
		taskInfo.status = DONE
		m.UpdateReduceTasks(taskDoneNotification.Filenames)

		//remove map task
		delete(m.mapAssignments, taskDoneNotification.TaskID)

		log.Printf("Updated a REDUCE task \n")
		taskDoneAck.Ack = true
	} else {
		// for reduce tasks
		log.Printf("Received REDUCE DONE notification from worker")
		taskInfo := m.reduceAssignments[taskDoneNotification.TaskID]
		taskInfo.status = DONE
		log.Printf("Output reduce tasks: %v", taskDoneNotification.Filenames)
		// m.reduceAssignments[taskDoneNotification.TaskID] = createReduceTask(taskDoneNotification.Filenames)
		log.Printf("Marked REDUCE task %v done\n", m.reduceAssignments[taskDoneNotification.TaskID])
		taskDoneAck.Ack = true

	}
	return nil
}

// UpdateReduceTasks takes bunch of intermediate files and creates or updates reduce tasks
func (m *Master) UpdateReduceTasks(mapIntermediateFiles []string) {
	for _, mapIntermediateFile := range mapIntermediateFiles {
		reduceTaskID := getReduceTaskID(mapIntermediateFile)
		// if there is already a reduce task, append filename to that reduce task
		if reduceTask, ok := m.reduceAssignments[reduceTaskID]; ok {
			reduceTask.filenames = append(reduceTask.filenames, mapIntermediateFile)
		} else {
			m.reduceAssignments[reduceTaskID] = createReduceTask(mapIntermediateFile)
		}
	}

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

	//check if all map tasks marked done and removed
	if len(m.mapAssignments) != 0 {
		// log.Printf("Map tasks not done\n")
		return false
	}

	for _, reduceAssignment := range m.reduceAssignments {
		if reduceAssignment.status != DONE {
			// log.Printf("REDUCE tasks not done \n %v\n", reduceAssignment)
			return false
		}
	}

	return true
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
		mapTaskInfo.filenames = []string{filename}
		mapTaskInfo.status = IDLE
		m.mapAssignments[i+1] = &mapTaskInfo
	}

	m.reduceAssignments = make(map[int]*TaskInfo)
	m.server()
	return &m
}

func createReduceTask(filename string) *TaskInfo {
	taskInfo := TaskInfo{}
	taskInfo.status = IDLE
	taskInfo.filenames = []string{filename}
	return &taskInfo
	// m.reduceAssignments[taskID] = &taskInfo
}

func getReduceTaskID(filename string) int {
	//TODO find a better way
	matches := intRegex.FindAllString(filename, -1)
	if len(matches) == 2 {
		reduceTaskID, _ := strconv.Atoi(matches[1])
		return reduceTaskID

	}
	log.Panicf("couldn't parse reduce task id from filename %v\n", filename)
	return -1
}
