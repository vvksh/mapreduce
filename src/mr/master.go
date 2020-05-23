package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"
)

// Master struct
type Master struct {
	files             []string
	nReduce           int
	mapAssignments    map[int]*TaskInfo
	reduceAssignments map[int]*TaskInfo
	mux               sync.Mutex
	wg                sync.WaitGroup
	done              bool
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
	filenames  []string
	workerID   string
	status     TaskStatus
	assignedAt time.Time
}

var intRegex = regexp.MustCompile("[0-9]+")
var TIMEOUT = time.Second * 10

// AssignTask assigns task to worker
func (m *Master) AssignTask(taskRequest *TaskRequest, taskAssignment *TaskAssignment) error {

	// only one goroutine should be able to access the master data structure at a time
	m.mux.Lock()
	defer m.mux.Unlock()

	//see if there are any idle map tasks available
	for taskID, taskInfo := range m.mapAssignments {
		// log.Printf("looking for idle MAP task %v\n", taskInfo)
		if taskInfo.status == IDLE || taskInfo.status == FAILED {
			taskInfo.workerID = taskRequest.WorkerID
			taskInfo.status = IN_PROGRESS
			taskInfo.assignedAt = time.Now()
			taskAssignment.TaskID = taskID
			taskAssignment.Filenames = taskInfo.filenames
			taskAssignment.Type = MAP
			taskAssignment.NReduce = m.nReduce
			log.Printf("MAP task assigned: taskId: %d to worker: %s ", taskAssignment.TaskID, taskInfo.workerID)
			return nil
		}
	}

	// if all map tasks done, look for any reduce tasks
	if m.mapDone() {
		for taskID, taskInfo := range m.reduceAssignments {
			// log.Printf("looking for idle REDUCE task %v\n", taskInfo)
			if taskInfo.status == IDLE || taskInfo.status == FAILED {
				taskInfo.workerID = taskRequest.WorkerID
				taskInfo.status = IN_PROGRESS
				taskInfo.assignedAt = time.Now()
				taskAssignment.TaskID = taskID
				taskAssignment.Filenames = taskInfo.filenames
				taskAssignment.Type = REDUCE
				taskAssignment.NReduce = m.nReduce
				log.Printf("REDUCE task assigned: taskId: %d to worker: %s ", taskAssignment.TaskID, taskInfo.workerID)
				return nil
			}
		}
	}

	//No tasks for now
	return nil
}

// TaskDone handles call from worker when worker is done with task
func (m *Master) TaskDone(taskDoneNotification *TaskDoneNotification, taskDoneAck *TaskDoneAck) error {
	// if map tasks, mark it done and create a reduce task
	if taskDoneNotification.Type == MAP {
		log.Printf("Received MAP DONE notification from worker %s for taskID: %d", taskDoneNotification.WorkerID, taskDoneNotification.TaskID)
		taskInfo := m.mapAssignments[taskDoneNotification.TaskID]

		// Ignore if map task already done
		m.mux.Lock()
		if taskInfo.status != DONE {
			taskInfo.status = DONE
			m.updateReduceTasks(taskDoneNotification.Filenames)
		} else {
			log.Printf("Received MAP done notification for a task: %v already marked done, ignoring it\n", taskDoneNotification)
		}
		taskDoneAck.Ack = true
		m.mux.Unlock()
	} else {
		// for reduce tasks
		taskInfo := m.reduceAssignments[taskDoneNotification.TaskID]

		// No need to handle if reduce task already done
		m.mux.Lock()
		taskInfo.status = DONE
		m.mux.Unlock()
		log.Printf("Marked REDUCE done for taskId %d by workerId %s \n", taskDoneNotification.TaskID, taskDoneNotification.WorkerID)
		taskDoneAck.Ack = true
	}
	return nil
}

//
// updateReduceTasks takes bunch of intermediate files and creates or updates reduce tasks,
// the caller is expectd to get locks on master data structure
//
func (m *Master) updateReduceTasks(mapIntermediateFiles []string) {
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
// monitorInProgressTasks monitors the tasks and marked them FAILED if not marked DONE within 10 secs
// acquires lock on master data structure while it checks the task state and marks them FAILED if necessary
//
func (m *Master) monitorInProgressTasks() {
	for {
		m.mux.Lock()
		if m.done {
			break
		}

		currentTime := time.Now()
		for taskID, taskInfo := range m.mapAssignments {
			if taskInfo.status == IN_PROGRESS {
				if currentTime.Sub(taskInfo.assignedAt).Seconds() > TIMEOUT.Seconds() {
					// if not marked done within timeout sec, mark it IDLE so that it can be assigned to another worker
					log.Printf("Found a stuck map task : {taskID: %d workerid: %s}, marking it FAILED\n", taskID, taskInfo.workerID)
					taskInfo.status = FAILED
				}
			}
		}

		for taskID, taskInfo := range m.reduceAssignments {
			if taskInfo.status == IN_PROGRESS {
				if currentTime.Sub(taskInfo.assignedAt).Seconds() > TIMEOUT.Seconds() {
					// if not marked done within timeout sec, mark it IDLE so that it can be assigned to another worker
					log.Printf("Found a stuck reduce task : {taskID: %d workerid: %s}, marking it FAILED\n", taskID, taskInfo.workerID)
					taskInfo.status = FAILED
				}
			}
		}
		m.mux.Unlock()
		time.Sleep(time.Second * 10)
	}
	m.mux.Unlock()
	// decrement waitGroup
	m.wg.Done()
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
	allDone := m.mapDone() && m.reduceDone()
	if allDone {
		// mark done true and wait for monitoring routine to be done
		m.done = true
		m.mux.Unlock()
		m.wg.Wait()
		return true
	} else {
		m.mux.Unlock()
		return false
	}
}

// Returns if all map tasks done, the caller is expected to get locks on master data structure
func (m *Master) mapDone() bool {
	for _, mapAssignment := range m.mapAssignments {
		if mapAssignment.status != DONE {
			// log.Printf("REDUCE tasks not done \n %v\n", reduceAssignment)
			return false
		}
	}
	return true
}

// Returns if all reduce tasks done, the caller is expected to get locks on master data structure
func (m *Master) reduceDone() bool {
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

	// monitoring task will run till m.done is false
	m.done = false

	// start a monitoring thread
	m.wg.Add(1)
	go m.monitorInProgressTasks()
	return &m
}

func createReduceTask(filename string) *TaskInfo {
	taskInfo := TaskInfo{}
	taskInfo.status = IDLE
	taskInfo.filenames = []string{filename}
	return &taskInfo
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
