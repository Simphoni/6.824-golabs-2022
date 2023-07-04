package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nReduce   int
	jobsDone  int
	files     []string
	status    []int
	timeStamp []time.Time
	threshold time.Duration
	mutex     sync.Mutex
}

func (c *Coordinator) GetUnifiedId(TaskType int, WorkerId int) int {
	if TaskType == 0 {
		return WorkerId
	} else {
		return WorkerId + len(c.files)
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *RequestArgs, reply *RequestReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	reply.NumFiles = len(c.files)
	reply.NumParts = c.nReduce
	reply.Action = 0
	if c.jobsDone == len(c.status) {
		reply.Action = 2
		return nil
	}
	if args.Action == 2 {
		return nil
	}

	if args.Action == 0 {
		var iterRange int
		if c.jobsDone < len(c.files) {
			reply.TaskType = 0
			iterRange = len(c.files)
		} else {
			reply.TaskType = 1
			iterRange = c.nReduce
		}
		curTime := time.Now()
		for j := 0; j < iterRange; j++ {
			id := c.GetUnifiedId(reply.TaskType, j)
			found := false
			if c.status[id] == 0 {
				found = true
			} else if c.status[id] == 1 && curTime.Sub(c.timeStamp[id]) > c.threshold {
				found = true
			}
			if found {
				reply.Action = 1
				if reply.TaskType == 0 {
					reply.FileName = c.files[j]
				}
				reply.WorkerId = j
				c.status[id] = 1
				c.timeStamp[id] = curTime
				return nil
			}
		}
	} else {
		id := c.GetUnifiedId(args.TaskType, args.WorkerId)
		if c.status[id] == 1 {
			c.status[id] = 2
			c.jobsDone++
		}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	ret := false

	// Your code here.
	if c.jobsDone == len(c.status) {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = make([]string, len(files))
	copy(c.files, files)
	// lab-mr: For this lab, have the coordinator wait for ten seconds
	c.threshold = time.Second * 10
	c.nReduce = nReduce
	c.status = make([]int, len(files)+nReduce)
	c.timeStamp = make([]time.Time, len(files)+nReduce)
	for i := range c.status {
		c.status[i] = 0
	}
	c.server()
	return &c
}
