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

var mutex sync.Mutex

type Coordinator struct {
	readyChan     chan *Task // to store tasks ready to be assigned
	doneChan      chan *Task // to store tasks done
	phase         Phase      // either MapPhase or ReducePhase or End
	tasks         []Task     // all tasks
	nMap          int        // the number of map tasks
	nReduce       int        // the number of reduce tasks
	intermediates [][]string // map procedure's output, reduce procedure's input
}

type Phase int

const (
	MapPhase = iota
	ReducePhase
	End
)

//
// AssignTask
// assign a task to a worker via RPC.
//
func (c *Coordinator) AssignTask(args *Args, reply *Assign) error {
	// log.Println("AssignTask begin")
	mutex.Lock()
	// log.Println("AssignTask lock")
	switch c.phase {
	case MapPhase:
		c.retrieveTask(reply)
	case ReducePhase:
		c.retrieveTask(reply)
	case End:
		reply.TaskType = Exit
	default:
		panic("unreachable")
	}
	// log.Println("AssignTask reply", reply)
	// log.Println("AssignTask end")
	// log.Println("AssignTask unlock")
	mutex.Unlock()
	return nil
}

//
// retrieveTask
// retrieve a task from the ready queue
//
func (c *Coordinator) retrieveTask(reply *Assign) {
	// log.Println("")
	// log.Println("retrieveTask begin")
	var task *Task
	if len(c.readyChan) > 0 {
		// log.Println("has unstarted tasks yet")
		// has unstarted tasks yet
		task = <-c.readyChan
		task.status = Running
		c.tasks[task.id].status = Running
		// log.Println("retrieveTask task            ", task)
		// log.Println("retrieveTask c.tasks[task.id]", c.tasks[task.id])
		go c.countdown(task)
	} else {
		// has no unstarted tasks yet
		task = &Task{}
		task.taskType = Wait
	}
	reply.TaskType = task.taskType
	reply.Inputfiles = task.inputfiles
	reply.ReduceNum = c.nReduce
	reply.ID = task.id
	// log.Println("reply:", reply)
	// log.Println("retrieveTask end")
	// log.Println("")
}

//
// countdown
// wait for several seconds and recycle the assigned task to readyChan
//
func (c *Coordinator) countdown(task *Task) {
	time.Sleep(20 * time.Second)
	mutex.Lock()
	// log.Println("")
	// log.Println("countdown lock")
	if c.tasks[task.id].status == Running {
		// log.Println("countdown recycle task", task)
		// log.Println("countdown recycle c.tasks[task.id]", c.tasks[task.id])
		c.tasks[task.id].status = Ready
		task.status = Ready
		// c.readyChan <- task
		c.readyChan <- &c.tasks[task.id]
	}
	// log.Println("countdown unlock")
	// log.Println("")
	mutex.Unlock()
}

//
// MarkDone
// mark a task done, if all tasks are done, move to the next phase
//
func (c *Coordinator) MarkDone(args *Report, reply *Reply) error {
	// log.Println("")
	// log.Println("MarkDone begin")
	// log.Println("MarkDone args", args)
	mutex.Lock()
	// log.Println("MarkDone lock")
	task := &c.tasks[args.ID]
	// log.Println("MarkDone task before update", task)
	// log.Println("MarkDone c.tasks[args.ID] before update", c.tasks[args.ID])
	if task.status == Done {
		// log.Println("MarkDone the task is already done")
		mutex.Unlock()
		return nil
	}
	task.status = Done
	c.tasks[args.ID].status = Done
	// log.Println("MarkDone task after update", task)
	// log.Println("MarkDone c.tasks[args.ID] after update", c.tasks[args.ID])
	c.doneChan <- task
	// log.Println("MarkDone len(c.doneChan)", len(c.doneChan))
	// log.Println("MarkDone task", task)
	// log.Println("MarkDone c.phase", c.phase)

	taskType := args.TaskType

	if taskType == Map && c.phase == MapPhase {
		// log.Println("MarkDone map begin")
		for _, file := range args.Outputfiles {
			mapID := args.ID
			c.intermediates[mapID] = append(c.intermediates[mapID], file)
		}
		if len(c.doneChan) == c.nMap {
			// log.Println("MarkDone map -> reduce")
			c.clearDone()
			c.prepReduce()
			c.phase = ReducePhase
		}
		// log.Println("MarkDone map end")
	}

	if taskType == Reduce && c.phase == ReducePhase && len(c.doneChan) == c.nReduce {
		// log.Println("MarkDone reduce -> end")
		c.phase = End
	}
	// log.Println("MarkDone unlock")
	mutex.Unlock()
	// log.Println("MarkDone end")
	// log.Println("")
	return nil
}

//
// server
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	// log.Println("server starting")
	err := rpc.Register(c)
	if err != nil {
		return
	}
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	err = os.Remove(sockname)
	if err != nil {
		return
	}
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		err := http.Serve(l, nil)
		if err != nil {

		}
	}()
}

//
// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	mutex.Lock()
	defer mutex.Unlock()
	// log.Println(c.phase == End)
	return c.phase == End
}

//
// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// log.Println("making coordinator")
	c.nMap = len(files)
	c.nReduce = nReduce
	capacity := c.nMap
	if c.nReduce > capacity {
		capacity = c.nReduce
	}
	c.readyChan = make(chan *Task, capacity)
	c.doneChan = make(chan *Task, capacity)

	for i := 0; i < c.nMap; i++ {
		c.intermediates = append(c.intermediates, []string{})
	}

	c.prepMap(files)

	c.server()
	return &c
}

//
// prepMap
// fill the readyChan with map tasks
//
func (c *Coordinator) prepMap(files []string) {
	// log.Println("prepMap begin")
	for i, file := range files {
		// log.Println("file: ", file)
		task := Task{Map, []string{file}, Ready, i}
		// log.Println("task: ", task)
		// log.Println(&task)
		c.tasks = append(c.tasks, task)
		// log.Println(c.readyChan)
		// log.Println(len(c.readyChan))
		// log.Println(cap(c.readyChan))
		c.readyChan <- &task
		// log.Println("preparing Map")
	}
	// log.Println("prepMap end")
}

//
// prepReduce
// fill the readyChan with reduce tasks
//
func (c *Coordinator) prepReduce() {
	// log.Println("")
	// log.Println("prepReduce begin")
	// log.Println("c.intermediates: ", c.intermediates)
	c.tasks = []Task{}
	for i := 0; i < c.nReduce; i++ {
		var files []string
		for _, intermediate := range c.intermediates {
			if len(intermediate) > 0 {
				// log.Println("intermediate: ", intermediate)
				files = append(files, intermediate[i])
			}

		}
		task := Task{Reduce, files, Ready, i}
		c.tasks = append(c.tasks, task)
		c.readyChan <- &task
	}
	// log.Println("prepReduce end")
	// log.Println("")
}

//
// clearDone
// clear the doneChan
//
func (c *Coordinator) clearDone() {
	for len(c.doneChan) > 0 {
		<-c.doneChan
		// log.Println(<-c.doneChan)
	}
	// log.Println("clearDone")
}
