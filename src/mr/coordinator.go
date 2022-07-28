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
	// TODO: Your definitions here.
	readyChan     chan *Task
	doneChan      chan *Task
	phase         Phase
	tasks         []Task
	nMap          int
	nReduce       int
	intermediates [][]string // map output, reduce input
}

type Phase int

const (
	MapPhase = iota
	ReducePhase
	End
)

func (c *Coordinator) AssignTask(args *Args, reply *Assign) error {
	// log.Println("AssignTask begin")
	mutex.Lock()
	// log.Println("AssignTask lock")
	switch c.phase {
	case MapPhase:
		//reply.ID = 999
		c.retrieveTask(c.nMap, reply)
	case ReducePhase:
		c.retrieveTask(c.nReduce, reply)
	case End:
		// reply = &Assign{}
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

func (c *Coordinator) retrieveTask(cap int, reply *Assign) {
	// log.Println("")
	// log.Println("retrieveTask begin")
	//mutex.Lock()
	var task *Task
	if len(c.readyChan) > 0 {
		// log.Println("has unstarted tasks yet")
		// has unstarted tasks yet
		task = <-c.readyChan
		task.status = Running
		c.tasks[task.id].status = Running
		// log.Println("retrieveTask task            ", task)
		// log.Println("retrieveTask c.tasks[task.id]", c.tasks[task.id])
		// c.tasks[task.id].status = Running
		go c.countdown(task)
	} else if len(c.doneChan) < cap {
		// has running tasks yet
		// log.Println("has running tasks yet")
		task = &Task{}
		task.taskType = Wait
	} else {
		// impossible, should be next phase
		log.Println("c.phase: ", c.phase)
		log.Println("cap: ", cap)
		log.Println("len(c.readyChan): ", len(c.readyChan))
		log.Println("len(c.doneChan): ", len(c.doneChan))
		log.Println("reply: ", reply)
		panic("Task")
	}
	reply.TaskType = task.taskType
	reply.Inputfiles = task.inputfiles
	reply.ReduceNum = c.nReduce
	reply.ID = task.id
	// log.Println("reply:", reply)
	// log.Println("retrieveTask end")
	// log.Println("")
	//mutex.Unlock()
}

func (c *Coordinator) countdown(task *Task) {
	time.Sleep(20 * time.Second)
	mutex.Lock()
	// log.Println("")
	//log.Println("countdown lock")
	if c.tasks[task.id].status == Running {

		// log.Println("countdown recycle task", task)
		// log.Println("countdown recycle c.tasks[task.id]", c.tasks[task.id])

		task.status = Ready
		c.readyChan <- task
	}
	//log.Println("countdown unlock")
	// log.Println("")
	mutex.Unlock()
}

// func (c *Coordinator) retrieveMap() *Task {
// 	var task *Task
// 	if len(c.mapReady) > 0 {
// 		// has unstarted tasks yet
// 		task = <-c.mapReady
// 		task.status = Running
// 		c.tasks[task.id].status = Running
// 	} else if len(c.mapDone) < c.nMap {
// 		// has running tasks yet
// 		task = &Task{}
// 		task.taskType = Wait
// 	} else {
// 		// impossible, should be next phase
// 		panic("Task")
// 	}
// 	return task
// }

// func (c *Coordinator) retrieveReduce() *Task {
// 	var task *Task
// 	if len(c.reduceReady) > 0 {
// 		// has unstarted tasks yet
// 		task = <-c.reduceReady
// 		task.status = Running
// 		c.tasks[task.id].status = Running
// 	} else if len(c.reduceDone) < c.nReduce {
// 		// has running tasks yet
// 		task = &Task{}
// 		task.taskType = Wait
// 	} else {
// 		// impossible, should be next phase
// 		panic("Task")
// 	}
// 	return task
// }

// mark a task done, if all tasks are done, move to the next phase
func (c *Coordinator) MarkDone(args *Report, reply *Reply) error {
	// log.Println("")
	// log.Println("MarkDone begin")
	// log.Println("MarkDone args", args)
	mutex.Lock()
	// log.Println("MarkDone lock")
	task := &c.tasks[args.ID]
	// log.Println("MarkDone task before update", task)
	// log.Println("MarkDone c.tasks[args.ID] before update", c.tasks[args.ID])
	task.status = Done
	// log.Println("MarkDone task after update", task)
	// log.Println("MarkDone c.tasks[args.ID] after update", c.tasks[args.ID])
	// log.Println("MarkDone len(c.doneChan)", len(c.doneChan))
	c.doneChan <- task
	// log.Println("MarkDone -----------------------------------------")

	//log.Println("task: ", task)

	taskType := args.TaskType

	// log.Println("MarkDone +++++++++++++++++++++++++++++++++++++++++")

	if taskType == Map {
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

	// if taskType == Map && len(c.doneChan) == c.nMap {
	// 	// move to ReducePhase
	// 	log.Println("MarkDone: ------------------")

	// 	log.Println("MarkDone: ++++++++++++++++++")
	// }
	// log.Println("MarkDone ***********************************")

	if taskType == Reduce && len(c.doneChan) == c.nReduce {
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
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	// log.Println("server starting")
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
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
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// TODO: Your code here.

	// log.Println("making coordinator")

	c.nMap = len(files)
	c.nReduce = nReduce
	cap := c.nMap
	if c.nReduce > cap {
		cap = c.nReduce
	}
	c.readyChan = make(chan *Task, cap)
	c.doneChan = make(chan *Task, cap)

	for i := 0; i < c.nMap; i++ {
		c.intermediates = append(c.intermediates, []string{})
	}

	c.prepMap(files)

	c.server()
	return &c
}

func (c *Coordinator) prepMap(files []string) {
	// log.Println("prepMap begin")
	for i, file := range files {
		//log.Println("file: ", file)
		task := Task{Map, []string{file}, Ready, i}
		//log.Println("task: ", task)
		//log.Println(&task)
		c.tasks = append(c.tasks, task)
		//log.Println(c.readyChan)
		//log.Println(len(c.readyChan))
		//log.Println(cap(c.readyChan))
		c.readyChan <- &task
		// log.Println("preparing Map")
	}
	// log.Println("prepMap end")
}

func (c *Coordinator) prepReduce() {
	// log.Println("")
	// log.Println("prepReduce begin")
	//log.Println("c.intermediates: ", c.intermediates)
	for i := 0; i < c.nReduce; i++ {
		files := []string{}
		for _, intermediate := range c.intermediates {
			if len(intermediate) > 0 {
				//log.Println("intermediate: ", intermediate)
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

func (c *Coordinator) clearDone() {
	for len(c.doneChan) > 0 {
		<-c.doneChan
		// log.Println(<-c.doneChan)
	}
	// log.Println("clearDone")
}
