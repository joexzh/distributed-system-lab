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

type taskWrap struct {
	task *Task
	done chan struct{}
	mu   sync.RWMutex
}

type Coordinator struct {
	// Your definitions here.
	mapTasks    map[int]*taskWrap
	mapDone     bool
	reduceTasks map[int]*taskWrap
	reduceDone  bool
	coMu        sync.RWMutex
	nReduce     int
	taskQueue   chan *taskWrap
	doneQueue   chan *taskWrap
	taskExpire  time.Duration
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.taskExpire = 10 * time.Second
	c.mapTasks = make(map[int]*taskWrap)
	c.reduceTasks = make(map[int]*taskWrap)
	c.taskQueue = make(chan *taskWrap, nReduce)
	c.doneQueue = make(chan *taskWrap, nReduce)

	// add map tasks
	taskWraps := make([]*taskWrap, 0, len(files))
	for i, file := range files {
		taskWrap := &taskWrap{
			done: make(chan struct{}),
			mu:   sync.RWMutex{},
			task: &Task{
				InputFiles: []string{file},
				TaskType:   taskTypeMap,
				TaskNum:    i,
				Status:     taskStatusIdle,
				NReduce:    nReduce,
				Version:    0,
			},
		}
		c.mapTasks[i] = taskWrap
		taskWraps = append(taskWraps, taskWrap)
	}
	go c.sendTasks(taskWraps...)
	go c.bgDone()
	c.server()
	return &c
}

func (c *Coordinator) sendTasks(tWraps ...*taskWrap) {
	for _, tWrap := range tWraps {
		c.taskQueue <- tWrap
	}
}

func (c *Coordinator) sendDone(tWraps ...*taskWrap) {
	for _, tWrap := range tWraps {
		c.doneQueue <- tWrap
	}
}

// bgDone for map done and reduce done
func (c *Coordinator) bgDone() {
	var maps []*taskWrap
	var reduces []*taskWrap
	for tWrap := range c.doneQueue {
		if tWrap.task.TaskType == taskTypeMap {
			maps = append(maps, tWrap)
			if len(maps) < len(c.mapTasks) {
				continue
			}
			c.coMu.Lock()
			c.mapDone = true

			// create reduce tasks
			taskWraps := make([]*taskWrap, 0, c.nReduce)
			for _, tWrap := range c.mapTasks {
				tWrap.mu.RLock()
				for reduceTaskNum, filename := range tWrap.task.OutputFiles {

					tWrap, ok := c.reduceTasks[reduceTaskNum]
					if !ok {
						tWrap = &taskWrap{
							done: make(chan struct{}),
							task: &Task{
								InputFiles:  make([]string, 0),
								Status:      taskStatusIdle,
								Version:     0,
								TaskType:    taskTypeReduce,
								TaskNum:     reduceTaskNum,
								OutputFiles: make(map[int]string),
							},
						}
						c.reduceTasks[reduceTaskNum] = tWrap
						taskWraps = append(taskWraps, tWrap)
					}
					tWrap.task.InputFiles = append(tWrap.task.InputFiles, filename)
				}
				tWrap.mu.RLock()
			}
			c.coMu.Unlock()
			go c.sendTasks(taskWraps...)

		} else if tWrap.task.TaskType == taskTypeReduce {
			reduces = append(reduces, tWrap)
			if len(reduces) < c.nReduce {
				continue
			}
			c.coMu.Lock()
			c.reduceDone = true
			c.coMu.Unlock()
			return
		}
	}
}

func (c *Coordinator) getTaskWrap(taskType, taskNum int) *taskWrap {
	if taskType == taskTypeMap {
		return c.mapTasks[taskNum]
	} else if taskType == taskTypeReduce {
		return c.reduceTasks[taskNum]
	}
	return nil
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignTask(_ *EmptyArgs, resp *Task) error {
	select {
	case tWrap, ok := <-c.taskQueue:
		if !ok {
			resp.TaskType = taskTypeJobDone
			break
		}
		tWrap.mu.Lock()
		tWrap.task.Status = taskStatusBusy

		resp.Status = tWrap.task.Status
		resp.TaskType = tWrap.task.TaskType
		resp.TaskNum = tWrap.task.TaskNum
		resp.Version = tWrap.task.Version
		resp.InputFiles = tWrap.task.InputFiles
		resp.NReduce = tWrap.task.NReduce
		tWrap.mu.Unlock()

		// check expire
		go func(tWrap *taskWrap) {
			select {
			case <-time.After(c.taskExpire):
				// expire
				tWrap.mu.Lock()
				if tWrap.task.Status == taskStatusBusy {
					tWrap.task.Version++
				}
				tWrap.task.Status = taskStatusFailed
				tWrap.mu.Unlock()
				go c.sendTasks(tWrap)
				return
			case <-tWrap.done:
				// done
				return
			}
		}(tWrap)

	default:
		resp.TaskType = taskTypeEmpty
	}
	return nil
}

func (c *Coordinator) TaskDone(req *Task, _ *EmptyArgs) error {
	tWrap := c.getTaskWrap(req.TaskType, req.TaskNum)
	if tWrap == nil {
		return nil
	}
	tWrap.mu.Lock()
	defer tWrap.mu.Unlock()

	// maybe expire
	if tWrap.task.Version != req.Version {
		return nil
	}

	tWrap.task.Version++
	switch req.Status {
	case taskStatusComplete:
		close(tWrap.done)
		tWrap.task.Status = taskStatusComplete
		tWrap.task.OutputFiles = req.OutputFiles
		go c.sendDone(tWrap)
	case taskStatusFailed:
		tWrap.task.Status = taskStatusFailed
		go c.sendTasks(tWrap)
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func(l net.Listener) {
		if err := http.Serve(l, nil); err != nil {
			l.Close()
		}
	}(l)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.coMu.RLock()
	defer c.coMu.RUnlock()

	return c.reduceDone
}
