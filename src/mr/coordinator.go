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
	mapWg       sync.WaitGroup
	mapDone     bool
	reduceTasks map[int]*taskWrap
	reduceWg    sync.WaitGroup
	reduceDone  bool
	coMu        sync.RWMutex
	nReduce     int
	assignTasks []*taskWrap
	taskExpire  time.Duration
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Y our code here.
	c.nReduce = nReduce
	c.taskExpire = 10 * time.Second
	c.mapTasks = make(map[int]*taskWrap)
	c.reduceTasks = make(map[int]*taskWrap)
	c.assignTasks = make([]*taskWrap, 0)

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
	c.mapWg.Add(len(taskWraps))
	c.assignTasks = append(c.assignTasks, taskWraps...)
	c.bgWait()

	c.server()
	return &c
}

// bgWait for map done and reduce done
func (c *Coordinator) bgWait() {
	go func() {
		c.mapWg.Wait()
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
		c.reduceWg.Add(len(taskWraps))
		c.assignTasks = append(c.assignTasks, taskWraps...)
		c.coMu.Unlock()

		c.reduceWg.Wait()
		c.coMu.Lock()
		c.reduceDone = true
		c.coMu.Unlock()
	}()
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
	c.coMu.Lock()
	defer c.coMu.Unlock()

	if c.reduceDone {
		resp.TaskType = taskTypeJobDone
		return nil
	}

	if len(c.assignTasks) == 0 {
		// assign no task
		resp.TaskType = taskTypeEmpty
		return nil
	}

	tWrap := c.assignTasks[0]
	c.assignTasks = c.assignTasks[1:]
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
			c.coMu.Lock()
			c.assignTasks = append(c.assignTasks, tWrap)
			c.coMu.Unlock()

			tWrap.mu.Unlock()
			return
		case <-tWrap.done:
			// done
			return
		}
	}(tWrap)
	return nil
}

func (c *Coordinator) TaskDone(req *Task, _ *EmptyArgs) error {
	tWrap := c.getTaskWrap(req.TaskType, req.TaskNum)
	tWrap.mu.Lock()
	defer tWrap.mu.Unlock()

	// maybe expire
	if tWrap.task.Version != req.Version {
		return nil
	}

	tWrap.task.Version++
	switch req.Status {
	case taskStatusComplete:
		if tWrap.task.TaskType == taskTypeMap {
			c.mapWg.Done()
		} else if tWrap.task.TaskType == taskTypeReduce {
			c.reduceWg.Done()
		}
		close(tWrap.done)
		tWrap.task.Status = taskStatusComplete
		tWrap.task.OutputFiles = req.OutputFiles
	case taskStatusFailed:
		tWrap.task.Status = taskStatusFailed
		c.coMu.Lock()
		c.assignTasks = append(c.assignTasks, tWrap)
		c.coMu.Unlock()
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
	var l net.Listener

	defer func() {
		if err := recover(); err != nil {
			if l != nil {
				l.Close()
				panic(err)
			}
		}
	}()

	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
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
	c.coMu.RLock()
	defer c.coMu.RUnlock()

	return c.reduceDone
}
