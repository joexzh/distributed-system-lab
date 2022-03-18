package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type mapf func(string, string) []KeyValue
type reducef func(string, []string) string

var emptyArgs = new(EmptyArgs)

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
func Worker(mapf mapf, reducef reducef) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	task := new(Task)

	for {
		err := askForTask(emptyArgs, task)
		// network error, consider mr job done, stop worker
		if err != nil {
			fmt.Printf("askForTask failed, shutdown worker, %v\n", err)
			return
		}

		switch task.TaskType {
		case 0:
			// no task assign, keep asking
			time.Sleep(time.Second)
			continue
		case 1:
			// map
			files, err := mapTask(task, mapf)
			if err != nil {
				failedTask := &Task{
					Files:    nil,
					TaskType: 0,
					TaskNum:  task.TaskNum,
					Err:      err,
				}
				if err = responseTask(failedTask); err != nil {
					// response err, consider master unavailable, stop worker
					fmt.Printf("responseTask failed, shutdown worker, %v\n", err)
					return
				}
			}
			succeedTask := &Task{Files: files, TaskType: task.TaskType, TaskNum: task.TaskNum, Err: nil}
			if err = responseTask(succeedTask); err != nil {
				// response err, consider master unavailable, stop worker
				fmt.Printf("responseTask failed, shutdown worker, %v\n", err)
				return
			}
		case 2:
			// reduce
			file, err := reduceTask(task, reducef)
			if err != nil {
				failedTask := &Task{
					Files:    nil,
					TaskType: 0,
					TaskNum:  task.TaskNum,
					Err:      err,
				}
				if err = responseTask(failedTask); err != nil {
					// response err, consider master unavailable, stop worker
					fmt.Printf("responseTask failed, shutdown worker, %v\n", err)
					return
				}
			}
			succeedTask := &Task{
				Files:    []string{file},
				TaskType: task.TaskType,
				TaskNum:  task.TaskNum,
				Err:      nil,
			}
			if err = responseTask(succeedTask); err != nil {
				// response err, consider master unavailable, stop worker
				fmt.Printf("responseTask failed, shutdown worker, %v\n", err)
				return
			}

		default:
			time.Sleep(time.Second)
		}
	}
}

func fmtIntermediate(mapTaskNum, reduceTaskNum int) string {
	return fmt.Sprintf("mr-%v-%v", mapTaskNum, reduceTaskNum)
}

func mapTask(task *Task, mapf mapf) ([]string, error) {
	var intermediates map[string][]KeyValue

	for _, filename := range task.Files {
		file, err := os.Open(filename)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("cannot open %v, %v\n", filename, err))
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("cannot read %v, %v\n", filename, err))
		}
		file.Close()
		kva := mapf(filename, string(content))

		for _, kv := range kva {
			reduceTaskNum := ihash(kv.Key) % task.NReduce
			interFileName := fmtIntermediate(task.TaskNum, reduceTaskNum)
			kvs := intermediates[interFileName]
			kvs = append(kvs, kv)
			intermediates[interFileName] = kvs
		}
	}

	var files []string
	for filename, kvs := range intermediates {
		ofile, _ := os.Create(filename)
		err := encode(ofile, kvs)
		ofile.Close()
		if err != nil {
			return nil, errors.New(fmt.Sprintf("cannot encode to file %v, %v\n", filename, err))
		}

		files = append(files, filename)
	}

	return files, nil
}

func reduceTask(task *Task, reducef reducef) (string, error) {
	var kva []KeyValue

	for _, filename := range task.Files {
		file, err := os.Open(filename)
		if err != nil {
			return "", errors.New(fmt.Sprintf("cannot open file %v, %v\n", filename, err))
		}
		kvs, err := decode(file)
		if err != nil {
			return "", errors.New(fmt.Sprintf("cannot decode from file %v, %v", filename, err))
		}
		kva = append(kva, kvs...)
	}

	sort.Sort(ByKey(kva))
	oname := "mr-out-" + strconv.Itoa(task.TaskNum)
	ofile, err := os.Create(oname)
	if err != nil {
		return "", errors.New(fmt.Sprintf("cannot create or truncate file %v, %v", oname, err))
	}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-{reduce task number}.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()

	return oname, nil
}

func askForTask(args *EmptyArgs, task *Task) error {
	if err := call("Coordinator.AssignTask", args, task); err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func responseTask(task *Task) error {
	if err := call("Coordinator.TaskDone", task, &EmptyArgs{}); err != nil {
		return err
	}
	return nil
}

func call(method string, args interface{}, reply interface{}) error {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	if err != nil {
		log.Fatal("dialing err:", err)
	}
	defer c.Close()

	err = c.Call(method, args, reply)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func encode(file io.Writer, kva []KeyValue) error {
	en := json.NewEncoder(file)
	err := en.Encode(&kva)
	if err != nil {
		return err
	}
	return nil
}

func decode(file io.Reader) ([]KeyValue, error) {
	var kva []KeyValue
	de := json.NewDecoder(file)
	err := de.Decode(&kva)
	if err != nil {
		return nil, err
	}
	return kva, nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {
//
// 	// declare an argument structure.
// 	args := ExampleArgs{}
//
// 	// fill in the argument(s).
// 	args.X = 99
//
// 	// declare a reply structure.
// 	reply := ExampleReply{}
//
// 	// send the RPC request, wait for the reply.
// 	call("Coordinator.Example", &args, &reply)
//
// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
// func call(rpcname string, args interface{}, reply interface{}) bool {
// 	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
// 	// sockname := coordinatorSock()
// 	// c, err := rpc.DialHTTP("unix", sockname)
// 	if err != nil {
// 		log.Fatal("dialing:", err)
// 	}
// 	defer c.Close()
//
// 	err = c.Call(rpcname, args, reply)
// 	if err == nil {
// 		return true
// 	}
//
// 	fmt.Println(err)
// 	return false
// }
