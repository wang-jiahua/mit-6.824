package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		// ask the coordinator for a task
		log.Println("Worker begin")
		assign := getTask()
		//log.Println("Worker +++++++++++++")
		report := Report{assign.TaskType, []string{}, assign.ID}

		switch assign.TaskType {
		case Exit:
			return
		case Wait:
			time.Sleep(time.Second)
		case Map:
			doMap(mapf, assign, &report)
		case Reduce:
			doReduce(reducef, assign, &report)
		default:
			panic("unknown task type")
		}

		reportDone(report)
		log.Println("Worker end")
	}
}

func getTask() Assign {
	log.Println("getTask begin")
	args := Args{}
	reply := Assign{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		fmt.Printf("reply %v\n", reply)
	} else {
		fmt.Printf("call failed!\n")
		// coordinator has exited
		// work exits too
		os.Exit(0)
	}
	log.Println("reply:", reply)
	log.Println("getTask end")
	return reply
}

func doMap(mapf func(string, string) []KeyValue, assign Assign, report *Report) {
	log.Println("doMap begin")
	log.Println(assign)
	filename := assign.Inputfiles[0]
	content := mapRead(filename)
	//log.Println("doMap 1111111111111111")
	kva := mapf(filename, string(content))
	//log.Println("doMap 00000000000000000")
	mapWrite(kva, assign, report)
	log.Println("doMap end")
}

func mapRead(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return content
}

func mapWrite(kva []KeyValue, assign Assign, report *Report) {
	log.Println("mapWrite begin")
	intermediates := [][]KeyValue{}
	for i := 0; i < assign.ReduceNum; i++ {
		intermediates = append(intermediates, []KeyValue{})
	}
	// log.Println("mapWrite 2222222222222")
	for _, kv := range kva {
		//log.Println(i)
		key := kv.Key
		reduceID := ihash(key) % assign.ReduceNum
		intermediates[reduceID] = append(intermediates[reduceID], kv)
		//log.Println(reduceID)
		//log.Println(intermediates[reduceID])
	}
	// log.Println("mapWrite 3333333333333333")
	for reduceID, intermediate := range intermediates {
		tmpname := "mr-tmp-" + strconv.Itoa(assign.ID) + "-" + strconv.Itoa(reduceID)
		//log.Println("tmpname", tmpname)
		dir, _ := os.Getwd()
		tmpfile, err := ioutil.TempFile(dir, "mr-tmp-*")
		//log.Println("tmpfile", tmpfile)
		if err != nil {
			log.Println("err: ", err)
			log.Fatalf("cannot create temporary file: %v", tmpname)
		}

		enc := json.NewEncoder(tmpfile)
		for _, kv := range intermediate {
			if err := enc.Encode(&kv); err != nil {
				break
			}
		}

		oname := "mr-" + strconv.Itoa(assign.ID) + "-" + strconv.Itoa(reduceID)
		os.Rename(tmpfile.Name(), oname)
		tmpfile.Close()
		report.Outputfiles = append(report.Outputfiles, oname)
	}
	log.Println("mapWrite end")
}

func doReduce(reducef func(string, []string) string, assign Assign, report *Report) {
	intermediate := reduceRead(assign)
	sort.Sort(ByKey(intermediate))
	reduceWrite(reducef, intermediate, assign, report)
}

func reduceRead(assign Assign) []KeyValue {
	log.Println("reduceRead begin")
	intermediate := []KeyValue{}
	for _, filename := range assign.Inputfiles {
		log.Println("filename: ", filename)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		// content, err := ioutil.ReadAll(file)
		// if content != nil {
		// 	log.Println("content is not empty")
		// }

		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				//log.Println("err: ", err)
				break
			}
			//log.Println("kv: ", kv)
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	log.Println("reduceRead end")
	return intermediate
}

func reduceWrite(reducef func(string, []string) string, intermediate []KeyValue, assign Assign, report *Report) {
	log.Println("reduceWrite begin")
	log.Println("len(intermediate): ", len(intermediate))
	tmpname := "mr-out-tmp-" + strconv.Itoa(assign.ID)
	dir, _ := os.Getwd()
	tmpfile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatalf("cannot create temporary file: %v", tmpname)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		//fmt.Printf("%v %v\n", intermediate[i].Key, output)
		fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	oname := "mr-out-" + strconv.Itoa(assign.ID)
	os.Rename(tmpfile.Name(), oname)
	tmpfile.Close()
	report.Outputfiles = append(report.Outputfiles, oname)
	log.Println("reduceWrite end")
}

func reportDone(args Report) {
	log.Println("reportDone begin")
	reply := Reply{}

	log.Println("args: ", args)

	ok := call("Coordinator.MarkDone", &args, &reply)
	if ok {
		fmt.Printf("reply %v\n", reply)
	} else {
		fmt.Printf("reportDone call failed!\n")
	}
	log.Println("reportDone end")
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
	log.Println("call begin")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	//log.Println("call: ----------------")
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	log.Println("call end")
	return false
}
