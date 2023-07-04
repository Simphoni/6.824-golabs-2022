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

const DEBUG bool = false

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func FileInputWrapper(filename string) string {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %s", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %s", filename)
	}
	return string(content)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	args := RequestArgs{2, 0, 0}
	reply := RequestReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if !ok || reply.Action == 2 {
		return
	}
	nReduce := reply.NumParts
	nFiles := reply.NumFiles
	partition := make([][]KeyValue, nReduce)

	// Your worker implementation here.
	for {
		args := RequestArgs{0, 0, 0}
		reply := RequestReply{}
		ok := call("Coordinator.AssignTask", &args, &reply)
		if !ok || reply.Action == 2 {
			return
		}
		WorkerId := reply.WorkerId
		if reply.Action == 0 {
			time.Sleep(10 * time.Millisecond) // wait 10ms
		} else {
			if reply.TaskType == 0 {
				for i := range partition {
					partition[i] = make([]KeyValue, 0, 1024)
				}
				if DEBUG {
					log.Printf("worker %v got [MAP] job for %s", WorkerId, reply.FileName)
				}
				content := FileInputWrapper(reply.FileName)
				kva := mapf(reply.FileName, content)
				// partitioning
				for i := range kva {
					keyHash := ihash(kva[i].Key) % nReduce
					partition[keyHash] = append(partition[keyHash], kva[i])
				}
				// write intermediate partitions
				for i := range partition {
					tempFilename := "mr-" + strconv.Itoa(WorkerId) + "-" + strconv.Itoa(i)
					f, _ := ioutil.TempFile("", "mrtemp-inter")
					encoder := json.NewEncoder(f)
					for _, kv := range partition[i] {
						e := encoder.Encode(&kv)
						if e != nil {
							return
						}
					}
					f.Close()
					os.Rename(f.Name(), tempFilename)
				}
				if DEBUG {
					log.Printf("worker %v [MAP] done", WorkerId)
				}
			} else {
				if DEBUG {
					log.Printf("worker %v got [REDUCE] job", WorkerId)
				}
				intermediate := make([]KeyValue, 0, 32768)
				for i := 0; i < nFiles; i++ {
					tempFilename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(WorkerId)
					f, e := os.Open(tempFilename)
					if e != nil {
						log.Fatal("failed to open ", tempFilename, e)
					}
					dec := json.NewDecoder(f)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv)
					}
					f.Close()
				}
				sort.Sort(ByKey(intermediate))

				oname := "mr-out-" + strconv.Itoa(WorkerId)
				ofile, _ := ioutil.TempFile("", "mrtemp-out")
				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := make([]string, 0, 512)
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)
					fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
					i = j
				}
				ofile.Close()
				os.Rename(ofile.Name(), oname)
				if DEBUG {
					log.Printf("worker %v [REDUCE] done", WorkerId)
				}
			}
			args.Action = 1
			args.TaskType = reply.TaskType
			args.WorkerId = reply.WorkerId
			reply = RequestReply{}
			ok := call("Coordinator.AssignTask", &args, &reply)
			if ok == false {
				return
			}
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
