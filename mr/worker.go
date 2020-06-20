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

func doMapTask(mapf func(string, string) []KeyValue, reply Task) int {

	filename := reply.Filename
	index := reply.Index
	numReducers := reply.NumReducers
	file, err := os.Open(filename)
	defer file.Close()

	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	kva := mapf(filename, string(content))

	encs := []*json.Encoder{}
	for i := 0; i < numReducers; i++ {

		filename = "Interim-" + strconv.Itoa(index) + "-" + strconv.Itoa(i) + ".json"
		file, err = os.Create(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		enc := json.NewEncoder(file)
		encs = append(encs, enc)
	}
	for _, kv := range kva {
		key := ihash(kv.Key) % numReducers
		err := encs[key].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode kv")
		}
	}
	return index
}

func doReduceTask(reducef func(string, []string) string, reply Task) int {

	index := reply.Index
	numFiles := reply.NumOfFiles
	kva := []KeyValue{}
	for i := 0; i < numFiles; i++ {
		filename := "Interim-" + strconv.Itoa(i) + "-" + strconv.Itoa(index) + ".json"
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

	}
	intermediate := kva
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(index)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	return reply.Index
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	//	CallExample()
	var args Request

	args.HasStatus = false
	for {
		var reply Task
		err := call("Master.GetTask", &args, &reply)
		// fmt.Println("worker: ", reply)
		if err == false {
			os.Exit(0)
		}

		if reply.Sleep {
			args.HasStatus = false
			time.Sleep(2 * time.Second)
			continue
		}
		if reply.Completed {
			break
		}
		if reply.OfType == "map" {
			args.Index = doMapTask(mapf, reply)
			args.HasStatus = true
			args.OfType = "map"
		} else if reply.OfType == "reduce" {
			args.Index = doReduceTask(reducef, reply)
			args.HasStatus = true
			args.OfType = "reduce"
		}

	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
