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

var mutex = &sync.Mutex{}

type Master struct {
	// Your definitions here.
	files         []string
	mapStatus     []int
	reduceStatus  []int
	numReducers   int
	mapCounter    int
	reduceCounter int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) GetTask(request *Request, reply *Task) error {
	//	time.Sleep(2*time.Second)
	mutex.Lock()
	defer mutex.Unlock()
	if request.HasStatus {
		switch request.OfType {
		case "map":
			m.mapStatus[request.Index] = 2
			m.mapCounter++
		case "reduce":
			m.reduceStatus[request.Index] = 2
			m.reduceCounter++
		default:
			log.Fatal("Invalid response type")

		}
	}
	if m.mapCounter != len(m.files) {

		// take care of situation where workers have to wait for othr map to get over
		for i := range m.files {
			if m.mapStatus[i] == 0 {
				reply.OfType = "map"
				reply.Filename = m.files[i]
				reply.Index = i
				reply.NumReducers = m.numReducers
				m.mapStatus[i] = 1

				go func(i int) {
					time.Sleep(10 * time.Second)
					mutex.Lock()
					defer mutex.Unlock()
					if m.mapStatus[i] == 1 {
						m.mapStatus[i] = 0
					}
				}(i)
				break
			}

		}
		if reply.OfType == "" {

			reply.Sleep = true
			// some other running, send reply to sleep for 2 seconds
		}
	} else if m.reduceCounter != m.numReducers {

		for i := 0; i < m.numReducers; i++ {
			if m.reduceStatus[i] == 0 {
				reply.OfType = "reduce"
				reply.Index = i
				reply.NumOfFiles = len(m.files)
				m.reduceStatus[i] = 1
				go func(i int) {
					time.Sleep(10 * time.Second)

					mutex.Lock()
					defer mutex.Unlock()
					if m.reduceStatus[i] == 1 {
						m.reduceStatus[i] = 0
					}
				}(i)
				break
			}

		}
		// some other running, send reply to sleep for 2 seconds
		if reply.OfType == "" {
			reply.Sleep = true
		}

	} else {
		reply.Completed = true

	}
	// fmt.Println("master: ", reply)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
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
	ret := false

	if m.mapCounter == len(m.files) && m.reduceCounter == m.numReducers {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// status := [len(files)]bool{}
	m := Master{files, make([]int, len(files)), make([]int, nReduce), nReduce, 0, 0}
	// Your code here.

	m.server()

	return &m
}
