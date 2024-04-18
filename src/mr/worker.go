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
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	done := 0
	var filenames []string // Define outside to use later in the loop

	for done < 10 {
		done += 1
		reply, err := CallRequestTask()
		if err != nil {
			log.Fatalf("Error calling RequestTask: %v", err)
		}

		log.Printf("Task %v", reply)
		if reply.TaskType == "Map" {
			file, err := os.Open(reply.TaskData[0])
			if err != nil {
				log.Fatalf("failed to open file: %v", err)
			}
			content, err := ioutil.ReadAll(file)
			file.Close() // Handle close here instead of defer in the loop
			if err != nil {
				log.Fatalf("failed to read file content: %v", err)
			}

			results := mapf("", string(content))
			intermediate := make([][]KeyValue, 10)
			for _, kv := range results {
				r := ihash(kv.Key) % 10
				intermediate[r] = append(intermediate[r], kv)
			}

			filenames = make([]string, 10) // Reset for new map task
			for i, kvs := range intermediate {
				filename := fmt.Sprintf("mr-%d-%d", reply.TaskNumber, i)
				filenames[i] = filename // Assign to correct index
				file, err := os.Create(filename)
				if err != nil {
					log.Fatalf("failed to create file: %v", err)
				}

				enc := json.NewEncoder(file)
				for _, kv := range kvs {
					if err := enc.Encode(&kv); err != nil {
						log.Fatalf("failed to encode KeyValue: %v", err)
					}
				}
				file.Close() // Close here after all operations are done
				log.Printf("%v", filenames)
			}
		} else if reply.TaskType == "Reduce" {

			// Load intermediate files
			intermediate := []KeyValue{}
			for m := 0; m < len(reply.TaskData); m++ {
				file, err := os.Open(reply.TaskData[m])
				if err != nil {
					log.Fatalf("cannot open %v", reply.TaskData[m])
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}

			// Sort intermediate key-value pairs by key
			sort.Slice(intermediate, func(i, j int) bool {
				return intermediate[i].Key < intermediate[j].Key
			})

			// Create output file
			oname := fmt.Sprintf("mr-out-%d", reply.TaskNumber)
			ofile, _ := ioutil.TempFile("", oname)

			// Apply reduce function
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
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}

			// Close output file
			ofile.Close()

			// Rename output file
			os.Rename(ofile.Name(), oname)

			filenames = append(filenames, oname)
		}

		log.Printf("%v", filenames)
		CallReturnTaskResults(reply.TaskType, reply.TaskNumber, filenames)
	}
}

func CallDone() bool {
	args := DoneArgs{}
	reply := DoneReply{}
	ok := call("Coordinator.CheckDone", &args, &reply)
	if ok {
		return reply.IsDone
	} else {
		return true
	}
}

func CallRequestTask() (RequestTaskReply, error) {
	log.Printf("request task")
	args := RequestTaskArgs{}
	args.Text = "test"
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		return reply, nil
	} else {
		return RequestTaskReply{}, fmt.Errorf("no strings returned")
	}
}

func CallReturnTaskResults(taskType string, taskNumber int, filenames []string) {

	args := ReturnTaskResultsArgs{}
	args.Results = filenames
	args.TaskNumber = taskNumber
	args.TaskType = taskType
	log.Printf("results: %v", args)
	reply := ReturnTaskResultsReply{}
	ok := call("Coordinator.ReturnTaskResults", &args, &reply)
	if ok {
		log.Printf("%s", reply.Value)
	} else {
		log.Printf("error")
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
