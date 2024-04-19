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
	"time"
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
		reply, err := CallRequestTask()
		if err != nil {
			log.Fatalf("Error calling RequestTask: %v", err)
		}

		if reply.TaskType == "Sleep" {
			time.Sleep(1 * time.Second)
		}
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

			results := mapf(reply.TaskData[0], string(content))
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
			}
		} else if reply.TaskType == "Reduce" {
			done += 1

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
	reply := ReturnTaskResultsReply{}
	call("Coordinator.ReturnTaskResults", &args, &reply)

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
