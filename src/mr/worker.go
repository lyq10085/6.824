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

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type KeyValues struct {
	Key    string
	Values []string
}

type interfile struct {
	Rid   int
	Fname string
}

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// periodically ask coordinator for task
	for {
		args := &AssignArgs{}
		reply := &AssignReply{}

		ok := call("Coordinator.Assign", args, reply)

		if ok {
			if reply.MapOrReduceOrWait == MAP { // get a map task
				file, err := os.Open(reply.Filename)
				if err != nil {
					log.Fatalf("cannot open1 %v", reply.Filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", reply.Filename)
				}
				file.Close()
				kva := mapf(reply.Filename, string(content))

				combine(kva, reply.Taskid, reply.NReduce)
			} else if reply.MapOrReduceOrWait == REDUCE { // get a reduce task

				fnames := reply.Intermediatefiles
				// fmt.Println(fnames)

				kva := map[string][]string{}

				// create tmp reduce out file
				outf, err := ioutil.TempFile(".", "w")
				if err != nil {
					log.Fatal("cannot create tmp reduce out file")
				}
				oldname := outf.Name()

				for _, fname := range fnames {
					file, err := os.Open(fname)
					if err != nil {
						log.Fatalf("cannot open2 %s", fname)
					}
					var kvs KeyValues
					// decode file err why
					dec := json.NewDecoder(file)
					for {
						if err := dec.Decode(&kvs); err != nil {
							// fmt.Println(err)
							break
						}

						kva[kvs.Key] = append(kva[kvs.Key], kvs.Values...)

					}
					file.Close()
				}

				// reduce kva
				for k, v := range kva {
					outv := reducef(k, v)
					fmt.Fprintf(outf, "%v %v\n", k, outv)
				}

				// rename tmp reduce file
				outf.Close()
				outfilename := fmt.Sprintf("mr-out-%v", reply.Taskid)
				os.Rename(oldname, outfilename)

				// info coordinator reduce task finished
				bargs := &AccomplishArgs{MapOrReduce: true, Taskid: reply.Taskid}
				breply := &AccomplishReply{}

				ok := call("Coordinator.Accomplish", bargs, breply)

				if ok {
					// fmt.Printf("reduce task finished msg send correctly %v\n", breply.Tmp)
				} else {

				}

			} else {
				// fmt.Println("wait for task...")
			}

			// wait for a while before make next assign request
			<-time.After(time.Millisecond * 100)

		} else { // Worker exit...(suppose coordinator has exited)
			break
		}
	}

}

// gather kvs according to key

func combine(kva []KeyValue, mapTaskid int, nReduce int) {
	// kv slice
	intermediate := []KeyValue{}
	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))

	// tmp files to be consumed by reduce task
	// to be renamed once current map task finish
	intermediatefiles := make([]interfile, nReduce)
	for i := range intermediatefiles {
		f, err := ioutil.TempFile(".", "w")
		if err != nil {
			log.Fatal(err)
		}
		intermediatefiles[i] = interfile{Rid: i, Fname: f.Name()}
		f.Close()
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

		reduceTaskid := ihash(intermediate[i].Key) % nReduce

		emit(mapTaskid, reduceTaskid, KeyValues{intermediate[i].Key, values}, &intermediatefiles)
		i = j
	}

	// rename tmp file
	for i := range intermediatefiles {
		fname := fmt.Sprintf("mr-%v-%v", mapTaskid, i)
		os.Rename(intermediatefiles[i].Fname, fname)
		intermediatefiles[i].Fname = fname
	}

	// sum up intermediate files , pass to coordinator
	// fmt.Println(intermediatefiles)
	// info coordinator : task finished
	args := &AccomplishArgs{MapOrReduce: false, Taskid: mapTaskid, Intermediatefiles: intermediatefiles}
	reply := &AccomplishReply{}
	ok := call("Coordinator.Accomplish", args, reply)

	// seem useless
	if ok {
		// fmt.Printf("map task finished msg send correctly %v\n", reply.Tmp)
	} else {
		// log.Fatal("cannot send accomplish msg to coordinator")
	}

}

// write intermediate file
func emit(mid int, rid int, kvs KeyValues, interf *[]interfile) {
	fname := (*interf)[rid].Fname
	f, err := os.OpenFile(fname, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	defer func() {
		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	if err != nil {
		log.Fatalf("cannot open file %v", f.Name())
	}

	enc := json.NewEncoder(f)
	// write intermediate result to file
	// is this write incremental?
	// fmt.Println(rid, " ", kvs)
	enc.Encode(&kvs)
	// if err != nil {
	// 	fmt.Println(err)
	// }

	// f.Close()
}

//
// example function to show how to make an RPC call to the coordinator.
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
