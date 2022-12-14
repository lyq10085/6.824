package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	UNASSGINED = iota
	INPROGRESS
	FINISHED
)

type TaskState struct {
	state int
	c     chan struct{}
	// lk    sync.Mutex // fine-grained lock
}

type Coordinator struct {
	// Your definitions here.
	// there are splits(number of input files) map tasks to be finished; there are [nReduce] reduce tasks to be finished
	splits, nReduce int
	mtasks, rtasks  []TaskState // map/reduce task state

	mu                  sync.Mutex // protect c.donemap, c.donereduce
	cond                sync.Cond  // cond for mu
	donemap, donereduce int        // false -> still in map phrase ; true -> in reduce phrase
	files               []string
	intermedatefiles    [][]string //
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Assign(args *AssignArgs, reply *AssignReply) error {

	// when to assgin a reduce task? when to assign a map task

	assigned := false

	c.mu.Lock()

	if c.donemap < c.splits { // assgin map task
		// fmt.Println("look for unassigned map task")
		for i := range c.mtasks {
			if c.mtasks[i].state == UNASSGINED { // or lasttime > 10 seconds
				reply.MapOrReduceOrWait = MAP
				c.mtasks[i].state = INPROGRESS
				reply.Filename = c.files[i]
				reply.Taskid = i
				reply.NReduce = c.nReduce

				assigned = true

				// set a time limit
				go func(i int) {
					clk := time.NewTimer(10 * time.Second)
					select {
					case <-clk.C:
						// fmt.Printf("map task %v TLE\n", i)
						// assgin to another task
						c.mu.Lock()
						c.mtasks[i].state = UNASSGINED
						c.mu.Unlock()

					case <-c.mtasks[i].c:
						// fmt.Printf("map task %v finished\n", i)

					}

				}(i)

				break
			}
		}
	} else if c.donereduce < c.nReduce { // assign reduce task
		// fmt.Println("look for unassined reduce task")

		for i := range c.rtasks {
			if c.rtasks[i].state == UNASSGINED {
				// fmt.Println("find one unassigned reduce ")
				reply.MapOrReduceOrWait = REDUCE
				c.rtasks[i].state = INPROGRESS
				// assign intermediate files
				reply.Intermediatefiles = c.intermedatefiles[i]
				// fmt.Println(reply.Intermediatefiles)
				reply.Taskid = i

				assigned = true

				// set a time limit
				go func(i int) {
					clk := time.NewTimer(10 * time.Second)
					select {
					case <-clk.C:
						// fmt.Printf("reduce task %v TLE\n", i)
						// assgin to another task
						c.mu.Lock()
						c.rtasks[i].state = UNASSGINED
						c.mu.Unlock()

					case <-c.rtasks[i].c:
						// fmt.Printf("reduce task %v finished\n", i)

					}

				}(i)

				break
			}
		}

	}

	if !assigned {
		// wait for task
		reply.MapOrReduceOrWait = WAIT
	}

	c.mu.Unlock()

	return nil
}

func (c *Coordinator) Accomplish(args *AccomplishArgs, reply *AccomplishReply) error {
	if args.MapOrReduce == false { // map task
		c.mtasks[args.Taskid].c <- struct{}{} // stop counting
		interfiles := args.Intermediatefiles

		// fmt.Println(args.Intermediatefiles) // todo

		c.mu.Lock()
		c.donemap += 1
		c.mtasks[args.Taskid].state = FINISHED
		// classify ?
		for _, v := range interfiles {
			c.intermedatefiles[v.Rid] = append(c.intermedatefiles[v.Rid], v.Fname)
		}
		c.mu.Unlock()

	} else {
		c.rtasks[args.Taskid].c <- struct{}{}

		// reduce task
		c.mu.Lock()

		c.donereduce += 1
		if c.donereduce == c.nReduce {
			c.cond.Broadcast()
		}

		c.mu.Unlock()

	}

	// reply.Tmp = true

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
	//l, e := net.Listen("tcp", ":1234")
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

	ret := false

	c.mu.Lock()

	for c.donereduce < c.nReduce {
		c.cond.Wait()
	}

	c.mu.Unlock()
	ret = true

	// Your code here.
	fmt.Println("MapReduce Job done")

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{splits: len(files), nReduce: nReduce, files: files}
	c.cond = *sync.NewCond(&c.mu)

	//
	c.mtasks = make([]TaskState, c.splits)

	for taskid := range c.mtasks {
		c.mtasks[taskid].state = UNASSGINED
		c.mtasks[taskid].c = make(chan struct{})
	}

	c.rtasks = make([]TaskState, c.nReduce)

	for taskid := range c.rtasks {
		c.rtasks[taskid].state = UNASSGINED
		c.rtasks[taskid].c = make(chan struct{})
	}

	c.intermedatefiles = make([][]string, c.nReduce)

	// Your code here.

	c.server()
	return &c
}
