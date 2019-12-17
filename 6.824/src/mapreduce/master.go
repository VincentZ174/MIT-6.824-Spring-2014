package mapreduce

import "container/list"
import "fmt"
import "strconv"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

// RunMaster does nothing for now
func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	map_workers := make(chan string, mr.nMap)       // channel of map workers
	reduce_workers := make(chan string, mr.nReduce) // channel of reduce workers
	completed_maps := make(chan int)                // completed map jobs
	completed_reduces := make(chan int)             // completed reduce

	go func() {
		for x := range mr.registerChannel { // iterate over channel
			map_workers <- x    // new map worker information passed in through registerChannel
			reduce_workers <- x // new reduce worker information passed in through registerChannel
		}
	}()

	for i := 0; i < mr.nMap; i++ { // generate jobs until channel buffer limit reached
		// map arguments given through MapReduce struct
		mapArgs := &DoJobArgs{}
		mapArgs.File = mr.file
		mapArgs.Operation = Map
		mapArgs.JobNumber = i
		mapArgs.NumOtherPhase = mr.nReduce

		go func() {
			map_worker := <-map_workers // read worker info into new variable
			fmt.Println("Assigning New Worker " + map_worker + " Job Number: " + strconv.Itoa(mapArgs.JobNumber))

			reply := &DoJobReply{} // empty currently, contains reply of RPC call

			ok := call(map_worker, "Worker.DoJob", mapArgs, &reply) // check for failure

			for !ok { // continue to check for failure
				fmt.Println("Worker Fail")
				map_worker := <-map_workers // try to read in worker info into new var again
				ok := call(map_worker, "Worker.DoJob", mapArgs, &reply)
				if ok { // if no failure occured
					map_workers <- map_worker           // return worker back into map_workers channel
					completed_maps <- mapArgs.JobNumber // put job number into completed maps
					return                              //exit function
				}
			}

			map_workers <- map_worker
			completed_maps <- mapArgs.JobNumber
		}()
	}

	for i := 0; i < mr.nMap; i++ { // continue listening / blocking until all jobs are done
		<-completed_maps
	}

	for i := 0; i < mr.nReduce; i++ { // generate nReduce number of jobs
		// arguments given by MapReduce struct
		redArgs := &DoJobArgs{}
		redArgs.File = mr.file
		redArgs.Operation = Reduce
		redArgs.JobNumber = i
		redArgs.NumOtherPhase = mr.nMap

		go func() {
			reduce_worker := <-reduce_workers //place worker info into new var
			fmt.Println("Assigning New Worker " + reduce_worker + " Job Number: " + strconv.Itoa(redArgs.JobNumber))
			reply := &DoJobReply{} // receieves output of RPC call

			ok := call(reduce_worker, "Worker.DoJob", redArgs, &reply)

			for !ok { //  checks for worker failure
				fmt.Println("Worker Fail")
				reduce_worker := <-reduce_workers
				ok := call(reduce_worker, "Worker.DoJob", redArgs, &reply)
				if ok {
					reduce_workers <- reduce_worker
					completed_reduces <- redArgs.JobNumber
					return
				}
			}

			reduce_workers <- reduce_worker
			completed_reduces <- redArgs.JobNumber
		}()
	}

	for i := 0; i < mr.nReduce; i++ {
		<-completed_reduces
	}

	return mr.KillWorkers() // kill all workers after all Maps and Reduces are done
}
