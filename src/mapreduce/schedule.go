package mapreduce

import (
	"fmt"
	"sync"
//	"time"
)
var wg_map sync.WaitGroup
var wg_reduce sync.WaitGroup
//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}
	
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)
	
	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	taskChan := make(chan int)
	go func() {
	    for i := 0;i < ntasks;i++ {
		    debug("begin to join %d to taskChan\n",i)
		    taskChan <- i	
		    debug("finish join %d to taskChan\n",i)		
		    switch phase {
				case mapPhase:
				wg_map.Add(1)
				case reducePhase:
				wg_reduce.Add(1)
		    }
		}
	    debug("\n begin to wait for taskChan finish\n")
	    switch phase {
			case mapPhase:
			wg_map.Wait()			
			case reducePhase:
			wg_reduce.Wait()
		}
	 	close(taskChan)   
		debug("\n\n\ntaskChan close\n")
	}()
	
	var workerAddr string
	for indexTask := range taskChan {
		//select {
		//	case workerAddr = <-registerChan:
			workerAddr = <-registerChan
			taskArgs := DoTaskArgs{jobName,mapFiles[indexTask],phase,indexTask,n_other}
			fmt.Printf("Phase: %s,index : %d\n", phase,indexTask)
			go func(workerAddr string,taskArgs DoTaskArgs) {
				if call(workerAddr,"Worker.DoTask",taskArgs,nil) {
					switch phase {
					case mapPhase:
					wg_map.Done()
					case reducePhase:
					wg_reduce.Done()
					}
					registerChan <- workerAddr
					debug("finish to add worker %s to chan\n",workerAddr)
				}else{
					fmt.Printf("Task Fail: worker:%s, task:%d\n", 
					workerAddr, taskArgs.TaskNumber)
					taskChan <- taskArgs.TaskNumber
				}
			}(workerAddr, taskArgs)		
			//case <-time.After(1000 * time.Millisecond):
			//fmt.Printf("Timed out before worker finished\n")
		//}
		
	}
	fmt.Printf("Schedule: %v done\n", phase)
}
