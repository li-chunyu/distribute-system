package mapreduce

import (
	"fmt"
	"log"
	"sync"
)

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
	// w: worker
	// arg: arguments
	var wg sync.WaitGroup
	startTask := func(w string, arg DoTaskArgs, rc chan string) {
		defer wg.Done()
		ret := call(w, "Worker.DoTask", arg, nil)
		if !ret {
			log.Fatal("schedule: call %s falied", w)
		} else {
			// worker 的channel是无缓冲的，也就是说 rc <- w 向管道 rc 中发送数据，
			// 如果没有接受方接受数据，那么就会一直阻塞在 rc<- w 直到有接收方接受数据
			// 无缓冲的 chan 是完全同步的。
			// 所以， 这里再开一个 gorotine 来向管道中发送数据，如果没有接收方接受数据，
			// 也不至于阻塞当前的 gorotine， 导致 schedule 无法正常退出（卡在 wg.Wati())，
			// 而是阻塞在这里新开的 gorotine。
			// 当后续调用 reduce 任务时，reduce 任务需要 worker，就会从 chan 中取 worker，
			// 那么 在 map 任务中阻塞的 gorotine 就会从阻塞状态中恢复。
			// 
			// TODO: 但是最后执行 reduce 任务时，当所有 reduce 任务全部执行完毕，那么最后会有两个
			// gorotine 阻塞在 rc<-w，我不确定这是否会导致一些问题(就目前看来这样做是可以正常工作的)，
			// 也许在我堆 go 有更深入的使用经验时再来解答这个问题。
			go func(){rc <- w}()
		}
	}
	// each task call a worker
	for t := 0; t < ntasks; t++ {
		worker := <-registerChan
		if phase == mapPhase{
			wg.Add(1)
			go startTask(worker,
						 DoTaskArgs{jobName, mapFiles[t], phase,
									t, n_other},
						 registerChan)
		} else if phase == reducePhase{
			wg.Add(1)
			go startTask(worker,
						 DoTaskArgs{jobName, "", phase, t, n_other},
						 registerChan)
		} else {
			log.Fatal("Invalide job phase: %s", phase)
		}
	}
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
