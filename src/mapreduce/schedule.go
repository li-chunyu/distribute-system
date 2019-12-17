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
	var wg sync.WaitGroup
	// doTask: 
	// 		1. 从 rc 中查询一个 worker
	//      2. 如果执行成功把 worker 放回rc
	//      3. 如果 worker falied，则丢弃 worker，并取一个新的 worker 执行任务。
	// 从实验的文档来看，并不会出现一个任务被2个 worker 同时执行的情况（实际中这是可能发生的）
	// 如果 call 是同步的那么，则可以完全避免这种情况（一个 task 被2个 worker 同时执行），
	// 但是把 call 实现为同步的，明显是不合理的。
	// TODO: 等我看完 mapreduce 的全部实现，再来解答这个问题。
	doTask := func(arg DoTaskArgs, rc chan string) {
		defer wg.Done()
		done := false
		var wk string
		for !done {
			wk = <-rc
			done = call(wk, "Worker.DoTask", arg, nil)
			if !done {
				log.Printf("worker <%s> falied", wk)
			}
		}
		// worker 的channel是无缓冲的，也就是说 rc <- w 向管道 rc 中发送数据，
		// 如果没有接受方接受数据，那么就会一直阻塞在 rc<- w 直到有接收方接受数据
		// 无缓冲的 chan 是完全同步的。
		// 无缓冲的 chan 可以看为缓冲为0的有缓冲 chan（读起来像是废话，事实上也确实是废话）,即便是缓冲
		// 为1的 chan 其行为也与无缓冲的 chan 是截然不同的。无缓冲的 chan ，其接收方和发送方必须是        
		// 手递手的，而有缓冲的 chan 则不必如此。
		// 所以， 这里再开一个 gorotine 来向管道中发送数据，如果没有接收方接受数据，
		// 也不至于阻塞当前的 gorotine， 导致 schedule 无法正常退出（卡在 wg.Wati())，
		// 而是阻塞在这里新开的 gorotine。
		// 当后续调用 reduce 任务时，reduce 任务需要 worker，就会从 chan 中取 worker，
		// 那么 在 map 任务中阻塞的 gorotine 就会从阻塞状态中恢复。
		// 
		// TODO: 但是最后执行 reduce 任务时，当所有 reduce 任务全部执行完毕，那么最后会有两个
		// gorotine 阻塞在 rc<-w，我不确定这是否会导致一些问题(就目前看来这样做是可以正常工作的)，
		// 也许在我堆 go 有更深入的使用经验时再来解答这个问题。
		go func(){rc <- wk}()
	}

	for t := 0; t < ntasks; t++ {
		if phase == mapPhase{
			wg.Add(1)
			go doTask(DoTaskArgs{jobName, mapFiles[t], phase,
									t, n_other},
						registerChan)
		} else if phase == reducePhase{
			wg.Add(1)
			go doTask(DoTaskArgs{jobName, "", phase, t, n_other},
						 registerChan)
		} else {
			log.Fatal("Invalide job phase: %s", phase)
		}
	}
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}