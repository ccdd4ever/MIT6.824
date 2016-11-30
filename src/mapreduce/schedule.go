package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	//
	var wg sync.WaitGroup //等待一组goroutine执行完成,类似Java CountDownLatch
	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		go func(taskIndex int) {
			defer wg.Done()
			w := <-mr.registerChannel
			for {
				var args DoTaskArgs
				args.File = mr.files[taskIndex]
				args.JobName = mr.jobName
				args.TaskNumber = taskIndex
				args.NumOtherPhase = nios
				args.Phase = phase

				//这么搞只能玩,一旦服务调不通电脑风扇秒变涡扇发动机,别问我怎么知道的
				ok := call(w, "Worker.DoTask", &args, new(struct{}))
				if ok {
					go func() {
						//go channel真是神器!
						mr.registerChannel <- w
					}()
					break
				} else {
					w = <-mr.registerChannel //失败时重新选取worker
					continue
				}
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
