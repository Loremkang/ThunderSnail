#include <barrier.h>
#include <defs.h>
#include "index_req.h"
#include "task_executor.h"

BARRIER_INIT(barrier1, NR_TASKLETS);
BARRIER_INIT(barrier2, NR_TASKLETS);

static void KernalInitial() {
    mem_reset();
    IndexAllocatorInit();
    IndexSpaceInit();
    // other initial function
}

static void KernalReduce() {}

static int Master() {
    KernalInitial();
    barrier_wait(&barrier1);
    barrier_wait(&barrier2);
    KernalReduce();
    return 0;
}

static int Slave() {
    barrier_wait(&barrier1);
    uint32_t taskletId = me(); // taskletId: 1-17
    __dma_aligned uint8_t taskBuf[TASK_MAX_LEN];
    Task *task = (Task*)taskBuf;
    while (true) {
        GetTaskStateT state = GetTaskletNextTask(taskletId, task);
        if (NO_MORE_TASK == state) { 
            // All tasks consumed
            break;
        }
        if (NEW_BLOCK == state) {
            TaskletBufferBuilderEndBlock(taskletId);
            TaskletBufferBuilderBeginBlock(taskletId);
        }
        TaskletExecuteTaskThenAppend(taskletId, task);
    }
    barrier_wait(&barrier2);
    return 0;
}

int main() {
    return me() == 0 ? Master() : Slave();
}