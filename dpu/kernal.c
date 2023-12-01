#include <barrier.h>
#include <defs.h>
#include <mutex.h>
#include "index_req.h"
#include "task_executor.h"
#include "shared_wram.h"

BARRIER_INIT(barrierPackage1, NR_TASKLETS);
BARRIER_INIT(barrierPackage2, NR_TASKLETS);
BARRIER_INIT(barrierBlock1, NR_TASKLETS);
BARRIER_INIT(barrierBlock2, NR_TASKLETS);

MUTEX_INIT(builderMutex);

static void KernalInitial() {
    mem_reset();
    IndexAllocatorInit();
    IndexSpaceInit();
    BufferDecoderInit(&g_decoder);
    BufferBuilderInit(&g_builder);
    // initial g_dpuId reservation
    // other initial function
}

static void KernalReduce() {}

static int Master() {
    KernalInitial();
    barrier_wait(&barrierPackage1);
    
    for (int i = 0; i < g_decoder.bufHeader.blockCnt; i++) {
        uint8_t taskType = g_decoder.blockHeader.taskType;
        BufferBuilderBeginBlock(&g_builder, RespTaskType(taskType));

        switch (taskType){
            case GET_OR_INSERT_REQ:
            {
                barrier_wait(&barrierBlock1);
                break;
            }
            default:
                Unimplemented("Other tasks need to be supported.\n");
        }
        barrier_wait(&barrierBlock2);
        BufferBuilderEndBlock(&g_builder);
    }

    barrier_wait(&barrierPackage2);
    KernalReduce();
    return 0;
}

static int Slave() {
    barrier_wait(&barrierPackage1);
    uint32_t slaveTaskletId = me() - 1; // slaveTaskletId: 0-16

    for (int i = 0; i < g_decoder.bufHeader.blockCnt; i++) {
        uint8_t taskType = g_decoder.blockHeader.taskType;
        uint32_t taskCnt = g_decoder.blockHeader.taskCount;
        __dma_aligned uint8_t taskBuf[TASK_MAX_LEN];
        Task *task = (Task *)taskBuf;

        switch (taskType){
            case GET_OR_INSERT_REQ:
            {
                barrier_wait(&barrierBlock1);
                uint32_t slaveTaskletTaskStart = BLOCK_LOW(slaveTaskletId, NR_SLAVE_TASKLETS, taskCnt);
                uint32_t slaveTaskletTaskCnt = BLOCK_SIZE(slaveTaskletId, NR_SLAVE_TASKLETS, taskCnt);
                __dma_aligned HashTableQueryReplyT reply_buffer;
                GetOrInsertReq *req;
                for(int j = slaveTaskletTaskStart; j < slaveTaskletTaskCnt; j++) {
                    GetKthTask(&g_decoder, j, task);
                    req = (GetOrInsertReq *)task;
                    primary_index_dpu *pid = IndexCheck(req->hashTableId);
                    IndexGetOrInsertReq(pid, (char *)(req->ptr), req->len, req->tid, &reply_buffer);
                    GetOrInsertResp resp = {
                        .base = {.taskType = GET_OR_INSERT_RESP},
                        .tupleIdOrMaxLinkAddr = reply_buffer};
                    mutex_lock(builderMutex);
                    BufferBuilderAppendTask(&g_builder, (Task *)&resp);
                    mutex_unlock(builderMutex);
                }
                break;
            }
            default:
                Unimplemented("Other tasks need to be supported.\n");
        }
        barrier_wait(&barrierBlock2);
    }
    barrier_wait(&barrierPackage2);
    return 0;
}

int main() {
    return me() == 0 ? Master() : Slave();
}