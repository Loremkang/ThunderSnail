#include <barrier.h>
#include <defs.h>
#include <mutex.h>
#include "index_req.h"
#include "task_executor.h"
#include "shared_wram.h"
#include "maxlink.h"
BARRIER_INIT(barrierPackagePrepare, NR_TASKLETS);
BARRIER_INIT(barrierPackageReduce, NR_TASKLETS);
BARRIER_INIT(barrierBlockInit, NR_TASKLETS);
BARRIER_INIT(barrierBlockPrepare, NR_TASKLETS);
BARRIER_INIT(barrierBlockReduce, NR_TASKLETS);

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
    barrier_wait(&barrierPackagePrepare);

    for (int i = 0; i < g_decoder.bufHeader.blockCnt; i++) {
        InitNextBlock(&g_decoder);
        barrier_wait(&barrierBlockInit);
        uint8_t taskType = g_decoder.blockHeader.taskType;
        BufferBuilderBeginBlock(&g_builder, RespTaskType(taskType));

        switch (taskType){
            case SET_DPU_ID_REQ:
            {
                __dma_aligned SetDpuIdReq req;
                GetKthTask(&g_decoder, 0, (Task *)(&req));
                g_dpuId = req.dpuId;
                printf("g_dpuId: %u\n", g_dpuId);
                barrier_wait(&barrierBlockPrepare);
                barrier_wait(&barrierBlockReduce);
                break;
            }
            case CREATE_INDEX_REQ:
            {
                __dma_aligned CreateIndexReq req;
                GetKthTask(&g_decoder, 0, (Task *)(&req));
                IndexCreate(req.hashTableId);
                barrier_wait(&barrierBlockPrepare);
                barrier_wait(&barrierBlockReduce);
                break;
            }
            case GET_OR_INSERT_REQ:
            {
                // do some prepare work
                barrier_wait(&barrierBlockPrepare);
                barrier_wait(&barrierBlockReduce);
                break;
            }
            case MERGE_MAX_LINK_REQ:
            {
                // do prepare work
                barrier_wait(&barrierBlockPrepare);
                barrier_wait(&barrierBlockReduce);
                // MaxLinkMerge();
                break;
            }
            case NEW_MAX_LINK_REQ:
            {
                barrier_wait(&barrierBlockPrepare);
                barrier_wait(&barrierBlockReduce);

                break;
            }
            case GET_MAX_LINK_SIZE_REQ:
            {
                barrier_wait(&barrierBlockPrepare);
                barrier_wait(&barrierBlockReduce);

                break;
            }
            default:
                Unimplemented("Other tasks need to be supported.\n");
        }
        BufferBuilderEndBlock(&g_builder);
    }

    barrier_wait(&barrierPackageReduce);
    KernalReduce();
    BufferBuilderFinish(&g_builder);
    return 0;
}

static int Slave() {
    barrier_wait(&barrierPackagePrepare);
    uint32_t slaveTaskletId = me() - 1; // slaveTaskletId: 0-16

    for (int i = 0; i < g_decoder.bufHeader.blockCnt; i++) {
        barrier_wait(&barrierBlockInit);
        uint8_t taskType = g_decoder.blockHeader.taskType;
        uint32_t taskCnt = g_decoder.blockHeader.taskCount;

        switch (taskType){
            case SET_DPU_ID_REQ:
            {
                barrier_wait(&barrierBlockPrepare);
                // do nothing
                break;
            }
            case CREATE_INDEX_REQ:
            {
                barrier_wait(&barrierBlockPrepare);
                __dma_aligned CreateIndexReq req;
                GetKthTask(&g_decoder, 0, (Task *)(&req));
                primary_index_dpu *pid = IndexCheck(req.hashTableId);
                IndexInitBuckets(pid, slaveTaskletId);
                break;
            }
            case GET_OR_INSERT_REQ:
            {
                barrier_wait(&barrierBlockPrepare);
                __dma_aligned uint8_t taskBuf[TASK_MAX_LEN];
                Task *task = (Task *)taskBuf;
                uint32_t slaveTaskletTaskStart = BLOCK_LOW(slaveTaskletId, NR_SLAVE_TASKLETS, taskCnt);
                uint32_t slaveTaskletTaskCnt = BLOCK_SIZE(slaveTaskletId, NR_SLAVE_TASKLETS, taskCnt);
                // printf("slaveTaskletTaskStart: %d, slaveTaskletTaskCnt: %d\n", slaveTaskletTaskStart, slaveTaskletTaskCnt);
                __dma_aligned HashTableQueryReplyT reply_buffer;
                GetOrInsertReq *req;
                for(int j = slaveTaskletTaskStart; j < slaveTaskletTaskStart + slaveTaskletTaskCnt; j++) {
                    GetKthTask(&g_decoder, j, task);
                    req = (GetOrInsertReq *)task;
                    // {
                    //     printf("key = %llx\n", *(uint64_t*)req->ptr);
                    //     printf("keylen = %llx\n", (uint64_t)req->len);
                    // }
                    primary_index_dpu *pid = IndexCheck(req->hashTableId);
                    IndexGetOrInsertReq(pid, (char *)(req->ptr), req->len, req->tid, &reply_buffer);
                    // {
                    //     printf("reply_buffer type: %d, %d, %p\n", reply_buffer.type, reply_buffer.value.hashAddr.rPtr.dpuId,
                    //         reply_buffer.value.hashAddr.rPtr.dpuAddr);
                    // }
                    GetOrInsertResp resp = {
                        .base = {.taskType = GET_OR_INSERT_RESP},
                        .taskIdx = req->taskIdx,
                        .tupleIdOrMaxLinkAddr = reply_buffer};
                    mutex_lock(builderMutex);
                    BufferBuilderAppendTask(&g_builder, (Task *)&resp);
                    mutex_unlock(builderMutex);
                }
                break;
            }
            case MERGE_MAX_LINK_REQ:
            {
                barrier_wait(&barrierBlockPrepare);
                __dma_aligned uint8_t taskBuf[TASK_MAX_LEN];
                Task *task = (Task *)taskBuf;
                uint32_t slaveTaskletTaskStart = BLOCK_LOW(slaveTaskletId, NR_SLAVE_TASKLETS, taskCnt);
                uint32_t slaveTaskletTaskCnt = BLOCK_SIZE(slaveTaskletId, NR_SLAVE_TASKLETS, taskCnt);
                MergeMaxLinkReq *req;
                for(int j = slaveTaskletTaskStart; j < slaveTaskletTaskStart + slaveTaskletTaskCnt; j++) {
                    GetKthTask(&g_decoder, j, task);
                    req = (MergeMaxLinkReq *)task;
                    // process MergeMaxLinkReq
                }
                break;
            }
            case NEW_MAX_LINK_REQ:
            {
                barrier_wait(&barrierBlockPrepare);
                __dma_aligned uint8_t taskBuf[TASK_MAX_LEN];
                Task *task = (Task *)taskBuf;
                uint32_t slaveTaskletTaskStart = BLOCK_LOW(slaveTaskletId, NR_SLAVE_TASKLETS, taskCnt);
                uint32_t slaveTaskletTaskCnt = BLOCK_SIZE(slaveTaskletId, NR_SLAVE_TASKLETS, taskCnt);
                NewMaxLinkReq *req;
                for(int j = slaveTaskletTaskStart; j < slaveTaskletTaskStart + slaveTaskletTaskCnt; j++) {
                    GetKthTask(&g_decoder, j, task);
                    req = (NewMaxLinkReq *)task;
                    // process MergeMaxLinkReq
                    __mram_ptr MaxLinkEntryT* entry = NewMaxLinkEntry(&req->maxLink);
                    NewMaxLinkResp resp = {
                        .base = {.taskType = NEW_MAX_LINK_RESP},
                        .ptr = {.dpuId = g_dpuId, .dpuAddr = entry }};
                        
                    mutex_lock(builderMutex);
                    BufferBuilderAppendTask(&g_builder, (Task *)&resp);
                    mutex_unlock(builderMutex);
                }
                break;
            }
            case GET_MAX_LINK_SIZE_REQ:
            {
                barrier_wait(&barrierBlockPrepare);
                __dma_aligned uint8_t taskBuf[TASK_MAX_LEN];
                Task *task = (Task *)taskBuf;
                uint32_t slaveTaskletTaskStart = BLOCK_LOW(slaveTaskletId, NR_SLAVE_TASKLETS, taskCnt);
                uint32_t slaveTaskletTaskCnt = BLOCK_SIZE(slaveTaskletId, NR_SLAVE_TASKLETS, taskCnt);
                GetMaxLinkSizeReq *req;
                for(int j = slaveTaskletTaskStart; j < slaveTaskletTaskStart + slaveTaskletTaskCnt; j++) {
                    GetKthTask(&g_decoder, j, task);
                    req = (GetMaxLinkSizeReq *)task;
                    // process MergeMaxLinkReq
                    __mram_ptr MaxLinkEntryT* entry = (__mram_ptr MaxLinkEntryT*)req->maxLinkAddr.rPtr.dpuAddr;
                    uint32_t size = GetMaxLinkSize(entry);
                    GetMaxLinkSizeResp resp = {
                        .base = {.taskType = GET_MAX_LINK_SIZE_RESP},
                        .taskIdx = req->taskIdx,
                        .maxLinkSize = size
                    };
                    mutex_lock(builderMutex);
                    BufferBuilderAppendTask(&g_builder, (Task *)&resp);
                    mutex_unlock(builderMutex);
                }
                break;
            }
            default:
                Unimplemented("Other tasks need to be supported.\n");
        }
        barrier_wait(&barrierBlockReduce);
    }
    barrier_wait(&barrierPackageReduce);
    return 0;
}

int main() {
    return me() == 0 ? Master() : Slave();
}
