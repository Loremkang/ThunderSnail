#include <barrier.h>
#include <defs.h>
#include <mutex.h>
#include <alloc.h>
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

    uint8_t taskType;
    for (int i = 0; i < g_decoder.bufHeader.blockCnt;) {
      if (g_decoder.remainingTaskCnt == 0) {
        DecoderStateT state = InitNextBlock(&g_decoder);
        taskType = g_decoder.blockHeader.taskType;
        // printf("%d %d\n", i, (int)taskType);
        BufferBuilderBeginBlock(&g_builder, RespTaskType(taskType));
      }
        InitTaskOffsets(&g_decoder);
        if (g_decoder.remainingTaskCnt == 0) {
          i = i + 1;
        }
        // printf("Next Block: %d\n", (int)state);
        barrier_wait(&barrierBlockInit);
        __dma_aligned uint8_t taskBuf[TASK_MAX_LEN];

        switch (taskType){
            case SET_DPU_ID_REQ:
            {
                SetDpuIdReq *req = (SetDpuIdReq*)taskBuf;
                GetKthTask(&g_decoder, 0, (Task *)req);
                g_dpuId = req->dpuId;
                counter = 0;
                printf("g_dpuId: %u\n", g_dpuId);
                barrier_wait(&barrierBlockPrepare);
                barrier_wait(&barrierBlockReduce);
                break;
            }
            case CREATE_INDEX_REQ:
            {
                CreateIndexReq *req = (CreateIndexReq *)taskBuf;
                GetKthTask(&g_decoder, 0, (Task *)req);
                IndexCreate(req->hashTableId);
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
            case UPDATE_POINTER_REQ:
            {
                // do some prepare work
                barrier_wait(&barrierBlockPrepare);
                barrier_wait(&barrierBlockReduce);
                break;
            }
            case MERGE_MAX_LINK_REQ:
            {
                barrier_wait(&barrierBlockPrepare);
                barrier_wait(&barrierBlockReduce);
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
            case FETCH_MAX_LINK_REQ:
            {
                barrier_wait(&barrierBlockPrepare);
                barrier_wait(&barrierBlockReduce);

                break;
            }
            case GET_VALID_MAXLINK_COUNT_REQ:
            {
                barrier_wait(&barrierBlockPrepare);
                __dma_aligned GetValidMaxLinkCountResp resp;
                resp.base.taskType = GET_VALID_MAXLINK_COUNT_RESP;
                resp.count = counter;
                BufferBuilderAppendTask(&g_builder, (Task *)&resp);
                barrier_wait(&barrierBlockReduce);
                break;
            }
            default:
                Unimplemented("Other tasks need to be supported.\n");
        }
        if (g_decoder.remainingTaskCnt == 0) {
          BufferBuilderEndBlock(&g_builder);
        }
    }

    barrier_wait(&barrierPackageReduce);
    KernalReduce();
    BufferBuilderFinish(&g_builder);
    return 0;
}

static int Slave() {
    barrier_wait(&barrierPackagePrepare);
    uint32_t slaveTaskletId = me() - 1; // slaveTaskletId: 0-16

    for (int i = 0; i < g_decoder.bufHeader.blockCnt;) {
        barrier_wait(&barrierBlockInit);
        if (g_decoder.remainingTaskCnt == 0) {
          i = i + 1;
        }
        uint8_t taskType = g_decoder.blockHeader.taskType;
        uint32_t taskCnt = g_decoder.blockHeader.taskCount;
        __dma_aligned uint8_t taskBuf[TASK_MAX_LEN];

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
                CreateIndexReq *req = (CreateIndexReq *)taskBuf;
                GetKthTask(&g_decoder, 0, (Task *)req);
                primary_index_dpu *pid = IndexCheck(req->hashTableId);
                IndexInitBuckets(pid, slaveTaskletId);
                break;
            }
            case GET_OR_INSERT_REQ:
            {
                barrier_wait(&barrierBlockPrepare);
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
                    IndexGetOrInsertReq(pid, (char *)(req->ptr), req->len, req->tid, req->hashTableId, &reply_buffer);
                    // {
                    //     printf("reply_buffer type: %d, %d, %p\n", reply_buffer.type, reply_buffer.value.hashAddr.rPtr.dpuId,
                    //         reply_buffer.value.hashAddr.rPtr.dpuAddr);
                    // }
                    __dma_aligned GetOrInsertResp resp = {
                        .base = {.taskType = GET_OR_INSERT_RESP},
                        .taskIdx = req->taskIdx,
                        .tupleIdOrMaxLinkAddr = reply_buffer};
                    mutex_lock(builderMutex);
                    BufferBuilderAppendTask(&g_builder, (Task *)&resp);
                    mutex_unlock(builderMutex);
                }
                break;
            }
            case UPDATE_POINTER_REQ:
            {
                barrier_wait(&barrierBlockPrepare);
                uint32_t slaveTaskletTaskStart = BLOCK_LOW(slaveTaskletId, NR_SLAVE_TASKLETS, taskCnt);
                uint32_t slaveTaskletTaskCnt = BLOCK_SIZE(slaveTaskletId, NR_SLAVE_TASKLETS, taskCnt);
                // printf("slaveTaskletTaskStart: %d, slaveTaskletTaskCnt: %d\n", slaveTaskletTaskStart, slaveTaskletTaskCnt);
                UpdatePointerReq *req = (UpdatePointerReq*)taskBuf;
                for(int j = slaveTaskletTaskStart; j < slaveTaskletTaskStart + slaveTaskletTaskCnt; j++) {
                    GetKthTask(&g_decoder, j, (Task*)req);
                    primary_index_dpu *pid = IndexCheck(req->hashAddr.edgeId);
                    printf("edgeId = %d j = %d\n", req->hashAddr.edgeId, j);
                    IndexUpdateReq(pid, req->hashAddr, req->maxLinkAddr);
                }
                break;
            }
            case MERGE_MAX_LINK_REQ:
            {
                barrier_wait(&barrierBlockPrepare);
                Task *task = (Task *)taskBuf;
                uint32_t slaveTaskletTaskStart = BLOCK_LOW(slaveTaskletId, NR_SLAVE_TASKLETS, taskCnt);
                uint32_t slaveTaskletTaskCnt = BLOCK_SIZE(slaveTaskletId, NR_SLAVE_TASKLETS, taskCnt);
                MergeMaxLinkReq *req;
                for(int j = slaveTaskletTaskStart; j < slaveTaskletTaskStart + slaveTaskletTaskCnt; j++) {
                    GetKthTask(&g_decoder, j, task);
                    req = (MergeMaxLinkReq *)task;
                    // printf("MergeMaxLink: j = %d; addr = ", j);
                    RemotePtrPrint(req->ptr);
                    __mram_ptr MaxLinkEntryT* entry = (__mram_ptr MaxLinkEntryT*) req->ptr.dpuAddr;
                    MergeMaxLink(entry, &req->maxLink);
                    // process MergeMaxLinkReq
                }
                break;
            }
            case NEW_MAX_LINK_REQ:
            {
                // printf("NewMaxLink\n");
                barrier_wait(&barrierBlockPrepare);
                Task *task = (Task *)taskBuf;
                uint32_t slaveTaskletTaskStart = BLOCK_LOW(slaveTaskletId, NR_SLAVE_TASKLETS, taskCnt);
                uint32_t slaveTaskletTaskCnt = BLOCK_SIZE(slaveTaskletId, NR_SLAVE_TASKLETS, taskCnt);
                NewMaxLinkReq *req;
                for(int j = slaveTaskletTaskStart; j < slaveTaskletTaskStart + slaveTaskletTaskCnt; j++) {
                    GetKthTask(&g_decoder, j, task);
                    req = (NewMaxLinkReq *)task;
                    // process MergeMaxLinkReq
                    // __mram_ptr MaxLinkEntryT* entry = INVALID_REMOTEPTR.dpuAddr;
                    __mram_ptr MaxLinkEntryT* entry = NewMaxLinkEntry(&req->maxLink);
                    __dma_aligned NewMaxLinkResp resp = {
                        .base = {.taskType = NEW_MAX_LINK_RESP},
                        .taskIdx = req->taskIdx,
                        .ptr = {.dpuId = g_dpuId, .dpuAddr = entry }};
                    
                    printf("taskIdx=%d\n", req->taskIdx);
                        
                    mutex_lock(builderMutex);
                    BufferBuilderAppendTask(&g_builder, (Task *)&resp);
                    mutex_unlock(builderMutex);
                }
                break;
            }
            case GET_MAX_LINK_SIZE_REQ:
            {
                barrier_wait(&barrierBlockPrepare);
                uint32_t slaveTaskletTaskStart = BLOCK_LOW(slaveTaskletId, NR_SLAVE_TASKLETS, taskCnt);
                uint32_t slaveTaskletTaskCnt = BLOCK_SIZE(slaveTaskletId, NR_SLAVE_TASKLETS, taskCnt);
                // printf("slaveTaskletTaskStart: %d, slaveTaskletTaskCnt: %d\n", slaveTaskletTaskStart, slaveTaskletTaskCnt);
                GetMaxLinkSizeReq *req = (GetMaxLinkSizeReq*)taskBuf;
                for(int j = slaveTaskletTaskStart; j < slaveTaskletTaskStart + slaveTaskletTaskCnt; j++) {
                    GetKthTask(&g_decoder, j, (Task*)req);
                    __mram_ptr MaxLinkEntryT* entry = (__mram_ptr MaxLinkEntryT*)req->maxLinkAddr.rPtr.dpuAddr;
                    // uint32_t size = 0;
                    uint32_t size = GetMaxLinkSize(entry);
                    // printf("%d %p %u\n", j, entry, size);
                    __dma_aligned GetMaxLinkSizeResp resp = {
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
            case FETCH_MAX_LINK_REQ:
            {
                barrier_wait(&barrierBlockPrepare);
                Task *task = (Task *)taskBuf;
                uint32_t slaveTaskletTaskStart = BLOCK_LOW(slaveTaskletId, NR_SLAVE_TASKLETS, taskCnt);
                uint32_t slaveTaskletTaskCnt = BLOCK_SIZE(slaveTaskletId, NR_SLAVE_TASKLETS, taskCnt);
                FetchMaxLinkReq *req;
                // slightly larger than what's really needed. but fine
                // int respSize = ROUND_UP_TO_8((sizeof(FetchMaxLinkResp) + MAX_LINK_ENTRY_SIZE));
                // FetchMaxLinkResp* resp = buddy_alloc(respSize);
                // __dma_aligned uint8_t respBuf[ROUND_UP_TO_8((sizeof(FetchMaxLinkResp) + MAX_LINK_ENTRY_SIZE))];
                // FetchMaxLinkResp* resp = (FetchMaxLinkResp*)respBuf;

                for(int j = slaveTaskletTaskStart; j < slaveTaskletTaskStart + slaveTaskletTaskCnt; j++) {
                    GetKthTask(&g_decoder, j, task);
                    req = (FetchMaxLinkReq *)task;
                    __mram_ptr MaxLinkEntryT* entry = (__mram_ptr MaxLinkEntryT*)req->maxLinkAddr.rPtr.dpuAddr;
                    // resp->base.taskType = FETCH_MAX_LINK_RESP;
                    // resp->taskIdx = req->taskIdx;

                    // A dirty work around to reduce stack usage shown in dpu_stack_analyser.
                    // Although I don't think this actually reduce space usage.
                    // Used to simplify debugging.
                    RetrieveMaxLinkAndAppendResp(entry, req->taskIdx, &builderMutex);

                    // RetrieveMaxLink(entry, &resp->maxLink);
                    // mutex_lock(builderMutex);
                    // BufferBuilderAppendTask(&g_builder, (Task *)resp);
                    // mutex_unlock(builderMutex);
                }
                // buddy_free(resp);
                break;
            }
            case GET_VALID_MAXLINK_COUNT_REQ:
            {
                barrier_wait(&barrierBlockPrepare);
                // do nothing
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
