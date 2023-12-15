#pragma once

#ifndef IO_MANAGER_H
#define IO_MANAGER_H

#include "cpu_buffer_builder.h"
#include "requests.h"
#include "requests_handler.h"

extern uint8_t GlobalIOBuffers[NUM_DPU][BUFFER_LEN];
extern Offset GlobalOffsetsBuffer[NUM_DPU][NUM_BLOCKS];
extern Offset GlobalVarlenBlockOffsetBuffer[NUM_DPU][BATCH_SIZE];


typedef struct IOManagerT {
    struct dpu_set_t* dpu_set;
    BufferBuilder builders[NUM_DPU];
    CpuToDpuBufferDescriptor bufferDescs[NUM_DPU];

    // send and receive may share the same buffer

    size_t sendSizes[NUM_DPU];
    size_t maxSendSize;
    uint8_t *sendIOBuffers[NUM_DPU];
    Offset *sendOffsetBuffers[NUM_DPU];
    Offset *sendVarlenBlockOffsetBuffers[NUM_DPU];

    size_t recvSizes[NUM_DPU];
    size_t maxReceiveSize;
    uint8_t *recvIOBuffers[NUM_DPU];

    OffsetsIterator blockIterators[NUM_DPU];
    OffsetsIterator taskIterators[NUM_DPU];
} IOManagerT;

static inline uint32_t max_in_array(int num, size_t *sizes) {
    size_t max = 0;
    for (int i = 0; i < num; i++) {
        if (sizes[i] > max) {
            max = sizes[i];
        }
    }
    return max;
}

static inline void IOManagerInit(IOManagerT *manager,
                                 struct dpu_set_t* dpu_set,
                                 uint8_t sendIOBuffers[NUM_DPU][BUFFER_LEN],
                                 Offset sendOffsetBuffers[NUM_DPU][NUM_BLOCKS],
                                 Offset sendVarlenBlockOffsetBuffers[NUM_DPU][BATCH_SIZE],
                                 uint8_t recvIOBuffers[NUM_DPU][BUFFER_LEN]) {
    manager->dpu_set = dpu_set;
    for (int i = 0; i < NUM_DPU; i++) {
        manager->sendSizes[i] = 0;
        manager->sendIOBuffers[i] = sendIOBuffers[i];
        manager->sendOffsetBuffers[i] = sendOffsetBuffers[i];
        manager->sendVarlenBlockOffsetBuffers[i] =
            sendVarlenBlockOffsetBuffers[i];
        manager->recvSizes[i] = 0;
        manager->recvIOBuffers[i] = recvIOBuffers[i];
    }
}

static inline void IOManagerStartBufferBuild(IOManagerT *manager) {
    uint8_t epochNumber = GetEpochNumber();
    for (int i = 0; i < NUM_DPU; i++) {
        memset(&manager->bufferDescs[i], 0, sizeof(CpuToDpuBufferDescriptor));
        manager->bufferDescs[i] =
            (CpuToDpuBufferDescriptor){.header = {
                                           .epochNumber = epochNumber,
                                       }};
        BufferBuilderInit(&manager->builders[i], &manager->bufferDescs[i],
                          manager->sendIOBuffers[i],
                          manager->sendOffsetBuffers[i],
                          manager->sendVarlenBlockOffsetBuffers[i]);
    }
}

static inline void IOManagerBeginBlock(IOManagerT *manager, uint8_t taskType) {
    for (int i = 0; i < NUM_DPU; i++) {
        BufferBuilderBeginBlock(&manager->builders[i], taskType);
    }
}

static inline void IOManagerAppendPlaceHolder(IOManagerT *manager, int dpuId, uint8_t taskType, size_t size) {
    BufferBuilderAppendPlaceHolder(&manager->builders[dpuId], taskType, size);
}

static inline void IOManagerAppendTask(IOManagerT *manager, int dpuId,
                                       Task *task) {
    BufferBuilderAppendTask(&manager->builders[dpuId], task);
}

static inline void IOManagerEndBlock(IOManagerT *manager) {
    for (int i = 0; i < NUM_DPU; i++) {
        BufferBuilderEndBlock(&manager->builders[i]);
    }
}

static inline void IOManagerFinish(IOManagerT *manager) {
    for (int i = 0; i < NUM_DPU; i++) {
        BufferBuilderFinish(&manager->builders[i], &manager->sendSizes[i]);
    }
    manager->maxSendSize = max_in_array(NUM_DPU, manager->sendSizes);
}

void IOManagerSend(IOManagerT *manager);
void IOManagerExec(IOManagerT *manager);
void IOManagerReceive(IOManagerT *manager);

static inline void IOManagerSendExecReceive(IOManagerT *manager) {
    IOManagerSend(manager);
    IOManagerExec(manager);
    IOManagerReceive(manager);
}

// static inline void IOManagerInitBlockIterators(IOManagerT *manager) {
//     for (int i = 0; i < NUM_DPU; i++) {
//         manager->blockIterators[i] =
//             BlockIteratorInit(manager->recvBuffers[i]);
//     }

// }

// static inline bool IOManagerHasBlock(IOManagerT *manager) {
//     bool hasNext = OffsetsIteratorHasNext(&manager->blockIterators[0]);
//     for (int i = 0; i < NUM_DPU; i++) {
//         bool dpuHasNext =
//         OffsetsIteratorHasNext(&manager->blockIterators[i]);
//         ValidValueCheck(hasNext == dpuHasNext);
//     }
//     return hasNext;
// }

// static inline bool IOManagerNextBlock(IOManagerT *manager) {
//     ValidValueCheck(IOManagerHasBlock(manager) == true);
//     for (int i = 0; i < NUM_DPU; i++) {
//         OffsetsIteratorNext(&manager->blockIterators[i]);
//     }
//     return hasNext;
// }

// static inline bool IOManagerNextTask(IOManagerT *manager, int dpuId, Task*
// task) {
//     task = OffsetsIteratorGetData(&manager->taskIterators[dpuId]);
//     bool hasNext = OffsetsIteratorHasNext(&manager->blockIterators[dpuId]);
//     if (hasNext) {
//         OffsetsIteratorNext(&manager->blockIterators[dpuId]);
//     }
//     return hasNext;
// }

// {
//     bool ret = OffsetsIteratorHasNext(&manager->blockIterators[dpuId]);
//     OffsetsIteratorNext(&manager->blockIterators[dpuId]);

//     for (int dpuId = 0; dpuId < NUM_DPU; dpuId++) {
//         OffsetsIterator blockIterator = BlockIteratorInit(recvBufs[dpuId]);
//         for (; OffsetsIteratorHasNext(&blockIterator);
//              OffsetsIteratorNext(&blockIterator)) {
//             OffsetsIterator taskIterator = TaskIteratorInit(&blockIterator);
//             for (; OffsetsIteratorHasNext(&taskIterator);
//                  OffsetsIteratorNext(&taskIterator)) {
//                 uint8_t *task = OffsetsIteratorGetData(&taskIterator);
//                 GetOrInsertResp *resp = (GetOrInsertResp *)task;
//                 EXPECT_FALSE(used[resp->taskIdx]);
//                 used[resp->taskIdx] = true;
//                 memcpy(responses + resp->taskIdx, resp,
//                        sizeof(GetOrInsertResp));
//             }
//         }
//     }
// }

#endif  // IO_MANAGER_H