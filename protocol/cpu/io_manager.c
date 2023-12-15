
#include "io_manager.h"

uint8_t GlobalIOBuffers[NUM_DPU][BUFFER_LEN];
Offset GlobalOffsetsBuffer[NUM_DPU][NUM_BLOCKS];
Offset GlobalVarlenBlockOffsetBuffer[NUM_DPU][BATCH_SIZE];

void IOManagerSend(IOManagerT *manager) {
    struct dpu_set_t dpu;
    uint32_t idx;

    DPU_FOREACH(*manager->dpu_set, dpu, idx) {
        DPU_ASSERT(dpu_prepare_xfer(dpu, manager->sendIOBuffers[idx]));
    }
    DPU_ASSERT(dpu_push_xfer(*manager->dpu_set, DPU_XFER_TO_DPU, "receiveBuffer", 0, manager->maxSendSize,
                             DPU_XFER_DEFAULT));
}

void IOManagerExec(IOManagerT *manager) {
    DPU_ASSERT(dpu_launch(*manager->dpu_set, DPU_SYNCHRONOUS));
    // ReadDpuSetLog(set);
}

void IOManagerReceive(IOManagerT *manager) {
    struct dpu_set_t dpu;
    uint32_t idx;

    DPU_FOREACH(*manager->dpu_set, dpu, idx) {
        DPU_ASSERT(dpu_prepare_xfer(dpu, manager->recvIOBuffers[idx]));
    }

    DPU_ASSERT(dpu_push_xfer(*manager->dpu_set, DPU_XFER_FROM_DPU, "replyBuffer", 0, sizeof(DpuBufferHeader),
                             DPU_XFER_DEFAULT));

    for (int i = 0; i < NUM_DPU; i++) {
        DpuBufferHeader* header = (DpuBufferHeader*)manager->recvIOBuffers[i];
        manager->recvSizes[i] = header->totalSize;
    }
    manager->maxReceiveSize = max_in_array(NUM_DPU, manager->recvSizes);

    ArrayOverflowCheck(manager->maxReceiveSize <= BUFFER_LEN);

    DPU_FOREACH(*manager->dpu_set, dpu, idx) {
        DPU_ASSERT(dpu_prepare_xfer(dpu, manager->recvIOBuffers[idx]));
    }
    
    DPU_ASSERT(dpu_push_xfer(*manager->dpu_set, DPU_XFER_FROM_DPU, "replyBuffer", 0, manager->maxReceiveSize,
                             DPU_XFER_DEFAULT));
}