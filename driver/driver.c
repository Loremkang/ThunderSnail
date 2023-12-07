#include "driver.h"
#include "fake_interface.h"
#include "safety_check_macro.h"
#include "protocol.h"

void BuildGetOrInsertReqOnHashTableIndex(int batchSize, int TableId, TupleIdT* tupleIds, int hashTableIndex) {
    ValidValueCheck(hashTableIndex >= 0 && hashTableIndex < CatalogHashTableCountGet(TableId));
    
}

void BatchInsertValidCheck(int batchSize, TupleIdT* tupleIds) {
    for (int i = 1; i < batchSize; i++) {
        ValidValueCheck(tupleIds[i].tableId == tupleIds[i - 1].tableId);
    }
    for (int i = 0; i < batchSize; i++) {
        ValidValueCheck(tupleIds[i].tableId >= 1 && tupleIds[i].tableId <= 5);
        ValidValueCheck(tupleIds[i].tupleAddr >= 0 && tupleIds[i].tupleAddr < (1 << 24));
    }
}

void BatchInsertTuple(int batchSize, TupleIdT *tupleIds) {
    BatchInsertValidCheck(batchSize, tupleIds);
    int HashTableCount = CatalogHashTableCountGet(tupleIds[0].tableId);
    for (int i = 0; i < HashTableCount; i++) {
        BuildGetOrInsertReqOnHashTableIndex(batchSize, tupleIds[0].tableId, tupleIds, i);
    }
    
}

// void SendGetOrInsertReq(struct dpu_set_t set, uint32_t tableId, HashTableId hashTableId, Key *keys, uint64_t *tupleAddrs,
//                         size_t batchSize, uint8_t *recvBuffers[])
// {
//   // prepare dpu buffers
//   uint8_t *buffers[NUM_DPU];
//   size_t sizes[NUM_DPU];
//   BufferBuilder builders[NUM_DPU];
//   CpuToDpuBufferDescriptor bufferDescs[NUM_DPU];
//   for (int i = 0; i < NUM_DPU; i++) {
//     memset(&bufferDescs[i], 0, sizeof(CpuToDpuBufferDescriptor));
//     bufferDescs[i] = (CpuToDpuBufferDescriptor) {
//       .header = {
// 	.epochNumber = GetEpochNumber(),
//       }
//     };
//   }
//   for (int i = 0; i < NUM_DPU; i++) {
//     BufferBuilderInit(&builders[i], &bufferDescs[i]);
//     BufferBuilderBeginBlock(&builders[i], GET_OR_INSERT_REQ);
//   }
//   for (int i = 0; i < batchSize; i++){
//     int dpuIdx = i % NUM_DPU;
//     size_t taskSize = ROUND_UP_TO_8(keys[i].len) + sizeof(GetOrInsertReq);
//     GetOrInsertReq *req = malloc(taskSize);
//     memset(req, 0, taskSize);
//     req->base = (Task) { .taskType = GET_OR_INSERT_REQ };
//     req->len = keys[i].len;
//     req->tid = (TupleIdT) { .tableId = tableId, .tupleAddr = tupleAddrs[0] };
//     req->hashTableId = hashTableId;
//     memcpy(req->ptr, keys[i].data, keys[i].len);
//   // append one task for each dpu
//     BufferBuilderAppendTask(&builders[dpuIdx], (Task*)req);
//   }
//   // end block
//   for (int i = 0; i < NUM_DPU; i++) {
//     BufferBuilderEndBlock(&builders[i]);
//     buffers[i] = BufferBuilderFinish(&builders[i], &sizes[i]);
//   }
//   // get max sizes
//   qsort(sizes, NUM_DPU, sizeof(size_t), cmpfunc);
//   // send
//   struct dpu_set_t dpu;

//   uint32_t idx;
//   DPU_FOREACH(set, dpu, idx) {
//     DPU_ASSERT(dpu_prepare_xfer(dpu, buffers[idx]));
//   }
//   DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "receiveBuffer", 0, sizes[0], DPU_XFER_DEFAULT));
//   DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));
//   // ReadDpuSetLog(set);
//   // receive
//   DPU_FOREACH(set, dpu, idx) {
//     DPU_ASSERT(dpu_prepare_xfer(dpu, recvBuffers[idx]));
//   }
//   // how to get the reply buffer size? it seems that the reply buffer size will less then send buffer size
//   DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_FROM_DPU, "replyBuffer", 0, sizes[0], DPU_XFER_DEFAULT));
//   //free
//   for (int i = 0; i < NUM_DPU; i++) {
//     free(buffers[i]);
//   }
// }