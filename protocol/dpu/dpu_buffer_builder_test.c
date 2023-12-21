#include "../protocol.h"
#include "dpu_buffer_builder.h"
#include <string.h>
#include <alloc.h>

// void CreateDpuToCpuBufferForEachDPU()
// {
//   buddy_init(1024);
//   size_t size;
//   BufferBuilder builder, *B;
//   B = &builder;
//   Task *tasks[BATCH_SIZE];
//   GetMaxLinkSizeResp *resps[BATCH_SIZE];
//   for (int i = 0; i < BATCH_SIZE; i++) {
//     resps[i] = buddy_alloc(sizeof(GetMaxLinkSizeResp));
//     resps[i] = &(GetMaxLinkSizeResp) {
//       .base = { .taskType = GET_MAX_LINK_SIZE_RESP },
//       .maxLinkSize = i % 255
//     };
//     tasks[i] = (Task*)&resps[i];
//   }
//   BufferBuilderInit(B);
//   // suppose we have only one block
//   BufferBuilderBeginBlock(B, GET_MAX_LINK_SIZE_RESP);
//   // input stream to get task
//   for (int i = 0; i < BATCH_SIZE; i++) {
//     BufferBuilderAppendTask(B, tasks[i]);
//   }
//   BufferBuilderEndBlock(B);
//   size = BufferBuilderFinish(B);
//   for (int i = 0; i < BATCH_SIZE; i++) {
//     buddy_free(resps[i]);
//   }

//   return;
// }
