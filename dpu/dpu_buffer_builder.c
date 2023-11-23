#include "dpu_buffer_builder.h"
#include <alloc.h>
#include <stdlib.h>
#include <string.h>

void BufferBuilderInit(BufferBuilder *builder)
{
  buddy_init(BUDDY_LEN);
  // alloc the whole buffer
  builder->bufferDesc.blockCnt = 0;
  builder->bufferDesc.totalSize = DPU_BUFFER_HEAD_LEN;
  // need clear the reply buffer?
  memset(replyBuffer, 0, sizeof(replyBuffer));
  replyBuffer[0] = BUFFER_STATE_OK;

  // skip blockCnt and totalSize, later to fill them
  builder->curBlockPtr = (uint8_t*)replyBuffer + DPU_BUFFER_HEAD_LEN;
  builder->curBlockOffset = DPU_BUFFER_HEAD_LEN;

  builder->varLenBlockIdx = 0;
  builder->fixedLenBlockIdx = 0;

  // offsets reset
  memset(builder->bufferDesc.offsets, 0, sizeof(builder->bufferDesc.offsets));
  return;
}
