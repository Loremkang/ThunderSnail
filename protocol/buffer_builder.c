#include "buffer_builder.h"


void BufferBuilderInit(BufferBuilder *builder, CpuToDpuBufferDescriptor *bufferDesc)
{
  // alloc the whole buffer
  builder->bufferDesc = bufferDesc;
  builder->buffer = malloc(BUFFER_LEN);
  *builder->buffer = bufferDesc->epochNumber;
  // skip blockCnt and totalSize, later to fill them
  builder->curPtr = builder->buffer + BUFFER_HEAD_LEN;
  builder->curOffset = BUFFER_HEAD_LEN;

  builder->curBlock = 0;
  return;
}

void BufferBuilderBeginBlock(BufferBuilder *builder, Task* firstTask)
{
  // task type
  uint8_t taskType = firstTask->taskType;
  *builder->curPtr = taskType;

  // block descriptor fill
  if (isVarLenTask(taskType)) {
    builder->bufferDesc->varLenBlockDescs[curBlock].blockDescBase = {
      .taskType = taskType,
      .taskCount = 1,
      .totalSize = GetTaskSize(firstTask, true);
    };
  } else {
    builder->bufferDesc->fixedLenBlockDescs[curBlock].blockDescBase = {
      .taskType = taskType,
      .taskCount = 1,
      .totalSize = GetTaskSize(firstTask);
    };
  }
}

void BufferBuilderEndBlock(BufferBuilder *builder)
{
}

void BufferBuilderFinish(BufferBuilder *builder)
{
}

void BufferBuilderAppendTask(BufferBuilder *builder, Task *task)
{

}
