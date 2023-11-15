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
  if (IsVarLenTask(taskType)) {
    builder->bufferDesc->varLenBlockDescs[builder->curBlock].blockDescBase = (BlockDescriptorBase) {
      .taskType = taskType,
      .taskCount = 1,
      .totalSize = GetTaskSize(firstTask, true)
    };
  } else {
    builder->bufferDesc->fixedLenBlockDescs[builder->curBlock].blockDescBase = (BlockDescriptorBase) {
      .taskType = taskType,
      .taskCount = 1,
      .totalSize = GetTaskSize(firstTask, false)
    };
  }
}

void BufferBuilderEndBlock(BufferBuilder *builder)
{
}

uint8_t* BufferBuilderFinish(BufferBuilder *builder, size_t *size)
{
  return builder->buffer;
}

void BufferBuilderAppendTask(BufferBuilder *builder, Task *task)
{

}
