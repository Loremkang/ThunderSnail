#pragma once
#ifndef TASK_H
#define TASK_H

#include <stdint.h>
#include "common_base_struct.h"

typedef struct {
  uint8_t taskType;
} Task;

#define TASK(NAME, ID, FIXED, LEN, CONTENT) \
  typedef ALIGN8 struct CONTENT NAME; \
  extern const int NAME##_id; \
  extern const bool NAME##_fixed; \
  extern const int NAME##_task_len;

#define task_fixed(NAME) (NAME##_fixed)
#define task_len(NAME) (NAME##_task_len)
#define task_id(NAME) (NAME##_id)


#include "task_def.h"

uint8_t RespTaskType(uint8_t taskType);
uint16_t GetFixedLenTaskSize(void *task);
bool IsVarLenTask(uint8_t taskType);
#endif