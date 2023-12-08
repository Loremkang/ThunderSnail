#pragma once
#ifndef TASK_H
#define TASK_H

#ifdef TASK 
#undef TASK
#endif
#include <stdint.h>

typedef struct {
  uint8_t taskType;
} Task;

#define TASK(NAME, ID, FIXED, LEN, CONTENT) \
  typedef ALIGN8 struct CONTENT NAME; \
  extern const int NAME##_id; \
  extern const bool NAME##_fixed; \
  extern const int NAME##_task_size;

#define task_fixed(NAME) (NAME##_fixed)
#define task_size(NAME) (NAME##_task_size)
#define task_id(NAME) (NAME##_id)

#include "task_def.h"
#endif