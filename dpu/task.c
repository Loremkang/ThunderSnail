
#include "task.h"
#ifdef TASK 
#undef TASK
#endif

#define TASK(NAME, ID, FIXED, LEN, CONTENT) \
  const int NAME##_id = (ID); \
  const bool NAME##_fixed = (FIXED); \
  const int NAME##_task_size = (LEN);

#include "task_def.h"
