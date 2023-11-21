#pragma once
#ifndef MRAM_SPACE_H
#define MRAM_SPACE_H

#include <mram.h>

#define CPU_DPU_COMMUNICATION_SPACE_ADDR ((__mram_ptr void *)DPU_MRAM_HEAP_POINTER)
#define CPU_DPU_COMMUNICATION_SPACE_SIZE (0x1000000 - DPU_MRAM_HEAP_POINTER)

#define MAX_LINK_SPACE_ADDR ((__mram_ptr void *)(0x1000000))
#define MAX_LINK_SPACE_SIZE (0x1000000)

#define INDEX_SPACE_ADDR ((__mram_ptr void *)(0x2000000))
#define INDEX_SPACE_SIZE (0x1000000)

#define HEAP_SPACE_ADDR ((__mram_ptr void *)(0x3000000))
#define HEAP_SPACE_SIZE (0x1000000)

#endif
