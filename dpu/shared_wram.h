#pragma once
#ifndef SHARED_WRAM_H
#define SHARED_WRAM_H

#include <stdint.h>
#include <mram.h>
#include "task_executor.h"
#include "protocol/dpu/dpu_buffer_builder.h"

extern uint32_t g_dpuId;
extern __host BufferDecoder g_decoder;
extern __host BufferBuilder g_builder;

#endif