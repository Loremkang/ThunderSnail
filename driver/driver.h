#ifndef DRIVER_H
#define DRIVER_H

#include <stdint.h>
#include "common_base_struct.h"
#include "requests.h"
#include "io_manager.h"

void BatchInsertTuple(int batchSize, TupleIdT *tupleIds, struct dpu_set_t* set, IOManagerT *ioManager);

#endif