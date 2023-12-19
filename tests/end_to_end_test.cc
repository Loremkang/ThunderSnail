#include <gtest/gtest.h>

extern "C" {
// #include "../protocol/cpu/requests.h"
// #include "../protocol/cpu/requests_handler.h"
#include "cpu_buffer_builder.h"
#include "hash_function.h"
#include "requests_handler.h"
#include "requests.h"
#include "io_manager.h"
#include "driver.h"
// #include "iterators.h"
}

#define TEST_BATCH (10)

TEST(Driver, Driver) {

    DriverT* driver = (DriverT*)malloc(sizeof(DriverT));
    DriverInit(driver);

    // init
    for (int hashTableId = 1; hashTableId < 4; hashTableId++) {
        SendCreateIndexReq(driver->dpu_set, hashTableId);
    }

    TupleIdT tupleIdT1[TEST_BATCH];
    TupleIdT tupleIdT2[TEST_BATCH];
    for (uint32_t i = 0; i < TEST_BATCH; i++) {
        tupleIdT1[i] = (TupleIdT){.tableId = 1, .tupleAddr = i + 1};
        tupleIdT2[i] = (TupleIdT){.tableId = 2, .tupleAddr = i + 1};
    }

    DriverBatchInsertTuple(driver, TEST_BATCH, tupleIdT1);
    DriverBatchInsertTuple(driver, TEST_BATCH, tupleIdT2);

    DriverFree(driver);
    free(driver);
}