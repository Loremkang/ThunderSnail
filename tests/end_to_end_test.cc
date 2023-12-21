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
#define TABLE_COUNT (5)

TEST(Driver, Driver) {

    DriverT* driver = (DriverT*)malloc(sizeof(DriverT));
    DriverInit(driver);

    // init
    for (int hashTableId = 1; hashTableId <= 4; hashTableId++) {
        SendCreateIndexReq(driver->dpu_set, hashTableId);
    }

    assert(TABLE_COUNT == 5);

    TupleIdT tupleId[TABLE_COUNT + 1][TEST_BATCH];
    for (int tableId = 1; tableId <= TABLE_COUNT; tableId++) {
        for (uint32_t i = 0; i < TEST_BATCH; i++) {
            tupleId[tableId][i] = (TupleIdT){.tableId = tableId, .tupleAddr = i + 1};
        }
        EXPECT_EQ(TEST_BATCH, DriverBatchInsertTuple(driver, TEST_BATCH, tupleId[tableId]));
    }

    DriverFree(driver);
    free(driver);
}