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

// assuming T1 --[H1]-- T2 --[H2]-- T3 --[H3]-- T4 --[H4]-- T5
// Tx = [k1 : 8 bytes, k2 : 8 bytes]
// Tuple(Table = i, Addr/Row = j) = [(i << 24) + j, ((i + 1) << 24) + j]
// Tuple(T1, j) = [0..j, 1..j]
// Tuple(T2, j) = [1..j, 2..j]
// H1(j) = [1..j]

#define TEST_BATCH (10)
#define TABLE_COUNT (5)

TEST(Driver, Driver) {

    DriverT* driver = (DriverT*)malloc(sizeof(DriverT));
    DriverInit(driver);

    for (int tableID = 1; tableID <= TABLE_COUNT; tableID ++) {
        int edgeCount = (i == 1 || i == 5) ? 1 : 2;
        EdgeIDT edges[2];
        if (i == 1) {
            edges[0] = 1;
        } else if (i >= 2 && i <= 4) {
            edges[0] = i - 1;
            edges[1] = i;
        } else {
            edges[0] = 4;
        }
        CatalogInitTable(&driver->catalog, i, edgeCount, edges);
    }

    // init
    for (int hashTableId = 1; hashTableId <= 4; hashTableId++) {
        SendCreateIndexReq(driver->dpu_set, hashTableId);
    }

    assert(TABLE_COUNT == 5);

    TupleIdT tupleId[TABLE_COUNT + 1][TEST_BATCH];
    uint64_t keys[TABLE_COUNT + 1][TEST_BATCH][2];
    KeyT keyBuffer[TABLE_COUNT + 1][TEST_BATCH * 2];
    for (int tableId = 1; tableId <= TABLE_COUNT; tableId++) {
        for (uint32_t i = 0; i < TEST_BATCH; i++) {
            keys[tableId][i][0] = ((tableId - 1) << 24) + i;
            keys[tableId][i][1] = ((tableId) << 24) + i;
            tupleId[tableId][i] = (TupleIdT){.tableId = tableId, .tupleAddr = i + 1};
        }
        int keyCount = 0;
        int edgeCount = (i == 1 || i == 5) ? 1 : 2;
        if (i > 1) { // left side
            for (uint32_t i = 0; i < TEST_BATCH; i ++) {
                keyBuffer[tableId][keyCount].size = sizeof(uint64_t);
                keyBuffer[tableId][keyCount].buf = (uint8_t*)&keys[tableId][i][0];
                keyCount ++;
            }
        }
        if (i < 5) { // right side 
            for (uint32_t i = 0; i < TEST_BATCH; i ++) {
                keyBuffer[tableId][keyCount].size = sizeof(uint64_t);
                keyBuffer[tableId][keyCount].buf = (uint8_t*)&keys[tableId][i][1];
                keyCount ++;
            }
        }
        ValidValueCheck(keyCount == edgeCount * TEST_BATCH);
    }

    for (int tableId = 1; tableId <= TABLE_COUNT; tableId++) {
        EXPECT_EQ(TEST_BATCH, DriverBatchInsertTupleWithKeys(driver, TEST_BATCH, tupleId[tableId], keyBuffer[tableId]));
    }

    DriverFree(driver);
    free(driver);
}