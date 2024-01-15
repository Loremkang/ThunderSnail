#include <gtest/gtest.h>

extern "C" {
// #include "protocol/cpu/requests.h"
// #include "protocol/cpu/requests_handler.h"
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

TEST(Driver, LargeBatchSize) {

    const int TOTAL_COUNT = 64000;
    const int TEST_BATCH = 6400;
    const int TABLE_COUNT = 5;
    DriverT* driver = (DriverT*)malloc(sizeof(DriverT));
    DriverInit(driver);

    for (int tableID = 1; tableID <= TABLE_COUNT; tableID ++) {
        int edgeCount = (tableID == 1 || tableID == 5) ? 1 : 2;
        EdgeIDT edges[2];
        if (tableID == 1) {
            edges[0] = 1;
        } else if (tableID >= 2 && tableID <= 4) {
            edges[0] = tableID - 1;
            edges[1] = tableID;
        } else {
            edges[0] = 4;
        }
        CatalogInitTable(&driver->catalog, tableID, edgeCount, edges);
    }

    // init
    for (int hashTableId = 1; hashTableId <= 4; hashTableId++) {
        SendCreateIndexReq(driver->dpu_set, hashTableId);
    }

    assert(TABLE_COUNT == 5);

    TupleIdT tupleId[TABLE_COUNT + 1][TOTAL_COUNT];
    uint64_t keys[TABLE_COUNT + 1][TOTAL_COUNT][2];
    KeyT keyBuffer[TABLE_COUNT + 1][TOTAL_COUNT * 2];

    for (int tableID = 1; tableID <= TABLE_COUNT; tableID++) {
        for (uint32_t i = 0; i < TOTAL_COUNT; i++) {
            keys[tableID][i][0] = ((tableID - 1) << 24) + i;
            keys[tableID][i][1] = ((tableID) << 24) + i;
            tupleId[tableID][i] = (TupleIdT){.tableId = tableID, .tupleAddr = i + 1};
        }
        int keyCount = 0;
        int edgeCount = (tableID == 1 || tableID == 5) ? 1 : 2;
        if (tableID > 1) { // left side
            for (uint32_t i = 0; i < TOTAL_COUNT; i ++) {
                keyBuffer[tableID][keyCount].size = sizeof(uint64_t);
                keyBuffer[tableID][keyCount].buf = (uint8_t*)&keys[tableID][i][0];
                keyCount ++;
            }
        }
        if (tableID < 5) { // right side 
            for (uint32_t i = 0; i < TOTAL_COUNT; i ++) {
                keyBuffer[tableID][keyCount].size = sizeof(uint64_t);
                keyBuffer[tableID][keyCount].buf = (uint8_t*)&keys[tableID][i][1];
                keyCount ++;
            }
        }
        ValidValueCheck(keyCount == edgeCount * TOTAL_COUNT);
    }

    for (int i = 0; i < TOTAL_COUNT; i += TEST_BATCH) {
        int len = std::min(TEST_BATCH, TOTAL_COUNT - i);
        int result = std::min(TOTAL_COUNT, i + len);
        EXPECT_EQ(result, DriverBatchInsertTupleWithKeys(driver, len, tupleId[1] + i, keyBuffer[1] + i));
        EXPECT_EQ(result, DriverBatchInsertTupleWithKeys(driver, len, tupleId[3] + i, keyBuffer[3] + i));
        EXPECT_EQ(result, DriverBatchInsertTupleWithKeys(driver, len, tupleId[5] + i, keyBuffer[5] + i));
        EXPECT_EQ(result, DriverBatchInsertTupleWithKeys(driver, len, tupleId[4] + i, keyBuffer[4] + i));
        EXPECT_EQ(result, DriverBatchInsertTupleWithKeys(driver, len, tupleId[2] + i, keyBuffer[2] + i));
    }
    

    DriverFree(driver);
    free(driver);
}

// TEST(Driver, Driver) {

//     const int TEST_BATCH = 10;
//     const int TABLE_COUNT = 5;
//     DriverT* driver = (DriverT*)malloc(sizeof(DriverT));
//     DriverInit(driver);

//     for (int tableID = 1; tableID <= TABLE_COUNT; tableID ++) {
//         int edgeCount = (tableID == 1 || tableID == 5) ? 1 : 2;
//         EdgeIDT edges[2];
//         if (tableID == 1) {
//             edges[0] = 1;
//         } else if (tableID >= 2 && tableID <= 4) {
//             edges[0] = tableID - 1;
//             edges[1] = tableID;
//         } else {
//             edges[0] = 4;
//         }
//         CatalogInitTable(&driver->catalog, tableID, edgeCount, edges);
//     }

//     // init
//     for (int hashTableId = 1; hashTableId <= 4; hashTableId++) {
//         SendCreateIndexReq(driver->dpu_set, hashTableId);
//     }

//     assert(TABLE_COUNT == 5);

//     TupleIdT tupleId[TABLE_COUNT + 1][TEST_BATCH];
//     uint64_t keys[TABLE_COUNT + 1][TEST_BATCH][2];
//     KeyT keyBuffer[TABLE_COUNT + 1][TEST_BATCH * 2];
//     for (int tableID = 1; tableID <= TABLE_COUNT; tableID++) {
//         for (uint32_t i = 0; i < TEST_BATCH; i++) {
//             keys[tableID][i][0] = ((tableID - 1) << 24) + i;
//             keys[tableID][i][1] = ((tableID) << 24) + i;
//             tupleId[tableID][i] = (TupleIdT){.tableId = tableID, .tupleAddr = i + 1};
//         }
//         int keyCount = 0;
//         int edgeCount = (tableID == 1 || tableID == 5) ? 1 : 2;
//         if (tableID > 1) { // left side
//             for (uint32_t i = 0; i < TEST_BATCH; i ++) {
//                 keyBuffer[tableID][keyCount].size = sizeof(uint64_t);
//                 keyBuffer[tableID][keyCount].buf = (uint8_t*)&keys[tableID][i][0];
//                 keyCount ++;
//             }
//         }
//         if (tableID < 5) { // right side 
//             for (uint32_t i = 0; i < TEST_BATCH; i ++) {
//                 keyBuffer[tableID][keyCount].size = sizeof(uint64_t);
//                 keyBuffer[tableID][keyCount].buf = (uint8_t*)&keys[tableID][i][1];
//                 keyCount ++;
//             }
//         }
//         ValidValueCheck(keyCount == edgeCount * TEST_BATCH);
//     }

//     EXPECT_EQ(TEST_BATCH, DriverBatchInsertTupleWithKeys(driver, TEST_BATCH, tupleId[1], keyBuffer[1]));
//     EXPECT_EQ(TEST_BATCH, DriverBatchInsertTupleWithKeys(driver, TEST_BATCH, tupleId[3], keyBuffer[3]));
//     EXPECT_EQ(TEST_BATCH, DriverBatchInsertTupleWithKeys(driver, TEST_BATCH, tupleId[5], keyBuffer[5]));
//     EXPECT_EQ(TEST_BATCH, DriverBatchInsertTupleWithKeys(driver, TEST_BATCH, tupleId[4], keyBuffer[4]));
//     EXPECT_EQ(TEST_BATCH, DriverBatchInsertTupleWithKeys(driver, TEST_BATCH, tupleId[2], keyBuffer[2]));
//     // for (int tableID = 1; tableID <= TABLE_COUNT; tableID++) {
//     //     EXPECT_EQ(TEST_BATCH, DriverBatchInsertTupleWithKeys(driver, TEST_BATCH, tupleId[tableID], keyBuffer[tableID]));
//     // }

//     DriverFree(driver);
//     free(driver);
// }