#include <gtest/gtest.h>

extern "C" {
#include "catalog.h"
#include "safety_check_macro.h"
}

TEST(FakeInterfaceTest, FakeInterfaceTest) {
    CatalogT catalog;
    InitCatalog(&catalog);
    
    const int TABLE_COUNT = 5;

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
        CatalogInitTable(&catalog, tableID, edgeCount, edges);
    }

    EXPECT_EQ(CatalogHashTableCountGet(&catalog, 1), 1);
    EXPECT_EQ(CatalogHashTableCountGet(&catalog, 2), 2);
    EXPECT_EQ(CatalogHashTableCountGet(&catalog, 3), 2);
    EXPECT_EQ(CatalogHashTableCountGet(&catalog, 4), 2);
    EXPECT_EQ(CatalogHashTableCountGet(&catalog, 5), 1);

    EXPECT_EQ(CatalogEdgeIdGet(&catalog, 3, 1), 3);
    EXPECT_EQ(CatalogEdgeIdGet(&catalog, 3, 0), 2);

    // uint64_t value;
    // TupleIdT tupleId = {.tableId = 3, .tupleAddr = 5};
    // CatalogHashTableKeyGet(tupleId, 0, (uint8_t*)&value);
    // EXPECT_EQ(value, ((3 - 1) << 24) + 5);
}