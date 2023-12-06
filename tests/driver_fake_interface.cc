#include <gtest/gtest.h>

extern "C" {
#include "fake_interface.h"
#include "safety_check_macro.h"
}

TEST(FakeInterfaceTest, FakeInterfaceTest) {
    EXPECT_EQ(CatalogHashTableIdGet(3, 1), 3);
    EXPECT_EQ(CatalogHashTableIdGet(3, 0), 2);

    uint64_t value;
    TupleIdT tupleId = {.tableId = 3, .tupleAddr = 5};
    CatalogHashTableKeyGet(tupleId, 0, (uint8_t*)&value);
    EXPECT_EQ(value, ((3 - 1) << 24) + 5);
}