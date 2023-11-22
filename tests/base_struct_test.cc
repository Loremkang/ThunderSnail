#include <gtest/gtest.h>

extern "C" {
#include "common_base_struct/common_base_struct.h"
}

TEST (BaseStructTest, MaxLinkSize) {
  EXPECT_EQ(GetMaxLinkSize(5, 5), 128);
}
