#include <gtest/gtest.h>

extern "C" {
#include "common_base_struct.h"
}

TEST(BaseStructTest, MaxLinkSize) {
  EXPECT_EQ(MaxLinkGetSize(5, 5), 168);
}

TEST(BaseStructTest, RemotePtrConvert) {
  uint32_t id = (uint32_t)rand();
  uint32_t addr = (uint32_t)rand();
  printf("id = %x\naddr = %x\n", id, addr);
  RemotePtrT x = (RemotePtrT){.dpuId = id, .dpuAddr = addr};
  uint64_t xi64 = RemotePtrToI64(x);
  RemotePtrT y = RemotePtrFromI64(xi64);
  uint64_t yi64 = RemotePtrToI64(y);
  EXPECT_EQ(x.dpuId, y.dpuId);
  EXPECT_EQ(x.dpuAddr, y.dpuAddr);
  EXPECT_EQ(xi64, yi64);
}