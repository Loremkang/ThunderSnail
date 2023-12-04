#include <gtest/gtest.h>

extern "C" {
#include "../protocol/cpu/requests.h"
#include "../protocol/cpu/requests_handler.h"
}

#define TEST_BATCH 4

TEST (BufferHandler, SetDpuIdReq) {
  SendSetDpuIdReq();
}

TEST (BufferHandler, DecodeBuffer) {
  uint64_t tupleAddrs[TEST_BATCH] = {0x1234, 0x2341, 0x3412, 0x4123};
  Key keys[TEST_BATCH];
  for (int i=0; i < TEST_BATCH; i++) {
    keys[i].data = (uint8_t *)malloc(8);
    keys[i].data[0] = 'a';
    keys[i].data[1] = 'b';
    keys[i].data[2] = 'c';
    keys[i].data[3] = 'd';
    keys[i].data[4] = '\0';
    keys[i].len = 4;
  }

  uint8_t** recvBufs;
  recvBufs = (uint8_t **)malloc(sizeof(uint8_t*));
  recvBufs[0] = (uint8_t *)malloc(65535);
  printf("Now calling SendGetOrInsertReq\n");

  SendGetOrInsertReq(/*tableId*/3, keys, tupleAddrs, TEST_BATCH, recvBufs);
  TraverseReceiveBuffer(recvBufs[0]);
}
