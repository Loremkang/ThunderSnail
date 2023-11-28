#include <gtest/gtest.h>

extern "C" {
#include "../protocol/cpu/requests.h"
}

#define TEST_BATCH 1

TEST (BufferHandler, DecodeBuffer) {
  //while (0 != getNextTask(handler, task)) {
  //  execute(task);
  //}
  uint64_t tupleAddrs[TEST_BATCH] = {0x1234};//, 0x2341, 0x3412, 0x4123};
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

  uint8_t** recvBuf;
  recvBuf = (uint8_t **)malloc(sizeof(uint8_t*));
  recvBuf[0] = (uint8_t *)malloc(65535);
  printf("Now calling SendGetOrInsertReq\n");

  SendGetOrInsertReq(/*tableId*/3, keys, tupleAddrs, TEST_BATCH, recvBuf);
}