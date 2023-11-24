#pragma once
#ifndef INDEX_REQ_H
#define INDEX_REQ_H

#include "../deps/pim-index/dpu/primary_index_dpu.h"
#include "../common_base_struct/common_base_struct.h"

primary_index_dpu *IndexCheck(uint32_t indexId);
void IndexGetOrInsertReq(primary_index_dpu *pid, char *key, uint32_t keyLen, TupleIdT tupleId, HashTableQueryReplyT *reply);
void IndexUpdateReq(primary_index_dpu *pid, char *key, HashAddrT hashAddr, MaxLinkAddrT maxLinkAddr);

#endif