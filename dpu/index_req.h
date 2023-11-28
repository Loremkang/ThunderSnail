#pragma once
#ifndef INDEX_REQ_H
#define INDEX_REQ_H

#include "../deps/pim-index/dpu/sto/primary_index_dpu.h"
#include "../common_base_struct/common_base_struct.h"

void IndexAllocatorInit();
void IndexSpaceInit();
primary_index_dpu *IndexCreate(uint32_t indexId);
void IndexInitBuckets(primary_index_dpu *pid, uint32_t taskletId);
primary_index_dpu *IndexCheck(uint32_t indexId);
void IndexGetOrInsertReq(primary_index_dpu *pid, char *key, uint32_t keyLen, TupleIdT tupleId, HashTableQueryReplyT *reply);
void IndexUpdateReq(primary_index_dpu *pid, char *key, HashAddrT hashAddr, MaxLinkAddrT maxLinkAddr);

#endif