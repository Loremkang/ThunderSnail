#pragma once
#ifndef INDEX_REQ_H
#define INDEX_REQ_H

#include "common_base_struct.h"
#include "dpu/sto/primary_index_dpu.h"

void IndexAllocatorInit();
void IndexSpaceInit();
primary_index_dpu* IndexCreate(uint32_t indexId);
void IndexInitBuckets(primary_index_dpu* pid, uint32_t taskletId);
primary_index_dpu* IndexCheck(uint32_t indexId);
void IndexGetOrInsertReq(primary_index_dpu* pid, char* key, uint32_t keyLen,
                         TupleIdT tupleId, uint32_t edgeId,
                         HashTableQueryReplyT* reply);
void IndexUpdateReq(primary_index_dpu* pid, HashAddrT hashAddr,
                    MaxLinkAddrT maxLinkAddr);

#endif