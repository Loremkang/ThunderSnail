#ifndef GET_OR_INSERT_RESULT_TO_NEWLINK_H
#define GET_OR_INSERT_RESULT_TO_NEWLINK_H

#include "disjoint_set.h"
#include "hash_table_for_newlink.h"
#include "newlink.h"
#include "variable_length_struct_buffer.h"

void BuildNewLinkFromHashTableGetOrInsertResultPerformanceTest();
void GetOrInsertResultToNewlink(int length, TupleIdT* tupleIds,
                                HashTableQueryReplyT* counterpart,
                                HashTableForNewLinkT* ht,
                                VariableLengthStructBufferT* buf);
void PrintNewLinkResult(VariableLengthStructBufferT* buf);

#endif