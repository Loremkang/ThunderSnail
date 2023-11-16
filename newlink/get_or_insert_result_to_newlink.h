#ifndef GET_OR_INSERT_RESULT_TO_NEWLINK_H
#define GET_OR_INSERT_RESULT_TO_NEWLINK_H

#include "newlink.h"
#include "disjoint_set.h"
#include "hash_table_for_newlink.h"
#include "variable_length_struct_buffer.h"


void BuildNewLinkFromHashTableGetOrInsertResult(
    int length, int* idxPos, bool* processed, DisjointSetNodeT* dsNode,
    HashTableForNewLinkT* ht, TupleIdT* tupleIDs,
    TupleIdOrMaxLinkAddrT* counterpart,
    VariableLengthStructBufferT* newLinkResultBuffer);

void BuildNewLinkFromHashTableGetOrInsertResultTest();

#endif