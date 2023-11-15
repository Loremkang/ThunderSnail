#include "newlink.h"
#include "disjoint_set.h"
#include "hash_table_for_newlink.h"

const int MAXBATCHSIZE = 2000;

void BuildNewLinkFromHashTableGetOrInsertResult(int length, DisjointSetNodeT* usNode, HashTableForNewLinkT* ht, TupleIdT* tupleIDs, TupleIdOrMaxLinkAddrT* counterpart) {
    assert(length < MAXBATCHSIZE);

    for (int i = 0; i < length; i ++) {
        DisjointSetInit(&usNode[i]);
    }
    HashTableForNewLinkExpand(ht, length);
    for (int i = 0; i < length; i ++) {
        TupleIdOrMaxLinkAddrT left = (TupleIdOrMaxLinkAddrT){.type = TupleId, .value.tupleId = tupleIDs[i]};
        TupleIdOrMaxLinkAddrT right = counterpart[i];
        int leftId = HashTableForNewlinkGetId(ht, left);
        int rightId = HashTableForNewlinkGetId(ht, right);
        DisjointSetJoin(&usNode[leftId], &usNode[rightId]);
    }
}

// entry of this part of code
void driver() {
    static DisjointSetNodeT usNode[MAXBATCHSIZE];
    static HashTableForNewLinkT ht;
}