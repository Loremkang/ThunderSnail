#include "newlink.h"
#include "disjoint_set.h"

const int MAXBATCHSIZE = 2000;

void BuildNewLinkFromHashTableGetOrInsertResult(int length, DisjointSetNodeT* usNode, TupleIdT* tupleIDs, TupleIdOrMaxLinkAddr* counterpart) {
    assert(length < MAXBATCHSIZE);
    for (int i = 0; i < length; i ++) {
        DisjointSetInit(&usNode[i]);
    }
}

// entry of this part of code
void driver() {
    static DisjointSetNodeT usNode[MAXBATCHSIZE];
}