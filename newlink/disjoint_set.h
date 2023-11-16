#pragma once

// union set: counts only valid for roots
typedef struct DisjointSetNodeT {
    int tupleIdCount;
    int maxLinkAddrCount;
    struct DisjointSetNodeT* parent;
} DisjointSetNodeT;

void DisjointSetInit(DisjointSetNodeT* node);
DisjointSetNodeT* DisjointSetFind(DisjointSetNodeT* node);
void DisjointSetJoin(DisjointSetNodeT* a, DisjointSetNodeT* b);
int DisjointSetTest();