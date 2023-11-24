#include <gtest/gtest.h>

extern "C" {
#include "newlink/disjoint_set.h"
#include "newlink/hash_table_for_newlink.h"
#include "newlink/hash_function.h"
#include "newlink/newlink.h"
}

// #define TEST_FAIL(x) if(!(x)) {goto fail;}
#define TEST_FAIL(x) assert(x);

// simple test code
bool DisjointSetTest() {
    
    
    return true;

fail:
    return false;
}

TEST(NewLinkTest, DisJointSet) {
    const int MAXN = 100;
    DisjointSetNodeT node[MAXN];
    for (int i = 0; i < MAXN; i ++) {
        DisjointSetInit(&node[i]);
        node[i].tupleIdCount = 1;
        node[i].maxLinkAddrCount = i;
        node[i].hashAddrCount = i + 1;
    }
    for (int i = 0; i < MAXN; i ++) {
        EXPECT_EQ(DisjointSetFind(&node[i]), &node[i]);
        DisjointSetJoin(&node[i], &node[0]);
        if (i > 0) {
            EXPECT_EQ(DisjointSetFind(&node[i]), DisjointSetFind(&node[i - 1]));
        }
        EXPECT_EQ(DisjointSetFind(&node[i])->tupleIdCount, i + 1);
        EXPECT_EQ(DisjointSetFind(&node[i])->maxLinkAddrCount, (i * (i + 1) / 2));
        EXPECT_EQ(DisjointSetFind(&node[i])->hashAddrCount, ((i + 2) * (i + 1) / 2));
    }
}

TEST(NewLinkTest, HashTableForNewLink) {
    const int MAXN = 1000;
    HashTableForNewLinkT ht;
    HashTableForNewLinkInit(&ht);
    HashTableForNewLinkExpandAndSoftReset(&ht, 1.5 * MAXN); // very dense
    HashTableQueryReplyT keys[MAXN];
    for (int i = 0; i < MAXN; i ++) {
        if (i & 1) {
            keys[i].type = TupleId;
            keys[i].value.tupleId.tableId = hash32(i);
            keys[i].value.tupleId.tupleAddr = hash64(i);
        } else {
            keys[i].type = MaxLinkAddr;
            keys[i].value.maxLinkAddr.rPtr = RemotePtrFromI64(hash64(i));
        }
        int id = HashTableForNewlinkGetId(&ht, keys[i]);
        // printf("i=%d\tid=%d\n", i, id);
        EXPECT_EQ(id, i + 1);
    }
    for (int i = 7; i != 0; i = (i + 7) % MAXN) {
        int id = HashTableForNewlinkGetId(&ht, keys[i]);
        // printf("i=%d\tid=%d\n", i, id);
        EXPECT_EQ(id, i + 1);
    }
}

TEST(NewLinkTest, NewLink) {
    const int MAXN = 10;
    int size = NewLinkGetSize(MAXN, MAXN, MAXN);
    uint8_t* buf = (uint8_t*)malloc(size);
    NewLinkT* newLink = (NewLinkT*)buf;
    printf("size=%d\n", NewLinkGetSize(MAXN, MAXN, MAXN));
    newLink->maxLinkAddrCount = newLink->tupleIDCount = newLink->hashAddrCount = MAXN;
    for (uint32_t i = 0; i < MAXN; i ++) {
        NewLinkGetTupleIDs(newLink)[i] = (TupleIdT){.tableId = 10, .tupleAddr = i};
        NewLinkGetMaxLinkAddrs(newLink)[i] =
            (MaxLinkAddrT){.rPtr = (RemotePtrT){.dpuId = 11, .dpuAddr = i}};
        NewLinkGetHashAddrs(newLink)[i] =
            (HashAddrT){.rPtr = (RemotePtrT){.dpuId = 12, .dpuAddr = i}};
    }

    EXPECT_EQ(sizeof(NewLinkT), sizeof(int) * 4);

    EXPECT_LT((uint8_t*)&(NewLinkGetTupleIDs(newLink)[MAXN - 1]), buf + size);
    EXPECT_LT((uint8_t*)&(NewLinkGetMaxLinkAddrs(newLink)[MAXN - 1]), buf + size);
    EXPECT_LT((uint8_t*)&(NewLinkGetHashAddrs(newLink)[MAXN - 1]), buf + size);

    EXPECT_EQ((uint8_t*)newLink + sizeof(NewLinkT), newLink->buffer);
    EXPECT_EQ((uint8_t*)newLink + sizeof(NewLinkT), (uint8_t*)&NewLinkGetTupleIDs(newLink)[0]);
    EXPECT_EQ((uint8_t*)&NewLinkGetTupleIDs(newLink)[MAXN], (uint8_t*)&NewLinkGetMaxLinkAddrs(newLink)[0]);
    EXPECT_EQ((uint8_t*)&NewLinkGetMaxLinkAddrs(newLink)[MAXN], (uint8_t*)&NewLinkGetHashAddrs(newLink)[0]);
    EXPECT_EQ((uint8_t*)&NewLinkGetHashAddrs(newLink)[MAXN], buf + size);

    free(newLink);
}