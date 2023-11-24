#pragma once 
#ifndef HASH_TABLE_FOR_NEWLINK_H
#define HASH_TABLE_FOR_NEWLINK_H

#include "common_base_struct.h"

typedef struct HashTableForNewLinkEntryT {
    HashTableQueryReplyT key;
    int value;
} HashTableForNewLinkEntryT;

typedef struct HashTableForNewLinkT {
    int capacity;
    int count;
    HashTableForNewLinkEntryT* entries;
} HashTableForNewLinkT;

void HashTableForNewLinkInit(HashTableForNewLinkT *hashTable);
void HashTableForNewLinkFree(HashTableForNewLinkT *hashTable);
void HashTableForNewLinkExpandAndSoftReset(HashTableForNewLinkT *hashTable, int capacity);
void HashTableForNewLinkShrink(HashTableForNewLinkT *hashTable, int capacity);
// Id Starting from 1
int HashTableForNewlinkGetId(HashTableForNewLinkT *hashTable, HashTableQueryReplyT key);
void HashTableForNewLinkSoftReset(HashTableForNewLinkT *hashTable);

#endif