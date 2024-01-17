#include "common_base_struct.h"
#include "path_config.h"

#define MAX_LINK_ENTRY_SIZE sizeof(MaxLinkEntryT)
#define STACK_BOTTOM_ADDR (MAX_LINK_SPACE_ADDR + MAX_LINK_SPACE_SIZE)
#define MAX_LINK_ENTRY_START_ADDR MAX_LINK_SPACE_ADDR;
#define HASH_ADDR_SIZE sizeof(HashAddrT)
#define TUPLE_ID_SIZE sizeof(TupleIdT)

// size should be less than 2K. Otherwise should use mram_read_large.
typedef   __attribute__((aligned(8))) struct {
    int tupleIDCount;
    int hashAddrCount;
    TupleIdT  tupleIds[TABLE_INDEX_LEN];
    HashAddrT hashAddrs[EDGE_INDEX_LEN];
} MaxLinkEntryT;

extern __host int counter;

bool IsNullTuple(TupleIdT* tid);
bool IsNullHash(HashAddrT* ha) ;
uint32_t GetTableIdIndex(int tid);
uint32_t GetEdgeIdIndex(int tid);
uint32_t EncodeMaxLink(MaxLinkT* link);
__mram_ptr MaxLinkEntryT* NewMaxLinkEntry(MaxLinkT* ml);
void MergeMaxLink(__mram_ptr MaxLinkEntryT* dst, MaxLinkT* src);
void MergeMaxLinkEntry(MaxLinkEntryT* target, MaxLinkEntryT* source);
void RetrieveMaxLink(__mram_ptr MaxLinkEntryT* src, MaxLinkT* res);
void RetrieveMaxLinkAndAppendResp(__mram_ptr MaxLinkEntryT* entry, uint32_t taskIdx, mutex_id_t *mutex);
uint32_t GetMaxLinkSize(__mram_ptr MaxLinkEntryT* src);