#include "common_base_struct.h"
#include "path_config.h"

#define MAX_LINK_ENTRY_SIZE sizeof(MaxLinkEntryT)
#define STACK_BOTTOM_ADDR (MAX_LINK_SPACE_ADDR + MAX_LINK_SPACE_SIZE)
#define MAX_LINK_ENTRY_START_ADDR MAX_LINK_SPACE_ADDR;
#define HASH_ADDR_SIZE sizeof(HashAddrT)
#define TUPLE_ID_SIZE sizeof(TupleIdT)

typedef struct {
    int tableIDCount;
    int hashAddrCount;
    TupleIdT  tupleIds[TABLE_INDEX_LEN];
    HashAddrT hashAddrs[EDGE_INDEX_LEN];
} MaxLinkEntryT;
