#include "common_base_struct.h"

// MaxLink Related Function

// private
static inline TupleIdT* GetTupleIDsFromMaxLink(MaxLink* maxLink) {
    return (TupleIdT*)(maxLink->buffer);
}

// Cannot pass reference. Passing pointer instead.
inline TupleIdT* GetKthTupleIDFromMaxLink(MaxLink* maxLink, int i) {
    overflowcheck(i >= 0 && i < maxLink->tupleIDCount);
    return GetTupleIDsFromMaxLink(maxLink) + i;
}

// private
static inline HashAddrT* GetHashAddrsFromMaxLink(MaxLink* maxLink) {
    return (HashAddrT*)(maxLink->buffer + sizeof(TupleIdT) * maxLink->tupleIDCount);
}

inline HashAddrT* GetKthHashAddrFromMaxLink(MaxLink* maxLink, int i) {
    overflowcheck(i >= 0 && i < maxLink->hashAddrCount);
    return GetHashAddrsFromMaxLink(maxLink) + i;
}

int GetMaxLinkSize(int tupleIdCount, int hashAddrCount) {
    return sizeof(MaxLink) + tupleIdCount * sizeof(TupleIdT) + hashAddrCount * sizeof(HashAddrT);
}

int BuildMaxLink(int tupleIdCount, int hashAddrCount, TupleIdT* tupleIds, HashAddrT* hashAddrs) {
    unimplemented("BuildMaxLink");
}