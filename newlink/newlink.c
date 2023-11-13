
#include "newlink.h"

TupleIdT* GetTupleIDs(NewLinkT* newLink) {
    return (TupleIdT*)(newLink->buffer);
}

MaxLinkAddrT* GetMaxLinkAddrs(NewLinkT* newLink) {
    return (MaxLinkAddrT*)(newLink->buffer + sizeof(TupleIdT) * newLink->tupleIDCount);
}