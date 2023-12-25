#ifndef CATALOG_H
#define CATALOG_H

#include "common_base_struct.h"
#include "variable_length_struct_buffer.h"

#define MAX_TABLE (256)

typedef struct CatalogT {
    int tableCount;
    int tableId[MAX_TABLE];
    VariableLengthStructBufferT edges;
} CatalogT;

void CatalogInit(CatalogT* catalog);
void CatalogInitTable(CatalogT* catalog, int tableId, int edgeCount, int *edgeIds);
OffsetT* CatalogHashTableListGet(CatalogT* catalog, int tableId);


#endif // CATALOG_H