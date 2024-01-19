#ifndef CATALOG_H
#define CATALOG_H

#include "common_base_struct.h"
#include "variable_length_struct_buffer.h"

#define MAX_TABLE (256)

typedef int TableIDT;
typedef int EdgeIDT;

typedef struct CatalogT {
  int tableCount;
  int tableID[MAX_TABLE];
  VariableLengthStructBufferT edges;
} CatalogT;

void InitCatalog(CatalogT* catalog);
void CatalogFree(CatalogT* catalog);
void CatalogInitTable(CatalogT* catalog, TableIDT tableID, size_t edgeCount,
                      EdgeIDT* edgeIDs);
OffsetT CatalogHashTableCountGet(CatalogT* catalog, TableIDT tableId);
OffsetT* CatalogHashTableListGet(CatalogT* catalog, TableIDT tableId);
EdgeIDT CatalogEdgeIdGet(CatalogT* catalog, TableIDT tableId,
                         OffsetT hashTableIndex);

#endif  // CATALOG_H