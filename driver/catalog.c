#include "catalog.h"

// Initialize the catalog
void InitCatalog(CatalogT* catalog) {
  catalog->tableCount = 0;
  for (int i = 0; i < MAX_TABLE; i++) {
    catalog->tableID[i] = -1;  // -1 indicates that the tableId is not used
  }
  VariableLengthStructBufferInit(&catalog->edges);
}

void CatalogFree(CatalogT* catalog) {
  VariableLengthStructBufferFree(&catalog->edges);
}

// Initialize a table in the catalog
void CatalogInitTable(CatalogT* catalog, TableIDT tableId, size_t edgeCount,
                      EdgeIDT* edgeIDs) {
  ArrayOverflowCheck(catalog->tableCount < MAX_TABLE);
  catalog->tableID[catalog->tableCount] = tableId;
  catalog->tableCount++;
  OffsetT offset = VariableLengthStructBufferAppendPlaceholder(
      &catalog->edges, edgeCount * sizeof(EdgeIDT));
  uint8_t* buffer = VariableLengthStructBufferGet(&catalog->edges, offset);
  memcpy(buffer, edgeIDs, edgeCount * sizeof(EdgeIDT));
}

OffsetT CatalogHashTableCountGet(CatalogT* catalog, int tableId) {
  for (int i = 0; i < catalog->tableCount; i++) {
    if (catalog->tableID[i] == tableId) {
      return VariableLengthStructBufferGetSize(&catalog->edges, i) /
             sizeof(EdgeIDT);
    }
  }
  return 0;  // Return 0 if the tableId is not found
}

// Get the hash table list from the catalog
OffsetT* CatalogHashTableListGet(CatalogT* catalog, int tableId) {
  for (int i = 0; i < catalog->tableCount; i++) {
    if (catalog->tableID[i] == tableId) {
      return (OffsetT*)VariableLengthStructBufferGet(&catalog->edges, i);
    }
  }
  return NULL;  // Return NULL if the tableId is not found
}

EdgeIDT CatalogEdgeIdGet(CatalogT* catalog, TableIDT tableId,
                         OffsetT hashTableIndex) {
  ArrayOverflowCheck(hashTableIndex <
                     CatalogHashTableCountGet(catalog, tableId));
  OffsetT* hashTableList = CatalogHashTableListGet(catalog, tableId);
  return hashTableList[hashTableIndex];
}