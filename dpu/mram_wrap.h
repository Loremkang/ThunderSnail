#include <mram.h>
#include <safety_check_macro.h>

inline uint32_t min(uint32_t a, uint32_t b) {
  return (a < b) ? a : b;
}

inline bool dma_aligned(uint32_t ptr) {
  return (ptr % 8) == 0;
}

inline bool valid_dma(uint32_t src, uint32_t dst, uint32_t granularity) {
  return dma_aligned(src) && dma_aligned(dst) && (granularity <= 2048) &&
         dma_aligned(granularity);
}
void mram_read_large(const __mram_ptr void* src, void* dst,
                     unsigned int nb_of_bytes) {
  ValidValueCheck(valid_dma((uint32_t)src, (uint32_t)dst, 2048));
  uint32_t granularity = 2048;
  for (int i = 0; i < nb_of_bytes; i += granularity) {
    int size = min(granularity, nb_of_bytes - i);
    mram_read(src + i, dst + i, size);
  }
}

void mram_read_small(const __mram_ptr void* src, void* dst,
                     unsigned int nb_of_bytes) {
  ValidValueCheck(valid_dma((uint32_t)src, (uint32_t)dst, nb_of_bytes));
  mram_read(src, dst, nb_of_bytes);
}

void mram_write_large(const void* src, __mram_ptr void* dst,
                      unsigned int nb_of_bytes) {
  ValidValueCheck(valid_dma((uint32_t)src, (uint32_t)dst, 2048));
  uint32_t granularity = 2048;
  for (int i = 0; i < nb_of_bytes; i += granularity) {
    int size = min(granularity, nb_of_bytes - i);
    mram_write(src + i, dst + i, size);
  }
}

void mram_write_small(const void* src, __mram_ptr void* dst,
                      unsigned int nb_of_bytes) {
  ValidValueCheck(valid_dma((uint32_t)src, (uint32_t)dst, nb_of_bytes));
  mram_write(src, dst, nb_of_bytes);
}