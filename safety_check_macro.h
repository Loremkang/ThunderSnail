#pragma once
#ifndef SAFETY_CHECK_MACRO_H
#define SAFETY_CHECK_MACRO_H
#include <assert.h>
#include <stdio.h>

static inline __attribute__((always_inline)) void PrintLoc(int cond, const char * file, int line) {
  if (cond) {
    return;
  }
  printf("Assert fail at %s:%d\n", file, line);
}

#ifdef DEBUG
#define ArrayOverflowCheck(x) assert((x))
#define ValidValueCheck(x) assert((x))
#define ValueOverflowCheck(x) assert((x))
#define Unimplemented(x) {printf(x);assert(0);}
#define NullPointerCheck(x) assert((x))
#elif defined(DPU_DEBUG)
#define ArrayOverflowCheck(x) {PrintLoc(x, __FILE__, __LINE__);}
#define ValidValueCheck(x) {PrintLoc(x, __FILE__, __LINE__);}
#define ValueOverflowCheck(x) {PrintLoc(x, __FILE__, __LINE__);}
#define Unimplemented(x) {printf(x);PrintLoc(0, __FILE__, __LINE__);}
#define NullPointerCheck(x) {PrintLoc(x, __FILE__, __LINE__);}
#else
#define ArrayOverflowCheck(x) {}
#define ValidValueCheck(x) {}
#define ValueOverflowCheck(x) {}
#define Unimplemented(x) {}
#define NullPointerCheck(x) {}
#endif


#endif
