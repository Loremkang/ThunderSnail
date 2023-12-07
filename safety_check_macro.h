#pragma once
#ifndef SAFETY_CHECK_MACRO_H
#define SAFETY_CHECK_MACRO_H
#include <assert.h>
#include <stdio.h>

// #define DEBUG

#ifdef DEBUG
#define ArrayOverflowCheck(x) assert((x))
#define ValidValueCheck(x) assert((x))
#define ValueOverflowCheck(x) assert((x))
#define Unimplemented(x) {printf(x);assert(0);}
#define NullPointerCheck(x) assert((x))
#else
#define ArrayOverflowCheck(x) {}
#define ValidValueCheck(x) {}
#define ValueOverflowCheck(x) {}
#define Unimplemented(x) {}
#define NullPointerCheck(x) {}
#endif


#endif
