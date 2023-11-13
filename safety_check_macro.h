#pragma once
#include "assert.h"
#include "stdio.h"

#define ArrayOverflowCheck(x) assert(x)
#define Unimplemented(x) {printf(x);assert(0);}
#define NullPointerCheck(x) assert(x)