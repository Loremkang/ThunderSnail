#pragma once
#include "assert.h"
#include "stdio.h"

#define overflowcheck(x) assert(x)
#define unimplemented(x) {printf(x);assert(0);}