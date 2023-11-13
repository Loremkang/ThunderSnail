#pragma once
#ifndef SAFETY_CHECK_MACRO_H
#define SAFETY_CHECK_MACRO_H

#include "assert.h"
#include "stdio.h"

#define overflowcheck(x) assert(x)
#define unimplemented(x) {printf(x);assert(0);}

#endif