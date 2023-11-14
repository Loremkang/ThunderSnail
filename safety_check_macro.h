#pragma once

#ifndef SAFETY_CHECK_MACRO_H
#define SAFETY_CHECK_MACRO_H

#include "assert.h"
#include "stdio.h"

#define ArrayOverflowCheck(x) assert((x))
#define ValidValueCheck(x) assert((x))
#define ValueOverflowCheck(x) assert((x))
#define Unimplemented(x) {printf(x);assert(0);}
#define NullPointerCheck(x) assert((x))

#endif