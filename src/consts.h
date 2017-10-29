#pragma once

#define CACHELINE_SIZE_BYTES      (64u)
#define CACHELINE_SIZE_FLOATS     (CACHELINE_SIZE_BYTES/sizeof(float))
#define CACHELINE_SIZE_BYTE_MASK  (CACHELINE_SIZE_BYTES-1)
#define CACHELINE_SIZE_FLOAT_MASK (CACHELINE_SIZE_FLOATS-1)

#define INSTRUCTION_VECTOR_SIZE           (CACHELINE_SIZE_FLOATS)
#define INSTRUCTION_VECTOR_SIZE_MASK      (CACHELINE_SIZE_FLOATS-1)
#define INSTRUCTION_VECTOR_SIZE_ADDR_MASK (CACHELINE_SIZE_BYTES-1)

#define PHUB_MAX_KEY_BITS 22
#define PHUB_MAX_KEY ((1<<PHUB_MAX_KEY_BITS) - 1)
#define PHUB_IMM_KEY_MASK PHUB_MAX_KEY
#define PHUB_IMM_SENDER_MASK ((1<<(32 - PHUB_MAX_KEY_BITS))-1)

static inline size_t RoundUp(size_t numToRound, size_t multiple)
{
    assert(multiple != 0);
    return ((numToRound + multiple - 1) / multiple) * multiple;
}
