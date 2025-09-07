#include "block.h"
#include <string.h>

int block_capacity_records(uint16_t record_size)
{
    if (record_size == 0)
        return 0;
    int cap = (BLOCK_SIZE - BLOCK_HDR_SIZE) / record_size;
    return (cap < 0) ? 0 : cap;
}

int block_write_record(Block *b, uint16_t record_size, int slot, const uint8_t *rec)
{
    int cap = block_capacity_records(record_size);
    if (slot < 0 || slot >= cap)
        return -1;
    size_t off = BLOCK_HDR_SIZE + (size_t)slot * record_size;
    memcpy(&b->bytes[off], rec, record_size);
    return 0;
}

int block_read_record(const Block *b, uint16_t record_size, int slot, uint8_t *out)
{
    int cap = block_capacity_records(record_size);
    if (slot < 0 || slot >= cap)
        return -1;
    size_t off = BLOCK_HDR_SIZE + (size_t)slot * record_size;
    memcpy(out, &b->bytes[off], record_size);
    return 0;
}
