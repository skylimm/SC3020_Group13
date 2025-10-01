#ifndef BPTREE_H
#define BPTREE_H
#include <stdint.h>
#include <stdio.h>

#define NODE_SIZE 4096
#define NODE_HDR_SIZE 15
#define KEY_SIZE 4
#define POINTER_SIZE 8

#define MAX_INTERNAL_KEYS (NODE_SIZE - NODE_HDR_SIZE) / (KEY_SIZE + POINTER_SIZE) - 1
#define MAX_LEAF_KEYS (NODE_SIZE - NODE_HDR_SIZE) / (KEY_SIZE + POINTER_SIZE) - 1
#define MIN_INTERNAL_KEYS MAX_INTERNAL_KEYS / 2
#define MIN_LEAF_KEYS (MIN_LEAF_KEYS + 1) / 2

// define the key, pointer size for the b plus tree
/*
the key is float -> 4B
the value is ??? -> block id and slot of the node; block id and slot of the record.
in her code, the block id is uint32_t -> 4B; the slot is int -> 4B
pointer size = 8B 
*/
// define the node structure for the b plus tree
/*
header for internal nodes -> 15B
- uint8_t node_type (0 = internal, 1 = leaf) -> 1B
- uint16_t key_count -> 2B
- uint32_t parent address -> 8B (pointer : block id and the "slot" in the block aka the index of the key)
- float lower_bound key -> 4B (minimum key in the node)

header for root node
- include the previous pointer??


uint8_t node_type; // 0 = internal, 1 = leaf
    uint16_t key_count;
    Pointer parent; // Pointer to parent node
    float keys[BPTREE_ORDER - 1];
    Pointer* children[BPTREE_ORDER];
    Pointer* next; // For leaf node chaining <- ??
    minimum key (lower bound) -> float -> 4B
*/

typedef struct  {
    uint8_t bytes[NODE_SIZE];
} Node;

#endif
