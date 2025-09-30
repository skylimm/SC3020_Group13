#ifndef BPTREE_H
#define BPTREE_H
#include <stdint.h>
#include <stdio.h>

#define MAX_INTERNAL_KEYS 3
#define MAX_LEAF_KEYS 3
#define MIN_INTERNAL_KEYS 1
#define MIN_LEAF_KEYS 1

#define NODE_SIZE 4096
#define NODE_HEADER_SIZE 11

// define the key, pointer size for the b plus tree
/*
the key is float -> 4B
the value is ??? -> block id and slot of the node; block id and slot of the record.
in her code, the block id is uint32_t -> 4B; the slot is int -> 4B
pointer size = 8B 
*/
// define the node structure for the b plus tree
/*
header for internal nodes -> 11B
- uint8_t node_type (0 = internal, 1 = leaf) -> 1B
- uint16_t key_count -> 2B
- uint32_t parent address -> 8B (pointer : block id and the "slot" in the block aka the index of the key)

header for root node 
- include the previous pointer??
*/

// define the b plus tree structure
typedef struct {
    uint32_t node_id;
    uint32_t slot;
} Pointer;

#define BPTREE_ORDER 4

typedef struct  {
    uint8_t node_type; // 0 = internal, 1 = leaf
    uint16_t key_count;
    Pointer parent; // Pointer to parent node
    float keys[BPTREE_ORDER - 1];
    Pointer* children[BPTREE_ORDER];
    Pointer* next; // For leaf node chaining <- ??
} Node;

typedef struct {
    Node* root;
} BPTree;


#endif
