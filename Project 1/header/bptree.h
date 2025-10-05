/*
Defining the key, pointer size for the b+tree

key is float -> 4B

pointer is 8B :
- block id is uint32_t -> 4B
- slot is int -> 4B



Defining the node structure for the b+tree

Node will be the same size as the block size -> 4096B

Header for nodes -> 11B
- uint8_t level -> 1B (starting with leaf level = 1)
- uint8 node_id -> 4B (the id of the node)
- uint16_t key_count -> 2B (the number of keys in the node)
- float lower_bound key -> 4B (the lower bound that can be accessed from that node)
*/

#ifndef BPTREE_H
#define BPTREE_H
#include <stdint.h>
#include <stdio.h>

#define NODE_SIZE 4096
#define NODE_HDR_SIZE 11
#define KEY_SIZE 4
#define RECORD_POINTER_SIZE 8
#define NODE_POINTER_SIZE 8

#define MAX_INTERNAL_KEYS (((NODE_SIZE - NODE_HDR_SIZE) / (KEY_SIZE + NODE_POINTER_SIZE)) - 1)
#define MAX_LEAF_KEYS     (((NODE_SIZE - NODE_HDR_SIZE) / (KEY_SIZE + RECORD_POINTER_SIZE)) - 1)
#define MIN_INTERNAL_KEYS ((MAX_INTERNAL_KEYS) / 2)
#define MIN_LEAF_KEYS     ((MAX_LEAF_KEYS + 1) / 2)
#define MAX_INT_CHILDREN (MAX_INTERNAL_KEYS + 1)


typedef struct  {
    uint16_t key_count;
    uint8_t level;
    float lower_bound;
    uint32_t node_id;
    uint8_t bytes[NODE_SIZE - NODE_HDR_SIZE];
} Node;

int node_init(Node* n, uint8_t node_type, uint32_t node_id);
int node_write_record_key(Node *n, float key, uint32_t block_id, int slot);
void set_int_node_lb(Node *n, float lower_bound);
int link_leaf_node(Node *left, uint32_t next_node_id);
int node_write_node_key(Node *n, float key, uint32_t node_id);
int encode_node(const Node *n, uint8_t *dst);
int decode_node(const uint8_t* src, Node* n);

#endif
