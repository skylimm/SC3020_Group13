#include "bptree.h"
#include <string.h>

int node_init(Node *n, uint8_t level, uint32_t node_id )
{
    n->key_count = 0;
    n->level = level; 
    n->node_id = node_id;
    // check??????
    n->lower_bound = -1.0f;
    
    return 0;
}


int node_write_record_key(Node *n, float key, uint32_t block_id, int slot)
{
    if (n->key_count >= MAX_LEAF_KEYS)
        return -1;
    
    if (n->key_count == 0){
        n->lower_bound = key;
    }
    
    size_t off = ((size_t)slot + block_id + (size_t)key) * n->key_count;
    uint8_t ptr[RECORD_POINTER_SIZE];

    memcpy(ptr, &block_id, 4);
    memcpy(ptr + 4, &slot, 4);

    memcpy(&n->bytes[off], ptr, RECORD_POINTER_SIZE);
    memcpy(&n->bytes[off + RECORD_POINTER_SIZE], &key, KEY_SIZE);

    n->key_count += 1;

    return 0;
}

// pointer input
int link_leaf_node(Node *left, uint32_t next_node_id)
{ 
    size_t off = NODE_SIZE - NODE_POINTER_SIZE;
    memcpy(&left->bytes[off], &next_node_id, NODE_POINTER_SIZE);
    return 0;
}

int set_int_node_lb(Node *n, float lower_bound)
{
    n->lower_bound = lower_bound;
}

int node_write_node_key(Node *n, float key, uint32_t node_id)
{
    if ((n->key_count) >= MAX_INTERNAL_KEYS)
        return -1;

    if ((n->key_count) == 1){
        memcpy(&n->bytes, &node_id-1, NODE_POINTER_SIZE);
    }

    size_t off = NODE_POINTER_SIZE + (node_id + (size_t)key) * n->key_count;

    memcpy(&n->bytes[off], &key, KEY_SIZE);
    memcpy(&n->bytes[off + KEY_SIZE], &node_id, NODE_POINTER_SIZE);
    return 0;
}

// encode and decode functions for node
int encode_node(const Node *n, uint8_t *dst)
{
    if (!n || !dst)
        return -1;

    uint8_t *p = dst;
    *p++ = n->level;

    memcpy(p, &n->node_id, sizeof(n->node_id));
    p += sizeof(n->node_id);

    memcpy(p, &n->key_count, sizeof(n->key_count));
    p += sizeof(n->key_count);

    memcpy(p, &n->lower_bound, sizeof(n->lower_bound));
    p += sizeof(n->lower_bound);

    memcpy(p, n->bytes, NODE_SIZE - NODE_HDR_SIZE);
    return 0;
}

int decode_node(const uint8_t *src, Node *n)
{
    if (!src || !n)
        return -1;

    const uint8_t *p = src;

    n->level = *p++;

    memcpy(&n->node_id, p, sizeof(n->node_id));
    p += sizeof(n->node_id);

    memcpy(&n->key_count, p, sizeof(n->key_count));
    p += sizeof(n->key_count);

    memcpy(&n->lower_bound, p, sizeof(n->lower_bound));
    p += sizeof(n->lower_bound);

    memcpy(n->bytes, p, NODE_SIZE - NODE_HDR_SIZE);
    return 0;
}

