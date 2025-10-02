#include "bptree.h"
#include <string.h>

// function to initallise the b+tree node
int node_init(Node *n, uint8_t level, uint32_t node_id )
{
    n->key_count = 0;
    n->level = level; 
    n->node_id = node_id;
    n->lower_bound = -1.0f; // a sentinel value to indicate that the lower bound is not set
    memset(n->bytes, 0, NODE_SIZE - NODE_HDR_SIZE);
    
    return 0;
}

// write the key and the pointer to the record into the leaf node
int node_write_record_key(Node *n, float key, uint32_t block_id, int slot)
{
    if (n->key_count >= MAX_LEAF_KEYS)
        return -1;
    
    if (n->key_count == 0){
        n->lower_bound = key;
    }
    
    size_t off = n->key_count * (RECORD_POINTER_SIZE + KEY_SIZE);
    uint8_t ptr[RECORD_POINTER_SIZE];

    memcpy(ptr, &block_id, 4);
    memcpy(ptr + 4, &slot, 4);

    memcpy(&n->bytes[off], ptr, RECORD_POINTER_SIZE);
    memcpy(&n->bytes[off + RECORD_POINTER_SIZE], &key, KEY_SIZE);

    n->key_count += 1;

    return 0;
}

// link the leaf nodes together
int link_leaf_node(Node *node, uint32_t next_node_id)
{
    size_t off = (NODE_SIZE - NODE_HDR_SIZE) - 4;
    memcpy(&node->bytes[off], &next_node_id, 4);

    return 0;
}

// set the lower bound of an internal node
void set_int_node_lb(Node *n, float lower_bound)
{
    n->lower_bound = lower_bound;
}

// write the key and the pointer to the child node into the internal node
int node_write_node_key(Node *n, float key, uint32_t node_id)
{
    if ((n->key_count) >= MAX_INTERNAL_KEYS)
        return -1;

    if ((n->key_count) == 0){
        memcpy(&n->bytes, &node_id, NODE_POINTER_SIZE);
    }
    
    size_t off = NODE_POINTER_SIZE + n->key_count * (KEY_SIZE + NODE_POINTER_SIZE);

    memcpy(&n->bytes[off], &key, KEY_SIZE);
    memcpy(&n->bytes[off + KEY_SIZE], &node_id, NODE_POINTER_SIZE);
    n->key_count += 1;
    return 0;
}

// encode (serialise) function for the node. converts to bytes to be saved on the disk
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

// decode (deserialise) function for the node. converts bytes from the disk into the node struct
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

