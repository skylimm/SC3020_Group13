#include <bptree.h>
#include <string.h>

static inline node_get_type(Node* n) {
    return n->bytes[0];
}

static inline node_get_key_count(Node* n) {
    return (uint16_t)(n->bytes[1] | (n->bytes[2] << 8));
}
static inline node_get_lower_bound(Node* n) {
    return (float)(n->bytes[11] | (n->bytes[12] << 8) | (n->bytes[13] << 16) | (n->bytes[14] << 24));
}


// static inline node_parse_header(const Node* n, uint8_t* node_type, uint16_t* key_count, uint32_t* parent_id, float* lower_bound) {
//     if (node_type)  *node_type  = n->bytes[0];
//     if (key_count)  *key_count  = (uint16_t)(n->bytes[1] | (n->bytes[2] << 8));
//     // is wrong
//     if (parent_id)  *parent_id  = (uint32_t)(n->bytes[3] | (n->bytes[4] << 8) | (n->bytes[5] << 16) | (n->bytes[6] << 24));
//     if (lower_bound)
//         *lower_bound = (float)(n->bytes[11] | (n->bytes[12] << 8) | (n->bytes[13] << 16) | (n->bytes[14] << 24));
//     ; // TODO
//     return 0;
// }

int node_write_record_key(Node *n, float key, uint32_t block_id, int slot)
{
    if (node_get_key_count(n) >= MAX_LEAF_KEYS)
        return -1;
    
        //needs for the pointer to next node

    size_t off = NODE_HDR_SIZE + ((size_t)slot + block_id + (size_t)key) * node_get_key_count(n);
    uint8_t ptr[POINTER_SIZE];

    memcpy(ptr, &block_id, 4);
    memcpy(ptr + 4, &slot, 4);

    memcpy(&n->bytes[off], &key, KEY_SIZE);
    memcpy(&n->bytes[off + KEY_SIZE], ptr, POINTER_SIZE);
    return 0;
}

int node_write_node_key(Node *n, float key, uint32_t node_id, int slot)
{
    int key_count = node_get_key_count(n);
    if ((key_count) >= MAX_LEAF_KEYS)
        return -1;
    // needs the initial pointer

    size_t off = NODE_HDR_SIZE + POINTER_SIZE + ((size_t)slot + node_id + (size_t)key) * key_count;
    uint8_t ptr[POINTER_SIZE];
    memcpy(ptr, &node_id, 4);
    memcpy(ptr + 4, &slot, 4);
    memcpy(&n->bytes[off], &key, KEY_SIZE);
    memcpy(&n->bytes[off + KEY_SIZE], ptr, POINTER_SIZE);
}
