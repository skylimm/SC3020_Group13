#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bptree.h"
#include "file_manager_btree.h"
#include "heapfile.h"

// Structure to store location of a record that needs to be deleted
typedef struct {
    uint32_t block_id;    // Which data block contains this record
    uint16_t slot_id;     // Which slot within that block
    float key_value;      // The FT_PCT_home value for this record
} RecordLocation;

// Structure to store results of the search operation
typedef struct {
    RecordLocation *records;           // Dynamic array of record locations to delete
    size_t count;                     // Number of records found
    size_t capacity;                  // Current capacity of the array
    uint32_t index_nodes_accessed;    // Count of internal nodes accessed during navigation
    uint32_t leaf_nodes_accessed;     // Count of leaf nodes accessed during search
    double total_key_value;           // Sum of all key values (for average calculation)
    double search_time_ms;            // Time taken for the search in milliseconds
} SearchResult;

// Helper function to ensure the records array has enough capacity
static int ensure_records_capacity(SearchResult *result, size_t needed)
{
    if (needed <= result->capacity)
        return 0;
    
    // Double the capacity or set to 256, whichever is larger
    size_t new_cap = (result->capacity == 0) ? 256 : result->capacity;
    while (new_cap < needed)
        new_cap *= 2;
    
    // Reallocate the array
    RecordLocation *tmp = realloc(result->records, new_cap * sizeof(RecordLocation));
    if (!tmp)
        return -1;
    
    result->records = tmp;
    result->capacity = new_cap;
    return 0;
}

// Helper function to read a key from a leaf node at a specific entry index
static float read_leaf_key(const Node *leaf, int entry_index)
{
    // In leaf nodes, each entry has format: [record_pointer][key]
    // Calculate offset to the key part of the entry
    size_t offset = entry_index * (RECORD_POINTER_SIZE + KEY_SIZE) + RECORD_POINTER_SIZE;
    
    float key;
    memcpy(&key, &leaf->bytes[offset], KEY_SIZE);
    return key;
}

// Helper function to read record pointer (block_id, slot_id) from a leaf node entry
static void read_leaf_record_pointer(const Node *leaf, int entry_index, uint32_t *block_id, uint16_t *slot_id)
{
    // Calculate offset to the record pointer part of the entry
    size_t offset = entry_index * (RECORD_POINTER_SIZE + KEY_SIZE);
    
    // Read block_id (first 4 bytes) and slot_id (next 4 bytes, but cast to uint16_t)
    memcpy(block_id, &leaf->bytes[offset], 4);
    
    uint32_t slot_temp;
    memcpy(&slot_temp, &leaf->bytes[offset + 4], 4);
    *slot_id = (uint16_t)slot_temp;
}

// Helper function to get the next leaf node ID from the current leaf's link
static uint32_t get_next_leaf_id(const Node *leaf)
{
    // Next leaf pointer is stored at the end of the node
    size_t off = (NODE_SIZE - NODE_HDR_SIZE) - 4;
    uint32_t next_id;
    memcpy(&next_id, &leaf->bytes[off], 4);
    
    return next_id;
}

// Main function to perform B+ tree range search for records with key >= min_key
int bptree_range_search(const char *btree_filename, float min_key, SearchResult *result)
{
    // Initialize result structure
    memset(result, 0, sizeof(SearchResult));
    
    // Record start time for performance measurement
    clock_t start_time = clock();
    
    // Open the B+ tree file
    BtreeFileManager btfm;
    
    if (btfm_open(&btfm, btree_filename, NODE_SIZE) != 0) {
        fprintf(stderr, "Failed to open B+ tree file: %s\n", btree_filename);
        return -1;
    }
    
    // Step 1: Find the root node and navigate to the appropriate leaf node
    
    // Find the root node - it should be the last allocated node (highest level)
    uint32_t root_node_id = 0;
    uint8_t max_level = 0;
    int found_root = 0;
    
    // Try reading nodes starting from 0 to find the root
    for (uint32_t test_id = 0; test_id < 100; test_id++) {
        Node test_node;
        int read_result = btfm_read_node(&btfm, test_id, &test_node);
        
        if (read_result == 0) {
            if (test_node.level > max_level) {
                max_level = test_node.level;
                root_node_id = test_id;
                found_root = 1;
            }
        } else {
            break; // No more nodes to read
        }
    }
    
    if (!found_root) {
        fprintf(stderr, "ERROR: Could not find any readable nodes in B+ tree file\n");
        btfm_close(&btfm);
        return -1;
    }
    

    
    uint32_t current_node_id = root_node_id;
    Node *current_node = malloc(sizeof(Node));
    if (!current_node) {
        btfm_close(&btfm);
        return -1;
    }
    
    // Navigate down the tree until we reach a leaf
    while (1) {
        // Read the current node
        if (btfm_read_node(&btfm, current_node_id, current_node) != 0) {
            fprintf(stderr, "Failed to read node %u\n", current_node_id);
            free(current_node);
            btfm_close(&btfm);
            return -1;
        }
        

        
        // Count this node access
        if (current_node->level == 1) {
            result->leaf_nodes_accessed++;
            break;  // We've reached a leaf node
        } else {
            result->index_nodes_accessed++;
        }
        
        // For internal nodes, find the appropriate child to follow
        // We need to find the rightmost child whose key is <= min_key
        if (current_node->level > 1) {
            // Read first child pointer (always at the beginning)
            uint32_t child_id;
            memcpy(&child_id, &current_node->bytes[0], 4);
            

            
            // Check each key in the internal node
            for (int i = 0; i < current_node->key_count; i++) {
                // Calculate offset for the i-th key-pointer pair
                size_t key_offset = NODE_POINTER_SIZE + i * (KEY_SIZE + NODE_POINTER_SIZE);
                
                float node_key;
                memcpy(&node_key, &current_node->bytes[key_offset], KEY_SIZE);
                

                
                // If this key is > min_key, we should go to the previous child
                // If this is the first key and it's > min_key, use first child
                if (node_key > min_key) {
                    if (i == 0) {
                        // Use first child pointer

                    } else {
                        // Use previous child pointer
                        size_t prev_ptr_offset = NODE_POINTER_SIZE + (i-1) * (KEY_SIZE + NODE_POINTER_SIZE) + KEY_SIZE;
                        memcpy(&child_id, &current_node->bytes[prev_ptr_offset], 4);

                    }
                    break;
                } else if (i == current_node->key_count - 1) {
                    // This is the last key and it's <= min_key, use its child pointer
                    size_t ptr_offset = NODE_POINTER_SIZE + i * (KEY_SIZE + NODE_POINTER_SIZE) + KEY_SIZE;
                    memcpy(&child_id, &current_node->bytes[ptr_offset], 4);

                }
            }
            
            current_node_id = child_id;

        }
    }
    
    // Step 2: Search through leaf nodes starting from the found leaf
    uint32_t leaf_id = current_node_id;
    int found_first = 0;  // Flag to indicate if we've found the first qualifying record
    

    
    while (leaf_id != UINT32_MAX) {  // Check for end of leaf chain marker
        // Read the current leaf node
        if (btfm_read_node(&btfm, leaf_id, current_node) != 0) {
            fprintf(stderr, "Failed to read leaf node %u\n", leaf_id);
            break;
        }
        
        result->leaf_nodes_accessed++;

        
        // Scan all entries in this leaf node
        for (int i = 0; i < current_node->key_count; i++) {
            float key = read_leaf_key(current_node, i);
            

            
            // If we haven't found the first qualifying record yet, check if this is it
            if (!found_first) {
                if (key > min_key) {
                    found_first = 1;

                } else {
                    continue;  // Skip this record, it's below our threshold
                }
            }
            
            // If we reach here, this record qualifies for deletion
            if (ensure_records_capacity(result, result->count + 1) != 0) {
                fprintf(stderr, "Failed to allocate memory for records\n");
                free(current_node);
                btfm_close(&btfm);
                return -1;
            }
            
            // Read the record location from the leaf entry
            uint32_t block_id;
            uint16_t slot_id;
            read_leaf_record_pointer(current_node, i, &block_id, &slot_id);
            
            // Store the record location
            result->records[result->count].block_id = block_id;
            result->records[result->count].slot_id = slot_id;
            result->records[result->count].key_value = key;
            result->total_key_value += key;
            result->count++;
        }
        
        // Move to the next leaf node
        uint32_t next_leaf_id = get_next_leaf_id(current_node);

        
        if (next_leaf_id == leaf_id) {
            // Prevent infinite loop if there's a circular reference

            break;
        }
        leaf_id = next_leaf_id;
        
        // If we haven't found any qualifying records in this leaf and we haven't
        // found the first qualifying record yet, continue to next leaf
        if (!found_first) {
            continue;
        }
        
        // If next_leaf_id is 0 or invalid, we've reached the end
        if (next_leaf_id == 0 || next_leaf_id >= 1000000) {  // Sanity check for valid node ID

            break;
        }
    }
    
    // Calculate elapsed time
    clock_t end_time = clock();
    result->search_time_ms = ((double)(end_time - start_time) / CLOCKS_PER_SEC) * 1000.0;
    
    // Clean up
    free(current_node);
    btfm_close(&btfm);
    
    // Print summary statistics
    printf("\nB+ Tree Search Results:\n");
    printf("  Records found: %zu\n", result->count);
    printf("  Internal nodes accessed: %u\n", result->index_nodes_accessed);
    printf("  Leaf nodes accessed: %u\n", result->leaf_nodes_accessed);
    printf("  Total nodes accessed: %u\n", result->index_nodes_accessed + result->leaf_nodes_accessed);
    
    if (result->count > 0) {
        double avg_key = result->total_key_value / result->count;
        printf("  Average FT_PCT_home: %.6f\n", avg_key);
    }
    printf("  Search time: %.3f ms\n", result->search_time_ms);
    
    return 0;
}

// Function to clean up the SearchResult structure
void cleanup_search_result(SearchResult *result)
{
    if (result->records) {
        free(result->records);
        result->records = NULL;
    }
    result->count = 0;
    result->capacity = 0;
}

// Brute force linear search function for comparison
int linear_scan_search(const char *db_filename, float min_key, SearchResult *result)
{
    // Initialize result structure
    memset(result, 0, sizeof(SearchResult));
    
    // Record start time for performance measurement
    clock_t start_time = clock();
    
    // Open the heap file (database)
    HeapFile hf;
    if (hf_open(&hf, db_filename, 64) != 0) { // Use buffer size of 64
        fprintf(stderr, "Failed to open database file: %s\n", db_filename);
        return -1;
    }
    
    uint8_t recbuf[512];  // Buffer to hold raw record bytes
    Row r;                // Decoded record structure
    
    // Scan every single block in the database
    for (uint32_t b = 0; b < hf.n_blocks; b++) {
        Block *cur = bp_fetch(&hf.bp, b);  // Get block from buffer pool
        if (!cur) {
            fprintf(stderr, "Failed to fetch block %u\n", b);
            hf_close(&hf);
            return -1;
        }
        
        // Count this as a data block access
        result->leaf_nodes_accessed++; // Using this field to count data blocks accessed
        
        int used = block_used_count(cur);
        int cap = block_capacity_records(hf.schema.record_size);
        if (used > cap) used = cap;
        
        // Scan every record in this block
        for (int s = 0; s < used; s++) {
            // Read raw bytes of record
            if (block_read_record(cur, hf.schema.record_size, s, recbuf) != 0) {
                fprintf(stderr, "Failed to read record at block %u, slot %d\n", b, s);
                continue;
            }
            
            // Convert raw bytes to Row structure
            decode_row(&hf.schema, recbuf, &r);
            
            // Check if this record qualifies for deletion
            if (r.ft_pct_home > min_key) {
                // Ensure we have space in our results array
                if (ensure_records_capacity(result, result->count + 1) != 0) {
                    fprintf(stderr, "Failed to allocate memory for records\n");
                    hf_close(&hf);
                    return -1;
                }
                
                // Store the record location
                result->records[result->count].block_id = b;
                result->records[result->count].slot_id = (uint16_t)s;
                result->records[result->count].key_value = r.ft_pct_home;
                result->total_key_value += r.ft_pct_home;
                result->count++;
            }
        }
        
        // Progress tracking (silent)
    }
    
    // Calculate elapsed time
    clock_t end_time = clock();
    result->search_time_ms = ((double)(end_time - start_time) / CLOCKS_PER_SEC) * 1000.0;
    
    // Clean up
    hf_close(&hf);
    
    // Print summary statistics
    printf("\nLinear Scan Search Results:\n");
    printf("  Records found: %zu\n", result->count);
    printf("  Data blocks accessed: %u\n", result->leaf_nodes_accessed); // We reused this field
    printf("  Search time: %.3f ms\n", result->search_time_ms);
    
    if (result->count > 0) {
        double avg_key = result->total_key_value / result->count;
        printf("  Average FT_PCT_home: %.6f\n", avg_key);
    }
    
    return 0;
}

// Function to run comparison tests between B+ tree and linear scan
void run_comparison_tests()
{
    SearchResult bptree_results[3];
    SearchResult linear_results[3];
    
    // Initialize all result structures to avoid cache/buffer issues
    for (int i = 0; i < 3; i++) {
        memset(&bptree_results[i], 0, sizeof(SearchResult));
        memset(&linear_results[i], 0, sizeof(SearchResult));
    }
    
    printf("=== PERFORMANCE COMPARISON: B+ Tree vs Linear Scan ===\n");
    printf("Target: Find all records with FT_PCT_home > 0.9\n\n");
    
    // Run B+ tree search 3 times
    printf("Running B+ Tree searches (3 times):\n");
    for (int i = 0; i < 3; i++) {
        printf("\n--- B+ Tree Search Run #%d ---\n", i + 1);
        
        // Ensure clean state for each run
        cleanup_search_result(&bptree_results[i]);
        memset(&bptree_results[i], 0, sizeof(SearchResult));
        
        if (bptree_range_search("btree.db", 0.9f, &bptree_results[i]) != 0) {
            printf("B+ Tree search #%d failed!\n", i + 1);
            return;
        }
    }
    
    // Run linear search 3 times
    printf("\n\nRunning Linear Scan searches (3 times):\n");
    for (int i = 0; i < 3; i++) {
        printf("\n--- Linear Scan Run #%d ---\n", i + 1);
        
        // Ensure clean state for each run
        cleanup_search_result(&linear_results[i]);
        memset(&linear_results[i], 0, sizeof(SearchResult));
        
        if (linear_scan_search("data.db", 0.9f, &linear_results[i]) != 0) {
            printf("Linear scan #%d failed!\n", i + 1);
            return;
        }
    }
    
    // Calculate and display comparison statistics
    printf("\n\n=== COMPARISON RESULTS ===\n");
    
    // Calculate averages for B+ tree
    double avg_bptree_time = 0;
    uint32_t avg_bptree_nodes = 0;
    for (int i = 0; i < 3; i++) {
        avg_bptree_time += bptree_results[i].search_time_ms;
        avg_bptree_nodes += (bptree_results[i].index_nodes_accessed + bptree_results[i].leaf_nodes_accessed);
    }
    avg_bptree_time /= 3;
    avg_bptree_nodes /= 3;
    
    // Calculate averages for linear scan
    double avg_linear_time = 0;
    uint32_t avg_linear_blocks = 0;
    for (int i = 0; i < 3; i++) {
        avg_linear_time += linear_results[i].search_time_ms;
        avg_linear_blocks += linear_results[i].leaf_nodes_accessed; // We reused this field for data blocks
    }
    avg_linear_time /= 3;
    avg_linear_blocks /= 3;
    
    printf("B+ Tree Search (average of 3 runs):\n");
    printf("  Time: %.3f ms\n", avg_bptree_time);
    printf("  Nodes accessed: %u\n", avg_bptree_nodes);
    printf("  Records found: %zu\n", bptree_results[0].count);
    
    printf("\nLinear Scan (average of 3 runs):\n");
    printf("  Time: %.3f ms\n", avg_linear_time);
    printf("  Blocks accessed: %u\n", avg_linear_blocks);
    printf("  Records found: %zu\n", linear_results[0].count);
    
    printf("\nPerformance Improvement:\n");
    printf("  Time speedup: %.2fx\n", avg_linear_time / avg_bptree_time);
    printf("  I/O reduction: %.2fx\n", (double)avg_linear_blocks / avg_bptree_nodes);
    
    // Clean up all results
    for (int i = 0; i < 3; i++) {
        cleanup_search_result(&bptree_results[i]);
        cleanup_search_result(&linear_results[i]);
    }
}

// Function to search for records to delete (without actually deleting them)
int bptree_range_search_for_deletion(const char *btree_filename, float min_key, SearchResult *result)
{
    printf("\n=== Starting Record Deletion Process ===\n");
    
    // Find all records to delete using B+ tree search
    if (bptree_range_search(btree_filename, min_key, result) != 0) {
        fprintf(stderr, "Failed to find records for deletion\n");
        return -1;
    }
    
    if (result->count == 0) {
        printf("No records found for deletion.\n");
        return 0;
    }
    
    printf("Found %zu records to delete.\n", result->count);
    
    return 0;
}

// Function to actually perform the deletion and rebuild B+ tree
int bptree_perform_deletion(const char *db_filename, SearchResult *result)
{
    if (result->count == 0) {
        printf("No records to delete.\n");
        return 0;
    }
    
    printf("\nProceeding with deletion of %zu records...\n", result->count);
    
    // Open the heap file and delete records
    HeapFile hf;
    if (hf_open(&hf, db_filename, 64) != 0) {
        fprintf(stderr, "Failed to open database file: %s\n", db_filename);
        return -1;
    }
    
    // Sort records by block_id and slot_id in descending order
    // This ensures we delete from end to beginning to avoid slot shifting issues
    for (size_t i = 0; i < result->count - 1; i++) {
        for (size_t j = i + 1; j < result->count; j++) {
            RecordLocation *a = &result->records[i];
            RecordLocation *b = &result->records[j];
            
            // Sort by block_id descending, then by slot_id descending
            if (a->block_id < b->block_id || 
                (a->block_id == b->block_id && a->slot_id < b->slot_id)) {
                RecordLocation temp = *a;
                *a = *b;
                *b = temp;
            }
        }
    }
    
    // Delete records from heap file
    size_t deleted_count = 0;
    for (size_t i = 0; i < result->count; i++) {
        if (hf_delete_record(&hf, result->records[i].block_id, result->records[i].slot_id) == 0) {
            deleted_count++;
        }
    }
    
    printf("Successfully deleted %zu records from database.\n", deleted_count);
    
    // Rebuild the B+ tree with remaining records
    printf("Rebuilding B+ tree index...\n");
    extern int scan_db(HeapFile *hf);
    
    if (scan_db(&hf) != 0) {
        fprintf(stderr, "Failed to rebuild B+ tree index\n");
        hf_close(&hf);
        return -1;
    }
    
    printf("B+ tree index rebuilt successfully.\n");
    
    // Clean up
    hf_close(&hf);
    
    printf("=== Record Deletion Process Complete ===\n\n");
    
    return 0;
}