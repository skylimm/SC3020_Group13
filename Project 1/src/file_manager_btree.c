#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "file_manager_btree.h"

#if defined(_WIN32)
  #define FTELL  _ftelli64
  #define FSEEK  _fseeki64
  typedef __int64 file_off_t;
#else
  #define FTELL  ftello
  #define FSEEK  fseeko
  #include <sys/types.h>
  typedef off_t file_off_t;
#endif

// --- internal helpers ---
static int btfm_seek_page(BtreeFileManager *fm, uint32_t node_id) {
    file_off_t off = (file_off_t)node_id * (file_off_t)fm->page_size;
    if (FSEEK(fm->fp, off, SEEK_SET) != 0) return -1;
    return 0;
}

static file_off_t btfm_file_size_bytes(FILE *fp) {
    file_off_t cur = FTELL(fp);
    if (cur < 0) return -1;
    if (FSEEK(fp, 0, SEEK_END) != 0) return -1;
    file_off_t end = FTELL(fp);
    (void)FSEEK(fp, cur, SEEK_SET);
    return end;
}

// --- public API ---
int btfm_open(BtreeFileManager *fm, const char *path, size_t page_size) {
    if (!fm || !path) return -1;
    if (page_size == 0 || page_size != (size_t)NODE_SIZE) return -2;

    FILE *fp = fopen(path, "wb+");
    if (!fp) {
        fp = fopen(path, "wb+");
        if (!fp) return -3;
    }
    fm->fp = fp;
    fm->page_size = page_size;
    return 0;
}

int btfm_close(BtreeFileManager *fm) {
    if (!fm || !fm->fp) return -1;
    int rc = fflush(fm->fp);
    int rc2 = fclose(fm->fp);
    fm->fp = NULL;
    return (rc == 0 && rc2 == 0) ? 0 : -1;
}

int btfm_sync(BtreeFileManager *fm) {
    if (!fm || !fm->fp) return -1;
    return fflush(fm->fp) == 0 ? 0 : -1;
}

int btfm_alloc_node(BtreeFileManager *fm, uint32_t *out_node_id) {
    if (!fm || !fm->fp || !out_node_id) return -1;

    file_off_t sz = btfm_file_size_bytes(fm->fp);
    if (sz < 0) return -2;

    if ((size_t)sz % fm->page_size != 0) {
        return -3;
    }
    uint32_t new_id = (uint32_t)((size_t)sz / fm->page_size);

    uint8_t *zero = (uint8_t *)calloc(1, fm->page_size);
    if (!zero) return -4;

    if (FSEEK(fm->fp, 0, SEEK_END) != 0) { free(zero); return -5; }
    size_t wr = fwrite(zero, 1, fm->page_size, fm->fp);
    free(zero);
    if (wr != fm->page_size) return -6;

    if (fflush(fm->fp) != 0) return -7;

    *out_node_id = new_id;
    return 0;
}

int btfm_write_node(BtreeFileManager *fm, const Node *n) {
    if (!fm || !fm->fp || !n) return -1;

    uint8_t *buf = (uint8_t *)malloc(fm->page_size);
    if (!buf) return -2;
    memset(buf, 0, fm->page_size);

    if (encode_node(n, buf) != 0) {
        free(buf);
        return -3;
    }

    if (btfm_seek_page(fm, n->node_id) != 0) {
        free(buf);
        return -4;
    }
    size_t wr = fwrite(buf, 1, fm->page_size, fm->fp);
    free(buf);
    if (wr != fm->page_size) return -5;

    return 0;
}

int btfm_read_node(BtreeFileManager *fm, uint32_t node_id, Node *out) {
    if (!fm || !fm->fp || !out) return -1;

    file_off_t sz = btfm_file_size_bytes(fm->fp);
    if (sz < 0) return -2;

    file_off_t max_pages = (file_off_t)(sz / (file_off_t)fm->page_size);
    if ((file_off_t)node_id >= max_pages) return -3;

    if (btfm_seek_page(fm, node_id) != 0) return -4;

    uint8_t *buf = (uint8_t *)malloc(fm->page_size);
    if (!buf) return -5;

    size_t rd = fread(buf, 1, fm->page_size, fm->fp);
    if (rd != fm->page_size) {
        free(buf);
        return -6;
    }

    // bptree.h says: int decode_node(const uint8_t* src, Node* n);
    int rc = decode_node((const uint8_t*)buf, out);  // <-- fixed arg order/types
    free(buf);
    if (rc != 0) return -7;

    return 0;
}