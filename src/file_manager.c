#include "file_manager.h"
#include <string.h>

int fm_open(FileManager* fm, const char* path, const char* mode){
    fm->fp = fopen(path, mode);
    fm->path = path;
    fm->data_reads = fm->data_writes = 0;
    return fm->fp?0:-1;
}
void fm_close(FileManager* fm){
    if(fm->fp) fclose(fm->fp);
    fm->fp = NULL;
}

int fm_read_block(FileManager* fm, uint32_t block_id, Block* out){
    if(!fm->fp) return -1;
    size_t off = (size_t)block_id * BLOCK_SIZE;
    if(fseek(fm->fp, (long)off, SEEK_SET)!=0) return -1;
    size_t n = fread(out->bytes, 1, BLOCK_SIZE, fm->fp);
    if(n!=BLOCK_SIZE) return -1;
    fm->data_reads++;
    return 0;
}

int fm_write_block(FileManager* fm, uint32_t block_id, const Block* in){
    if(!fm->fp) return -1;
    size_t off = (size_t)block_id * BLOCK_SIZE;
    if(fseek(fm->fp, (long)off, SEEK_SET)!=0) return -1;
    size_t n = fwrite(in->bytes, 1, BLOCK_SIZE, fm->fp);
    if(n!=BLOCK_SIZE) return -1;
    fflush(fm->fp);
    fm->data_writes++;
    return 0;
}

uint32_t fm_alloc_block(FileManager* fm, Block* zeroed){
    memset(zeroed->bytes, 0, BLOCK_SIZE);
    // append at end
    if(fseek(fm->fp, 0, SEEK_END)!=0) return (uint32_t)-1;
    long pos = ftell(fm->fp);
    uint32_t new_id = (uint32_t)(pos / BLOCK_SIZE);
    fwrite(zeroed->bytes, 1, BLOCK_SIZE, fm->fp);
    fflush(fm->fp);
    return new_id;
}
