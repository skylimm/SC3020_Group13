CC=gcc
CFLAGS=-std=c11 -O2 -Iinclude -Wall -Wextra

SRC=src/schema.c src/block.c src/file_manager.c src/buffer_pool.c src/heapfile.c src/cli.c src/main.c
OBJ=$(SRC:.c=.o)
BIN=project_c

all: $(BIN)

$(BIN): $(OBJ)
	$(CC) $(CFLAGS) -o $@ $(OBJ)

clean:
	rm -f $(OBJ) $(BIN)
