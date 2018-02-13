# Compiler Flags

CC=g++
CFLAGS=-std=c++1z -Wall -g

# Input

INCLUDE=-Iinclude -Ilib/libcuckoo -Ilib/pmdk/src/include
SRC_DIR=src
TEST_DIR=test

LIB_DIR=-Llib/pmdk/src/debug/
ST_LIBS=-lpmemobj -lpmem
DY_LIBS=-ldl -lstdc++fs -pthread

LDFLAGS=$(LIB_DIR) -Wl,-Bstatic $(ST_LIBS) -Wl,-Bdynamic $(DY_LIBS)

# Artifacts

BIN_DIR=bin

# Targets

all : main

main : makeDir
	$(CC) $(CFLAGS) $(INCLUDE) $(SRC_DIR)/*.cpp $(LDFLAGS) -o $(BIN_DIR)/$@

test : makeDir
	$(CC) $(CFLAGS) $(INCLUDE) $(TEST_DIR)/*.cpp $(LDFLAGS) -o $(BIN_DIR)/$@

listTest : makeDir
	$(CC) $(CFLAGS) $(INCLUDE) $(TEST_DIR)/$@.cpp $(LDFLAGS) -o $(BIN_DIR)/$@

hashTest : makeDir
	$(CC) $(CFLAGS) $(INCLUDE) $(TEST_DIR)/$@.cpp $(LDFLAGS) -o $(BIN_DIR)/$@

makeDir :
	mkdir -p $(BIN_DIR)
	cd $(BIN_DIR)

clean :
	rm -rf $(BIN_DIR)/*
