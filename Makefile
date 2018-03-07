# Compiler Flags

CC=g++
CFLAGS=-std=c++1z -Wall -O2
#CFLAGS=-std=c++1z -Wall -g

# Input

INCLUDE=-Iinclude -Ilib/libcuckoo -Ilib/pmdk/src/include
SRC_DIR=src
TEST_DIR=test

#LIB_DIR=-Llib/pmdk/src/debug/
LIB_DIR=-Llib/pmdk/src/nondebug/
ST_LIBS=-lpmemobj -lpmem
DY_LIBS=-ldl -lstdc++fs -pthread

LDFLAGS=$(LIB_DIR) -Wl,-Bstatic $(ST_LIBS) -Wl,-Bdynamic $(DY_LIBS)

# Artifacts

BIN_DIR=bin

# Targets

all : main

lib : makeDir base
	ar rcs $(BIN_DIR)/libmidas.a $(BIN_DIR)/*.o

main : makeDir
	$(CC) $(CFLAGS) $(INCLUDE) $(SRC_DIR)/*.cpp $(LDFLAGS) -o $(BIN_DIR)/$@

test : makeDir
	$(CC) $(CFLAGS) $(INCLUDE) $(TEST_DIR)/*.cpp $(LDFLAGS) -o $(BIN_DIR)/$@

listTest : makeDir
	$(CC) $(CFLAGS) $(INCLUDE) $(TEST_DIR)/$@.cpp $(LDFLAGS) -o $(BIN_DIR)/$@

hashTest : makeDir
	$(CC) $(CFLAGS) $(INCLUDE) $(TEST_DIR)/$@.cpp $(LDFLAGS) -o $(BIN_DIR)/$@

dirtyRead : makeDir base
	$(CC) -c $(CFLAGS) $(INCLUDE) $(TEST_DIR)/$@.cpp -o $(BIN_DIR)/$@.o
	$(CC) $(CFLAGS) $(BIN_DIR)/*.o $(LDFLAGS) -o $(BIN_DIR)/$@

fuzzyRead : makeDir base
	$(CC) -c $(CFLAGS) $(INCLUDE) $(TEST_DIR)/$@.cpp -o $(BIN_DIR)/$@.o
	$(CC) $(CFLAGS) $(BIN_DIR)/*.o $(LDFLAGS) -o $(BIN_DIR)/$@

lostUpdate1 : makeDir base
	$(CC) -c $(CFLAGS) $(INCLUDE) $(TEST_DIR)/$@.cpp -o $(BIN_DIR)/$@.o
	$(CC) $(CFLAGS) $(BIN_DIR)/*.o $(LDFLAGS) -o $(BIN_DIR)/$@

lostUpdate2 : makeDir base
	$(CC) -c $(CFLAGS) $(INCLUDE) $(TEST_DIR)/$@.cpp -o $(BIN_DIR)/$@.o
	$(CC) $(CFLAGS) $(BIN_DIR)/*.o $(LDFLAGS) -o $(BIN_DIR)/$@

writeSkew : makeDir base
	$(CC) -c $(CFLAGS) $(INCLUDE) $(TEST_DIR)/$@.cpp -o $(BIN_DIR)/$@.o
	$(CC) $(CFLAGS) $(BIN_DIR)/*.o $(LDFLAGS) -o $(BIN_DIR)/$@

badTiming : makeDir base
	$(CC) -c $(CFLAGS) $(INCLUDE) $(TEST_DIR)/$@.cpp -o $(BIN_DIR)/$@.o
	$(CC) $(CFLAGS) $(BIN_DIR)/*.o $(LDFLAGS) -o $(BIN_DIR)/$@

base :
	$(CC) -c $(CFLAGS) $(INCLUDE) $(SRC_DIR)/store.cpp -o $(BIN_DIR)/store.o
	$(CC) -c $(CFLAGS) $(INCLUDE) $(SRC_DIR)/string.cpp -o $(BIN_DIR)/string.o

makeDir :
	mkdir -p $(BIN_DIR)
	cd $(BIN_DIR)

clean :
	rm -rf $(BIN_DIR)/*
