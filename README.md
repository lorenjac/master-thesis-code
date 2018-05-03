# Midas - An NVRAM-Resident Key-Value Store with Serializable Transactions

## Prequisistes

* A **C++17** compliant installation of gcc (`-std=c++1z` is sufficient)
* A local copy of **pmdk**
* A local copy of **libcuckoo**

## Build Setup

In this folder, create directory `lib`. Inside `lib` create soft links to your
local copies of **pmdk** and **libcuckoo**. The Makefile requires these names.

```
mkdir lib
ln -s <path to pmdk repository> lib/pmdk
ln -s <path to libcuckoo repository> lib/libcuckoo
```

In the end, build the **pmdk** libraries using `make`. Installing the binaries
is not required. Instead the symlinks will be used to link against the **pmdk**
statically.

## Build

Simply run `make` (it will create a bin folder automatically). In order to build
static libraries, run `make lib`.
