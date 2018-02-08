# A Persistent Hash Table

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

Simply run `make` (it will create a bin folder automatically)

```
make
```

## Running

If you are working in an NFS environment or inside a folder shared with a
virtual machine, you may experience crashes as the pmdk library tries to acquire
file locks which seems to cause trouble on some systems. Therefore it is
**strongly recommended** that you only use local folders (e.g. `/tmp`) or stay
within the virtual machine filesystem at all times, respectively. Currently,
The main executable is hard-coded to use `/tmp/nvm`. Use or change at will.

The main executable does not require arguments. Providing no arguments simply
does nothing but to load the key-value store and shut it down again. There are
commands for writing, reading, and deleting items. For help provide `h` or
`help`.

```
./bin/<program>
```
