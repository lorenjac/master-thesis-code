# A Persistent Hash Table

## Prequisistes

* a c++17 compliant installation of gcc
* local copy of pmdk

## Building

1. Create directory `lib`
2. Inside `lib` create a (soft) link to your local copy of *pmdk*
3. Unless you have not yet done so, build the pmdk libraries (need not install)
4. Run `make` (it will create a bin folder automatically)

```
mkdir lib
ln -s <path to pmdk repository> lib/pmdk
make
```

## Running

All programs in this repository require the user to supply the path to a file which
acts as non-volatile memory.

If you are working in an NFS environment or inside a folder shared with a virtual machine,
you may experience crashes as the pmdk library tries to acquire file locks which seems
to cause trouble on some systems. Therefore it is **strongly recommended** that you
only use local folders (e.g. `/tmp`) or stay within the virtual machine filesystem
at all times, respectively.

Simply run the desired binary and follow the instructions.

```
./bin/<program> 
```

