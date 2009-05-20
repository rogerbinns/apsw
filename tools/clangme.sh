#!/bin/bash

# Run the clang analyser

CLANGDIR=/space/llvm

if [ $# = 0 ]
then
  args="src/apsw.c"
else
  args="$@"
fi


export PATH=$CLANGDIR/Release/bin:$CLANGDIR/bin:$PATH 

# ensure clang is up to date
(
    cd $CLANGDIR
    make update
    cd tools/clang
    svn up
    cd ..
    make
)

$CLANGDIR/tools/clang/utils/scan-build -analyze-headers gcc -DSQLITE_ENABLE_FTS -DSQLITE_ENABLE_RTREE -DSLITE_ENABLE_ICU -DSQLITE_DEBUG -DAPSW_TESTFIXTURES -DAPSW_NO_NDEBUG -DEXPERIMENTAL -DSQLITE_THREADSAFE=1 -DAPSW_USE_SQLITE_AMALGAMATION=\"sqlite3.c\" -I. -Isqlite3 -Isrc -I/usr/include/python2.6 -c $args
