#!/bin/bash

# Run the clang analyser

CLANGDIR=/space/llvm

if [ $# = 0 ]
then
  args="p-apsw.c"
else
  args="$@"
fi


export PATH=$CLANGDIR/Release/bin:$CLAMGDIR/bin:$PATH 

# ensure clang is up to date
(
    cd $CLANGDIR
    svn up
    cd tools/clang
    svn up
    cd ../..
    make -j4
)

# clang doesn't analyse files that are #included so we have to generate an output file that has already done the inclusions

rm -f p-apsw.c

cp apsw.c p-apsw.c

# ::TODO:: work out if I can insert #line directives and if clang pays any attention

sed -i -e '/#include APSW_USE_SQLITE_AMALGAMATION/{r sqlite3.c' -e 'd;}' p-apsw.c 

for i in traceback pointerlist statementcache
do
  sed -i -e "/#include \"${i}.c\"/{r ${i}.c" -e 'd;}' p-apsw.c
done

$CLANGDIR/tools/clang/utils/scan-build gcc -DSQLITE_DEBUG -DAPSW_TESTFIXTURES -DAPSW_NO_NDEBUG -DEXPERIMENTAL -DSQLITE_THREADSAFE=1 -DAPSW_USE_SQLITE_AMALGAMATION=\"sqlite3.c\" -I. -I/usr/include/python2.5 -c $args