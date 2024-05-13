#!/bin/bash
# Run valgrind on testsuite
#
# A debug Python build is recommended - see Makefile

if [ $# = 0 ]
then
  args="-m apsw.tests"
else
  args="$@"
fi

DEFS="-DAPSW_USE_SQLITE_AMALGAMATION -DAPSW_USE_SQLITE_CONFIG -DSQLITE_ENABLE_FTS5"

if [ -z "$CALLGRIND" ]
then
   cflags="-DAPSW_TESTFIXTURES -DAPSW_NO_NDEBUG"
   opt=""
   showleaks="--leak-check=full --leak-resolution=high --show-leak-kinds=all --track-origins=yes --expensive-definedness-checks=yes"
   options="--track-fds=yes --num-callers=50 $showleaks --freelist-vol=500000000 --suppressions=`dirname $0`/sqlite3.supp --track-origins=yes"
   APSW_TEST_ITERATIONS=${APSW_TEST_ITERATIONS:=1}
   apswopt="APSW_NO_MEMLEAK=t APSW_TEST_ITERATIONS=$APSW_TEST_ITERATIONS"
   APSW_VALGRIND=t
   export APSW_VALGRIND
   DEFS="$DEFS -DAPSW_NO_NDEBUG -DAPSW_TESTFIXTURES"
else
   options="--tool=callgrind --dump-line=yes --trace-jump=yes"
   cflags=""
   opt="-O2"
   apswopt=""
fi



# find python
PYTHON=${PYTHON:-python3} # use whatever is in the path
INCLUDEDIR=`$PYTHON -c "import sysconfig; print(sysconfig.get_path('include'))"`
CC=`$PYTHON -c "import sysconfig; print(sysconfig.get_config_var('CC'))"`
CFLAGS=`$PYTHON -c "import sysconfig; print(sysconfig.get_config_var('CFLAGS'))"`
MOREFLAGS=`$PYTHON -c "import sysconfig; print(sysconfig.get_config_var('CCSHARED'))"`
LINKER=`$PYTHON -c "import sysconfig; print(sysconfig.get_config_var('LDSHARED'))"`
SOSUFFIX=`$PYTHON -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"`

rm -f apsw/*.so
set -ex

$CC $CFLAGS $MOREFLAGS $opt $cflags $DEFS -Isqlite3/ -I$INCLUDEDIR -Isrc -I. -c src/apsw.c
$LINKER -g $opt apsw.o -o apsw/__init__$SOSUFFIX
$CC $CFLAGS $MOREFLAGS $opt $cflags $DEFS -I$INCLUDEDIR -Isrc -c src/unicode.c
$LINKER -g $opt unicode.o -o apsw/_unicode$SOSUFFIX
time env $apswopt valgrind $options $PYTHON $args
