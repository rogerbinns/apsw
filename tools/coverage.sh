#!/bin/bash
#

set -e

if [ $# = 0 ]
then
  args="-m apsw.tests"
else
  args="$@"
fi

# Measure code coverage
GCOVOPTS="-b -H"
GCOVOPTS=""
rm -f *.gcda *.gcov *.gcno sqlite3/*.gcov apsw/*.so
# find python
PYTHON=${PYTHON:-python3} # use whatever is in the path
INCLUDEDIR=`$PYTHON -c "import sysconfig; print(sysconfig.get_path('include'))"`
CC=`$PYTHON -c "import sysconfig; print(sysconfig.get_config_var('CC'))"`
CFLAGS=`$PYTHON -c "import sysconfig; print(sysconfig.get_config_var('CFLAGS'))"`
MOREFLAGS=`$PYTHON -c "import sysconfig; print(sysconfig.get_config_var('CCSHARED'))"`
LINKER=`$PYTHON -c "import sysconfig; print(sysconfig.get_config_var('LDSHARED'))"`
SOSUFFIX=`$PYTHON -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"`

PROFILE="-O0 --coverage"

set -ex

if [ ! -z "$USE_CLANG" ]
then
  CC=clang
  LINKER="clang -shared"
  GCOVWRAPPER="llvm-cov"
fi

if [ -f sqlite3/sqlite3config.h ]
then
    CFLAGS="$CFLAGS -DAPSW_USE_SQLITE_CONFIG"
fi

export APSW_TEST_LARGE=t

set -ex
$CC $CFLAGS $MOREFLAGS $PROFILE -DAPSW_NO_NDEBUG -DSQLITE_ENABLE_API_ARMOR -DAPSW_USE_SQLITE_AMALGAMATION  -DAPSW_TESTFIXTURES -I$INCLUDEDIR -Isrc -Isqlite3 -I. -c src/apsw.c
$LINKER $PROFILE apsw.o -o apsw/__init__$SOSUFFIX
set +ex
$PYTHON $args
res=$?
[ $res -eq 0 ] && env PYTHONPATH=. $PYTHON tools/fi.py
$GCOVWRAPPER gcov $GCOVOPTS apsw.gcno > /dev/null
mv sqlite3.c.gcov sqlite3/
rm -f src/*.gcov
mv *.gcov src/
$PYTHON tools/coverageanalyser.py
exit $res
