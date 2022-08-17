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
GCOVOPTS="-b -c"
GCOVOPTS=""
rm -f *.gcda *.gcov *.gcno sqlite3/*.gcov apsw.so apsw.*.so
# find python
PYTHON=${PYTHON:-python3} # use whatever is in the path
INCLUDEDIR=`$PYTHON -c "import sysconfig; print(sysconfig.get_path('include'))"`
CC=`$PYTHON -c "import sysconfig; print(sysconfig.get_config_var('CC'))"`
CFLAGS=`$PYTHON -c "import sysconfig; print(sysconfig.get_config_var('CFLAGS'))"`
MOREFLAGS=`$PYTHON -c "import sysconfig; print(sysconfig.get_config_var('CCSHARED'))"`
LINKER=`$PYTHON -c "import sysconfig; print(sysconfig.get_config_var('LDSHARED'))"`
SOSUFFIX=`$PYTHON -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"`
set -ex

if [ -f sqlite3/sqlite3config.h ]
then
    CFLAGS="$CFLAGS -DAPSW_USE_SQLITE_CONFIG"
fi

PROFILE="-ftest-coverage -fprofile-arcs -g"

set -ex
$CC $CFLAGS $MOREFLAGS $PROFILE -DAPSW_NO_NDEBUG -DSQLITE_ENABLE_API_ARMOR -DAPSW_USE_SQLITE_AMALGAMATION  -DAPSW_TESTFIXTURES -I$INCLUDEDIR -Isrc -Isqlite3 -I. -c src/apsw.c
$LINKER $PROFILE apsw.o -o apsw/__init__$SOSUFFIX
$PYTHON setup.py build_test_extension
set +ex
$PYTHON $args
res=$?
gcov $GCOVOPTS apsw.gcno > /dev/null
mv sqlite3.c.gcov sqlite3/
rm -f src/*.gcov
mv *.gcov src/
$PYTHON tools/coverageanalyser.py
exit $res
