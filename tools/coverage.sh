#!/bin/bash
#

set -e

if [ $# = 0 ]
then
  args="-m apsw.tests -vf"
else
  args="$@"
fi

# Measure code coverage
GCOVOPTS="-b -H"
GCOVOPTS=""
rm -f *.gcda *.gcov *.gcno sqlite3/*.gcov apsw/*.so src/*.gcov
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
  PROFILE="-O2 --coverage"
fi

case "$CC" in
  *clang*)
    GCOVWRAPPER="llvm-cov"
    ;;
esac


if [ -f sqlite3/sqlite_cfg.h ]
then
    CFLAGS="$CFLAGS -D_HAVE_SQLITE_CONFIG_H"
fi

export APSW_TEST_LARGE=t COVERAGE_RUN=true

env CC=$CC $PYTHON setup.py build_test_extension

OUR_CFLAGS=" -UNDEBUG  -DAPSW_FAULT_INJECT -DAPSW_DEBUG -DSQLITE_DEBUG"
OUR_EXTENSIONS="-DSQLITE_ENABLE_COLUMN_METADATA=1 -DSQLITE_ENABLE_FTS4=1 -DSQLITE_ENABLE_FTS3=1 -DSQLITE_ENABLE_FTS3_PARENTHESIS=1 -DSQLITE_ENABLE_RTREE=1 -DSQLITE_ENABLE_STAT4=1 -DSQLITE_ENABLE_FTS5=1 -DSQLITE_ENABLE_GEOPOLY=1 -DSQLITE_ENABLE_MATH_FUNCTIONS=1 -DSQLITE_ENABLE_DBSTAT_VTAB=1 -DSQLITE_ENABLE_SESSION=1 -DSQLITE_ENABLE_PERCENTILE=1 -DSQLITE_ENABLE_CARRAY=1 -DSQLITE_ENABLE_GEOPOLY=1"

set -ex
$CC $CFLAGS $MOREFLAGS $PROFILE $OUR_CFLAGS $OUR_EXTENSIONS -DAPSW_USE_SQLITE_AMALGAMATION -I$INCLUDEDIR -Isrc -Isqlite3 -I. -c src/apsw.c
$LINKER $PROFILE apsw.o -o apsw/__init__$SOSUFFIX
$CC $CFLAGS $MOREFLAGS $PROFILE $OUR_CFLAGS -I$INCLUDEDIR -Isrc -c src/unicode.c
$LINKER $PROFILE unicode.o -o apsw/_unicode$SOSUFFIX
set +ex
echo "Running $PYTHON $args"
env PYTHONPATH=. $PYTHON $args && $PYTHON -m apsw.tests.async_meta
res=$?
[ $res -eq 0 -a -z "$NO_FI" ] && echo "Running $PYTHON tools/fi.py $FI_ARGS" && env PYTHONPATH=. $PYTHON tools/fi.py $FI_ARGS
$GCOVWRAPPER gcov $GCOVOPTS *.gcno > /dev/null

echo ; echo
mv sqlite3.c.gcov sqlite3/
rm -f src/*.gcov .coverage*
mv *.gcov src/
$PYTHON tools/coverageanalyser.py
exit $res
