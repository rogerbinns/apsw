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
  GCOVWRAPPER="llvm-cov"
  PROFILE="-O2 --coverage"
fi

if [ -f sqlite3/sqlite3config.h ]
then
    CFLAGS="$CFLAGS -DAPSW_USE_SQLITE_CONFIG"
fi

export APSW_TEST_LARGE=t COVERAGE_RUN=true

set -ex
$CC $CFLAGS $MOREFLAGS $PROFILE -DAPSW_NO_NDEBUG -DSQLITE_ENABLE_API_ARMOR -DAPSW_USE_SQLITE_AMALGAMATION  -DAPSW_TESTFIXTURES -DSQLITE_ENABLE_FTS5 -I$INCLUDEDIR -Isrc -Isqlite3 -I. -c src/apsw.c
$LINKER $PROFILE apsw.o -o apsw/__init__$SOSUFFIX
$CC $CFLAGS $MOREFLAGS $PROFILE -DAPSW_TESTFIXTURES -I$INCLUDEDIR -Isrc -UNDEBUG -c src/unicode.c
$LINKER $PROFILE unicode.o -o apsw/_unicode$SOSUFFIX
set +ex
echo "Running $PYTHON $args"
env PYTHONPATH=. $PYTHON $args
res=$?
[ $res -eq 0 -a -z "$NO_FI" ] && echo "Running $PYTHON tools/fi.py" && env PYTHONPATH=. $PYTHON tools/fi.py
$GCOVWRAPPER gcov $GCOVOPTS apsw.gcno unicode.gcno > /dev/null
mv sqlite3.c.gcov sqlite3/
rm -f src/*.gcov
mv *.gcov src/
$PYTHON tools/coverageanalyser.py
exit $res
