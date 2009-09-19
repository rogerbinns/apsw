#!/bin/bash 
#

set -e

if [ $# = 0 ]
then
  args="tests.py"
else
  args="$@"
fi

# Measure code coverage
GCOVOPTS="-b -c"
GCOVOPTS=""
rm -f *.gcda *.gcov *.gcno apsw.so
# find python
PYTHON=python # use whatever is in the path
INCLUDEDIR=`$PYTHON -c "import distutils.sysconfig,sys; sys.stdout.write(distutils.sysconfig.get_python_inc())"`

CFLAGS=''

if [ -f sqlite3async.c ]
then
   CFLAGS='-DAPSW_USE_SQLITE_ASYNCVFS_C="sqlite3async.c" -DAPSW_USE_SQLITE_ASYNCVFS_H="sqlite3async.h"'
fi
if [ -f sqlite3genfkey.c ]
then
   CFLAGS="$CFLAGS -DAPSW_USE_SQLITE_GENFKEY=\"sqlite3genfkey.c\""
fi
set -x
gcc -pthread -fno-strict-aliasing -ftest-coverage -fprofile-arcs -g -fPIC -Wall -Wextra $CFLAGS -DEXPERIMENTAL -DSQLITE_DEBUG -DAPSW_USE_SQLITE_AMALGAMATION=\"sqlite3.c\" -DAPSW_NO_NDEBUG -DAPSW_TESTFIXTURES -I$INCLUDEDIR -I. -Isqlite3 -Isrc -c src/apsw.c
gcc -pthread -ftest-coverage -fprofile-arcs -g -shared apsw.o -o apsw.so
gcc -fPIC -shared -Isqlite3 -I. -o testextension.sqlext -Isqlite3 src/testextension.c
set +e
$PYTHON $args
res=$?
gcov $GCOVOPTS src/apsw.c > /dev/null
python tools/coverageanalyser.py
exit $res
