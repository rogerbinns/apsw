#!/bin/sh 
#
# Run callgrind on testsuite
#
#

# find python
PYTHON=python # use whatever is in the path
INCLUDEDIR=`$PYTHON -c "import distutils.sysconfig; print distutils.sysconfig.get_python_inc()"`
set -x
gcc -pthread -fno-strict-aliasing  -g -O2 -fPIC -Wall -DEXPERIMENTAL -DSQLITE_OMIT_LOAD_EXTENSION -Isqlite3 -I$INCLUDEDIR -c apsw.c
gcc -pthread  -g -O2 -shared apsw.o -Lsqlite3 -lsqlite3 -o apsw.so
env APSW_NO_MEMLEAK=t APSW_TEST_ITERATIONS=5 valgrind --tool=callgrind $PYTHON tests.py

