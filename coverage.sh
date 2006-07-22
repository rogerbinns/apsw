#!/bin/sh 
#
# Measure code coverage
rm -f *.gcda *.gcov *.gcno apsw.so
# find python
PYTHON=python # use whatever is in the path
INCLUDEDIR=`$PYTHON -c "import distutils.sysconfig; print distutils.sysconfig.get_python_inc()"`
set -x
gcc -pthread -fno-strict-aliasing -ftest-coverage -fprofile-arcs -g -fPIC -Wall -DEXPERIMENTAL -Isqlite3 -I$INCLUDEDIR -c apsw.c
gcc -pthread -ftest-coverage -fprofile-arcs -g -shared apsw.o -Lsqlite3 -lsqlite3 -o apsw.so
$PYTHON tests.py
gcov apsw.c
