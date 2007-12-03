#!/bin/sh 
#
# Run valgrind on testsuite
#
# You should build a debug Python, with this being an example using /space/pydebug as
# the root.
#
# mkdir /space/pydebug
# cd /space/pydebug
# wget -O -  http://www.python.org/ftp/python/2.5.1/Python-2.5.1.tar.bz2 | tar xfj -
# cd Python-2.5.1
# ./configure --with-pydebug --without-pymalloc --prefix=/space/pydebug
# make install
#
# Then put /space/pydebug/bin/ first on your path

# find python
PYTHON=python # use whatever is in the path
INCLUDEDIR=`$PYTHON -c "import distutils.sysconfig; print distutils.sysconfig.get_python_inc()"`
set -x
gcc -pthread -fno-strict-aliasing  -g -fPIC -W -Wall -DEXPERIMENTAL -DSQLITE_THREADSAFE=1 -DAPSW_USE_SQLITE_AMALGAMATION=\"sqlite3/sqlite3.c\" -I$INCLUDEDIR -c apsw.c
gcc -pthread  -g -shared apsw.o -o apsw.so
env APSW_NO_MEMLEAK=t APSW_TEST_ITERATIONS=150 valgrind --num-callers=50 --leak-check=full --show-reachable=yes --freelist-vol=50000000 $PYTHON tests.py

