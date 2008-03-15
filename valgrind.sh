#!/bin/sh 
#
# Run valgrind on testsuite
#
# You should build a debug Python, with this being an example using /space/pydebug as
# the root.
#
# ver=2.5.2
# mkdir /space/pydebug
# cd /space/pydebug
# wget -O -  http://www.python.org/ftp/python/$ver/Python-$ver.tar.bz2 | tar xfj -
# cd Python-$ver
# ./configure --with-pydebug --without-pymalloc --prefix=/space/pydebug
# make install
#
# Then put /space/pydebug/bin/ first on your path

# find python
PYTHON=python # use whatever is in the path
INCLUDEDIR=`$PYTHON -c "import distutils.sysconfig; print distutils.sysconfig.get_python_inc()"`
set -x
gcc -pthread -fno-strict-aliasing  -g -fPIC -W -Wall -DEXPERIMENTAL -DSQLITE_THREADSAFE=1 -DAPSW_USE_SQLITE_AMALGAMATION=\"sqlite3.c\" -I$INCLUDEDIR -c apsw.c
gcc -pthread  -g -shared apsw.o -o apsw.so
if [ $# -eq 0 ]
then
    env APSW_NO_MEMLEAK=t APSW_TEST_ITERATIONS=150 valgrind --num-callers=50 --leak-check=full --show-reachable=yes --freelist-vol=50000000 $PYTHON tests.py
else
    valgrind --num-callers=50 --leak-check=full --show-reachable=yes --freelist-vol=50000000 $PYTHON "$@"
fi

