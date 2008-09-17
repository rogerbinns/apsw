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
# # MAXSAVESIZE prevents caching of unused tuples which show up as still reachable at exit
# # __INSURE__ forces freeing of interned strings at exit
# # MAXFREEDICTS prevents caching of dictionaries - you will need to edit Objects/dictobject.c to allow
# #              this override
# ./configure --with-pydebug --without-pymalloc --prefix=/space/pydebug CPPFLAGS="-DMAXSAVESIZE=0 -D__INSURE__ -DMAXFREEDICTS=0"
# make install
#
# Then put /space/pydebug/bin/ first on your path.  The CPPFLAGS setting is to make sure no tuples are saved on freelists

# find python
PYTHON=python # use whatever is in the path
INCLUDEDIR=`$PYTHON -c "import distutils.sysconfig, sys; sys.stdout.write(distutils.sysconfig.get_python_inc())"`
APSW_TEST_ITERATIONS=${APSW_TEST_ITERATIONS:=150}
set -x
rm -f apsw.o apsw.so
gcc -pthread -fno-strict-aliasing  -Os -g -D_FORTIFY_SOURCE=2 -fPIC -W -Wall -DAPSW_TESTFIXTURES -DAPSW_NO_NDEBUG -DEXPERIMENTAL -DSQLITE_THREADSAFE=1 -DAPSW_USE_SQLITE_AMALGAMATION=\"sqlite3.c\" -I$INCLUDEDIR -c apsw.c
gcc -pthread  -g -shared apsw.o -o apsw.so
if [ $# -eq 0 ]
then
    env APSW_NO_MEMLEAK=t APSW_TEST_ITERATIONS=$APSW_TEST_ITERATIONS valgrind --num-callers=100 --leak-check=full --leak-resolution=high --show-reachable=yes --freelist-vol=500000000 $PYTHON tests.py 
else
    env APSW_NO_MEMLEAK=t APSW_TEST_ITERATIONS=$APSW_TEST_ITERATIONS valgrind --num-callers=100 --leak-check=full --leak-resolution=high --show-reachable=yes --freelist-vol=500000000 $PYTHON "$@"
fi

