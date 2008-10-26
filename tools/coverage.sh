#!/bin/sh 
#

set -e

if [ $# = 0 ]
then
  args="tests.py"
else
  args="$@"
fi

OPT=

# Measure code coverage
rm -f *.gcda *.gcov *.gcno apsw.so
# find python
PYTHON=python # use whatever is in the path
INCLUDEDIR=`$PYTHON -c "import distutils.sysconfig,sys; sys.stdout.write(distutils.sysconfig.get_python_inc())"`
set -x
gcc -pthread -fno-strict-aliasing -ftest-coverage -fprofile-arcs -g $(OPT) -fPIC -Wall -Wextra -D_FORTIFY_SOURCE=2 -DEXPERIMENTAL -DSQLITE_THREADSAFE=1 -DAPSW_USE_SQLITE_AMALGAMATION=\"sqlite3.c\" -DAPSW_NO_NDEBUG -DAPSW_TESTFIXTURES -I$INCLUDEDIR -I. -Isqlite3 -Isrc -c src/apsw.c
gcc -pthread -ftest-coverage -fprofile-arcs -g -shared apsw.o -o apsw.so
gcc -fPIC -shared -Isqlite3 -I. -o testextension.sqlext -Isqlite3 src/testextension.c
set +e
$PYTHON $args
res=$?
gcov src/apsw.c > /dev/null
python tools/coverageanalyser.py
exit $res
