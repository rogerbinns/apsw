#!/bin/bash
# Run valgrind on testsuite
#
# See the accompanying LICENSE file.
#
# You should build a debug Python, with this being an example using /space/pydebug as
# the root.
#
# ver=3.9.4
# mkdir /space/pydebug
# cd /space/pydebug
# wget -O -  https://www.python.org/ftp/python/$ver/Python-$ver.tar.xz | tar xfJ -
# cd Python-$ver
# # As an optimization Python keeps lists of various objects around for quick recycling
# # instead of freeing and then mallocing.  Unfortunately that obfuscates which code
# # was actually responsible for their existence.  Consequently we set all these to zero
# # so that normal malloc/frees happen and valgrind can do its magic.  Before Python 2.6
# # they all had different arbitrary names and in many cases could not be overridden.
#
# s="_MAXFREELIST=0"
# ./configure --with-pydebug --with-valgrind --without-pymalloc --prefix=/space/pydebug \
# CPPFLAGS="-DPyDict$s -DPyFloat$s -DPyTuple$s -DPyList$s -DPyFrame$s"
#
# make install
#
# Then put /space/pydebug/bin/ first on your path.  The CPPFLAGS setting is to make sure no tuples are saved on freelists

if [ $# = 0 ]
then
  args="tests.py"
else
  args="$@"
fi

if [ -z "$SHOWINUSE" ]
then
   showleaks=""
else
   showleaks="--leak-check=full --leak-resolution=high --show-reachable=yes"
fi

if [ -z "$CALLGRIND" ]
then
   options="--track-fds=yes --num-callers=50 $showleaks --freelist-vol=500000000 --suppressions=`dirname $0`/sqlite3.supp"
   cflags="-DAPSW_TESTFIXTURES -DAPSW_NO_NDEBUG"
   opt="-Os -g"
   APSW_TEST_ITERATIONS=${APSW_TEST_ITERATIONS:=150}
   apswopt="APSW_NO_MEMLEAK=t APSW_TEST_ITERATIONS=$APSW_TEST_ITERATIONS"
else
   options="--tool=callgrind --dump-instr=yes --trace-jump=yes"
   cflags=""
   opt="-O2"
   apswopt=""
fi

DEFS="-DAPSW_NO_NDEBUG -DAPSW_TESTFIXTURES"

if [ -f sqlite3/sqlite3config.h ]
then
    DEFS="$DEFS -DAPSW_USE_SQLITE_CONFIG=\"sqlite3/sqlite3config.h\""
fi
# find python
PYTHON=${PYTHON:-python3} # use whatever is in the path
INCLUDEDIR=`$PYTHON -c "import sysconfig; print(sysconfig.get_path('include'))"`
CC=`$PYTHON -c "import sysconfig; print(sysconfig.get_config_var('CC'))"`
CFLAGS=`$PYTHON -c "import sysconfig; print(sysconfig.get_config_var('CFLAGS'))"`
MOREFLAGS=`$PYTHON -c "import sysconfig; print(sysconfig.get_config_var('CCSHARED'))"`
LINKER=`$PYTHON -c "import sysconfig; print(sysconfig.get_config_var('LDSHARED'))"`
SOSUFFIX=`$PYTHON -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"`

rm -f apsw.o apsw.*.so apsw.so
set -ex

$CC $CFLAGS $MOREFLAGS $opt $cflags -DEXPERIMENTAL $DEFS -DAPSW_USE_SQLITE_AMALGAMATION=\"sqlite3/sqlite3.c\" -I$INCLUDEDIR -Isrc -I. -c src/apsw.c
$LINKER -g $opt apsw.o -o apsw$SOSUFFIX
time env $apswopt valgrind $options $PYTHON $args
