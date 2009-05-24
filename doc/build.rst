.. _building:

Building
********

setup.py
========

Short story:  You run :file:`setup.py`

+-------------------------------------------------------------+-------------------------------------------------------------------------+
| Command                                                     |  Result                                                                 |
+=============================================================+=========================================================================+
| | python setup.py install                                   | Compiles APSW with default Python compiler and                          |
|                                                             | installs it into Python site library directory.                         |
+-------------------------------------------------------------+-------------------------------------------------------------------------+
| | python setup.py install :option:`--user`                  | (Python 2.6+, 3). Compiles APSW with default Python                     |
|                                                             | compiler and installs it into a subdirectory of your home directory.    |
|                                                             | See :pep:`370` for more details.                                        |
+-------------------------------------------------------------+-------------------------------------------------------------------------+
| | python setup.py build :option:`--compile=mingw32` install | On Windows this will use the                                            |
|                                                             | `free <http://www.gnu.org/philosophy/free-sw.html>`_                    |
|                                                             | `MinGW compiler <http://mingw.org>`_ `instead of                        |
|                                                             | <http://boodebr.org/main/python/build-windows-extensions>`_ the         |
|                                                             | Microsoft compilers.                                                    |
+-------------------------------------------------------------+-------------------------------------------------------------------------+
| | python setup.py build                                     | Compiles the extension but doesn't install it. The resulting file       |
|                                                             | will be in a :file:`build` subdirectory named apsw.so or apsw.pyd.      |
|                                                             | For example on a Linux 64 bit Python 2.5 installation the file is       |
|                                                             | :file:`build/lib.linux-x86_64-2.5/apsw.so`. You can copy this file      |
|                                                             | anywhere that is convenient for your scripts.                           |
+-------------------------------------------------------------+-------------------------------------------------------------------------+
| | python setup.py build :option:`--debug` install           | Compiles APSW with debug information.  This also turns on `assertions   |
|                                                             | <http://en.wikipedia.org/wiki/Assert.h>`_                               |
|                                                             | in APSW that double check the code assumptions.  If you are using the   |
|                                                             | SQLite amalgamation then assertions are turned on in that too.  Note    |
|                                                             | that this will considerably slow down APSW and SQLite.                  |
+-------------------------------------------------------------+-------------------------------------------------------------------------+

.. _setup_py_flags:

Additional :file:`setup.py` flags
=================================

There are a number of APSW specific flags you can specify.

+----------------------------------------+--------------------------------------------------------------------------------------+
| | :option:`--fetch-sqlite`             | Automatically downloads the latest or the specified version of the SQLite            |
| | :option:`--fetch-sqlite=VERSION`     | amalgamation and uses it for the APSW extension. On non-Windows platforms it         |
|                                        | will also work out what compile flags SQLite needs (for example                      |
|                                        | :const:`HAVE_USLEEP`, :const:`HAVE_LOCALTIME_R`). The amalgamation is the            |
|                                        | preferred way to use SQLite as you have total control over what components are       |
|                                        | included or excluded (see below) and have no dependencies on any existing            |
|                                        | libraries on your developer or deployment machines. You can set the environment      |
|                                        | variable :const:`http_proxy` to control proxy usage for the download.                |
+----------------------------------------+--------------------------------------------------------------------------------------+
| | :option:`--enable=fts3`              | Enables the `full text search <http://www.sqlite.org/cvstrac/wiki?p=FtsUsage>`_      |
|                                        | extension.                                                                           |
|                                        | This flag only helps when using the amalgamation. If not using the                   | 
|                                        | amalgamation then you need to seperately ensure fts is enabled in the SQLite         |
|                                        | install.                                                                             |
+----------------------------------------+--------------------------------------------------------------------------------------+
| | :option:`--enable=rtree`             | Enables the spatial table `rtree <http://en.wikipedia.org/wiki/R-tree>`_             |
|                                        | (`README <http://www.sqlite.org/cvstrac/fileview?f=sqlite/ext/rtree/README>`_)       |
|                                        | This flag only helps when using the amalgamation. If not using the                   | 
|                                        | amalgamation then you need to seperately ensure rtree is enabled in the SQLite       |
|                                        | install.                                                                             |
+----------------------------------------+--------------------------------------------------------------------------------------+
| | :option:`--enable=icu`               | Enables the `International Components for Unicode                                    |
|                                        | <http://en.wikipedia.org/wiki/International_Components_for_Unicode>`_  extension     |
|                                        | (`README.txt <http://www.sqlite.org/cvstrac/fileview?f=sqlite/ext/icu/README.txt>`_) |
|                                        | Note that you must have the ICU libraries on your machine which setup will           |
|                                        | automatically try to find using :file:`icu-config`.                                  |
|                                        | This flag only helps when using the amalgamation. If not using the                   | 
|                                        | amalgamation then you need to seperately ensure ICU is enabled in the SQLite         |
|                                        | install.                                                                             |
+----------------------------------------+--------------------------------------------------------------------------------------+
| | :option:`--omit=ITEM`                | Causes various functionality to be omitted. For example                              |
|                                        | :option:`--omit=load_extension` will omit code to do with loading extensions. If     |
|                                        | using the amalgamation then this will omit the functionality from APSW and           |
|                                        | SQLite, otherwise the functionality will only be omitted from APSW (ie the code      |
|                                        | will still be in SQLite, APSW just won't call it)                                    |
+----------------------------------------+--------------------------------------------------------------------------------------+

Finding SQLite 3
================

SQLite 3 is needed during the build process. If you specify
:option:`--fetch-sqlite` anywhere on the :file:`setup.py` command line
then it will automatically fetch the current version of the SQLite
amalgamation. (The current version is determined by parsing the
`SQLite download page <http://www.sqlite.org/download.html>`_). You
can manually specify the version, for example
:option:`--fetch-sqlite=3.6.1`.

These methods are tried in order:

  `Amalgamation <http://www.sqlite.org/cvstrac/wiki?p=TheAmalgamation>`_

      The file :file:`sqlite3.c` and then :file:`sqlite3/sqlite3.c` is
      looked for. The SQLite code is then statically compiled into the
      APSW extension and is invisible to the rest of the
      process. There are no runtime library dependencies on SQLite as
      a result.

  Local build

    The header :file:`sqlite3/sqlite3.h` and library :file:`sqlite3/libsqlite3.{a,so,dll}` is looked for.


  User directories

    If you are using Python 2.6+ or Python 3 and specified
    :option:`--user` then your user directory is searched first. See
    :pep:`370` for more details.

  System directories

    The default compiler include path (eg :file:`/usr/include`) and library path (eg :file:`/usr/lib`) are used.


.. note::

  If you compiled SQLite with any OMIT flags (eg
  :const:`SQLITE_OMIT_LOAD_EXTENSION`) then you should include them in
  the :file:`setup.py` command. For this example you would use
  :option:`setup.py --omit=load_extension` to add the same flags.

.. _recommended_build:

Recommended
===========

These instructions show how to build automatically downloading and
using the amalgamation. Any existing SQLite on your system is ignored
at build time and runtime. (Note that you can even use APSW in the
same process as a different SQLite is used by other libraries - this
happens a lot on Mac.) You should follow these instructions with your
current directory being where you extracted the APSW source to.

  Windows::

      # Leave out --compile=mingw32 flag if using Microsoft compiler
    > python setup.py build --compile=mingw32 install --fetch-sqlite 
    > python -c "import apsw ; print  apsw.sqlitelibversion(), apsw.apswversion()"
    > python tests.py       # optional - checks everything works correctly


  Mac/Linux etc::

    $ python setup.py install --fetch-sqlite
    $ python -c "import apsw ; print  apsw.sqlitelibversion(), apsw.apswversion()"     
    $ python tests.py       # optional - checks everything works correctly

.. note::

  There will be many warnings during the compilation step about
  sqlite3.c, `but they are harmless <http://sqlite.org/faq.html#q17>`_


The extension just turns into a single file apsw.so (Linux/Mac) or
apsw.pyd (Windows). You don't need to install it and can drop it into
any directory that is more convenient for you and that your code can
reach. To just do the build and not install, leave out *install* from
the lines above and add *build* if it isn't already there.

If you want to check that your build is correct then you can run the
unit tests. Run :file:`python tests.py`. It will print the APSW file
used, APSW and SQLite versions and then run lots of tests all of which
should pass.

Testing
=======

SQLite itself is extensively tested. It has considerably more code
dedicated to testing than makes up the actual database functionality.

APSW includes a :file:`tests.py` file which uses the standard Python
testing modules to verify correct operation. New code is developed
alongside the tests. Reported issues also have test cases to ensure
the issue doesn't happen or doesn't happen again.::
  
  $ python tests.py
                  Python /usr/bin/python (2, 6, 2, 'final', 0)
  Testing with APSW file /space/apsw/apsw.so
            APSW version 3.6.14.1-r1
      SQLite lib version 3.6.14.1
  SQLite headers version 3006014
      Using amalgamation True
  ............................................................
  ----------------------------------------------------------------------
  Ran 60 tests in 90.770s
  
  OK

The tests also ensure that as much APSW code as possible is executed
including alternate paths through the code.  95.5% of the APSW code is
executed by the tests. If you checkout the APSW source then there is
an script `coverage.sh
<http://code.google.com/p/apsw/source/browse/apsw/trunk/tools/coverage.sh>`_
that enables extra code that deliberately induces extra conditions
such as memory allocation failures, SQLite returning undocumented
error codes etc. That brings coverage up to 99.6% of the code.

A memory checker `Valgrind <http://valgrind.org>`_ is used while
running the test suite. The test suite is run 150 times to makes any
memory leaks or similar issues stand out. A checking version of Python
is also used.  See `valgrind.sh
<http://code.google.com/p/apsw/source/browse/apsw/trunk/tools/valgrind.sh>`_
in the source.

To ensure compatibility with the various Python versions, a script
downloads and compiles all supported Python versions in both 2 byte
and 4 byte Unicode character configurations against the APSW and
SQLite supported versions running the tests. See `megatest.py
<http://code.google.com/p/apsw/source/browse/apsw/trunk/tools/megatest.py>`_
in the source.

In short both SQLite and APSW have a lot of testing!
