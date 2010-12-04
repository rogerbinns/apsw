.. currentmodule:: apsw

.. _extensions:

Extensions
**********

SQLite includes a number of extensions providing additional
functionality.  All extensions are disabled by default and you need to
take steps to have them available at compilation time, to enable them
and then to use them.

.. _ext-asyncvfs:

Asynchronous VFS
================

This extension does SQLite I/O in a background thread processing a
queue of requests.  To enable it you must have used :option:`fetch
--asyncvfs` to :file:`setup.py` at some point.  It is enabled by the
downloaded file :file:`sqlite3async.c` being present in the same
directory as :file:`setup.py`.  See the `SQLite documentation
<http://www.sqlite.org/asyncvfs.html>`__.

To use you must first call :func:`apsw.async_initialize` which will
register the VFS.  If you didn't make it the default VFS then you need
to specify it when opening your database.  From this point on, any
operations you perform that require writing to the database will be
queued and the database locked.  You should start a background thread
to perform the write operations calling :func:`apsw.async_run` to do
the work.  You can call :func:`apsw.async_control` to set and get
various things (eg adding delays, controlling locking) as well as
telling it when the writer code should exit.  This is a simple example::

    # Inherit from default vfs, do not make this the new default
    asyncvfsname=apsw.async_initialize("", False)
    # Open database
    db=apsw.Connection("database", vfs=asyncvfsname)
    # Make a worker thread
    t=threading.Thread(target=apsw.async_run)
    t.start()
    # do some work
    cur=db.cursor()
    cur.execute("..")
    # Close db
    db.close()
    # Tell worker to quit when queue is empty
    apsw.async_control(apsw.SQLITEASYNC_HALT, apsw.SQLITEASYNC_HALT_IDLE)

.. _ext-fts3:

FTS3/4
======

This is the third version of the `full text search
<http://www.sqlite.org/fts3.html>`__ extension.  It
makes it easy to find words in multi-word text fields.  You must
enable the extension via :ref:`setup.py build flags
<setup_build_flags>` before it will work.  There are no additional
APIs and the `documented SQL
<http://www.sqlite.org/fts3.html>`__ works as is.

Note that FTS4 is some augmentations to FTS3 and are enabled whenever
FTS3 is enabled as described in the `documentation
<http://www.sqlite.org/fts3.html#fts4>`__

.. _ext-icu:

ICU
===

The ICU extension provides an `International Components for Unicode
<http://en.wikipedia.org/wiki/International_Components_for_Unicode>`__
interface, in particular enabling you do sorting and regular
expressions in a locale aware way.  The `documentation
<http://www.sqlite.org/src/finfo?name=ext/icu/README.txt>`__
shows how to use it.

.. _ext-rtree:

RTree
=====

The RTree extension provides a `spatial table
<http://en.wikipedia.org/wiki/R-tree>`_ - see the `documentation
<http://www.sqlite.org/rtree.html>`__.
You must enable the extension via :ref:`setup.py build flags
<setup_build_flags>` before it will work.  There are no additional
APIs and the `documented SQL
<http://www.sqlite.org/rtree.html>`__
works as is.
