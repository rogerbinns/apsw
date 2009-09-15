Extensions
**********

SQLite includes a number of extensions providing additional
functionality.  All extensions are disabled by default and you need to
take steps to enable them.

.. _ext-asyncvfs:

Asynchronous VFS
================

This extension does SQLite I/O in a background thread processing a
queue of requests.  To enable it you must have used :option:`fetch
--asyncvfs` to :file:`setup.py` at some point.  It is enabled by the
downloaded file :file:`sqlite3async.c` being present in the same
directory as :file:`setup.py`.  See the `SQLite documentation
<http://www.sqlite.org/asyncvfs.html>`__.

EXPLAIN USAGE

FTS3
====



Genfkey
=======

The code  
 is extracted from the SQLite shell (see `SQLite ticket 3687                          
 <http://www.sqlite.org/cvstrac/tktview?tn=3687>`__)                        


ICU
===

RTree
=====
