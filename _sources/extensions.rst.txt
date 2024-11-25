.. currentmodule:: apsw

.. _extensions:

Extensions
**********

SQLite includes a number of extensions providing additional
functionality.  All extensions are disabled by default and you need to
:ref:`take steps <setup_build_flags>` to have them available at compilation
time, to enable them and then to use them.

If you get APSW from PyPI or are using SQLite from your Linux/BSD platform
then they all enabled usually.

.. _ext-fts3:

FTS5
====

`FTS5 <https://www.sqlite.org/fts5.html>`__ is the full text search extension.
APSW includes comprehensive :doc:`functionality  <textsearch>`..

.. _ext-icu:

ICU
===

The ICU extension provides an `International Components for Unicode
<https://en.wikipedia.org/wiki/International_Components_for_Unicode>`__
interface, in particular enabling you do sorting and regular
expressions in a locale aware way.  The `documentation
<https://sqlite.org/src/tree?name=ext/icu>`__
shows how to use it.

.. _ext-rtree:

RTree
=====

The RTree extension provides a `spatial table
<https://en.wikipedia.org/wiki/R-tree>`_ - see the `documentation
<https://sqlite.org/rtree.html>`__.  There are no additional APIs and
the `documented SQL <https://sqlite.org/rtree.html>`__ works as is.
