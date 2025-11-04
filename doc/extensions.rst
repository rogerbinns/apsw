.. currentmodule:: apsw

.. _extensions:

Extensions
**********

SQLite includes a number of extensions providing additional
functionality.  All extensions are disabled by default and you need to
:ref:`take steps <setup_build_flags>` to have them available at compilation
time, to enable them and then to use them.

If you get APSW from PyPI they are all enabled.  If APSW comes from
your Linux/BSD platform then it will match your platform
configuration.

CARRAY
======

`Runtime array of values extension <https://sqlite.org/carray.html>`__ used
with :meth:`apsw.carray` for providing bulk numbers, strings, and blobs.

.. note::

    When APSW downloads the amalgamation, a :source:`patch
    <tools/carray.patch>` is applied.  Without the patch, the
    extension has to make a duplicate copy of all the data each time
    the binding happens.

Session
=======

:doc:`session`

Math functions
==============

Several `SQL functions <https://sqlite.org/lang_mathfunc.html>`__

Percentile (media)
==================

Several `SQL functions <https://sqlite.org/percentile.html>`__ related
to `percentiles <https://en.wikipedia.org/wiki/Percentile>`__.  Python
has a :mod:`statistics` module with some of the same functions, but
this extension is more convenient.

.. _ext-fts3:

FTS5
====

`FTS5 <https://www.sqlite.org/fts5.html>`__ is the full text search extension.
APSW includes comprehensive :doc:`functionality  <textsearch>`.

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

Geopoly
=======

A Geojson `compatible interface to RTree
<Mhttps://www.sqlite.org/geopoly.html>`__.