.. currentmodule:: apsw

.. _extensions:

Extensions
**********

SQLite includes a number of extensions providing additional
functionality.  All extensions are disabled by default and you need to
take steps to have them available at compilation time, to enable them
and then to use them.

.. _ext-fts3:

FTS3/4/5
========

FTS3 is the third version of the `full text search
<https://sqlite.org/fts3.html>`__ extension.  It
makes it easy to find words in multi-word text fields.  You must
enable the extension via :ref:`setup.py build flags
<setup_build_flags>` before it will work.  There are no additional
APIs and the `documented SQL
<https://sqlite.org/fts3.html>`__ works as is.

Note that FTS4 is some augmentations to FTS3 and are enabled whenever
FTS3 is enabled as described in the `documentation
<https://sqlite.org/fts3.html#fts4>`__

`FTS5 <https://www.sqlite.org/fts5.html>`__ addresses some issues in the earlier
FTS versions by `breaking backwards <https://www.sqlite.org/fts5.html#appendix_a>`__
compatibility.

.. _ext-icu:

ICU
===

The ICU extension provides an `International Components for Unicode
<http://en.wikipedia.org/wiki/International_Components_for_Unicode>`__
interface, in particular enabling you do sorting and regular
expressions in a locale aware way.  The `documentation
<https://sqlite.org/src/finfo?name=ext/icu/README.txt>`__
shows how to use it.

.. _ext-rtree:

RTree
=====

The RTree extension provides a `spatial table
<http://en.wikipedia.org/wiki/R-tree>`_ - see the `documentation
<https://sqlite.org/rtree.html>`__.
You must enable the extension via :ref:`setup.py build flags
<setup_build_flags>` before it will work.  There are no additional
APIs and the `documented SQL
<https://sqlite.org/rtree.html>`__
works as is.
