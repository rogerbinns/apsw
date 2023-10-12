.. _types:

Types
*****

.. currentmodule:: apsw

Read about `SQLite 3 types
<https://sqlite.org/datatype3.html>`_. ASPW always maintains the
correct type for values, and never converts them to something
else. Note however that SQLite may convert types based on column
affinity as `described <https://sqlite.org/datatype3.html>`_. ASPW
requires that all values supplied are one of the corresponding
Python/SQLite types (or a subclass).

Mapping
=======

* None in Python is NULL in SQLite

* Python int is INTEGER in SQLite. The value represented must fit
  within a 64 bit signed quantity or an overflow exception is
  generated.

* Python's float type is used for REAL in SQLite. (They are both 8
  byte quantities and there is no loss of precision).

* The str (unicode) type is used for strings.

* The bytes type is used for binary data, although you can use
  anything meeting the `buffer protocol
  <https://docs.python.org/3/c-api/buffer.html>`__


.. index:: Unicode

Unicode
=======

All SQLite strings are Unicode. The actual binary representations can
be UTF8, or UTF16 in either byte order. ASPW uses the UTF8 interface
to SQLite which results in the binary string representation in your
database defaulting to UTF8 as well. This is totally transparent
to your Python code.

If you want to do manipulation of unicode text such as upper/lower
casing or sorting then you need to know about locales.  This is
because the exact same sequence of characters sort, upper case, lower
case etc differently depending on where you are.  As an example Turkic
languages have multiple letter i, German has ß which behaves like ss,
various accents sort differently in different European countries.  A
default SQLite compilation only deals with the 26 letter Roman
alphabet.

The `ICU library
<https://en.wikipedia.org/wiki/International_Components_for_Unicode>`_
can do locale aware casing and sorting.  SQLite optionally `supports
ICU <https://sqlite.org/src/finfo?name=ext/icu/README.txt>`_.  See the
:ref:`building <building>` documentation on how to enable ICU for
SQLite with APSW.

Note that Python does not currently include ICU support and hence
sorting, upper/lower casing etc are limited and do not take locales
into account.

.. index:: single: Unicode; Normalization

Normalization
-------------

The same appearing text can be represented in different ways in
unicode.  For example a letter with an accent can be directly
represented as one code point, or as two separate ones - the bare
letter and a combining accent.  (`Read more
<https://en.wikipedia.org/wiki/Unicode_equivalence>`__)

SQLite does not alter the unicode text it receives or returns.  You
will need to take this into account depending on where text came from,
what code added it to the database, and ensuring your code behaves
appropriately.  The Python standard library has
:func:`unicodedata.normalize` to help.

