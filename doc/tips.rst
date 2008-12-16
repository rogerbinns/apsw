Tips
****

.. currentmodule:: apsw

These tips are based on mailing list postings.  You are recommended to
read all the documentation as well.

Bindings
========

When using a cursor, always use bindings.  `String interpolation
<http://docs.python.org/library/stdtypes.html#string-formatting-operations>`_
may seem more convenient but you will encounter difficulties.  You may
feel that you have complete control over all data accessed but if your
code is at all useful then you will find it being used more and more
widely.  The computer will always be better than you at parsing SQL
and the bad guys have years of experience finding and using `SQL
injection attacks <http://en.wikipedia.org/wiki/SQL_injection>`_ in
ways you never even thought possible.

The :ref:`documentation <cursors>` gives many examples of how to use
various forms of bindings.

Unicode
=======

SQLite only stores text as Unicode.  However it relies on SQLite API
users to provide valid UTF-8 and does not double check.  (APSW only
provides valid UTF-8).  It is possible using other wrappers and tools
to cause invalid UTF-8 to appear in the database which will then cause
retrieval errors.  You can work around this by using the SQL *CAST*
operator.  For example::

  SELECT id, CAST(label AS blob) from table

Then proceed to give the `Joel Unicode article
<http://www.joelonsoftware.com/articles/Unicode.html>`_ to all people
involved.

Unexpected behaviour
====================

Occasionally you may get different results than you expected.  Before
littering your code with *print*, try :ref:`apswtrace <apswtrace>`
with all options turned on to see exactly what is going on. You can
also use the SQLite shell to dump the contents of your database to a
text file.  For example you could dump it before and after a run to
see what changed.

One fairly common gotcha is using double quotes instead of single
quotes.  (This wouldn't be a problem if you use bindings!)  SQL
strings use single quotes.  If you use double quotes then it will
mostly appear to work, but they are intended to be used for
identifiers such as column names.  For example if you have a column
named ``a b`` (a space b) then you would need to use::

  SELECT "a b" from table

If you use double quotes and happen to use a string whose contents are
the same as a table, alias, column etc then unexpected results will
occur.

Customizing cursors
===================

Some developers want to customize the behaviour of cursors.  An
example would be wanting a :ref:`rowcount <rowcount>` or batching returned rows.
(These don't make any sense with SQLite but the desire may be to make
the code source compatible with other database drivers).

APSW does not provide a way to subclass the cursor class or any other
form of factory.  Consequently you will have to subclass the
:class:`Connection` and provide an alternate implementation of
:meth:`Connection.cursor`.  You should encapsulate the APSW cursor -
ie store it as a member of your cursor class and forward calls as
appropriate.  The cursor only has two important methods -
:meth:`Cursor.execute` and :meth:`Cursor.executemany`.

If you want to change the rows returned then use a :ref:`row tracer
<rowtracer>`.  For example you could call
:meth:`Cursor.getdescription` and return a dictionary instead of a
tuple.

Database schema
===============

When starting a new database, it can be quite difficult to decide what
tables and fields to have and how to link them.  The technique used to
design SQL schemas is called `normalization
<http://en.wikipedia.org/wiki/Database_normalization>`_.  The page
also shows common pitfalls if you don't normalize your schema.
