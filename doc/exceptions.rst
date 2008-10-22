.. _exceptions:

Exceptions
**********
.. currentmodule:: apsw


:class:`apsw.Error` is the base for apsw exceptions.

.. class:: Error

   .. attribute:: result

         For exceptions corresponding to `SQLite error codes
         <http://sqlite.org/c3ref/c_abort.html>`_ codes this attribute
         is the numeric error code.

   .. attribute:: extendedresult

         APSW runs with `extended result codes
         <http://sqlite.org/c3ref/c_ioerr_blocked.html>`_ turned on.
         This attribute includes the detailed code.

As an example, if SQLite issued a read request and the system returned
less data than expected then :attr:`~Error.result` would have the value
:const:`SQLITE_IOERR` while :attr:`~Error.extendedresult` would have
the value :const:`SQLITE_IOERR` binary orred with
:const:`SQLITE_IOERR_SHORT_READ`.
   

APSW specific exceptions
========================

The following exceptions happen when APSW detects various problems.

ThreadingViolationError
  You have used an object concurrently in two threads. For example you
  may try to use the same cursor in two different threads at the same
  time, or tried to close the same connection in two threads at the
  same time.
        
IncompleteExecutionError
  You have tried to start a new SQL execute call before executing all
  the previous ones. See the :ref:`execution model <executionmodel>`
  for more details.
        
ConnectionNotClosedError 
  This exception is no longer generated.  It was required in earlier
  releases due to constraints in threading usage with SQLite.

ConnectionClosedError
  You have called :meth:`Connection.close` and then continued to use
  the :class:`Connection` or associated :class:`cursors <Cursor>`.

BindingsError
  There are several causes for this exception.  When using tuples, an incorrect number of bindings where supplied::

     cursor.execute("select ?,?,?", (1,2))     # too few bindings
     cursor.execute("select ?,?,?", (1,2,3,4)) # too many bindings

  You are using named bindings, but not all bindings are named.  You should either use entirely the
  named style or entirely numeric (unnamed) style::

     cursor.execute("select * from foo where x=:name and y=?")

  .. note::

     It is not considered an error to have missing keys in a dictionary. For example this is perfectly valid::

          cursor.execute("insert into foo values($a,:b,$c)", {'a': 1})

     `b` and `c` are not in the dict.  For missing keys, None/NULL
     will be used. This is so you don't have to add lots of spurious
     values to the supplied dict. If your schema requires every column
     have a value, then SQLite will generate an error due to some
     values being None/NULL so that case will be caught.


ExecutionCompleteError
  A statement is complete but you try to run it more anyway!


ExecTraceAbort
  The :ref:`execution tracer <executiontracer>` returned False so
  execution was aborted.


ExtensionLoadingError
  An error happened loading an `extension
  <http://www.sqlite.org/cvstrac/wiki/wiki?p=LoadableExtensions>`_.

VFSNotImplementedError
  A call cannot be made to an inherited :ref:`VFS` method as the VFS
  does not implement the method.

VFSFileClosedError
  The VFS file is closed so the operation cannot be performed.


SQLite exceptions
=================

The table lists which Exception classes correspond to which `SQLite
error codes <http://sqlite.org/c3ref/c_abort.html>`_.


+--------------------------------------+---------------------------------------+
| **General Errors**                   |  **Abort/Busy etc**                   |
+-------------------+------------------+-------------------+-------------------+
| SQLITE_ERROR      | SQLError         | SQLITE_ABORT      | AbortError        |
+-------------------+------------------+-------------------+-------------------+
| SQLITE_MISMATCH   | MismatchError    | SQLITE_BUSY       | BusyError         |
+-------------------+------------------+-------------------+-------------------+
|                                      | SQLITE_LOCKED     | LockedError       |
+-------------------+------------------+-------------------+-------------------+
| **Internal Errors**                  | SQLITE_INTERRUPT  | InterruptError    |
+-------------------+------------------+-------------------+-------------------+
| *SQLITE_INTERNAL* | InternalError    | SQLITE_SCHEMA     | SchemaChangeError |
+-------------------+------------------+-------------------+-------------------+
| *SQLITE_PROTOCOL* | ProtocolError    | SQLITE_CONSTRAINT | ConstraintError   |
+-------------------+------------------+-------------------+-------------------+
| SQLITE_MISUSE     | MisuseError      |                                       |
+-------------------+------------------+-------------------+-------------------+
| SQLITE_RANGE      | RangeError       | **Memory/Disk Etc**                   |
+-------------------+------------------+-------------------+-------------------+
|                                      | SQLITE_NOMEM      | NoMemError        |
+--------------------------------------+-------------------+-------------------+
| **Permissions etc**                  | SQLITE_IOERR      | IOError           |
+-------------------+------------------+-------------------+-------------------+
| SQLITE_PERM       | PermissionsError | SQLITE_CORRUPT    | CorruptError      |
+-------------------+------------------+-------------------+-------------------+
| SQLITE_READONLY   | ReadOnlyError    | SQLITE_FULL       | FullError         |
+-------------------+------------------+-------------------+-------------------+
| SQLITE_CANTOPEN   | CantOpenError    | SQLITE_TOOBIG     | TooBigError       |
+-------------------+------------------+-------------------+-------------------+
| SQLITE_AUTH       | AuthError        | SQLITE_NOLFS      | NoLFSError        |
+-------------------+------------------+-------------------+-------------------+
|                                      | SQLITE_EMPTY      | EmptyError        |
+--------------------------------------+-------------------+-------------------+
|                                      | SQLITE_FORMAT     | FormatError       |
+--------------------------------------+-------------------+-------------------+
|                                      | SQLITE_NOTADB     | NotADBError       |
+--------------------------------------+-------------------+-------------------+

Codes in *italics* are no longer issued by the SQLite core

.. _augmentedstacktraces:

Augmented stack traces
======================

When an exception occurs, Python does not include frames from
non-Python code (ie the C code called from Python).  This can make it
more difficult to work out what was going on when an exception
occurred for example when there are callbacks to collations, functions
or virtual tables, triggers firing etc.

This is an example showing the difference between the tracebacks you
would have got with earlier versions of apsw and the augmented
traceback::

  import apsw

  def myfunc(x):
    1/0

  con=apsw.Connection(":memory:")
  con.createscalarfunction("foo", myfunc)
  con.createscalarfunction("fam", myfunc)
  cursor=con.cursor()
  cursor.execute("create table bar(x,y,z);insert into bar values(1,2,3)")
  cursor.execute("select foo(1) from bar")

+-----------------------------------------------------------+----------------------------------------------------------+
| Original Traceback                                        |      Augmented Traceback                                 |
+===========================================================+==========================================================+
| ::                                                        | ::                                                       |
|                                                           |                                                          |
|   Traceback (most recent call last):                      |   Traceback (most recent call last):                     |
|     File "t.py", line 11, in <module>                     |     File "t.py", line 11, in <module>                    |
|       cursor.execute("select foo(1) from bar")            |       cursor.execute("select foo(1) from bar")           |
|     File "t.py", line 4, in myfunc                        |     File "apsw.c", line 3412, in resetcursor             |
|       1/0                                                 |     File "apsw.c", line 1597, in user-defined-scalar-FOO |
|   ZeroDivisionError: integer division or modulo by zero   |     File "t.py", line 4, in myfunc                       |
|                                                           |       1/0                                                |
|                                                           |   ZeroDivisionError: integer division or modulo by zero  |
+-----------------------------------------------------------+----------------------------------------------------------+

In the original traceback you can't even see that code in apsw was
involved. The augmented traceback shows that there were indeed two
function calls within apsw and gives you line numbers should you need
to examine the code. Also note how you are told that the call was in
`user-defined-scalar-FOO` (all user defined function names are
uppercased).

*But wait, there is more!!!* In order to further aid troubleshooting,
the augmented stack traces make additional information available. Each
frame in the traceback has local variables defined with more
information. You can print out the variables using `ASPN recipe 52215 <http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/52215>`_

  In the recipe, the initial code in :func:`print_exc_plus` is far
  more complicated than need be, and also won't work correctly with
  all tracebacks (it depends on :attr:`f_prev` being set which isn't always
  the case). Change the function to start like this::

    tb = sys.exc_info()[2]
    stack = []
    
    while tb:
        stack.append(tb.tb_frame)
        tb = tb.tb_next

    traceback.print_exc()
    print "Locals by frame, innermost last"


Here is a far more complex example from some :ref:`virtual tables
<Virtualtables>` code I was writing. The BestIndex method in my code
had returned an incorrect value. The augmented traceback includes
local variables using recipe 52215. I can see what was passed in to my
method, what I returned and which item was erroneous. The original
traceback is almost completely useless.

Original traceback::

  Traceback (most recent call last):
    File "tests.py", line 1387, in testVtables
      cursor.execute(allconstraints)
  TypeError: Bad constraint (#2) - it should be one of None, an integer or a tuple of an integer and a boolean

Augmented traceback with local variables::

  Traceback (most recent call last):
    File "tests.py", line 1387, in testVtables
      cursor.execute(allconstraints)
                  VTable =  __main__.VTable
                     cur =  <apsw.Cursor object at 0x988f30>
                       i =  10
                    self =  testVtables (__main__.APSW)
          allconstraints =  select rowid,* from foo where rowid>-1000 ....

    File "apsw.c", line 4050, in Cursor_execute.sqlite3_prepare
              Connection =  <apsw.Connection object at 0x978800>
               statement =  select rowid,* from foo where rowid>-1000 ....

    File "apsw.c", line 2681, in VirtualTable.xBestIndex
                    self =  <__main__.VTable instance at 0x98d8c0>
                    args =  (((-1, 4), (0, 32), (1, 8), (2, 4), (3, 64)), ((2, False),))
                  result =  ([4, (3,), [2, False], [1], [0]], 997, u'\xea', False)

    File "apsw.c", line 2559, in VirtualTable.xBestIndex.result_constraint
                 indices =  [4, (3,), [2, False], [1], [0]]
                    self =  <__main__.VTable instance at 0x98d8c0>
                  result =  ([4, (3,), [2, False], [1], [0]], 997, u'\xea', False)
              constraint =  (3,)

  TypeError: Bad constraint (#2) - it should be one of None, an integer or a tuple of an integer and a boolean


