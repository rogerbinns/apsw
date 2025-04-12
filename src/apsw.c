/*
  Another Python Sqlite Wrapper

  This wrapper aims to be the minimum necessary layer over SQLite 3
  itself.

  It assumes we are running as 32 bit int with a 64 bit long long type
  available.

  See the accompanying LICENSE file.
*/

/**

.. module:: apsw
  :synopsis: Python access to SQLite database library

APSW Module
***********

The module is the main interface to SQLite.  Methods and data on the
module have process wide effects.

.. _type_stubs:

Type Annotations
================

Comprehensive :mod:`type annotations <typing>` :source:`are included
<apsw/__init__.pyi>`, and your code can be checked using tools like
`mypy <https://mypy-lang.org/>`__.  You can refer to the types below
for your annotations (eg as :class:`apsw.SQLiteValue`)

Your source files should include::

    from __future__ import annotations

.. note::

  These types are **not** available at run time, and have no effect when
  your code is running.  They are only referenced when running a type
  checker, or using an `IDE
  <https://en.wikipedia.org/wiki/Language_Server_Protocol>`__.

You will require a recent version of Python to use the type
annotations.

.. include:: ../doc/typing.rstgen

API Reference
=============
*/

#ifdef APSW_USE_SQLITE_CFG_H
#include "sqlite_cfg.h"
#endif

#ifdef APSW_USE_SQLITE_CONFIG
#include "sqlite3config.h"
#endif

/* SQLite amalgamation */
#ifdef APSW_USE_SQLITE_AMALGAMATION

#define SQLITE_OMIT_DEPRECATED

#define SQLITE_OMIT_SHARED_CACHE

#ifndef SQLITE_MAX_ATTACHED
#define SQLITE_MAX_ATTACHED 125
#endif

#ifndef SQLITE_MAX_MMAP_SIZE
#define SQLITE_MAX_MMAP_SIZE 0x1000000000000LL
#endif

#ifndef SQLITE_DEBUG
#define SQLITE_API static
#define SQLITE_EXTERN static
#endif

#include "sqlite3.c"

#else
/* SQLite 3 headers */
#include "sqlite3.h"
#endif

#if SQLITE_VERSION_NUMBER < 3049000
#error Your SQLite version is too old.  It must be at least 3.49.0
#endif

#include "sqlite_debug.h"

/* system headers */
#include <assert.h>
#include <stdarg.h>
#ifdef _MSC_VER
#include <malloc.h>
#endif

/* Get the version number */
#include "apswversion.h"

#include "apsw.docstrings"

/* Python headers */
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <pythread.h>
#include "structmember.h"

/* This function does nothing in regular builds, but in faultinjection
builds allows for an existing exception to be injected in callbacks */
static int
MakeExistingException(void)
{
  return 0;
}

#ifdef APSW_FAULT_INJECT

#include "faultinject.h"

/* Fault injection */
#define APSW_FAULT(faultName, good, bad)                                                                               \
  do                                                                                                                   \
  {                                                                                                                    \
    if (APSW_Should_Fault(#faultName))                                                                                 \
    {                                                                                                                  \
      do                                                                                                               \
      {                                                                                                                \
        bad;                                                                                                           \
      } while (0);                                                                                                     \
    }                                                                                                                  \
    else                                                                                                               \
    {                                                                                                                  \
      do                                                                                                               \
      {                                                                                                                \
        good;                                                                                                          \
      } while (0);                                                                                                     \
    }                                                                                                                  \
  } while (0)

static int APSW_Should_Fault(const char *);

/* Are we doing 64 bit? - _LP64 is best way I can find as sizeof isn't valid in cpp #if */
#if defined(_LP64) && _LP64
#define APSW_TEST_LARGE_OBJECTS
#endif

#else /* APSW_FAULT_INJECT */
#define APSW_FAULT(faultName, good, bad)                                                                               \
  do                                                                                                                   \
  {                                                                                                                    \
    good;                                                                                                              \
  } while (0)
#endif

/* The module object */
static PyObject *apswmodule;

/* root exception class */
static PyObject *APSWException;

/* no change sentinel for vtable updates */
static PyTypeObject apsw_no_change_object = {
  PyVarObject_HEAD_INIT(NULL, 0)
  .tp_name = "apsw.no_change",
  .tp_doc = Apsw_no_change_DOC,
};

typedef struct
{
  PyObject_HEAD
  long long blobsize;
  int init_was_called;
} ZeroBlobBind;

typedef struct
{
  PyObject_HEAD
  PyObject *object;
} PyObjectBind;

/* forward reference */
static PyTypeObject PyObjectBindType;

/* name used in sqlite3 pointer interfaces to identify pointer type */
#define PYOBJECT_BIND_TAG "apsw-pyobject"

/* destructor for bind/result */
static void
pyobject_bind_destructor(void *value)
{
  PyGILState_STATE gilstate = PyGILState_Ensure();
  Py_DECREF((PyObject *)value);
  PyGILState_Release(gilstate);
}

static void apsw_write_unraisable(PyObject *hookobject);

/* string constants struct */
#include "stringconstants.c"

/* Make various versions of Python code compatible with each other */
#include "pyutil.c"

/* Augment tracebacks */
#include "traceback.c"

/* various utility functions and macros */
#include "util.c"

/* Argument parsing helpers */
#include "argparse.c"

/* Exceptions we can raise */
#include "exceptions.c"

/* The statement cache */
#include "statementcache.c"

/* connections */
#include "connection.c"

/* backup */
#include "backup.c"

/* Zeroblob and blob */
#include "blob.c"

static int allow_missing_dict_bindings = 0;

/* cursors */
#include "cursor.c"

/* virtual tables */
#include "vtable.c"

/* virtual file system */
#include "vfs.c"

/* constants */
#include "constants.c"

/* MODULE METHODS */

/* Although pyobject is marked as a method, it is really a class but
   we are trying to hide the implementation details as much as possible.
*/

/** .. method:: pyobject(object: Any)

  Indicates a Python object is being provided as a
  :ref:`runtime value <pyobject>`.
*/

/** .. method:: sqlite_lib_version() -> str

  Returns the version of the SQLite library.  This value is queried at
  run time from the library so if you use shared libraries it will be
  the version in the shared library.

  -* sqlite3_libversion
*/

static PyObject *
get_sqlite_version(void)
{
  return PyUnicode_FromString(sqlite3_libversion());
}

/** .. method:: sqlite3_sourceid() -> str

    Returns the exact checkin information for the SQLite 3 source
    being used.

    -* sqlite3_sourceid
*/

static PyObject *
get_sqlite3_sourceid(void)
{
  return PyUnicode_FromString(sqlite3_sourceid());
}

/** .. method:: apsw_version() -> str

  Returns the APSW version.
*/
static PyObject *
get_apsw_version(void)
{
  return PyUnicode_FromString(APSW_VERSION);
}

/** .. method:: enable_shared_cache(enable: bool) -> None

  `Discouraged
  <https://sqlite.org/sharedcache.html#use_of_shared_cache_is_discouraged>`__.

  -* sqlite3_enable_shared_cache
*/
static PyObject *
enable_shared_cache(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs,
                    PyObject *fast_kwnames)
{
  int enable = 0;
  {
    Apsw_enable_shared_cache_CHECK;
    ARG_PROLOG(1, Apsw_enable_shared_cache_KWNAMES);
    ARG_MANDATORY ARG_bool(enable);
    ARG_EPILOG(NULL, Apsw_enable_shared_cache_USAGE, );
  }
#ifdef SQLITE_OMIT_SHARED_CACHE
  return PyErr_Format(PyExc_Exception, "sqlite3_enable_shared_cache has been omitted");
#else
  int res = sqlite3_enable_shared_cache(enable);
  SET_EXC(res, NULL);

  if (PyErr_Occurred())
    return NULL;

  Py_RETURN_NONE;
#endif
}

/** .. method:: connections() -> list[Connection]

  Returns a list of the connections

*/
static PyObject *the_connections;
static PyObject *
apsw_connections(PyObject *Py_UNUSED(self))
{
  Py_ssize_t i;
  PyObject *res = PyList_New(0), *item = NULL;
  for (i = 0; i < PyList_GET_SIZE(the_connections); i++)
  {
    if (PyWeakref_GetRef(PyList_GET_ITEM(the_connections, i), &item) < 0)
      goto fail;
    if (item)
    {
      if (PyList_Append(res, item))
        goto fail;
      Py_CLEAR(item);
    }
  }
  return res;
fail:
  Py_XDECREF(res);
  Py_XDECREF(item);
  return NULL;
}

static void
apsw_connection_remove(Connection *con)
{
  Py_ssize_t i;
  for (i = 0; i < PyList_GET_SIZE(the_connections);)
  {
    PyObject *wr = PyList_GET_ITEM(the_connections, i);
    PyObject *wo = NULL;
    if (PyWeakref_GetRef(wr, &wo) < 0)
    {
      apsw_write_unraisable(NULL);
      continue;
    }
    if (!wo || wo == (PyObject *)con)
    {
      if (PyList_SetSlice(the_connections, i, i + 1, NULL))
        apsw_write_unraisable(NULL);
      if (!wo)
        continue;
      Py_DECREF(wo);
      return;
    }
    Py_DECREF(wo);
    i++;
  }
}

static int
apsw_connection_add(Connection *con)
{
  PyObject *weakref = PyWeakref_NewRef((PyObject *)con, NULL);
  if (!weakref)
    return -1;
  int res = PyList_Append(the_connections, weakref);
  Py_DECREF(weakref);
  return res;
}

/** .. method:: initialize() -> None

  It is unlikely you will want to call this method as SQLite automatically initializes.

  -* sqlite3_initialize
*/

static PyObject *
initialize(void)
{
  int res;

  res = sqlite3_initialize();
  SET_EXC(res, NULL);
  if (PyErr_Occurred())
    return NULL;

  Py_RETURN_NONE;
}

/** .. method:: shutdown() -> None

  It is unlikely you will want to call this method and there is no
  need to do so.  It is a **really** bad idea to call it unless you
  are absolutely sure all :class:`connections <Connection>`,
  :class:`blobs <Blob>`, :class:`cursors <Cursor>`, :class:`vfs <VFS>`
  etc have been closed, deleted and garbage collected.

  -* sqlite3_shutdown
*/
#ifdef APSW_FORK_CHECKER
static void free_fork_checker(void);
#endif

static PyObject *
sqliteshutdown(void)
{
  int res;

  res = sqlite3_shutdown();
  SET_EXC(res, NULL);

  if (PyErr_Occurred())
    return NULL;

#ifdef APSW_FORK_CHECKER
  free_fork_checker();
#endif

  Py_RETURN_NONE;
}

/** .. method:: config(op: int, *args: Any) -> None

  :param op: A `configuration operation <https://sqlite.org/c3ref/c_config_chunkalloc.html>`_
  :param args: Zero or more arguments as appropriate for *op*

  Some operations don't make sense from a Python program.  All the
  remaining are supported.

  -* sqlite3_config
*/

static PyObject *logger_cb = NULL;

static void
apsw_logger(void *arg, int errcode, const char *message)
{
  PyGILState_STATE gilstate;
  PyObject *res = NULL;

  gilstate = PyGILState_Ensure();
  MakeExistingException();
  assert(arg == logger_cb);
  assert(arg);
  PY_ERR_FETCH(exc);

  PyObject *vargs[] = { NULL, PyLong_FromLong(errcode), PyUnicode_FromString(message) };
  if (vargs[1] && vargs[2])
    res = PyObject_Vectorcall(arg, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[1]);
  Py_XDECREF(vargs[2]);
  if (!res)
  {
    /* apsw_write_unraisable writes to sqlite3_log so if we are in too
       much recursion, avoid going further */
    if (PyErr_ExceptionMatches(PyExc_RecursionError))
      PyErr_Clear();
    else
    {
      AddTraceBackHere(__FILE__, __LINE__, "apsw_sqlite3_log_receiver", "{s: O, s: i, s: s}", "logger", OBJ(arg),
                       "errcode", errcode, "message", message);

      apsw_write_unraisable(NULL);
    }
  }
  else
    Py_DECREF(res);

  if (PY_ERR_NOT_NULL(exc))
    PY_ERR_RESTORE(exc);
  PyGILState_Release(gilstate);
}

static PyObject *
apsw_config(PyObject *Py_UNUSED(self), PyObject *args)
{
  int res, optdup;
  int opt;

  if (PyTuple_GET_SIZE(args) < 1 || !PyLong_Check(PyTuple_GET_ITEM(args, 0)))
    return PyErr_Format(PyExc_TypeError, "There should be at least one argument with the first being a number");

  opt = PyLong_AsInt(PyTuple_GET_ITEM(args, 0));
  if (PyErr_Occurred())
    return NULL;

  switch (opt)
  {
  case SQLITE_CONFIG_SINGLETHREAD:
  case SQLITE_CONFIG_MULTITHREAD:
  case SQLITE_CONFIG_SERIALIZED:
    if (!PyArg_ParseTuple(args, "i", &optdup))
      return NULL;
    assert(opt == optdup);
    res = sqlite3_config(opt);
    break;

  case SQLITE_CONFIG_PCACHE_HDRSZ: {
    int outval = -1;
    if (!PyArg_ParseTuple(args, "i", &optdup))
      return NULL;
    assert(opt == optdup);
    res = sqlite3_config(opt, &outval);
    SET_EXC(res, NULL);
    if (PyErr_Occurred())
      return NULL;

    return PyLong_FromLong(outval);
  }

  case SQLITE_CONFIG_URI:
  case SQLITE_CONFIG_MEMSTATUS:
  case SQLITE_CONFIG_COVERING_INDEX_SCAN:
  case SQLITE_CONFIG_PMASZ:
  case SQLITE_CONFIG_STMTJRNL_SPILL:
  case SQLITE_CONFIG_SORTERREF_SIZE:
  case SQLITE_CONFIG_LOOKASIDE:
  case SQLITE_CONFIG_SMALL_MALLOC: {
    int intval;
    if (!PyArg_ParseTuple(args, "ii", &optdup, &intval))
      return NULL;
    assert(opt == optdup);
    res = sqlite3_config(opt, intval);
    break;
  }

  case SQLITE_CONFIG_LOG: {
    PyObject *logger;
    if (!PyArg_ParseTuple(args, "iO", &optdup, &logger))
      return NULL;
    if (Py_IsNone(logger))
    {
      res = sqlite3_config(opt, NULL);
      if (res == SQLITE_OK)
        Py_CLEAR(logger_cb);
    }
    else if (!PyCallable_Check(logger))
    {
      return PyErr_Format(PyExc_TypeError, "Logger should be None or a callable");
    }
    else
    {
      res = sqlite3_config((int)opt, apsw_logger, logger);
      if (res == SQLITE_OK)
      {
        Py_CLEAR(logger_cb);
        logger_cb = Py_NewRef(logger);
      }
    }
    break;
  }

  case SQLITE_CONFIG_MMAP_SIZE: {
    sqlite3_int64 default_limit, max_limit;
    if (!PyArg_ParseTuple(args, "iLL", &optdup, &default_limit, &max_limit))
      return NULL;
    assert(opt == optdup);
    res = sqlite3_config(opt, default_limit, max_limit);
    break;
  }

  case SQLITE_CONFIG_MEMDB_MAXSIZE: {
    sqlite3_int64 limit;
    if (!PyArg_ParseTuple(args, "iL", &optdup, &limit))
      return NULL;
    assert(opt == optdup);
    res = sqlite3_config(opt, limit);
    break;
  }

  default:
    return PyErr_Format(PyExc_TypeError, "Unknown config type %d", (int)opt);
  }

  SET_EXC(res, NULL);
  if (PyErr_Occurred())
    return NULL;

  Py_RETURN_NONE;
}

/** .. method:: memory_used() -> int

  Returns the amount of memory SQLite is currently using.

  .. seealso::
    :meth:`status`


  -* sqlite3_memory_used
*/
static PyObject *
memory_used(void)
{
  return PyLong_FromLongLong(sqlite3_memory_used());
}

/** .. method:: memory_high_water(reset: bool = False) -> int

  Returns the maximum amount of memory SQLite has used.  If *reset* is
  True then the high water mark is reset to the current value.

  .. seealso::

    :meth:`status`

  -* sqlite3_memory_highwater
*/
static PyObject *
memory_high_water(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int reset = 0;

  {
    Apsw_memory_high_water_CHECK;
    ARG_PROLOG(1, Apsw_memory_high_water_KWNAMES);
    ARG_OPTIONAL ARG_bool(reset);
    ARG_EPILOG(NULL, Apsw_memory_high_water_USAGE, );
  }
  return PyLong_FromLongLong(sqlite3_memory_highwater(reset));
}

/** .. method:: soft_heap_limit(limit: int) -> int

  Requests SQLite try to keep memory usage below *limit* bytes and
  returns the previous limit.

  .. seealso::

      :meth:`hard_heap_limit`

  -* sqlite3_soft_heap_limit64
*/
static PyObject *
soft_heap_limit(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  sqlite3_int64 limit, oldlimit;
  {
    Apsw_soft_heap_limit_CHECK;
    ARG_PROLOG(1, Apsw_soft_heap_limit_KWNAMES);
    ARG_MANDATORY ARG_int64(limit);
    ARG_EPILOG(NULL, Apsw_soft_heap_limit_USAGE, );
  }
  oldlimit = sqlite3_soft_heap_limit64(limit);

  return PyLong_FromLongLong(oldlimit);
}

/** .. method:: hard_heap_limit(limit: int) -> int

  Enforces SQLite keeping memory usage below *limit* bytes and
  returns the previous limit.

  .. seealso::

      :meth:`soft_heap_limit`

  -* sqlite3_hard_heap_limit64
*/
static PyObject *
apsw_hard_heap_limit(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs,
                     PyObject *fast_kwnames)
{
  sqlite3_int64 limit, oldlimit;
  {
    Apsw_hard_heap_limit_CHECK;
    ARG_PROLOG(1, Apsw_hard_heap_limit_KWNAMES);
    ARG_MANDATORY ARG_int64(limit);
    ARG_EPILOG(NULL, Apsw_hard_heap_limit_USAGE, );
  }
  oldlimit = sqlite3_hard_heap_limit64(limit);

  return PyLong_FromLongLong(oldlimit);
}

/** .. method:: randomness(amount: int)  -> bytes

  Gets random data from SQLite's random number generator.

  :param amount: How many bytes to return

  -* sqlite3_randomness
*/
static PyObject *
randomness(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int amount;
  PyObject *bytes;

  {
    Apsw_randomness_CHECK;
    ARG_PROLOG(1, Apsw_randomness_KWNAMES);
    ARG_MANDATORY ARG_int(amount);
    ARG_EPILOG(NULL, Apsw_randomness_USAGE, );
  }
  if (amount < 0)
    return PyErr_Format(PyExc_ValueError, "Can't have negative number of bytes");

  bytes = PyBytes_FromStringAndSize(NULL, amount);
  if (!bytes)
    return bytes;
  sqlite3_randomness(amount, PyBytes_AS_STRING(bytes));
  return bytes;
}

/** .. method:: release_memory(amount: int) -> int

  Requests SQLite try to free *amount* bytes of memory.  Returns how
  many bytes were freed.

  -* sqlite3_release_memory
*/

static PyObject *
release_memory(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int amount;

  {
    Apsw_release_memory_CHECK;
    ARG_PROLOG(1, Apsw_release_memory_KWNAMES);
    ARG_MANDATORY ARG_int(amount);
    ARG_EPILOG(NULL, Apsw_release_memory_USAGE, );
  }
  return PyLong_FromLong(sqlite3_release_memory(amount));
}

/** .. method:: status(op: int, reset: bool = False) -> tuple[int, int]

  Returns current and highwater measurements.

  :param op: A `status parameter <https://sqlite.org/c3ref/c_status_malloc_size.html>`_
  :param reset: If *True* then the highwater is set to the current value
  :returns: A tuple of current value and highwater value

  .. seealso::

    * :meth:`Connection.status` for statistics about a :class:`Connection`
    * :ref:`Status example <example_status>`

  -* sqlite3_status64

*/
static PyObject *
status(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int res, op, reset = 0;
  sqlite3_int64 current = 0, highwater = 0;

  {
    Apsw_status_CHECK;
    ARG_PROLOG(2, Apsw_status_KWNAMES);
    ARG_MANDATORY ARG_int(op);
    ARG_OPTIONAL ARG_bool(reset);
    ARG_EPILOG(NULL, Apsw_status_USAGE, );
  }

  res = sqlite3_status64(op, &current, &highwater, reset);
  SET_EXC(res, NULL);

  if (PyErr_Occurred())
    return NULL;

  return Py_BuildValue("(LL)", current, highwater);
}

/** .. method:: vfs_names() -> list[str]

  Returns a list of the currently installed :ref:`vfs <vfs>`.  The first
  item in the list is the default vfs.

  -* sqlite3_vfs_find
*/
static PyObject *
vfs_names(PyObject *Py_UNUSED(self))
{
  PyObject *result = NULL, *str = NULL;
  int res;
  sqlite3_vfs *vfs = sqlite3_vfs_find(0);

  result = PyList_New(0);
  if (!result)
    goto error;

  while (vfs)
  {
    str = convertutf8string(vfs->zName);
    if (!str)
      goto error;
    res = PyList_Append(result, str);
    if (res)
      goto error;
    Py_DECREF(str);
    vfs = vfs->pNext;
  }
  return result;

error:
  Py_XDECREF(str);
  Py_XDECREF(result);
  return NULL;
}

/* macros to build the format string and values.  int, string and pointers */
#undef I
#undef S
#undef P
#define I(n) #n, vfs->n
#define S(n) #n, vfs->n
#define P(n) #n, PyLong_FromVoidPtr, vfs->n

#define VFS1_BUILD "si si si ss sO& sO& sO& sO& sO& sO& sO& sO& sO& sO& sO& sO& sO&"

#define VFS1_FIELDS                                                                                                    \
  I(iVersion), I(szOsFile), I(mxPathname), S(zName), P(pAppData), P(xOpen), P(xDelete), P(xAccess), P(xFullPathname),  \
      P(xDlOpen), P(xDlError), P(xDlSym), P(xDlClose), P(xRandomness), P(xSleep), P(xGetLastError), P(xCurrentTime)

#define VFS2_BUILD "sO&"
#define VFS2_FIELDS P(xCurrentTimeInt64)

#define VFS3_BUILD "sO& sO& sO&"
#define VFS3_FIELDS P(xSetSystemCall), P(xGetSystemCall), P(xNextSystemCall)

/** .. method:: vfs_details() -> list[dict[str, int | str]]

Returns a list with details of each :ref:`vfs <vfs>`.  The detail is a
dictionary with the keys being the names of the `sqlite3_vfs
<https://sqlite.org/c3ref/vfs.html>`__ data structure, and their
corresponding values.

Pointers are converted using :c:func:`PyLong_FromVoidPtr`.

-* sqlite3_vfs_find
*/
static PyObject *
vfs_details(PyObject *Py_UNUSED(self))
{
  PyObject *result, *dict;
  sqlite3_vfs *vfs = sqlite3_vfs_find(0);
  int res;

  result = PyList_New(0);
  if (!result)
    return NULL;
  while (vfs)
  {
    switch (vfs->iVersion)
    {
    case 0: /* some older sqlite source does this */
    case 1:
      dict = Py_BuildValue("{" VFS1_BUILD "}", VFS1_FIELDS);
      break;
    case 2:
      dict = Py_BuildValue("{" VFS1_BUILD VFS2_BUILD "}", VFS1_FIELDS, VFS2_FIELDS);
      break;
    default: /* handle 4+ */
    case 3:
      dict = Py_BuildValue("{" VFS1_BUILD VFS2_BUILD VFS3_BUILD "}", VFS1_FIELDS, VFS2_FIELDS, VFS3_FIELDS);
    }
    if (!dict)
    {
      Py_DECREF(result);
      return NULL;
    }
    res = PyList_Append(result, dict);
    Py_DECREF(dict);
    if (res != 0)
    {
      Py_DECREF(result);
      return NULL;
    }
    vfs = vfs->pNext;
  }
  return result;
}

#undef I
#undef S
#undef P

/** .. method:: exception_for(code: int) -> Exception

  If you would like to raise an exception that corresponds to a
  particular SQLite `error code
  <https://sqlite.org/c3ref/c_abort.html>`_ then call this function.
  It also understands `extended error codes
  <https://sqlite.org/c3ref/c_ioerr_access.html>`_.

  For example to raise `SQLITE_IOERR_ACCESS <https://sqlite.org/c3ref/c_ioerr_access.html>`_::

    raise apsw.exception_for(apsw.SQLITE_IOERR_ACCESS)

*/
static PyObject *
get_apsw_exception_for(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs,
                       PyObject *fast_kwnames)
{
  int code = 0, i;
  PyObject *result = NULL, *tmp = NULL;

  {
    Apsw_exception_for_CHECK;
    ARG_PROLOG(1, Apsw_exception_for_KWNAMES);
    ARG_MANDATORY ARG_int(code);
    ARG_EPILOG(NULL, Apsw_exception_for_USAGE, );
  }

  for (i = 0; exc_descriptors[i].name; i++)
    if (exc_descriptors[i].code == (code & 0xff))
    {
      PyObject *vargs[] = { NULL };
      result = PyObject_Vectorcall(exc_descriptors[i].cls, vargs + 1, 0 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
      if (!result)
        return result;
      break;
    }
  if (!result)
    return PyErr_Format(PyExc_ValueError, "%d is not a known error code", code);

  tmp = PyLong_FromLong(code);
  if (!tmp)
    goto error;
  if (0 != PyObject_SetAttr(result, apst.extendedresult, tmp))
    goto error;
  Py_DECREF(tmp);
  tmp = PyLong_FromLong(code & 0xff);
  if (!tmp)
    goto error;
  if (0 != PyObject_SetAttr(result, apst.result, tmp))
    goto error;
  Py_DECREF(tmp);
  return result;
error:
  Py_XDECREF(tmp);
  Py_CLEAR(result);
  return NULL;
}

/** .. method:: complete(statement: str) -> bool

  Returns True if the input string comprises one or more complete SQL
  statements by looking for an unquoted trailing semi-colon.  It does
  not consider comments or blank lines to be complete.

  An example use would be if you were prompting the user for SQL
  statements and needed to know if you had a whole statement, or
  needed to ask for another line::

    statement = input("SQL> ")
    while not apsw.complete(statement):
       more = input("  .. ")
       statement = statement + "\\n" + more

  -* sqlite3_complete
*/
static PyObject *
apswcomplete(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *statement = NULL;
  int res;

  {
    Apsw_complete_CHECK;
    ARG_PROLOG(1, Apsw_complete_KWNAMES);
    ARG_MANDATORY ARG_str(statement);
    ARG_EPILOG(NULL, Apsw_complete_USAGE, );
  }

  res = sqlite3_complete(statement);

  if (res)
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

#if defined(APSW_DEBUG) || defined(APSW_FAULT_INJECT)
static PyObject *
apsw_fini(PyObject *Py_UNUSED(self))
{
  fini_apsw_strings();
  Py_RETURN_NONE;
}
#endif

#ifdef __SANITIZE_ADDRESS__
#include <sanitizer/lsan_interface.h>

static PyObject *
apsw_leak_check(PyObject *Py_UNUSED(self))
{
  int res = __lsan_do_recoverable_leak_check();
  return PyLong_FromLong(res);
}
#endif

#ifdef APSW_FORK_CHECKER

/*
   We want to verify that SQLite objects are not used across forks.
   One way is to modify all calls to SQLite to do the checking but
   this is a pain as well as a performance hit.  Instead we use the
   approach of providing an alternative mutex implementation since
   pretty much every SQLite API call takes and releases a mutex.

   Our diverted functions check the process id on calls and set the
   process id on allocating a mutex.  We have to avoid the checks for
   the static mutexes.

   This code also doesn't bother with some things like checking malloc
   results.  It is intended to only be used to verify correctness with
   test suites.  The code that sets Python exceptions is also very
   brute force and is likely to cause problems.  That however is a
   good thing - you will really be sure there is a problem!
 */

typedef struct
{
  pid_t pid;
  sqlite3_mutex *underlying_mutex;
} apsw_mutex;

static apsw_mutex *apsw_mutexes[]
    = { NULL, /* not used - fast */
        NULL, /* not used - recursive */
        NULL, /* from this point on corresponds to the various static mutexes */
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL };

static sqlite3_mutex_methods apsw_orig_mutex_methods;

static int
apsw_xMutexInit(void)
{
  return apsw_orig_mutex_methods.xMutexInit();
}

static int
apsw_xMutexEnd(void)
{
  return apsw_orig_mutex_methods.xMutexEnd();
}

#define MUTEX_MAX_ALLOC 20
static apsw_mutex *fork_checker_mutexes[MUTEX_MAX_ALLOC];
static int current_apsw_fork_mutex = 0;

static sqlite3_mutex *
apsw_xMutexAlloc(int which)
{
  switch (which)
  {
  case SQLITE_MUTEX_FAST:
  case SQLITE_MUTEX_RECURSIVE: {
    apsw_mutex *am;
    sqlite3_mutex *m = apsw_orig_mutex_methods.xMutexAlloc(which);

    if (!m)
      return m;
    assert(current_apsw_fork_mutex < MUTEX_MAX_ALLOC);
    fork_checker_mutexes[current_apsw_fork_mutex++] = am = malloc(sizeof(apsw_mutex));
    am->pid = getpid();
    am->underlying_mutex = m;
    return (sqlite3_mutex *)am;
  }
  default:
    /* verify we have space */
    assert((unsigned)which < sizeof(apsw_mutexes) / sizeof(apsw_mutexes[0]));
    /* fill in if missing */
    if (!apsw_mutexes[which])
    {
      apsw_mutexes[which] = malloc(sizeof(apsw_mutex));
      apsw_mutexes[which]->pid = 0;
      apsw_mutexes[which]->underlying_mutex = apsw_orig_mutex_methods.xMutexAlloc(which);
    }
    return (sqlite3_mutex *)apsw_mutexes[which];
  }
}

static void
free_fork_checker(void)
{
  unsigned i;
  for (i = 0; i < sizeof(apsw_mutexes) / sizeof(apsw_mutexes[0]); i++)
  {
    free(apsw_mutexes[i]);
    apsw_mutexes[i] = NULL;
  }
  for (i = 0; i < MUTEX_MAX_ALLOC; i++)
  {
    free(fork_checker_mutexes[i]);
    fork_checker_mutexes[i] = 0;
  }
  current_apsw_fork_mutex = 0;
}

static int
apsw_check_mutex(apsw_mutex *am)
{
  if (am->pid && am->pid != getpid())
  {
    PyGILState_STATE gilstate;
    gilstate = PyGILState_Ensure();
    PyErr_Format(ExcForkingViolation,
                 "SQLite object allocated in one process is being used in another (across a fork)");
    apsw_write_unraisable(NULL);
    PyErr_Format(ExcForkingViolation,
                 "SQLite object allocated in one process is being used in another (across a fork)");
    PyGILState_Release(gilstate);
    return SQLITE_MISUSE;
  }
  return SQLITE_OK;
}

static void
apsw_xMutexFree(sqlite3_mutex *mutex)
{
  apsw_mutex *am = (apsw_mutex *)mutex;
  apsw_check_mutex(am);
  apsw_orig_mutex_methods.xMutexFree(am->underlying_mutex);
}

static void
apsw_xMutexEnter(sqlite3_mutex *mutex)
{
  apsw_mutex *am = (apsw_mutex *)mutex;
  apsw_check_mutex(am);
  apsw_orig_mutex_methods.xMutexEnter(am->underlying_mutex);
}

static int
apsw_xMutexTry(sqlite3_mutex *mutex)
{
  apsw_mutex *am = (apsw_mutex *)mutex;
  if (apsw_check_mutex(am))
    return SQLITE_MISUSE;
  return apsw_orig_mutex_methods.xMutexTry(am->underlying_mutex);
}

static void
apsw_xMutexLeave(sqlite3_mutex *mutex)
{
  apsw_mutex *am = (apsw_mutex *)mutex;
  apsw_check_mutex(am);
  apsw_orig_mutex_methods.xMutexLeave(am->underlying_mutex);
}

#ifdef SQLITE_DEBUG
static int
apsw_xMutexHeld(sqlite3_mutex *mutex)
{
  apsw_mutex *am = (apsw_mutex *)mutex;
  apsw_check_mutex(am);
  return apsw_orig_mutex_methods.xMutexHeld(am->underlying_mutex);
}

static int
apsw_xMutexNotheld(sqlite3_mutex *mutex)
{
  apsw_mutex *am = (apsw_mutex *)mutex;
  apsw_check_mutex(am);
  return apsw_orig_mutex_methods.xMutexNotheld(am->underlying_mutex);
}
#endif

static sqlite3_mutex_methods apsw_mutex_methods
    = { apsw_xMutexInit,  apsw_xMutexEnd,    apsw_xMutexAlloc, apsw_xMutexFree,
        apsw_xMutexEnter, apsw_xMutexTry,    apsw_xMutexLeave,
#ifdef SQLITE_DEBUG
        apsw_xMutexHeld,  apsw_xMutexNotheld
#else
        0,
        0
#endif
      };

/** .. method:: fork_checker() -> None

  **Note** This method is not available on Windows as it does not
  support the fork system call.

  SQLite does not allow the use of database connections across `forked
  <https://en.wikipedia.org/wiki/Fork_(operating_system)>`__ processes
  (see the `SQLite FAQ Q6 <https://sqlite.org/faq.html#q6>`__).
  (Forking creates a child process that is a duplicate of the parent
  including the state of all data structures in the program.  If you
  do this to SQLite then parent and child would both consider
  themselves owners of open databases and silently corrupt each
  other's work and interfere with each other's locks.)

  One example of how you may end up using fork is if you use the
  :mod:`multiprocessing module <multiprocessing>` which can use
  fork to make child processes.

  If you do use fork or multiprocessing on a platform that supports fork
  then you **must** ensure database connections and their objects
  (cursors, backup, blobs etc) are not used in the parent process, or
  are all closed before calling fork or starting a `Process
  <https://docs.python.org/3/library/multiprocessing.html#process-and-exceptions>`__.
  (Note you must call close to ensure the underlying SQLite objects are
  closed.  It is also a good idea to call :func:`gc.collect(2)
  <gc.collect>` to ensure anything you may have missed is also
  deallocated.)

  Once you run this method, extra checking code is inserted into
  SQLite's mutex operations (at a very small performance penalty) that
  verifies objects are not used across processes.  You will get a
  :exc:`ForkingViolationError` if you do so.  Note that due to the way
  Python's internals work, the exception will be delivered to
  :func:`sys.excepthook` in addition to the normal exception mechanisms and
  may be reported by Python after the line where the issue actually
  arose.  (Destructors of objects you didn't close also run between
  lines.)

  You should only call this method as the first line after importing
  APSW, as it has to shutdown and re-initialize SQLite.  If you have
  any SQLite objects already allocated when calling the method then
  the program will later crash.  The recommended use is to use the fork
  checking as part of your test suite.
*/
static PyObject *
apsw_fork_checker(PyObject *Py_UNUSED(self))
{
  int rc;

  /* ignore multiple attempts to use this routine */
  if (apsw_orig_mutex_methods.xMutexInit)
    goto ok;

  /* Ensure mutex methods available and installed */
  rc = sqlite3_initialize();
  if (rc)
    goto fail;

  /* then do a shutdown as we can't get or change mutex while sqlite is running */
  rc = sqlite3_shutdown();
  if (rc)
    goto fail;

  rc = sqlite3_config(SQLITE_CONFIG_GETMUTEX, &apsw_orig_mutex_methods);
  if (rc)
    goto fail;

  rc = sqlite3_config(SQLITE_CONFIG_MUTEX, &apsw_mutex_methods);
  if (rc)
    goto fail;

  /* start back up again */
  rc = sqlite3_initialize();
  if (rc)
    goto fail;

ok:
  Py_RETURN_NONE;

fail:
  assert(rc != SQLITE_OK);
  SET_EXC(rc, NULL);
  return NULL;
}
#endif

/** .. attribute:: compile_options
    :type: tuple[str, ...]

    A tuple of the options used to compile SQLite.  For example it
    will be something like this, but with around 50 entries::

        ('ENABLE_LOCKING_STYLE=0', 'TEMP_STORE=1', 'THREADSAFE=1', 'ENABLE_FTS5',
         'OMIT_SHARED_CACHE', 'SYSTEM_MALLOC')

    -* sqlite3_compileoption_get
*/
static PyObject *
get_compile_options(void)
{
  int i, count = 0;
  const char *opt;
  PyObject *tmpstring;
  PyObject *res = 0;

  /* this method is only called once at startup */

  for (i = 0;; i++)
  {
    opt = sqlite3_compileoption_get(i);
    if (!opt)
      break;
  }
  count = i;

  res = PyTuple_New(count);
  if (!res)
    goto fail;
  for (i = 0; i < count; i++)
  {
    opt = sqlite3_compileoption_get(i);
    assert(opt);
    tmpstring = PyUnicode_FromString(opt);
    if (!tmpstring)
      goto fail;
    PyTuple_SET_ITEM(res, i, tmpstring);
  }

  return res;
fail:
  Py_XDECREF(res);
  return NULL;
}

/** .. attribute:: keywords
    :type: set[str]

    A set containing every SQLite keyword

    -* sqlite3_keyword_count sqlite3_keyword_name

*/
static PyObject *
get_keywords(void)
{
  int i, j, count, size;
  PyObject *res = NULL, *tmpstring;
  const char *name;

  res = PySet_New(0);
  if (!res)
    goto fail;

  count = sqlite3_keyword_count();
  for (i = 0; i < count; i++)
  {
    j = sqlite3_keyword_name(i, &name, &size);
    assert(j == SQLITE_OK);
    tmpstring = PyUnicode_FromStringAndSize(name, size);
    if (!tmpstring)
      goto fail;
    j = PySet_Add(res, tmpstring);
    Py_DECREF(tmpstring);
    if (j)
      goto fail;
  }

  return res;
fail:
  Py_XDECREF(res);
  return NULL;
}

/** .. method:: format_sql_value(value: SQLiteValue) -> str

  Returns a Python string representing the supplied value in SQLite
  syntax.

  Note that SQLite represents floating point `Nan
  <https://en.wikipedia.org/wiki/NaN>`__ as :code:`NULL`, infinity as
  :code:`1e999` and loses the sign on `negative zero
  <https://en.wikipedia.org/wiki/Signed_zero>`__.
*/
static PyObject *
formatsqlvalue(PyObject *Py_UNUSED(self), PyObject *value)
{
  /* NULL/None */
  if (Py_IsNone(value))
    return Py_NewRef(apst.sNULL);

  /* Integer */
  if (PyLong_Check(value))
    return PyObject_Str(value);

  /* float */
  if (PyFloat_Check(value))
  {
    double d = PyFloat_AS_DOUBLE(value);
    if (isnan(d))
      return Py_NewRef(apst.sNULL);
    if (isinf(d))
      return Py_NewRef(signbit(d) ? apst.s_1e999 : apst.s1e999);
    if (d == 0 && signbit(d))
      return Py_NewRef(apst.s0_0);
    return PyObject_Str(value);
  }

  /* Unicode */
  if (PyUnicode_Check(value))
  {
    Py_ssize_t needed_chars = 2; /* leading and trailing quote */
    unsigned int input_kind = PyUnicode_KIND(value), output_kind;
    void *input_data = PyUnicode_DATA(value);
    Py_ssize_t input_length = PyUnicode_GET_LENGTH(value);
    Py_ssize_t pos, outpos;
    int simple = 1;
    Py_UCS4 ch;

    PyObject *strres;
    void *output_data;

    for (pos = 0; pos < input_length; pos++)
    {
      switch (PyUnicode_READ(input_kind, input_data, pos))
      {
      case '\'':
        needed_chars += 2;
        simple = 0;
        break;
      case 0:
        /* To output an embedded null we have to concatenate a blob
           containing only a null to a string and sqlite does the
           necessary co-ercion and gets things right irrespective of
           the underlying string being utf8 or utf16.  It takes 11
           characters to do that. */
        needed_chars += 11;
        simple = 0;
        break;
      default:
        needed_chars += 1;
      }
    }

    strres = PyUnicode_New(needed_chars, PyUnicode_MAX_CHAR_VALUE(value));
    if (!strres)
      return NULL;
    output_kind = PyUnicode_KIND(strres);
    output_data = PyUnicode_DATA(strres);

    PyUnicode_WRITE(output_kind, output_data, 0, '\'');
    PyUnicode_WRITE(output_kind, output_data, needed_chars - 1, '\'');

    if (simple)
    {
#ifdef PYPY_VERSION
      PyErr_Format(PyExc_NotImplementedError, "PyPy has not implemented PyUnicode_CopyCharacters");
      return NULL;
#else
      PyUnicode_CopyCharacters(strres, 1, value, 0, input_length);
      return strres;
#endif
    }

    outpos = 1;

    for (pos = 0; pos < input_length; pos++)
    {
      switch (ch = PyUnicode_READ(input_kind, input_data, pos))
      {
      case 0: {
        int i;
        for (i = 0; i < 11; i++)
          PyUnicode_WRITE(output_kind, output_data, outpos++, "'||X'00'||'"[i]);
      }
      break;
      case '\'':
        PyUnicode_WRITE(output_kind, output_data, outpos++, ch);
        /* fall through */
      default:
        PyUnicode_WRITE(output_kind, output_data, outpos++, ch);
      }
    }
    return strres;
  }
  /* Blob */
  if (PyBytes_Check(value))
  {
    int asrb;
    PyObject *strres;
    void *unidata;
    Py_ssize_t unipos = 0;
    Py_buffer buffer;
    Py_ssize_t buflen;
    const unsigned char *bufferc;

    asrb = PyObject_GetBufferContiguous(value, &buffer, PyBUF_SIMPLE);
    if (asrb == -1)
      return NULL;

    strres = PyUnicode_New(buffer.len * 2 + 3, 127);
    if (!strres)
      goto bytesfinally;

    bufferc = buffer.buf;
    buflen = buffer.len;
    unidata = PyUnicode_DATA(strres);
    PyUnicode_WRITE(PyUnicode_1BYTE_KIND, unidata, unipos++, 'X');
    PyUnicode_WRITE(PyUnicode_1BYTE_KIND, unidata, unipos++, '\'');
    /* About the billionth time I have written a hex conversion routine */
    for (; buflen; buflen--)
    {
      PyUnicode_WRITE(PyUnicode_1BYTE_KIND, unidata, unipos++, "0123456789ABCDEF"[(*bufferc) >> 4]);
      PyUnicode_WRITE(PyUnicode_1BYTE_KIND, unidata, unipos++, "0123456789ABCDEF"[(*bufferc++) & 0x0f]);
    }
    PyUnicode_WRITE(PyUnicode_1BYTE_KIND, unidata, unipos++, '\'');

  bytesfinally:
    PyBuffer_Release(&buffer);
    return strres;
  }

  return PyErr_Format(PyExc_TypeError, "Expected str, bytes, int, float or None, not %s", Py_TypeName(value));
}

/** .. method:: log(errorcode: int, message: str) -> None

    Calls the SQLite logging interface.  You must format the
    message before passing it to this method::

        apsw.log(apsw.SQLITE_NOMEM, f"Need { needed } bytes of memory")

    -* sqlite3_log
 */
static PyObject *
apsw_log(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int errorcode;
  const char *message;
  {
    Apsw_log_CHECK;
    ARG_PROLOG(2, Apsw_log_KWNAMES);
    ARG_MANDATORY ARG_int(errorcode);
    ARG_MANDATORY ARG_str(message);
    ARG_EPILOG(NULL, Apsw_log_USAGE, );
  }
  sqlite3_log(errorcode, "%s", message);

  if (PyErr_Occurred())
    return NULL;

  Py_RETURN_NONE;
}

/** .. method:: strlike(glob: str, string: str, escape: int = 0) -> int

  Does string LIKE matching.  Zero is returned on a match.

  -* sqlite3_strlike
*/
static PyObject *
apsw_strlike(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *glob = NULL, *string = NULL;
  int escape = 0;
  int res;

  {
    Apsw_strlike_CHECK;
    ARG_PROLOG(3, Apsw_strlike_KWNAMES);
    ARG_MANDATORY ARG_str(glob);
    ARG_MANDATORY ARG_str(string);
    ARG_OPTIONAL ARG_int(escape);
    ARG_EPILOG(NULL, Apsw_strlike_USAGE, );
  }

  res = sqlite3_strlike(glob, string, escape);

  return PyLong_FromLong(res);
}

/** .. method:: strglob(glob: str, string: str) -> int

  Does string GLOB matching.  Zero is returned on a match.

  -* sqlite3_strglob
*/
static PyObject *
apsw_strglob(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *glob = NULL, *string = NULL;
  int res;

  {
    Apsw_strglob_CHECK;
    ARG_PROLOG(2, Apsw_strglob_KWNAMES);
    ARG_MANDATORY ARG_str(glob);
    ARG_MANDATORY ARG_str(string);
    ARG_EPILOG(NULL, Apsw_strglob_USAGE, );
  }

  res = sqlite3_strglob(glob, string);

  return PyLong_FromLong(res);
}

/** .. method:: stricmp(string1: str, string2: str) -> int

  Does string case-insensitive comparison.  Zero is returned
  on a match.

  -* sqlite3_stricmp
*/
static PyObject *
apsw_stricmp(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *string1 = NULL, *string2 = NULL;
  int res;

  {
    Apsw_stricmp_CHECK;
    ARG_PROLOG(2, Apsw_stricmp_KWNAMES);
    ARG_MANDATORY ARG_str(string1);
    ARG_MANDATORY ARG_str(string2);
    ARG_EPILOG(NULL, Apsw_stricmp_USAGE, );
  }

  res = sqlite3_stricmp(string1, string2);

  return PyLong_FromLong(res);
}

/** .. method:: strnicmp(string1: str, string2: str, count: int) -> int

  Does string case-insensitive comparison.  Zero is returned
  on a match.

  -* sqlite3_strnicmp
*/
static PyObject *
apsw_strnicmp(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *string1 = NULL, *string2 = NULL;
  int count, res;

  {
    Apsw_strnicmp_CHECK;
    ARG_PROLOG(3, Apsw_strnicmp_KWNAMES);
    ARG_MANDATORY ARG_str(string1);
    ARG_MANDATORY ARG_str(string2);
    ARG_MANDATORY ARG_int(count);
    ARG_EPILOG(NULL, Apsw_strnicmp_USAGE, );
  }

  res = sqlite3_strnicmp(string1, string2, count);

  return PyLong_FromLong(res);
}

/** .. method:: set_default_vfs(name: str) -> None

 Sets the default vfs to *name* which must be an existing vfs.
 See :meth:`vfs_names`.

 -* sqlite3_vfs_register sqlite3_vfs_find
*/
static PyObject *
apsw_set_default_vfs(PyObject *Py_UNUSED(module), PyObject *const *fast_args, Py_ssize_t fast_nargs,
                     PyObject *fast_kwnames)
{
  const char *name;
  sqlite3_vfs *vfs;
  int res;

  {
    Apsw_set_default_vfs_CHECK;
    ARG_PROLOG(1, Apsw_set_default_vfs_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_EPILOG(NULL, Apsw_set_default_vfs_USAGE, );
  }

  vfs = sqlite3_vfs_find(name);
  if (!vfs)
    return PyErr_Format(PyExc_ValueError, "vfs named \"%s\" not known", name);
  res = sqlite3_vfs_register(vfs, 1);
  SET_EXC(res, NULL);
  if (PyErr_Occurred())
    return NULL;
  Py_RETURN_NONE;
}

/** .. method:: unregister_vfs(name: str) -> None

 Unregisters the named vfs.  See :meth:`vfs_names`.

 -* sqlite3_vfs_unregister sqlite3_vfs_find
*/
static PyObject *
apsw_unregister_vfs(PyObject *Py_UNUSED(module), PyObject *const *fast_args, Py_ssize_t fast_nargs,
                    PyObject *fast_kwnames)
{
  const char *name;
  sqlite3_vfs *vfs;
  int res;

  {
    Apsw_unregister_vfs_CHECK;
    ARG_PROLOG(1, Apsw_unregister_vfs_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_EPILOG(NULL, Apsw_unregister_vfs_USAGE, );
  }

  vfs = sqlite3_vfs_find(name);
  if (!vfs)
    return PyErr_Format(PyExc_ValueError, "vfs named \"%s\" not known", name);
  res = sqlite3_vfs_unregister(vfs);
  SET_EXC(res, NULL);
  if (PyErr_Occurred())
    return NULL;
  Py_RETURN_NONE;
}

/** .. method:: sleep(milliseconds: int) -> int

  Sleep for at least the number of `milliseconds`, returning how many
  milliseconds were requested from the operating system.

 -* sqlite3_sleep
*/
static PyObject *
apsw_sleep(PyObject *Py_UNUSED(module), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int milliseconds, res;
  {
    Apsw_sleep_CHECK;
    ARG_PROLOG(1, Apsw_sleep_KWNAMES);
    ARG_MANDATORY ARG_int(milliseconds);
    ARG_EPILOG(NULL, Apsw_sleep_USAGE, );
  }

  /* https://sqlite.org/forum/forumpost/5a95013827 */
  if (milliseconds < 0)
    milliseconds = 0;

  res = sqlite3_sleep(milliseconds);
  return PyLong_FromLong(res);
}

/** .. method:: allow_missing_dict_bindings(value: bool) -> bool

  Changes how missing bindings are handled when using a :class:`dict`.
  Historically missing bindings were treated as *None*.  It was
  anticipated that dict bindings would be used when there were lots
  of columns, so having missing ones defaulting to *None* was
  convenient.

  Unfortunately this also has the side effect of not catching typos
  and similar issues.

  APSW 3.41.0.0 changed the default so that missing dict entries
  will result in an exception.  Call this with *True* to restore
  the earlier behaviour, and *False* to have an exception.

  The previous value is returned.
*/
static PyObject *
apsw_allow_missing_dict_bindings(PyObject *Py_UNUSED(module), PyObject *const *fast_args, Py_ssize_t fast_nargs,
                                 PyObject *fast_kwnames)
{
  int curval = allow_missing_dict_bindings;
  int value;
  {
    Apsw_allow_missing_dict_bindings_CHECK;
    ARG_PROLOG(1, Apsw_allow_missing_dict_bindings_KWNAMES);
    ARG_MANDATORY ARG_bool(value);
    ARG_EPILOG(NULL, Apsw_allow_missing_dict_bindings_USAGE, );
  }
  allow_missing_dict_bindings = value;
  if (curval)
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

static PyObject *
apsw_getattr(PyObject *Py_UNUSED(module), PyObject *name)
{
  PyObject *shellmodule = NULL, *res = NULL;
#undef PyUnicode_AsUTF8
  /* we can't do this because it messes up the import machinery */
  const char *cname = PyUnicode_AsUTF8(name);
#include "faultinject.h"

  if (!cname)
    return NULL;

  if (strcmp(cname, "Shell") && strcmp(cname, "main"))
    return PyErr_Format(PyExc_AttributeError, "Unknown apsw attribute %R", name);

  shellmodule = PyImport_ImportModule("apsw.shell");
  if (shellmodule)
    res = PyObject_GetAttr(shellmodule, name);
  Py_XDECREF(shellmodule);
  return res;
}

static PyMethodDef module_methods[] = {
  { "sqlite3_sourceid", (PyCFunction)get_sqlite3_sourceid, METH_NOARGS, Apsw_sqlite3_sourceid_DOC },
  { "sqlite_lib_version", (PyCFunction)get_sqlite_version, METH_NOARGS, Apsw_sqlite_lib_version_DOC },
  { "apsw_version", (PyCFunction)get_apsw_version, METH_NOARGS, Apsw_apsw_version_DOC },
  { "vfs_names", (PyCFunction)vfs_names, METH_NOARGS, Apsw_vfs_names_DOC },
  { "vfs_details", (PyCFunction)vfs_details, METH_NOARGS, Apsw_vfs_details_DOC },
  { "enable_shared_cache", (PyCFunction)enable_shared_cache, METH_FASTCALL | METH_KEYWORDS,
    Apsw_enable_shared_cache_DOC },
  { "initialize", (PyCFunction)initialize, METH_NOARGS, Apsw_initialize_DOC },
  { "shutdown", (PyCFunction)sqliteshutdown, METH_NOARGS, Apsw_shutdown_DOC },
  { "format_sql_value", (PyCFunction)formatsqlvalue, METH_O, Apsw_format_sql_value_DOC },
  { "config", (PyCFunction)apsw_config, METH_VARARGS, Apsw_config_DOC },
  { "log", (PyCFunction)apsw_log, METH_FASTCALL | METH_KEYWORDS, Apsw_log_DOC },
  { "memory_used", (PyCFunction)memory_used, METH_NOARGS, Apsw_memory_used_DOC },
  { "memory_high_water", (PyCFunction)memory_high_water, METH_FASTCALL | METH_KEYWORDS, Apsw_memory_high_water_DOC },
  { "status", (PyCFunction)status, METH_FASTCALL | METH_KEYWORDS, Apsw_status_DOC },
  { "soft_heap_limit", (PyCFunction)soft_heap_limit, METH_FASTCALL | METH_KEYWORDS, Apsw_soft_heap_limit_DOC },
  { "hard_heap_limit", (PyCFunction)apsw_hard_heap_limit, METH_FASTCALL | METH_KEYWORDS, Apsw_hard_heap_limit_DOC },
  { "release_memory", (PyCFunction)release_memory, METH_FASTCALL | METH_KEYWORDS, Apsw_release_memory_DOC },
  { "randomness", (PyCFunction)randomness, METH_FASTCALL | METH_KEYWORDS, Apsw_randomness_DOC },
  { "exception_for", (PyCFunction)get_apsw_exception_for, METH_FASTCALL | METH_KEYWORDS, Apsw_exception_for_DOC },
  { "complete", (PyCFunction)apswcomplete, METH_FASTCALL | METH_KEYWORDS, Apsw_complete_DOC },
  { "strlike", (PyCFunction)apsw_strlike, METH_FASTCALL | METH_KEYWORDS, Apsw_strlike_DOC },
  { "strglob", (PyCFunction)apsw_strglob, METH_FASTCALL | METH_KEYWORDS, Apsw_strglob_DOC },
  { "stricmp", (PyCFunction)apsw_stricmp, METH_FASTCALL | METH_KEYWORDS, Apsw_stricmp_DOC },
  { "strnicmp", (PyCFunction)apsw_strnicmp, METH_FASTCALL | METH_KEYWORDS, Apsw_strnicmp_DOC },
  { "set_default_vfs", (PyCFunction)apsw_set_default_vfs, METH_FASTCALL | METH_KEYWORDS, Apsw_set_default_vfs_DOC },
  { "unregister_vfs", (PyCFunction)apsw_unregister_vfs, METH_FASTCALL | METH_KEYWORDS, Apsw_unregister_vfs_DOC },
  { "allow_missing_dict_bindings", (PyCFunction)apsw_allow_missing_dict_bindings, METH_FASTCALL | METH_KEYWORDS,
    Apsw_allow_missing_dict_bindings_DOC },
#if defined(APSW_FAULT_INJECT) || defined(APSW_DEBUG)
  { "_fini", (PyCFunction)apsw_fini, METH_NOARGS, "Frees all caches and recycle lists" },
#endif
#ifdef __SANITIZE_ADDRESS__
  { "leak_check", (PyCFunction)apsw_leak_check, METH_NOARGS, "Runs sanitizer leak check now" },
#endif
#ifdef APSW_FORK_CHECKER
  { "fork_checker", (PyCFunction)apsw_fork_checker, METH_NOARGS, Apsw_fork_checker_DOC },
#endif
  { "__getattr__", (PyCFunction)apsw_getattr, METH_O, "module getattr" },
  { "connections", (PyCFunction)apsw_connections, METH_NOARGS, Apsw_connections_DOC },
  { "sleep", (PyCFunction)apsw_sleep, METH_FASTCALL | METH_KEYWORDS, Apsw_sleep_DOC },
#ifndef APSW_OMIT_OLD_NAMES
  { Apsw_sqlite_lib_version_OLDNAME, (PyCFunction)get_sqlite_version, METH_NOARGS, Apsw_sqlite_lib_version_OLDDOC },
  { Apsw_apsw_version_OLDNAME, (PyCFunction)get_apsw_version, METH_NOARGS, Apsw_apsw_version_OLDDOC },
  { Apsw_vfs_names_OLDNAME, (PyCFunction)vfs_names, METH_NOARGS, Apsw_vfs_names_OLDDOC },
  { Apsw_enable_shared_cache_OLDNAME, (PyCFunction)enable_shared_cache, METH_FASTCALL | METH_KEYWORDS,
    Apsw_enable_shared_cache_OLDDOC },
  { Apsw_memory_used_OLDNAME, (PyCFunction)memory_used, METH_NOARGS, Apsw_memory_used_OLDDOC },
  { Apsw_memory_high_water_OLDNAME, (PyCFunction)memory_high_water, METH_FASTCALL | METH_KEYWORDS,
    Apsw_memory_high_water_OLDDOC },
  { Apsw_soft_heap_limit_OLDNAME, (PyCFunction)soft_heap_limit, METH_FASTCALL | METH_KEYWORDS,
    Apsw_soft_heap_limit_OLDDOC },
  { Apsw_release_memory_OLDNAME, (PyCFunction)release_memory, METH_FASTCALL | METH_KEYWORDS,
    Apsw_release_memory_OLDDOC },
  { Apsw_exception_for_OLDNAME, (PyCFunction)get_apsw_exception_for, METH_FASTCALL | METH_KEYWORDS,
    Apsw_exception_for_OLDDOC },
#endif
  { 0, 0, 0, 0 } /* Sentinel */
};

static struct PyModuleDef apswmoduledef = { PyModuleDef_HEAD_INIT, "apsw", NULL, -1, module_methods, 0, 0, 0, 0 };

PyMODINIT_FUNC
PyInit_apsw(void)
{
  PyObject *m = NULL;
  PyObject *hooks;

  assert(sizeof(int) == 4);       /* we expect 32 bit ints */
  assert(sizeof(long long) == 8); /* we expect 64 bit long long */

  /* Check SQLite was compiled with thread safety */
  if (!sqlite3_threadsafe())
  {
    PyErr_Format(PyExc_EnvironmentError, "SQLite was compiled without thread safety and cannot be used.");
    goto fail;
  }

  if (PyType_Ready(&ConnectionType) < 0 || PyType_Ready(&APSWCursorType) < 0 || PyType_Ready(&ZeroBlobBindType) < 0
      || PyType_Ready(&APSWBlobType) < 0 || PyType_Ready(&APSWVFSType) < 0 || PyType_Ready(&APSWVFSFileType) < 0
      || PyType_Ready(&apswfcntl_pragma_Type) < 0 || PyType_Ready(&APSWURIFilenameType) < 0
      || PyType_Ready(&FunctionCBInfoType) < 0 || PyType_Ready(&APSWBackupType) < 0
      || PyType_Ready(&SqliteIndexInfoType) < 0 || PyType_Ready(&apsw_no_change_object) < 0
      || PyType_Ready(&APSWFTS5TokenizerType) < 0 || PyType_Ready(&APSWFTS5ExtensionAPIType) < 0
      || PyType_Ready(&PyObjectBindType) < 0)
    goto fail;

  /* PyStructSequence_NewType is broken in some Pythons
      https://github.com/python/cpython/issues/72895
    You also can't call InitType2 more than once otherwise
    internal errors are raised based on looking at the
    refcount.
  */
  if (Py_REFCNT(&apsw_unraisable_info_type) == 0)
    if (PyStructSequence_InitType2(&apsw_unraisable_info_type, &apsw_unraisable_info))
      goto fail;

  m = apswmodule = PyModule_Create2(&apswmoduledef, PYTHON_API_VERSION);

  if (m == NULL)
    goto fail;

  the_connections = PyList_New(0);
  if (!the_connections)
    goto fail;

  if (init_exceptions(m))
    goto fail;

  if (init_apsw_strings())
    goto fail;

/* we can't avoid leaks with failures until multi-phase initialisation is done */
#define ADD(name, item)                                                                                                \
  do                                                                                                                   \
  {                                                                                                                    \
    if (PyModule_AddObject(m, #name, (PyObject *)&item))                                                               \
      goto fail;                                                                                                       \
    Py_INCREF(&item);                                                                                                  \
  } while (0)

  ADD(Connection, ConnectionType);
  ADD(Cursor, APSWCursorType);
  ADD(Blob, APSWBlobType);
  ADD(Backup, APSWBackupType);
  ADD(zeroblob, ZeroBlobBindType);
  ADD(VFS, APSWVFSType);
  ADD(VFSFile, APSWVFSFileType);
  ADD(VFSFcntlPragma, apswfcntl_pragma_Type);
  ADD(URIFilename, APSWURIFilenameType);
  ADD(IndexInfo, SqliteIndexInfoType);
  ADD(FTS5Tokenizer, APSWFTS5TokenizerType);
  ADD(FTS5ExtensionApi, APSWFTS5ExtensionAPIType);
  ADD(pyobject, PyObjectBindType);
#undef ADD

  /** .. attribute:: connection_hooks
       :type: list[Callable[[Connection], None]]

       The purpose of the hooks is to allow the easy registration of
       :meth:`functions <Connection.create_scalar_function>`,
       :ref:`virtual tables <virtualtables>` or similar items with
       each :class:`Connection` as it is created. The default value is an empty
       list. Whenever a Connection is created, each item in
       apsw.connection_hooks is invoked with a single parameter being
       the new Connection object. If the hook raises an exception then
       the creation of the Connection fails.
  */
  hooks = PyList_New(0);
  if (!hooks)
    goto fail;
  if (PyModule_AddObject(m, "connection_hooks", hooks))
    goto fail;

  /** .. attribute:: SQLITE_VERSION_NUMBER
    :type: int

    The integer version number of SQLite that APSW was compiled
    against.  For example SQLite 3.44.1 will have the value *3440100*.
    This number may be different than the actual library in use if the
    library is shared and has been updated.  Call
    :meth:`sqlite_lib_version` to get the actual library version.

    */
  if (PyModule_AddIntConstant(m, "SQLITE_VERSION_NUMBER", SQLITE_VERSION_NUMBER))
    goto fail;

  /** .. attribute:: using_amalgamation
      :type: bool

      If True then `SQLite amalgamation
      <https://www.sqlite.org/amalgamation.html>`__ is in
      use (statically compiled into APSW).  Using the amalgamation means
      that SQLite shared libraries are not used and will not affect your
      code.

      */

#ifdef APSW_USE_SQLITE_AMALGAMATION
  if (PyModule_AddObject(m, "using_amalgamation", Py_NewRef(Py_True)))
    goto fail;
#else
  if (PyModule_AddObject(m, "using_amalgamation", Py_NewRef(Py_False)))
    goto fail;
#endif

  /** .. attribute:: no_change
    :type: object

    A sentinel value used to indicate no change in a value when
    used with :meth:`VTCursor.ColumnNoChange` and
    :meth:`VTTable.UpdateChangeRow`
  */

  if (PyModule_AddObject(m, "no_change", Py_NewRef((PyObject *)&apsw_no_change_object)))
    goto fail;

  /* undocumented sentinel to do no bindings */
  if (!apsw_cursor_null_bindings)
    apsw_cursor_null_bindings = PyObject_CallObject((PyObject *)&PyBaseObject_Type, NULL);
  if (!apsw_cursor_null_bindings)
    goto fail;

  if (PyModule_AddObject(m, "_null_bindings", Py_NewRef(apsw_cursor_null_bindings)))
    goto fail;

#ifdef APSW_FAULT_INJECT
  if (PyModule_AddObject(m, "apsw_fault_inject", Py_NewRef(Py_True)))
    goto fail;
#endif

  /**

.. _sqliteconstants:

SQLite constants
================

SQLite has `many constants
<https://sqlite.org/c3ref/constlist.html>`_ used in various
interfaces.  To use a constant such as *SQLITE_OK*, just
use ``apsw.SQLITE_OK``.

The same values can be used in different contexts. For example
*SQLITE_OK* and *SQLITE_CREATE_INDEX* both have a value
of zero. For each group of constants there is also a mapping (dict)
available that you can supply a string to and get the corresponding
numeric value, or supply a numeric value and get the corresponding
string. These can help improve diagnostics/logging, calling other
modules etc. For example::

      apsw.mapping_authorizer_function["SQLITE_READ"] == 20
      apsw.mapping_authorizer_function[20] == "SQLITE_READ"


    */

  if (add_apsw_constants(m))
    goto fail;

  PyModule_AddObject(m, "compile_options", get_compile_options());
  PyModule_AddObject(m, "keywords", get_keywords());

  if (!PyErr_Occurred())
  {
    PyObject *mod = PyImport_ImportModule("collections.abc");
    if (mod)
    {
      collections_abc_Mapping = PyObject_GetAttrString(mod, "Mapping");
      Py_DECREF(mod);
    }
  }

  if (!PyErr_Occurred())
  {
    return m;
  }

fail:
  assert(PyErr_Occurred());
  Py_XDECREF(m);
  return NULL;
}

#ifdef _WIN32
/* This exists because of issue #327 with the Windows compiler
   looking to export this.  It isn't called in my testing */
PyMODINIT_FUNC
PyInit___init__(void)
{
  return PyInit_apsw();
}
#endif

#ifdef APSW_FAULT_INJECT
#include "faultinject.c"
#endif
