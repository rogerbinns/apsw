/*
  Utility macros and functions

  See the accompanying LICENSE file.
*/

/* msvc doesn't support vla, so do it the hard way */
#if defined(_MSC_VER) || defined(__STDC_NO_VLA__)
#define VLA(name, size, type) type *name = alloca(sizeof(type) * (size))
#else
#define VLA(name, size, type) type name[size]
#endif

#define VLA_PYO(name, size) VLA(name, size, PyObject *)

/* use this most of the time where an exception is raised if we can't get the db mutex */
#define DBMUTEX_ENSURE(mutex)                                                                                          \
  do                                                                                                                   \
  {                                                                                                                    \
    if (sqlite3_mutex_try(mutex) != SQLITE_OK)                                                                         \
    {                                                                                                                  \
      make_thread_exception(NULL);                                                                                     \
      return NULL;                                                                                                     \
    }                                                                                                                  \
  } while (0)

#define DBMUTEXES_ENSURE(mutex1, msg1, mutex2, msg2)                                                                   \
  do                                                                                                                   \
  {                                                                                                                    \
    if (sqlite3_mutex_try(mutex1) != SQLITE_OK)                                                                        \
    {                                                                                                                  \
      make_thread_exception(msg1);                                                                                     \
      return NULL;                                                                                                     \
    }                                                                                                                  \
    if (sqlite3_mutex_try(mutex2) != SQLITE_OK)                                                                        \
    {                                                                                                                  \
      sqlite3_mutex_leave(mutex1);                                                                                     \
      make_thread_exception(msg2);                                                                                     \
      return NULL;                                                                                                     \
    }                                                                                                                  \
  } while (0)

/* use this when we have to get the dbmutex - eg in dealloc functions
   - where we busy wait releasing gil until dbmutex is acquired.

  a different thread could be running a sqlite3_step with the GIL
  released and holding the mutex.  when it finishes it will want
  the GIL so it can copy error messages etc, but we are holding the
  GIL.  only after it has copied data into python will it then
  release the db mutex.

   if the fork checker is in use and this object was allocated in one
   process and then freed in the next, it will busy loop forever
   on SQLITE_MISUSE and spamming the unraisable exception hook with
   forking violation */
#define DBMUTEX_FORCE(mutex)                                                                                           \
  do                                                                                                                   \
  {                                                                                                                    \
    while (sqlite3_mutex_try(mutex) != SQLITE_OK)                                                                      \
    {                                                                                                                  \
      Py_BEGIN_ALLOW_THREADS Py_END_ALLOW_THREADS;                                                                     \
    }                                                                                                                  \
  } while (0)

/*
   The default Python PyErr_WriteUnraisable is almost useless, and barely used
   by CPython.  It gives the developer no clue whatsoever where in
   the code it is happening.  It also does funky things to the passed
   in object which can cause the destructor to fire twice.
   Consequently we use our version here.  It makes the traceback
   complete, and then tries the following, going to the next if
   the hook isn't found or returns an error:

   * excepthook of hookobject (if not NULL)
   * unraisablehook of sys module
   * excepthook of sys module
   * PyErr_Display

   If any return an error then then the next one is tried.  When we
   return, any error will be cleared.
*/

/* used for calling sys.unraisablehook */
static PyStructSequence_Field apsw_unraisable_info_fields[]
    = { { "exc_type", "Exception type" },
        { "exc_value", "Execption value, can be None" },
        { "exc_traceback", "Exception traceback, can be None" },
        { "err_msg", "Error message, can be None" },
        { "object", "Object causing the exception, can be None" },
        { 0 } };

static PyStructSequence_Desc apsw_unraisable_info = { .name = "apsw.unraisable_info",
                                                      .doc = "Glue for sys.unraisablehook",
                                                      .n_in_sequence = 5,
                                                      .fields = apsw_unraisable_info_fields };

static PyTypeObject apsw_unraisable_info_type;

static void
apsw_write_unraisable(PyObject *hookobject)
{
  assert(PyErr_Occurred());

  PyObject *excepthook = NULL;
  PyObject *result = NULL;

  /* fill in the rest of the traceback */
#ifdef PYPY_VERSION
  /* do nothing */
#else
  PyFrameObject *prev = NULL, *frame = PyThreadState_GetFrame(PyThreadState_GET());
  while (frame)
  {
    PyTraceBack_Here(frame);
    prev = PyFrame_GetBack(frame);
    Py_DECREF(frame);
    frame = prev;
  }
#endif

  /* Get the exception details */
  PY_ERR_FETCH(exc);
  PY_ERR_NORMALIZE(exc);

  /* tell sqlite3_log */
  if (exc && 0 == Py_EnterRecursiveCall("apsw_write_unraisable forwarding to sqlite3_log"))
  {
    PyObject *message = PyObject_Str(exc);
    const char *utf8 = message ? PyUnicode_AsUTF8(message) : "failed to get string of error";
    PyErr_Clear();
    sqlite3_log(SQLITE_ERROR, "apsw_write_unraisable %s: %s", Py_TYPE(exc)->tp_name, utf8);
    Py_CLEAR(message);
    Py_LeaveRecursiveCall();
  }
  else
    PyErr_Clear(); /* we are already reporting an error so ignore recursive */

  if (hookobject)
  {
    excepthook = PyObject_GetAttr(hookobject, apst.excepthook);
    PyErr_Clear();
    if (excepthook)
    {
#if PY_VERSION_HEX < 0x030c0000
      PyObject *vargs[] = { NULL, OBJ(exctype), OBJ(exc), OBJ(exctraceback) };
#else
      PyObject *vargs[] = { NULL, (PyObject *)Py_TYPE(OBJ(exc)), OBJ(exc), Py_None };
#endif
      result = PyObject_Vectorcall(excepthook, vargs + 1, 3 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
      if (result)
        goto finally;
    }
    Py_CLEAR(excepthook);
  }

  excepthook = PySys_GetObject("unraisablehook");
  if (excepthook)
  {
    Py_INCREF(excepthook); /* borrowed reference from PySys_GetObject so we increment */
    PyErr_Clear();
    PyObject *arg = PyStructSequence_New(&apsw_unraisable_info_type);
    if (arg)
    {
#if PY_VERSION_HEX < 0x030c0000
      PyStructSequence_SetItem(arg, 0, Py_NewRef(OBJ(exctype)));
      PyStructSequence_SetItem(arg, 1, Py_NewRef(OBJ(exc)));
      PyStructSequence_SetItem(arg, 2, Py_NewRef(OBJ(exctraceback)));
#else
      PyStructSequence_SetItem(arg, 0, Py_NewRef((PyObject *)Py_TYPE(OBJ(exc))));
      PyStructSequence_SetItem(arg, 1, Py_NewRef(exc));
#endif
      PyObject *vargs[] = { NULL, arg };
      result = PyObject_Vectorcall(excepthook, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
      Py_DECREF(arg);
      if (result)
        goto finally;
    }
    Py_CLEAR(excepthook);
  }

  excepthook = PySys_GetObject("excepthook");
  if (excepthook)
  {
    Py_INCREF(excepthook); /* borrowed reference from PySys_GetObject so we increment */
    PyErr_Clear();
#if PY_VERSION_HEX < 0x030c0000
    PyObject *vargs[] = { NULL, OBJ(exctype), OBJ(exc), OBJ(exctraceback) };
#else
    PyObject *vargs[] = { NULL, (PyObject *)Py_TYPE(OBJ(exc)), OBJ(exc), Py_None };
#endif

    result = PyObject_Vectorcall(excepthook, vargs + 1, 3 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
    if (result)
      goto finally;
  }

  /* remove any error from callback failure since we'd have to call
     ourselves to raise it! */
  PyErr_Clear();
#if PY_VERSION_HEX < 0x030c0000
  PyErr_Display(exctype, exc, exctraceback);
#else
  PyErr_DisplayException(exc);
#endif

finally:
  Py_XDECREF(excepthook);
  Py_XDECREF(result);
  PY_ERR_CLEAR(exc);
  PyErr_Clear(); /* being paranoid - make sure no errors on return */
}

#undef convert_value_to_pyobject
/* Converts sqlite3_value to PyObject.  Returns a new reference. */
static PyObject *
convert_value_to_pyobject(sqlite3_value *value, int in_constraint_possible, int no_change_possible)
{
#include "faultinject.h"

  int coltype = sqlite3_value_type(value);
  sqlite3_value *in_value;

  if (no_change_possible && sqlite3_value_nochange(value))
    return Py_NewRef((PyObject *)&apsw_no_change_object);

  switch (coltype)
  {
  case SQLITE_INTEGER: {
    sqlite3_int64 val = sqlite3_value_int64(value);
    return PyLong_FromLongLong(val);
  }

  case SQLITE_FLOAT:
    return PyFloat_FromDouble(sqlite3_value_double(value));

  case SQLITE_TEXT:
    assert(sqlite3_value_text(value));
    return PyUnicode_FromStringAndSize((const char *)sqlite3_value_text(value), sqlite3_value_bytes(value));

  default:
  case SQLITE_NULL:
    if (in_constraint_possible && sqlite3_vtab_in_first(value, &in_value) == SQLITE_OK)
    {
      int res;
      PyObject *v = NULL, *set = PySet_New(NULL);
      if (!set)
        return NULL;
      while (in_value)
      {
        v = convert_value_to_pyobject(in_value, 0, 0);
        if (!v || 0 != PySet_Add(set, v))
          goto error;
        Py_CLEAR(v);
        res = sqlite3_vtab_in_next(value, &in_value);
        if (res != SQLITE_DONE && res != SQLITE_OK)
        {
          /* this should use SET_EXC but there is a circular dependency between that
             file and this one, so we punt on this unlikely scenario */
          PyErr_Format(PyExc_ValueError, "Failed in sqlite3_vtab_in_next result %d", res);
          goto error;
        }
      }
      return set;
    error:
      Py_XDECREF(v);
      Py_XDECREF(set);
      return NULL;
    }
    void *pointer = sqlite3_value_pointer(value, PYOBJECT_BIND_TAG);
    if (pointer)
      return Py_NewRef((PyObject *)pointer);
    Py_RETURN_NONE;

  case SQLITE_BLOB:
    return PyBytes_FromStringAndSize(sqlite3_value_blob(value), sqlite3_value_bytes(value));
  }
}

static PyObject *
convert_value_to_pyobject_not_in(sqlite3_value *value)
{
  return convert_value_to_pyobject(value, 0, 0);
}

/* Converts column to PyObject.  Returns a new reference. Almost identical to above
   but we cannot just use sqlite3_column_value and then call the above function as
   SQLite doesn't allow that ("unprotected values") and assertion failure */
#undef convert_column_to_pyobject
static PyObject *
convert_column_to_pyobject(sqlite3_stmt *stmt, int col)
{
#include "faultinject.h"
  int coltype;

  coltype = sqlite3_column_type(stmt, col);

  switch (coltype)
  {
  case SQLITE_INTEGER: {
    sqlite3_int64 val;
    val = sqlite3_column_int64(stmt, col);
    return PyLong_FromLongLong(val);
  }

  case SQLITE_FLOAT: {
    double d;
    d = sqlite3_column_double(stmt, col);
    return PyFloat_FromDouble(d);
  }
  case SQLITE_TEXT: {
    const char *data;
    size_t len;
    data = (const char *)sqlite3_column_text(stmt, col);
    len = sqlite3_column_bytes(stmt, col);
    return PyUnicode_FromStringAndSize(data, len);
  }

  default:
  case SQLITE_NULL: {
    void *pointer;
    pointer = sqlite3_value_pointer(sqlite3_column_value(stmt, col), PYOBJECT_BIND_TAG);
    if (pointer)
      return Py_NewRef((PyObject *)pointer);
    Py_RETURN_NONE;
  }

  case SQLITE_BLOB: {
    const void *data;
    size_t len;
    data = sqlite3_column_blob(stmt, col);
    len = sqlite3_column_bytes(stmt, col);
    return PyBytes_FromStringAndSize(data, len);
  }
  }
}

/* Some macros used for frequent operations */

/* used by Connection */
#define CHECK_CLOSED(connection, e)                                                                                    \
  do                                                                                                                   \
  {                                                                                                                    \
    if (!(connection) || !(connection)->db)                                                                            \
    {                                                                                                                  \
      PyErr_Format(ExcConnectionClosed, "The connection has been closed");                                             \
      return e;                                                                                                        \
    }                                                                                                                  \
  } while (0)

/* used by cursor */
#define CHECK_CURSOR_CLOSED(e)                                                                                         \
  do                                                                                                                   \
  {                                                                                                                    \
    if (!self->connection)                                                                                             \
    {                                                                                                                  \
      PyErr_Format(ExcCursorClosed, "The cursor has been closed");                                                     \
      return e;                                                                                                        \
    }                                                                                                                  \
    else if (!self->connection->db)                                                                                    \
    {                                                                                                                  \
      PyErr_Format(ExcConnectionClosed, "The connection has been closed");                                             \
      return e;                                                                                                        \
    }                                                                                                                  \
  } while (0)

#undef apsw_strdup
/* This adds double nulls on the end - needed if string is a filename
   used near vfs as SQLite puts extra info after the first null */
static char *
apsw_strdup(const char *source)
{
#include "faultinject.h"

  size_t len = strlen(source);
  char *res = PyMem_Calloc(1, len + 3);
  if (res)
  {
    res[len] = res[len + 1] = res[len + 2] = 0;
    PyOS_snprintf(res, len + 1, "%s", source);
  }
  return res;
}

/*

Some SQLite methods can only be called from within callbacks (eg
sqlite3_vtab_config can only be called when in xCreate/xConnect).
These macros help implement that.

For example, to use for Connection and xCreate:

- Add CALL_TRACK(xCreate) in struct Connection

- Add CALL_ENTER(xCreate) at the start of the xCreate callback

- Add CALL_EXIT(xCreate) at the end of the xCreate callback

- Use CALL_CHECK(xCreate) where sqlite3_vtab_config could be called
  to see if we are in an xCreate call

The implementation uses a sentinel on the stack with a magic number on
it.  That way if the call exits without CALL_LEAVE a later check will
assert fail or cause a sanitizer error.

*/

#define CALL_TRACK(name) int *in_call##name

#define CALL_TRACK_INIT(name) self->in_call##name = NULL

#define CALL_ENTER(name)                                                                                               \
  assert(self->in_call##name == NULL || *(self->in_call##name) == MAGIC_##name);                                       \
  int *enter_call_save_##name = self->in_call##name;                                                                   \
  int enter_call_here_##name = MAGIC_##name;                                                                           \
  self->in_call##name = &enter_call_here_##name

#define CALL_LEAVE(name) self->in_call##name = enter_call_save_##name

#define CALL_CHECK(name) (self->in_call##name != NULL)

/* some arbitrary magic numbers for call track */
#define MAGIC_xConnect 0x008295ab
#define MAGIC_xUpdate 0x119306bc