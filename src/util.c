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

#define DBMUTEX_ENSURE_RETURN(check_thread, CONN, RETVAL)                                                              \
  do                                                                                                                   \
  {                                                                                                                    \
    if (check_thread)                                                                                                  \
      assert(IN_WORKER_THREAD(CONN));                                                                                  \
    if (sqlite3_mutex_try((CONN)->dbmutex) != SQLITE_OK)                                                               \
    {                                                                                                                  \
      make_thread_exception(NULL);                                                                                     \
      return (RETVAL);                                                                                                 \
    }                                                                                                                  \
  } while (0)

/* use this most of the time where an exception is raised if we can't get the db mutex */
#define DBMUTEX_ENSURE(CONN) DBMUTEX_ENSURE_RETURN(1, (CONN), NULL)

/* any thread can do it - used mainly for close */
#define DBMUTEX_ENSURE_ANY_THREAD(CONN) DBMUTEX_ENSURE_RETURN(0, (CONN), NULL)

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

#define DBMUTEX_RETRY_2(conn1, conn2, func)                                                                            \
  do                                                                                                                   \
  {                                                                                                                    \
    sqlite3_mutex *mutex_one = NULL;                                                                                   \
    if (conn1)                                                                                                         \
    {                                                                                                                  \
      mutex_one = (conn1)->dbmutex;                                                                                    \
      switch (sqlite3_mutex_try(mutex_one))                                                                            \
      {                                                                                                                \
      case SQLITE_MISUSE:                                                                                              \
        PyErr_SetString(ExcForkingViolation,                                                                           \
                        "SQLite object allocated in one process is being used in another (across a fork)");            \
        return -1;                                                                                                     \
      case SQLITE_BUSY:                                                                                                \
        return apsw_AddPendingCall(func, self);                                                                        \
      case SQLITE_OK:                                                                                                  \
        break;                                                                                                         \
      default:                                                                                                         \
        Py_UNREACHABLE();                                                                                              \
      }                                                                                                                \
    }                                                                                                                  \
    if (conn2)                                                                                                         \
    {                                                                                                                  \
      switch (sqlite3_mutex_try((conn2)->dbmutex))                                                                     \
      {                                                                                                                \
      case SQLITE_MISUSE:                                                                                              \
        PyErr_SetString(ExcForkingViolation,                                                                           \
                        "SQLite object allocated in one process is being used in another (across a fork)");            \
        sqlite3_mutex_leave(mutex_one);                                                                                \
        return -1;                                                                                                     \
      case SQLITE_BUSY:                                                                                                \
        sqlite3_mutex_leave(mutex_one);                                                                                \
        return apsw_AddPendingCall(func, self);                                                                        \
      case SQLITE_OK:                                                                                                  \
        break;                                                                                                         \
      default:                                                                                                         \
        Py_UNREACHABLE();                                                                                              \
      }                                                                                                                \
    }                                                                                                                  \
  } while (0)

#define DBMUTEX_RETRY(connection, func) DBMUTEX_RETRY_2(connection, (Connection *)0, func)

/* Py_AddPendingCall only has 32 slots so if we end up with more than
   that many objects waiting mutex to destruct it returns errors.
   Annoyingly this means having to manage our own list of pending calls.
*/

static size_t pending_call_slots_count = 0;
typedef struct
{
  int (*func)(void *);
  void *arg;
} pending_call_entry;
static pending_call_entry *pending_call_slots = 0;

static int pending_call_registered = 0;

static int
pending_call_callback(void *ignored)
{
  assert(!PyErr_Occurred());

  pending_call_registered = 0;

  size_t ran = 0;

  size_t i;
  for (i = 0; i < pending_call_slots_count; i++)
  {
    if (pending_call_slots[i].func)
    {
      int (*func)(void *) = pending_call_slots[i].func;
      void *arg = pending_call_slots[i].arg;
      pending_call_slots[i].func = 0;
      pending_call_slots[i].arg = 0;
      ran++;
      int res = func(arg);
      assert((res == 0 && !PyErr_Occurred()) || (res != 0 && PyErr_Occurred()));
      if (PyErr_Occurred())
        apsw_write_unraisable(NULL);
      (void)res;
    }
    else
    {
      assert(pending_call_slots[i].func == 0);
      assert(pending_call_slots[i].arg == 0);
    }
  }

  assert(!PyErr_Occurred());
  return 0;
}

static int
apsw_AddPendingCall(int (*func)(void *), void *arg)
{
  assert(func);
  assert(arg);

  int res = 0;

  size_t i;
  for (i = 0; i < pending_call_slots_count; i++)
    if (!pending_call_slots[i].func)
      break;
  if (i == pending_call_slots_count)
  {
    pending_call_entry *pending_call_slots_new
        = PyMem_Resize(pending_call_slots, pending_call_entry, pending_call_slots_count + 1);
    if (!pending_call_slots_new)
    {
      res = -1;
      goto exit;
    }
    pending_call_slots_count++;
  }
  pending_call_slots[i].func = func;
  pending_call_slots[i].arg = arg;

  if (!pending_call_registered)
  {
    res = Py_AddPendingCall(pending_call_callback, NULL);
    if (res != 0)
      PyErr_SetString(PyExc_RuntimeError,
                      "APSW: Py_AddPendingCall failed which means destructors will not be able to complete");
    else
      pending_call_registered = 1;
  }
exit:
  assert((res == 0 && !PyErr_Occurred()) || (res != 0 && PyErr_Occurred()));
  return res;
}

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
        { "exc_value", "Exception value, can be None" },
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
  PyFrameObject *prev = NULL, *frame = PyThreadState_GetFrame(PyThreadState_GET());
  while (frame)
  {
    PyTraceBack_Here(frame);
    prev = PyFrame_GetBack(frame);
    Py_DECREF(frame);
    frame = prev;
  }

  /* Get the exception details - we have to use the legacy deprecated API because
     unraisable hook structure has the three separate exception fields despite
     Python 3.12 moving to a singe value */
  PyObject *exc_type = NULL, *exc_value = NULL, *exc_traceback = NULL;
  PyErr_Fetch(&exc_type, &exc_value, &exc_traceback);
  PyErr_NormalizeException(&exc_type, &exc_value, &exc_traceback);

  /* tell sqlite3_log */
  if (exc_value && 0 == Py_EnterRecursiveCall("apsw_write_unraisable forwarding to sqlite3_log"))
  {
    PyObject *message = PyObject_Str(exc_value);
    const char *utf8 = message ? PyUnicode_AsUTF8(message) : "failed to get string of error";
    PyErr_Clear();
    sqlite3_log(SQLITE_ERROR, "apsw_write_unraisable %s: %s", Py_TypeName(OBJ(exc_value)), utf8);
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
      PyObject *vargs[] = { NULL, OBJ(exc_type), OBJ(exc_value), OBJ(exc_traceback) };
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
      PyStructSequence_SetItem(arg, 0, Py_NewRef(OBJ(exc_type)));
      PyStructSequence_SetItem(arg, 1, Py_NewRef(OBJ(exc_value)));
      PyStructSequence_SetItem(arg, 2, Py_NewRef(OBJ(exc_traceback)));
      PyStructSequence_SetItem(arg, 3, Py_NewRef(Py_None));
      PyStructSequence_SetItem(arg, 4, Py_NewRef(Py_None));
      PyObject *vargs[] = { NULL, arg };
      result = PyObject_Vectorcall(excepthook, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
      Py_DECREF(arg);
      if (result)
        goto finally;
    }
    else
      PyErr_Clear();
    Py_CLEAR(excepthook);
  }

  excepthook = PySys_GetObject("excepthook");
  if (excepthook)
  {
    Py_INCREF(excepthook); /* borrowed reference from PySys_GetObject so we increment */
    PyErr_Clear();

    PyObject *vargs[] = { NULL, OBJ(exc_type), OBJ(exc_value), OBJ(exc_traceback) };
    result = PyObject_Vectorcall(excepthook, vargs + 1, 3 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
    if (result)
      goto finally;
  }

  /* remove any error from callback failure since we'd have to call
     ourselves to raise it! */
  PyErr_Clear();
#if PY_VERSION_HEX < 0x030c0000
  PyErr_Display(exc_type, exc_value, exc_traceback);
#else
  PyErr_DisplayException(exc_value);
#endif

finally:
  Py_XDECREF(excepthook);
  Py_XDECREF(result);
  Py_XDECREF(exc_type);
  Py_XDECREF(exc_value);
  Py_XDECREF(exc_traceback);
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
    return Py_NewRef(apsw_no_change_object);

  switch (coltype)
  {
  case SQLITE_INTEGER: {
    sqlite3_int64 val = sqlite3_value_int64(value);
    return PyLong_FromLongLong(val);
  }

  case SQLITE_FLOAT:
    return PyFloat_FromDouble(sqlite3_value_double(value));

  case SQLITE_TEXT: {
    const char *data = (const char *)sqlite3_value_text(value);
    if (!data)
      return PyErr_NoMemory();
    return PyUnicode_FromStringAndSize(data, sqlite3_value_bytes(value));
  }

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

  case SQLITE_BLOB: {
    const void *data = sqlite3_value_blob(value);
    int len = sqlite3_value_bytes(value);
    if (!data && len)
      return PyErr_NoMemory();
    return PyBytes_FromStringAndSize(data, len);
  }
  }
}

static PyObject *
convert_value_to_pyobject_not_in(sqlite3_value *value)
{
  return convert_value_to_pyobject(value, 0, 0);
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