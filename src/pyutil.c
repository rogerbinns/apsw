/*
  Cross Python version compatibility and utility code

  See the accompanying LICENSE file.
*/

/* used in calls to AddTraceBackHere where O format takes non-null but
   we often have null so convert to None.  This can't be done as a portable
   macro because v would end up double evaluated */
static PyObject *
OBJ(PyObject *v)
{
  return v ? v : Py_None;
}

/* we clear weakref lists when close is called on a blob/cursor as
   well as when it is deallocated */
#define APSW_CLEAR_WEAKREFS                                                                                            \
  do                                                                                                                   \
  {                                                                                                                    \
    if (self->weakreflist)                                                                                             \
    {                                                                                                                  \
      PyObject_ClearWeakRefs((PyObject *)self);                                                                        \
      self->weakreflist = 0;                                                                                           \
    }                                                                                                                  \
  } while (0)

/* CONVENIENCE FUNCTIONS */

/* decref an array of PyObjects */
static void
Py_DECREF_ARRAY(PyObject *array[], int argc)
{
  int i;
  for (i = 0; i < argc; i++)
    Py_DECREF(array[i]);
}

/* get contiguous buffer */
#undef PyObject_GetBufferContiguous
static int
PyObject_GetBufferContiguous(PyObject *source, Py_buffer *buffer, int flags)
{
#include "faultinject.h"
  /* PyBUF_SIMPLE is C contiguous so no extra contiguous check needed. */
  assert(flags == PyBUF_SIMPLE || flags == (PyBUF_SIMPLE | PyBUF_WRITABLE));
  int res = PyObject_GetBuffer(source, buffer, flags);

  /* but check anyway */
  assert(res != 0 || (res == 0 && PyBuffer_IsContiguous(buffer, 'C')));

  return res;
}


/* several places require contiguous buffer with limit - eg < 2GB (int size) */
#undef PyObject_GetBufferContiguousBounded
static int
PyObject_GetBufferContiguousBounded(PyObject *source, Py_buffer *buffer, int flags, Py_ssize_t size_limit)
{
#include "faultinject.h"
  int res = PyObject_GetBufferContiguous(source, buffer, flags);

  if (res == 0)
  {
    if (buffer->len > size_limit)
    {
      PyErr_Format(PyExc_ValueError, "Object buffer is %zd bytes, but at most %zd can be accepted%s", buffer->len,
                   size_limit,
                   (size_limit == INT32_MAX) ? " (32 bit signed integer accepted by SQLite)" : "");
      PyBuffer_Release(buffer);
      res = -1;
    }
  }
  return res;
}

#undef convertutf8string
/* Convert a NULL terminated UTF-8 string into a Python object.  None
   is returned if NULL is passed in. */
static PyObject *
convertutf8string(const char *str)
{
#include "faultinject.h"
  if (!str)
    Py_RETURN_NONE;

  return PyUnicode_FromStringAndSize(str, strlen(str));
}

#if PY_VERSION_HEX < 0x030d0000
#undef PyLong_AsInt
static int
PyLong_AsInt(PyObject *val)
{
#include "faultinject.h"
  int ival = -1;
  long lval = PyLong_AsLong(val);
  if (!PyErr_Occurred())
  {
    ival = (int)lval;
    if (lval != ival)
    {
      PyErr_Format(PyExc_OverflowError, "%R overflowed C int", val);
      ival = -1;
    }
  }
  return ival;
}
#endif

#if PY_VERSION_HEX < 0x030d0000
#undef PyWeakref_GetRef
static int
PyWeakref_GetRef(PyObject *ref, PyObject **pobj)
{
#include "faultinject.h"
  PyObject *obj = PyWeakref_GetObject(ref);
  if (!obj)
  {
    assert(PyErr_Occurred());
    *pobj = NULL;
    return -1;
  }
  if (Py_IsNone(obj))
  {
    *pobj = NULL;
    return 0;
  }
  *pobj = Py_NewRef(obj);
  return 1;
}
#endif

/* some we made up in the same spirit*/
static void
Py_TpFree(PyObject *o)
{
  Py_TYPE(o)->tp_free(o);
}

static const char *
Py_TypeName(PyObject *o)
{
  return o ? (Py_TYPE(o)->tp_name) : "NULL";
}

/* ::TODO:: rename this to IsBoolStrict */
#undef PyObject_IsTrueStrict
static int
PyObject_IsTrueStrict(PyObject *o)
{
#include "faultinject.h"
  if (!PyBool_Check(o) && !PyLong_Check(o))
  {
    PyErr_Format(PyExc_TypeError, "Expected a bool, not %s", Py_TypeName(o));
    return -1;
  }
  return PyObject_IsTrue(o);
}

/* Python 3.12+ only has exception, earlier has type, value, and
traceback.  Earlier versions also won't set all 3 as a performance
optimisation, or the type might be TypeError while value is a string.
NormalizeException is then used to make sure all 3 are set, the value
is an exception (ie the constructor has been run) etc.  These macros
hide all this.
 */
#if PY_VERSION_HEX < 0x030c0000
#define PY_ERR_FETCH_IF(condition, name)                                                                               \
  PyObject *name##type = NULL, *name = NULL, *name##traceback = NULL;                                                  \
  if (condition)                                                                                                       \
  PyErr_Fetch(&name##type, &name, &name##traceback)

#define PY_ERR_FETCH(name) PY_ERR_FETCH_IF(1, name)

#define PY_ERR_RESTORE(name) PyErr_Restore(name##type, name, name##traceback)

#define PY_ERR_NORMALIZE(name) PyErr_NormalizeException(&name##type, &name, &name##traceback)

#define PY_ERR_CLEAR(name)                                                                                             \
  Py_CLEAR(name##type);                                                                                                \
  Py_CLEAR(name);                                                                                                      \
  Py_CLEAR(name##traceback);

#define PY_ERR_NOT_NULL(name) (name##type || name || name##traceback)

#else
/* Python 3.12+ */
#define PY_ERR_FETCH_IF(condition, name)                                                                               \
  PyObject *name = NULL;                                                                                               \
  if (condition)                                                                                                       \
  name = PyErr_GetRaisedException()

#define PY_ERR_FETCH(name) PY_ERR_FETCH_IF(1, name)

#define PY_ERR_RESTORE(name) PyErr_SetRaisedException(name)

#define PY_ERR_NORMALIZE(name)                                                                                         \
  do                                                                                                                   \
  {                                                                                                                    \
  } while (0)

#define PY_ERR_CLEAR(name) Py_CLEAR(name)

#define PY_ERR_NOT_NULL(name) (name)
#endif

/* if there was an exception coming in and the call to
   `x` results in an exception, then `x` exception is
   chained to the incoming exception.  The type is that
   from `x` though!

   Exception incoming exception
   During the handling of the above, another occurred:
       `x exception`
   */
#if PY_VERSION_HEX < 0x030c0000
#define _chainexcapi(name) _PyErr_ChainExceptions(name##type, name, name##traceback)
#else
#define _chainexcapi(name) _PyErr_ChainExceptions1(name)
#endif
#define CHAIN_EXC_BEGIN                                                                                                \
  do                                                                                                                   \
  {                                                                                                                    \
    PY_ERR_FETCH(chain_exc);                                                                                           \
    do                                                                                                                 \
    {

/* the seemingly spurious first do-while0 is because immediately
   preceding this can be a label in which case the compiler would
   complain that the block didn't end in a statement, so we put a
   pointless one there;
*/
#define CHAIN_EXC_END                                                                                                  \
  do                                                                                                                   \
  {                                                                                                                    \
  } while (0);                                                                                                         \
  }                                                                                                                    \
  while (0)                                                                                                            \
    ;                                                                                                                  \
  if (PY_ERR_NOT_NULL(chain_exc))                                                                                      \
  {                                                                                                                    \
    if (PyErr_Occurred())                                                                                              \
      _chainexcapi(chain_exc);                                                                                         \
    else                                                                                                               \
      PY_ERR_RESTORE(chain_exc);                                                                                       \
  }                                                                                                                    \
  }                                                                                                                    \
  while (0)

#define CHAIN_EXC(x)                                                                                                   \
  CHAIN_EXC_BEGIN x;                                                                                                   \
  CHAIN_EXC_END

/* Some functions can clear the error indicator
   so this keeps it */
#define PRESERVE_EXC(x)                                                                                                \
  do                                                                                                                   \
  {                                                                                                                    \
    PY_ERR_FETCH(preserve_exc);                                                                                        \
    x;                                                                                                                 \
    PY_ERR_RESTORE(preserve_exc);                                                                                      \
  } while (0)

/* See PEP 678 */
static void
PyErr_AddExceptionNoteV(const char *format, ...)
{
  (void)format;
#if PY_VERSION_HEX >= 0x030b0000
#ifndef DISABLE_PyErr_AddExceptionNoteV
  va_list fmt_args;
  va_start(fmt_args, format);

  PyObject *message;
  message = PyUnicode_FromFormatV(format, fmt_args);

  if (message)
  {
    PyObject *nres;
    PY_ERR_FETCH(exc);
    PY_ERR_NORMALIZE(exc);
    PY_ERR_RESTORE(exc);

    PyObject *vargs[] = { NULL, exc, message };
    CHAIN_EXC(nres = PyObject_VectorcallMethod(apst.add_note, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL));
    Py_XDECREF(nres);

    Py_DECREF(message);
  }

  va_end(fmt_args);
#endif
#endif
}

/* Automatically handle coroutines (async) in callbacks

If a callback returns a coroutine, we ship it back to the event loop
(set in a context var) and get the result of that.

*/
static void AddTraceBackHere(const char *filename, int lineno, const char *functionname, const char *localsformat, ...);

/* asyncio.run_coroutine_threadsafe method initialized on first use.
asyncio is expensive to import so wait until it is used */
static PyObject *asyncio_run_coroutine_threadsafe;

/* PyContextVar where the top level caller needs to stash the event loop to use */
static PyObject *async_loop_context_var;

/* timeout parameter to concurrent.futures.Future.result() */
static PyObject *async_timeout_context_var;

/* how coroutine is submitted to loop (callable) */
static PyObject *async_run_from_thread_context_var;

static PyObject *
asyncio_run_coroutine(PyObject *coro, PyObject *loop, PyObject *timeout)
{
  static PyObject *asyncio_run_coroutine_threadsafe;

  if (!asyncio_run_coroutine_threadsafe)
  {
    PyObject *asyncio = PyImport_ImportModule("asyncio");
    if (!asyncio)
    {
      assert(PyErr_Occurred());
      return NULL;
    }
    asyncio_run_coroutine_threadsafe = PyObject_GetAttrString(asyncio, "run_coroutine_threadsafe");
    Py_DECREF(asyncio);
    if (!asyncio_run_coroutine_threadsafe)
      return NULL;
  }

  PyObject *vargs_run[] = { NULL, coro, loop };
  PyObject *future
      = PyObject_Vectorcall(asyncio_run_coroutine_threadsafe, vargs_run + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  if (!future)
  {
    AddTraceBackHere(__FILE__, __LINE__, "asyncio_run_coroutine.submit_async", "{s: O, s: O}", "coroutine", coro,
                     "loop", loop);
    return NULL;
  }

  PyObject *vargs_result[] = { NULL, future, timeout };
  PyObject *result = PyObject_VectorcallMethod(apst.result, vargs_result + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  if (!result)
    AddTraceBackHere(__FILE__, __LINE__, "asyncio_run_coroutine.future_result", "{s: O, s: O, s: O}", "coroutine", coro,
                     "loop", loop, "future", future);
  Py_DECREF(future);
  return result;
}

/* returns 0 on success, something else on error */
static int
run_get_async(PyObject **runner, PyObject **loop, PyObject **timeout, PyObject *coro)
{
  int res;

  if (0 != PyContextVar_Get(async_run_from_thread_context_var, NULL, timeout))
  {
    AddTraceBackHere(__FILE__, __LINE__, "run_get_async.run_from_thread", "{s: O}", "coro", coro);
    return -1;
  }

  res = PyContextVar_Get(async_loop_context_var, NULL, loop);
  if (res == 0 && !*loop)
  {
    PyErr_Format(PyExc_RuntimeError,
                 "A coroutine (async) was passed as a callback to APSW, but apsw.async_loop "
                 "has not been set to the loop to use. See the APSW async documentation for more details.");
    AddTraceBackHere(__FILE__, __LINE__, "run_get_async.loop", "{s: O}", "coro", coro);
    return -1;
  }

  if (0 != PyContextVar_Get(async_timeout_context_var, NULL, timeout))
  {
    AddTraceBackHere(__FILE__, __LINE__, "run_get_async.timeout", "{s: O}", "coro", coro);
    return -1;
  }

  return 0;
}

static PyObject *
run_in_event_loop(PyObject *coro)
{
  assert(coro);

  PyObject *runner = NULL, *loop = NULL, *timeout = NULL, *result = NULL;

  if (run_get_async(&runner, &loop, &timeout, coro))
    goto error;

  if (runner)
  {
    PyObject *vargs_run[] = { NULL, coro, loop, timeout };
    result = PyObject_Vectorcall(runner, vargs_run + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  }
  else
    result = asyncio_run_coroutine(coro, loop, timeout);

  if (result)
  {
    Py_XDECREF(runner);
    Py_DECREF(loop);
    Py_DECREF(timeout);
    return result;
  }

  AddTraceBackHere(__FILE__, __LINE__, "run_in_event_loop.returned_exception", "{s: O, s: O, s: O, s: O}", "coroutine",
                   coro, "loop", loop, "timeout", OBJ(timeout), "runner", OBJ(runner));

error:
  Py_XDECREF(runner);
  Py_XDECREF(loop);
  Py_XDECREF(timeout);
  Py_XDECREF(result);
  return NULL;
}

static PyObject *
PyObject_VectorcallMethod_AutoAsync(PyObject *name, PyObject *const *args, size_t nargsf, PyObject *kwnames)
{
  PyObject *result = PyObject_VectorcallMethod(name, args, nargsf, kwnames);
  if (result && PyCoro_CheckExact(result))
  {
    PyObject *new_result = run_in_event_loop(result);
    Py_DECREF(result);
    result = new_result;
  }
  return result;
}

static PyObject *
PyObject_Vectorcall_AutoAsync(PyObject *callable, PyObject *const *args, size_t nargsf, PyObject *kwnames)
{
  PyObject *result = PyObject_Vectorcall(callable, args, nargsf, kwnames);
  if (result && PyCoro_CheckExact(result))
  {
    PyObject *new_result = run_in_event_loop(result);
    Py_DECREF(result);
    result = new_result;
  }
  return result;
}

static PyObject *
PyObject_VectorcallMethod_NoAsync(PyObject *name, PyObject *const *args, size_t nargsf, PyObject *kwnames)
{
  return PyObject_VectorcallMethod(name, args, nargsf, kwnames);
}

static PyObject *
PyObject_Vectorcall_NoAsync(PyObject *callable, PyObject *const *args, size_t nargsf, PyObject *kwnames)
{
  return PyObject_Vectorcall(callable, args, nargsf, kwnames);
}

#define PyObject_VectorcallMethod PyObject_VectorcallMethod_AutoAsync
#define PyObject_Vectorcall PyObject_Vectorcall_AutoAsync