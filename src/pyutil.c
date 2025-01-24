/*
  Cross Python version compatibility and utility code

  See the accompanying LICENSE file.
*/

/* Various routines added in python 3.10 */
#if PY_VERSION_HEX < 0x030a0000
static PyObject *
Py_NewRef(PyObject *o)
{
  Py_INCREF(o);
  return o;
}

static int
Py_Is(const PyObject *left, const PyObject *right)
{
  return left == right;
}

static int
Py_IsTrue(const PyObject *val)
{
  return Py_Is(val, Py_True);
}

static int
Py_IsFalse(const PyObject *val)
{
  return Py_Is(val, Py_False);
}

static int
Py_IsNone(const PyObject *val)
{
  return Py_Is(val, Py_None);
}

#endif

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

/* ::TODO:: PyBUF_SIMPLE is C contiguous so this is not necessary */
/* get buffer and check it is contiguous */
#undef PyObject_GetBufferContiguous
static int
PyObject_GetBufferContiguous(PyObject *source, Py_buffer *buffer, int flags)
{
#include "faultinject.h"
  int res = PyObject_GetBuffer(source, buffer, flags);
  if (res == 0 && !PyBuffer_IsContiguous(buffer, 'C'))
  {
    PyBuffer_Release(buffer);
    PyErr_Format(PyExc_TypeError, "Expected a contiguous buffer");
    res = -1;
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