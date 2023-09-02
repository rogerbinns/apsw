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
#define APSW_CLEAR_WEAKREFS                     \
  do                                            \
  {                                             \
    if (self->weakreflist)                      \
    {                                           \
      PyObject_ClearWeakRefs((PyObject *)self); \
      self->weakreflist = 0;                    \
    }                                           \
  } while (0)

/* CONVENIENCE FUNCTIONS */

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

/* similar space to the above but if there was an
   exception coming in and the call to `x` results
   in an exception, then `x` exception is  chained \
   to the incoming exception.  The type is that
   from `x` though!

   Exception incoming exception
   During the handling of the above, another occurred:
      `x exception`
   */
#if PY_VERSION_HEX < 0x030c0000
#define _chainexcapi(a1, a2, a3) _PyErr_ChainExceptions(a1, a2, a3)
#else
#define _chainexcapi(a1, a2, a3) _PyErr_ChainExceptions1(a2)
#endif
#define CHAIN_EXC_BEGIN                \
  do                                   \
  {                                    \
    PyObject *_exc = PyErr_Occurred(); \
    PyObject *_e1, *_e2, *_e3;         \
    if (_exc)                          \
      PyErr_Fetch(&_e1, &_e2, &_e3);   \
    {

#define CHAIN_EXC_END               \
  }                                 \
  if (_exc)                         \
  {                                 \
    if (PyErr_Occurred())           \
      _chainexcapi(_e1, _e2, _e3);  \
    else                            \
      PyErr_Restore(_e1, _e2, _e3); \
  }                                 \
  }                                 \
  while (0)

#define CHAIN_EXC(x) \
  CHAIN_EXC_BEGIN x; \
  CHAIN_EXC_END

/* Some functions can clear the error indicator
   so this keeps it */
#define PRESERVE_EXC(x)            \
  do                               \
  {                                \
    PyObject *_e1, *_e2, *_e3;     \
    PyErr_Fetch(&_e1, &_e2, &_e3); \
                                   \
    x;                             \
                                   \
    PyErr_Restore(_e1, _e2, _e3);  \
  } while (0)
