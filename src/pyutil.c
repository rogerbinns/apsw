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

#undef Call_PythonMethod
/* Calls the named method of object with the provided args */
static PyObject *
Call_PythonMethod(PyObject *obj, const char *methodname, int mandatory, PyObject *args)
{
#include "faultinject.h"
  PyObject *method = NULL;
  PyObject *res = NULL;

  /* we may be called when there is already an error.  eg if you return an error in
     a cursor method, then SQLite calls vtabClose which calls us.  We don't want to
     clear pre-existing errors, but we do want to clear ones when the function doesn't
     exist but is optional */
  PyObject *etype = NULL, *evalue = NULL, *etraceback = NULL;
  void *pyerralreadyoccurred = PyErr_Occurred();
  if (pyerralreadyoccurred)
    PyErr_Fetch(&etype, &evalue, &etraceback);

  /* we should only be called with ascii methodnames so no need to do
   character set conversions etc */
  method = PyObject_GetAttrString(obj, methodname);

  assert(method != obj);
  if (!method)
  {
    if (!mandatory)
    {
      /* pretend method existed and returned None */
      PyErr_Clear();
      res = Py_NewRef(Py_None);
    }
    goto finally;
  }

  assert(!PyErr_Occurred());

  res = PyObject_CallObject(method, args);
  if (!pyerralreadyoccurred && PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "Call_PythonMethod", "{s: s, s: i, s: O, s: O}",
                     "methodname", methodname,
                     "mandatory", mandatory,
                     "args", OBJ(args),
                     "method", OBJ(method));

finally:
  if (pyerralreadyoccurred)
    PyErr_Restore(etype, evalue, etraceback);
  Py_XDECREF(method);
  return res;
}

#undef Call_PythonMethodV
static PyObject *
Call_PythonMethodV(PyObject *obj, const char *methodname, int mandatory, const char *format, ...)
{
#include "faultinject.h"
  PyObject *args = NULL, *result = NULL;
  va_list list;
  va_start(list, format);
  args = Py_VaBuildValue(format, list);
  va_end(list);

  if (args)
    result = Call_PythonMethod(obj, methodname, mandatory, args);

  Py_XDECREF(args);
  return result;
}

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

/*

This is necessary for calling into CPython for methods that cannot
handle an existing exception, such as PyObject_Call*.  If those
methods are called with an existing exception then when they return
CPython will raise:

SystemError: _PyEval_EvalFrameDefault returned a result with an
exception set

If there was an exception coming in, then this macro will turn any
exception in `x` into an unraisable exception and requires the
parameters for AddTraceBackHere to be provided.

*/

#define PY_EXC_HANDLE(x, func_name, locals_dict_spec, ...)                              \
  do                                                                                    \
  {                                                                                     \
    PyObject *e_type = NULL, *e_value = NULL, *e_traceback = NULL;                      \
    PyErr_Fetch(&e_type, &e_value, &e_traceback);                                       \
                                                                                        \
    x;                                                                                  \
                                                                                        \
    if ((e_type || e_value || e_traceback))                                             \
    {                                                                                   \
      if (PyErr_Occurred())                                                             \
      {                                                                                 \
        /* report the new error as unraisable because of the existing error */          \
        AddTraceBackHere(__FILE__, __LINE__, func_name, locals_dict_spec, __VA_ARGS__); \
        apsw_write_unraisable(NULL);                                                    \
      } /* put the old error back */                                                    \
      PyErr_Restore(e_type, e_value, e_traceback);                                      \
    }                                                                                   \
                                                                                        \
  } while (0)

/* similar space to the above but if there was an
   exception coming in and the call to `x` results
   in an exception, then the incoming exception
   is chained to the `x` exception so you'd get

   Exception in `x`
     which happened while handling
        incoming exception
   */
#if PY_VERSION_HEX < 0x030c0000
#define _chainexcapi(a1,a2,a3) _PyErr_ChainExceptions(a1,a2,a3)
#else
#define _chainexcapi(a1,a2,a3) _PyErr_ChainExceptions1(a2)
#endif
#define CHAIN_EXC(x)                           \
  do                                           \
  {                                            \
    PyObject *_exc = PyErr_Occurred();         \
    PyObject *_e1, *_e2, *_e3;                 \
    if (_exc)                                  \
      PyErr_Fetch(&_e1, &_e2, &_e3);           \
    {                                          \
      x;                                       \
    }                                          \
    if (_exc)                                  \
    {                                          \
      if (PyErr_Occurred())                    \
        _chainexcapi(_e1, _e2, _e3);           \
      else                                     \
        PyErr_Restore(_e1, _e2, _e3);          \
    }                                          \
  } while (0)

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
