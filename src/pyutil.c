/*
  Cross Python version compatibility code

  See the accompanying LICENSE file.
*/

/* How to make a string from a utf8 constant */
#define MAKESTR PyUnicode_FromString

#define APSW_PYTYPE_INIT PyVarObject_HEAD_INIT(NULL, 0)

#define APSW_PYTYPE_VERSION , 0

#define APSW_Unicode_Return(r)                 \
  do                                           \
  {                                            \
    int i__ = (r) ? (PyUnicode_READY(r)) : -1; \
    if (i__ != 0)                              \
      Py_CLEAR(r);                             \
    return (r);                                \
  } while (0)


#define PyIntLong_Check PyLong_Check
#define PyIntLong_AsLong PyLong_AsLong
#define PyInt_FromLong PyLong_FromLong
#define PyIntLong_AsLongLong PyLong_AsLongLong
#define PyObject_Unicode PyObject_Str

#define PyIntLong_FromLongLong(val) PyLong_FromLongLong(val)

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

/* Calls the named method of object with the provided args */
static PyObject *
Call_PythonMethod(PyObject *obj, const char *methodname, int mandatory, PyObject *args)
{
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
      res = Py_None;
      Py_INCREF(res);
    }
    goto finally;
  }

  res = PyEval_CallObject(method, args);
  if (!pyerralreadyoccurred && PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "Call_PythonMethod", "{s: s, s: i, s: O, s: O}",
                     "methodname", methodname,
                     "mandatory", mandatory,
                     "args", args,
                     "method", method);

finally:
  if (pyerralreadyoccurred)
    PyErr_Restore(etype, evalue, etraceback);
  Py_XDECREF(method);
  return res;
}

static PyObject *
Call_PythonMethodV(PyObject *obj, const char *methodname, int mandatory, const char *format, ...)
{
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

#define converttobytes PyBytes_FromStringAndSize

/* Convert a pointer and size UTF-8 string into a Python object.
   Pointer must be non-NULL.  New behaviour in 3.3.8 - always return
   Unicode strings
*/
static PyObject *
convertutf8stringsize(const char *str, Py_ssize_t size)
{
  assert(str);
  assert(size >= 0);

  {
    PyObject *r = PyUnicode_DecodeUTF8(str, size, NULL);
    APSW_Unicode_Return(r);
  }
}

/* Convert a NULL terminated UTF-8 string into a Python object.  None
   is returned if NULL is passed in. */
static PyObject *
convertutf8string(const char *str)
{
  if (!str)
    Py_RETURN_NONE;

  return convertutf8stringsize(str, strlen(str));
}

/* Returns a PyBytes/String encoded in UTF8 - new reference.
   Use PyBytes/String_AsString on the return value to get a
   const char * to utf8 bytes */
static PyObject *
getutf8string(PyObject *string)
{
  PyObject *inunicode = NULL;
  PyObject *utf8string = NULL;

  if (PyUnicode_CheckExact(string))
  {
    inunicode = string;
    Py_INCREF(string);
  }

  if (!inunicode)
    inunicode = PyUnicode_FromObject(string);

  if (!inunicode)
    return NULL;

  assert(!PyErr_Occurred());

  utf8string = PyUnicode_AsUTF8String(inunicode);
  Py_DECREF(inunicode);
  return utf8string;
}

/* PyObject_AsReadBuffer was deprecated in Py 3 and removed in 3.10 */
#define READBUFFERVARS Py_buffer py3buffer

/* ::TODO:: take a name for fault injection */
#define compat_PyObjectReadBuffer(o)                        \
  do                                                        \
  {                                                         \
    memset(&py3buffer, 0, sizeof(py3buffer));               \
    buffer = NULL;                                          \
    buflen = 0;                                             \
    asrb = PyObject_GetBuffer(o, &py3buffer, PyBUF_SIMPLE); \
    if (asrb == 0)                                          \
    {                                                       \
      buffer = py3buffer.buf;                               \
      buflen = py3buffer.len;                               \
    }                                                       \
  } while (0)

#define ENDREADBUFFER             \
  do                              \
  {                               \
    PyBuffer_Release(&py3buffer); \
  } while (0)

#define compat_CheckReadBuffer(o) PyObject_CheckBuffer(o)