
/* These are Python header files */
#undef PyFrame_New
#include "frameobject.h"
#include "faultinject.h"

/* Add a dummy frame to the traceback so the developer has a better idea of what C code was doing

   @param filename: Use __FILE__ for this - it will be the filename reported in the frame
   @param lineno: Use __LINE__ for this - it will be the line number reported in the frame
   @param functionname: Name of the function reported
   @param localsformat: Format string for Py_BuildValue() that must specify a dictionary or NULL to make
                        an empty dictionary.  An example is "{s:i, s: s}" with the varargs then conforming
      to this format (the corresponding params could be "seven", 7, "foo", "bar"

*/
static void
AddTraceBackHere(const char *filename, int lineno, const char *functionname, const char *localsformat, ...)
{
  /* See the implementation of _PyTraceback_Add for a template of what
     this code should do. That method does everything we need, except
     attaching variables */

  PyObject *localargs = 0, *empty_dict;
  PyCodeObject *code = 0;
  PyFrameObject *frame = 0;
  va_list localargsva;

  va_start(localargsva, localsformat);

  /* we have to save and restore the error indicators otherwise intermediate code has no effect! */
  assert(PyErr_Occurred());
  PY_ERR_FETCH(exc_save);
  empty_dict = PyDict_New();
  if (!empty_dict)
    goto end;

  assert(!localsformat || localsformat[0] == '{');
  localargs = localsformat ? (Py_VaBuildValue((char *)localsformat, localargsva)) : NULL;
  /* this will typically happen due to error in Py_BuildValue, usually
     because NULL was passed to O (PyObject*) format */
  if (PyErr_Occurred())
    goto end;

  /* make the dummy code object */
  code = PyCode_NewEmpty(filename, functionname, lineno);
  if (!code)
    goto end;

  /* make the dummy frame */
  frame = PyFrame_New(PyThreadState_Get(), /* PyThreadState *tstate */
                      code,                /* PyCodeObject *code */
                      empty_dict,          /* PyObject *globals */
                      localargs            /* PyObject *locals */
  );
  if (!frame)
    goto end;

#if PY_VERSION_HEX < 0x030b0000
  frame->f_lineno = lineno;
#endif

end:
  /* try to report errors that happened above */
  if (PyErr_Occurred())
    apsw_write_unraisable(NULL);
  /* add dummy frame to traceback after restoring exception info */
  PY_ERR_RESTORE(exc_save);
  if (frame)
    PyTraceBack_Here(frame);

  va_end(localargsva);
  Py_XDECREF(localargs);
  Py_XDECREF(empty_dict);
  Py_XDECREF(code);
  Py_XDECREF(frame);
}
