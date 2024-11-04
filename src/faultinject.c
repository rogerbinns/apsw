#define APSW_FAULT_CLEAR
#include "faultinject.h"

static long long
APSW_FaultInjectControl(const char *faultfunction, const char *filename, const char *funcname, int linenum, const char *args)
{
  PyObject *callable, *res = NULL;
  const char *err_details = NULL;
  long long ficres = 0;
  int suppress = 0;
  int recursion_limit;

  PyGILState_STATE gilstate = PyGILState_Ensure();
  recursion_limit = Py_GetRecursionLimit();
  Py_SetRecursionLimit(recursion_limit + 50);
  PY_ERR_FETCH(exc);

  callable = PySys_GetObject("apsw_fault_inject_control");
  if (!callable || Py_IsNone(callable))
  {
    /* during interpreter shutdown the attribute becomes None */
    static int whined;
    if (!whined && !Py_IsNone(callable))
    {
      whined++;
      err_details = "APSW debug build: missing sys.apsw_fault_inject_control";
    }
    suppress = 1;
    goto errorexit;
  }

  PyObject *key = PyTuple_New(5);
  if (!key)
    goto errorexit;
  PyTuple_SET_ITEM(key, 0, PyUnicode_FromString(faultfunction));
  PyTuple_SET_ITEM(key, 1, PyUnicode_FromString(filename));
  PyTuple_SET_ITEM(key, 2, PyUnicode_FromString(funcname));
  PyTuple_SET_ITEM(key, 3, PyLong_FromLong(linenum));
  PyTuple_SET_ITEM(key, 4, PyUnicode_FromString(args));

  PyObject *vargs[] = {NULL, key};
  res = PyObject_Vectorcall(callable, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_DECREF(vargs[1]);
  if (!res)
  {
    err_details = "Calling sys.apsw_fault_inject_control";
    goto errorexit;
  }

  if (PyLong_Check(res))
  {
    ficres = PyLong_AsLongLong(res);
    if (PyErr_Occurred())
    {
      err_details = "Converting PyLong return from sys.apsw_fault_inject_control";
      goto errorexit;
    }
    goto success;
  }

  if (!PyTuple_Check(res) || 3 != PyTuple_GET_SIZE(res) || !PyLong_Check(PyTuple_GET_ITEM(res, 0)) || !PyUnicode_Check(PyTuple_GET_ITEM(res, 2)))
  {
    err_details = "Expected int or 3 item tuple (int, class, str) from sys.apsw_fault_inject_control";
    goto errorexit;
  }

  ficres = PyLong_AsLongLong(PyTuple_GET_ITEM(res, 0));
  if (PyErr_Occurred())
  {
    err_details = "Converting tuple return int";
    goto errorexit;
  }

  const char *utf8 = PyUnicode_AsUTF8(PyTuple_GET_ITEM(res, 2));
  if (!utf8)
  {
    err_details = "Getting utf8 of tuple return";
    goto errorexit;
  }

  assert(!PyErr_Occurred());
  PY_ERR_CLEAR(exc);
  PyErr_SetString(PyTuple_GET_ITEM(res, 1), utf8);

success:
  if (PY_ERR_NOT_NULL(exc))
    PY_ERR_RESTORE(exc);
  Py_CLEAR(res);
  Py_SetRecursionLimit(recursion_limit);
  PyGILState_Release(gilstate);
  return ficres;

errorexit:
  Py_CLEAR(res);
  PY_ERR_FETCH(exc_errexit);
  if (!suppress)
    fprintf(stderr, "FaultInjectControl ERROR: {\"%s\", \"%s\", \"%s\", %d, \"%s\"}\n", faultfunction, filename, funcname, linenum, args);
  if (err_details)
    fprintf(stderr, "%s\n", err_details);
  if (PY_ERR_NOT_NULL(exc_errexit))
  {
    PY_ERR_NORMALIZE(exc_errexit);
    fprintf(stderr, "\nException value: ");
    PyObject_Print(exc_errexit, stderr, 0);
    fprintf(stderr, "\n");
    PY_ERR_CLEAR(exc_errexit);
  }
  PY_ERR_RESTORE(exc);
  Py_SetRecursionLimit(recursion_limit);
  PyGILState_Release(gilstate);
  return 0x1FACADE;
}

static int
APSW_Should_Fault(const char *name)
{
  PyGILState_STATE gilstate;
  PyObject *res, *callable;
  int callres = 0;

  gilstate = PyGILState_Ensure();

  PY_ERR_FETCH(exc_save);

  callable = PySys_GetObject("apsw_should_fault");
  if (!callable)
  {
    static int whined;
    if (!whined)
    {
      whined++;
      fprintf(stderr, "APSW debug build: missing sys.apsw_should_fault\n");
    }
    goto end;
  }

  PyObject *vargs[] = { NULL,
                        PyUnicode_FromString(name),
#if PY_VERSION_HEX < 0x030c0000
                        PyTuple_Pack(3, OBJ(exc_savetype), OBJ(exc_save), OBJ(exc_savetraceback))
#else
                        PyTuple_Pack(1, OBJ(exc_save))
#endif
  };
  res = PyObject_Vectorcall(callable, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_DECREF(vargs[1]);
  Py_DECREF(vargs[2]);
  if (!res)
    abort();

  assert(PyBool_Check(res));
  assert(Py_IsTrue(res) || Py_IsFalse(res));
  callres = Py_IsTrue(res);
  Py_DECREF(res);

end:
  PY_ERR_RESTORE(exc_save);
  PyGILState_Release(gilstate);
  return callres;
}
