static int
argcheck_Optional_Callable(PyObject *object, void *result)
{
    PyObject **res = (PyObject **)result;
    if (object == Py_None)
        *res = NULL;
    else if (PyCallable_Check(object))
        *res = object;
    else
    {
        PyErr_Format(PyExc_TypeError, "Function argument expected a Callable or None");
        return 0;
    }
    return 1;
}

/* Standard PyArg_Parse considers anything truthy to be True such as
   non-empty strings, tuples etc.  This is a footgun for args eg:

      method("False")  # considered to be method(True)

   This converter only accepts bool / int (or subclasses)
*/
static int
argcheck_bool(PyObject *object, void *result)
{
    int *res = (int *)result;
    int val;

    if (!PyBool_Check(object) && !PyLong_Check(object))
    {
        PyErr_Format(PyExc_TypeError, "Function argument expected a bool");
        return 0;
    }

    val = PyObject_IsTrue(object);
    switch (val)
    {
    case 0:
    case 1:
        *res = val;
        return 1;
    default:
        return 0;
    }
}