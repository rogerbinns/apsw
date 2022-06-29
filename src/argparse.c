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

/* Doing this here avoids cleanup in the calling function */
static int
argcheck_List_int_int(PyObject *object, void *result)
{
    int i;
    PyObject **output = (PyObject **)result;

    if (!PyList_Check(object))
    {
        PyErr_Format(PyExc_TypeError, "Function argument expected a list");
        return 0;
    }

    if (PySequence_Length(object) != 2)
    {
        PyErr_Format(PyExc_ValueError, "Function argument expected a two item list");
        return 0;
    }

    for (i = 0; i < 2; i++)
    {
        int check;
        PyObject *list_item = PySequence_GetItem(object, i);
        if (!list_item)
            return 0;
        check = PyLong_Check(list_item);
        Py_DECREF(list_item);
        if (!check)
        {
            PyErr_Format(PyExc_TypeError, "Function argument list[int,int] expected int for item %d", i);
            return 0;
        }
    }
    *output = object;
    return 1;
}

static PyTypeObject APSWURIFilenameType;
static int
argcheck_Optional_str_URIFilename(PyObject *object, void *result)
{
    PyObject **output = (PyObject **)result;

    if (object == Py_None || PyUnicode_Check(object) || PyObject_IsInstance(object, (PyObject *)&APSWURIFilenameType))
    {
        *output = object;
        return 1;
    }
    PyErr_Format(PyExc_TypeError, "Function argument expect None | str | apsw.URIFilename");
    return 0;
}

static int
argcheck_pointer(PyObject *object, void *result)
{
    void **output = (void **)result;
    if (!PyLong_Check(object))
    {
        PyErr_Format(PyExc_TypeError, "Function argument expected int (to be used as a pointer)");
        return 0;
    }
    *output = PyLong_AsVoidPtr(object);
    return PyErr_Occurred() ? 0 : 1;
}

static int
argcheck_Optional_Bindings(PyObject *object, void *result)
{
    PyObject **output = (PyObject **)result;
    if (object == Py_None)
    {
        *output = NULL;
        return 1;
    }
    /* PySequence_Check is too strict and rejects things that are
        accepted by PySequence_Fast like sets and generators,
        so everything is accepted */
    *output = object;
    return 1;
}