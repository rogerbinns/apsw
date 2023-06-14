typedef struct
{
    PyObject **result;
    const char *message;
} argcheck_Optional_Callable_param;

static int
argcheck_Optional_Callable(PyObject *object, void *vparam)
{
    argcheck_Optional_Callable_param *param = (argcheck_Optional_Callable_param *)vparam;
    if (Py_IsNone(object))
        *param->result = NULL;
    else if (PyCallable_Check(object))
        *param->result = object;
    else
    {
        PyErr_Format(PyExc_TypeError, "Function argument expected a Callable or None: %s", param->message);
        return 0;
    }
    return 1;
}

/* Standard PyArg_Parse considers anything truthy to be True such as
   non-empty strings, tuples etc.  This is a footgun for args eg:

      method("False")  # considered to be method(True)

   This converter only accepts bool / int (or subclasses)
*/
typedef struct
{
    int *result;
    const char *message;
} argcheck_bool_param;

static int
argcheck_bool(PyObject *object, void *vparam)
{
    argcheck_bool_param *param = (argcheck_bool_param *)vparam;

    int val = PyObject_IsTrueStrict(object);
    switch (val)
    {
    case -1:
        assert(PyErr_Occurred());
        CHAIN_EXC(
            PyErr_Format(PyExc_TypeError, "Function argument expected a bool: %s", param->message););
        return 0;
    default:
        assert(val == 0 || val == 1);
        *param->result = val;
        return 1;
    }
}

typedef struct
{
    PyObject **result;
    const char *message;
} argcheck_Optional_set_param;

static int
argcheck_Optional_set(PyObject *object, void *vparam)
{
    argcheck_Optional_set_param *param = (argcheck_Optional_set_param *)vparam;
    if (Py_IsNone(object))
    {
        *param->result = NULL;
        return 1;
    }
    if (!PySet_Check(object))
    {
        PyErr_Format(PyExc_TypeError, "Function argument expected a set: %s", param->message);
        return 0;
    }
    *param->result = object;
    return 1;
}

typedef struct
{
    PyObject **result;
    const char *message;
} argcheck_List_int_int_param;

/* Doing this here avoids cleanup in the calling function */
static int
argcheck_List_int_int(PyObject *object, void *vparam)
{
    int i;
    argcheck_List_int_int_param *param = (argcheck_List_int_int_param *)vparam;

    if (!PyList_Check(object))
    {
        PyErr_Format(PyExc_TypeError, "Function argument expected a list: %s", param->message);
        return 0;
    }

    if (PyList_Size(object) != 2)
    {
        if (!PyErr_Occurred())
            PyErr_Format(PyExc_ValueError, "Function argument expected a two item list: %s", param->message);
        return 0;
    }

    for (i = 0; i < 2; i++)
    {
        PyObject *list_item = PyList_GetItem(object, i);
        if (!list_item)
            return 0;
        if (!PyLong_Check(list_item))
        {
            PyErr_Format(PyExc_TypeError, "Function argument list[int,int] expected int for item %d: %s", i, param->message);
            return 0;
        }
    }
    *param->result = object;
    return 1;
}

static PyTypeObject APSWURIFilenameType;

typedef struct
{
    PyObject **result;
    const char *message;
} argcheck_Optional_str_URIFilename_param;

static int
argcheck_Optional_str_URIFilename(PyObject *object, void *vparam)
{
    argcheck_Optional_str_URIFilename_param *param = (argcheck_Optional_str_URIFilename_param *)vparam;

    if (Py_IsNone(object) || PyUnicode_Check(object) || PyObject_IsInstance(object, (PyObject *)&APSWURIFilenameType))
    {
        *param->result = object;
        return 1;
    }
    PyErr_Format(PyExc_TypeError, "Function argument expect None | str | apsw.URIFilename: %s", param->message);
    return 0;
}

typedef struct
{
    void **result;
    const char *message;
} argcheck_pointer_param;

static int
argcheck_pointer(PyObject *object, void *vparam)
{
    argcheck_pointer_param *param = (argcheck_pointer_param *)vparam;
    if (!PyLong_Check(object))
    {
        PyErr_Format(PyExc_TypeError, "Function argument expected int (to be used as a pointer): %s", param->message);
        return 0;
    }
    *param->result = PyLong_AsVoidPtr(object);
    return PyErr_Occurred() ? 0 : 1;
}

typedef struct
{
    PyObject **result;
    const char *message;
} argcheck_Optional_Bindings_param;

static int
argcheck_Optional_Bindings(PyObject *object, void *vparam)
{
    argcheck_Optional_Bindings_param *param = (argcheck_Optional_Bindings_param *)vparam;
    if (Py_IsNone(object))
    {
        *param->result = NULL;
        return 1;
    }
    /* PySequence_Check is too strict and rejects things that are
        accepted by PySequence_Fast like sets and generators,
        so everything is accepted */
    *param->result = object;
    return 1;
}