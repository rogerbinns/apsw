
/* The destructor API does a callback on the void * passed in which means
   we can't get back to the PyObject that owns it.  This array is
   used to store the mapping.  Hopefully it can be deleted at some point.  */

typedef struct
{
  void *aData;
  PyObject *owner;
} carray_pyobject_owner;

static carray_pyobject_owner *carray_owner_array = 0;
size_t carray_owner_array_length = 0;

/* returns 0 on success, -1 on failure */
static int
carray_add_owner(void *aData, PyObject *owner)
{
  assert(aData && owner);

  for (size_t i = 0; i < carray_owner_array_length; i++)
  {
    if (!carray_owner_array[i].aData)
    {
      carray_owner_array[i].aData = aData;
      carray_owner_array[i].owner = owner;
      return 0;
    }
  }
  carray_pyobject_owner *new_array
      = realloc(carray_owner_array, sizeof(carray_pyobject_owner) * (carray_owner_array_length + 1));
  if (!new_array)
    return -1;
  carray_owner_array = new_array;
  carray_owner_array[carray_owner_array_length].aData = aData;
  carray_owner_array[carray_owner_array_length].owner = owner;
  carray_owner_array_length++;
  return 0;
}

/* returns the owner and clears the entry - ie can only be called once */
static PyObject *
carray_get_owner(void *aData)
{
  for (size_t i = 0; i < carray_owner_array_length; i++)
  {
    if (carray_owner_array[i].aData == aData)
    {
      PyObject *owner = carray_owner_array[i].owner;

      carray_owner_array[i].aData = 0;
      carray_owner_array[i].owner = 0;
      return owner;
    }
  }
  Py_UNREACHABLE();
}

typedef struct
{
  PyObject_HEAD
  Py_buffer view;
  void *aData;
  int nData;
  int mFlags;
  int init_was_called;
} CArrayBind;

static int
CArrayBind_init(PyObject *self_, PyObject *args, PyObject *kwargs)
{
  CArrayBind *self = (CArrayBind *)self_;
  PyObject *object = NULL;
  int64_t start = 0;
  int64_t stop = -1;
  int flags = -1;

  {
#define CARRAY_kwnames "object", "start", "stop", "flags"
#define CARRAY_usage "carray(object: Buffer, *, start: int = 0, stop: int = -1, flags: int = -1)"

    PREVENT_INIT_MULTIPLE_CALLS;
    ARG_CONVERT_VARARGS_TO_FASTCALL;
    ARG_PROLOG(1, CARRAY_kwnames);
    ARG_MANDATORY ARG_Buffer(object);
    ARG_OPTIONAL ARG_int64(start);
    ARG_OPTIONAL ARG_int64(stop);
    ARG_OPTIONAL ARG_int(flags);
    ARG_EPILOG(-1, CARRAY_usage, Py_XDECREF(fast_kwnames));
  }
  self->init_was_called = 1;

  int res = -1;

  if (start < 0)
  {
    PyErr_Format(PyExc_ValueError, "start is %lld needs to be >=0", start);
    goto error;
  }

  res = PyObject_GetBuffer(object, &self->view, PyBUF_FORMAT | PyBUF_C_CONTIGUOUS);
  if (res != 0)
    goto error;
  if (!PyBuffer_IsContiguous(&self->view, 'C') || self->view.ndim != 1)
  {
    PyErr_Format(PyExc_ValueError, "object is not contiguous scalar array");
    goto error;
  }

  self->aData = self->view.buf;
  self->nData = self->view.len / 8;
  self->mFlags = flags;

  if (carray_add_owner(self->aData, (PyObject *)self))
  {
    PyErr_NoMemory();
    goto error;
  }

  return 0;
error:
  if (res == 0)
    PyBuffer_Release(&self->view);
  return -1;
}

static void
carray_bind_destructor(void *value)
{
  PyGILState_STATE gilstate = PyGILState_Ensure();
  CArrayBind *self = (CArrayBind *)carray_get_owner(value);
  PyBuffer_Release(&self->view);
  /* undo incref in APSWCursor_dobinding */
  Py_DECREF((PyObject *)self);
  PyGILState_Release(gilstate);
}

static PyTypeObject CArrayBindType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.carray",
  .tp_basicsize = sizeof(CArrayBind),
  .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
  .tp_init = CArrayBind_init,
  .tp_new = PyType_GenericNew,
  .tp_doc = Apsw_carray_DOC,
};