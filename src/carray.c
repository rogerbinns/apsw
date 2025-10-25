
/* The destructor API does a callback on the void * passed in, but we
could have multiple PyObject owners referencing the same array so the
data always has to be duplicated when passed to sqlite3_carray_bind/.
   */

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

  if (flags == -1)
  {
    /* try to auto-detect format */
    if (0 == strcmp(self->view.format, "i"))
      flags = SQLITE_CARRAY_INT32;
    else if (0 == strcmp(self->view.format, "l"))
      flags = SQLITE_CARRAY_INT64;
    else if (0 == strcmp(self->view.format, "d"))
      flags = SQLITE_CARRAY_DOUBLE;
    else
    {
      PyErr_Format(PyExc_ValueError, "unable to detect array type from format \"%s\"", self->view.format);
      goto error;
    }
  }

  switch (flags)
  {
  case SQLITE_CARRAY_INT32:
  case SQLITE_CARRAY_INT64:
  case SQLITE_CARRAY_DOUBLE:
    break;
  default:
    PyErr_Format(PyExc_ValueError, "Unsupported flags value %d", flags);
    goto error;
  }

  const unsigned item_size = (flags == SQLITE_CARRAY_INT32) ? 4 : 8;
  if (self->view.len % item_size)
  {
    PyErr_Format(PyExc_ValueError, "Array size %lld bytes is not a multiple of item size %u bytes", self->view.len,
                 item_size);
    goto error;
  }

  size_t nitems = self->view.len / item_size;
  if (start > nitems)
  {
    PyErr_Format(PyExc_ValueError, "Start %lld is beyond end of %lld item array", start, nitems);
    goto error;
  }
  if (stop < 0)
    stop = nitems;
  if (stop > nitems)
  {
    PyErr_Format(PyExc_ValueError, "Stop %lld is beyond end of %lld item array", stop, nitems);
    goto error;
  }

  if (stop < start)
  {
    PyErr_Format(PyExc_ValueError, "Stop %lld is before start %lld", stop, start);
    goto error;
  }

  nitems = stop - start;
  if (!nitems)
  {
    PyErr_Format(PyExc_ValueError, "CARRAY can't work with a zero item array");
    goto error;
  }

  if (nitems >= INT32_MAX)
  {
    PyErr_Format(PyExc_ValueError, "CARRAY supports a maximum of 2 billion items");
    goto error;
  }

  self->aData = ((uint8_t *)self->view.buf) + (start * item_size);
  self->nData = nitems;
  self->mFlags = flags;
  return 0;

error:
  if (res == 0)
    PyBuffer_Release(&self->view);
  self->aData = 0;
  return -1;
}

static void
CArrayBind_dealloc(PyObject *self_)
{

  CArrayBind *self = (CArrayBind *)self_;

  if (self->aData)
  {
    PyBuffer_Release(&self->view);
    self->aData = 0;
  }
  Py_TpFree(self_);
}

static PyTypeObject CArrayBindType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.carray",
  .tp_basicsize = sizeof(CArrayBind),
  .tp_flags = Py_TPFLAGS_DEFAULT,
  .tp_init = CArrayBind_init,
  .tp_new = PyType_GenericNew,
  .tp_dealloc = CArrayBind_dealloc,
  .tp_doc = Apsw_carray_DOC,
};