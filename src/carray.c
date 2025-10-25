
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

  self->aData = self->view.buf;
  self->nData = self->view.len / 8;
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