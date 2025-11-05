
/* The destructor API does a callback on the void * passed in, but we
could have multiple PyObject owners referencing the same array so the
data always has to be duplicated when passed to sqlite3_carray_bind/.
   */

typedef struct
{
  PyObject_HEAD
  /* used for int32/64 & float */
  Py_buffer view;
  int view_active; /* needs a release */
  /* used for text and blob source */
  PyObject *tuple;
  /* used for blob */
  Py_buffer *views;
  /* how many have had PyObject_GetBuffer called and hence need a release */
  size_t views_active;
  /* passed to sqlite */
  void *aData;
  int nData;
  int mFlags;
  /* housekeeping */
  int free_aData;
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
    ARG_MANDATORY ARG_carray(object);
    ARG_OPTIONAL ARG_int64(start);
    ARG_OPTIONAL ARG_int64(stop);
    ARG_OPTIONAL ARG_int(flags);
    ARG_EPILOG(-1, CARRAY_usage, Py_XDECREF(fast_kwnames));
  }
  self->init_was_called = 1;

  if (start < 0)
  {
    PyErr_Format(PyExc_ValueError, "Start %lld is negative", start);
    goto error;
  }

  if (PyTuple_CheckExact(object))
  {
    if (flags >= 0 && flags != SQLITE_CARRAY_TEXT && flags != SQLITE_CARRAY_BLOB)
    {
      PyErr_Format(PyExc_ValueError, "Flags %d is invalid for a tuple", flags);
      goto error;
    }
    Py_ssize_t nitems = PyTuple_GET_SIZE(object);

    if (start > nitems)
    {
      PyErr_Format(PyExc_ValueError, "Start %lld is beyond end of %lld item tuple", start, nitems);
      goto error;
    }

    if (stop < 0)
      stop = nitems;

    if (stop > nitems)
    {
      PyErr_Format(PyExc_ValueError, "Stop %lld is beyond end of %lld item tuple", stop, nitems);
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

    for (size_t i = 0; i < (size_t)nitems; i++)
    {
      PyObject *item = PyTuple_GET_ITEM(object, i + start);
      if (flags < 0)
      {
        if (PyUnicode_CheckExact(item))
          flags = SQLITE_CARRAY_TEXT;
        else if (PyObject_CheckBuffer(item))
          flags = SQLITE_CARRAY_BLOB;
        else
        {
          PyErr_Format(PyExc_TypeError, "Tuple item %lld is not str or binary data but %s", i + start,
                       Py_TypeName(item));
          goto error;
        }
      }
      if (flags == SQLITE_CARRAY_TEXT)
      {
        if (!self->aData)
        {
          self->aData = PyMem_Malloc(sizeof(const char *) * nitems);
          self->free_aData = 1;
          if (!self->aData)
            goto error;
        }

        const char **array = self->aData;
        Py_ssize_t length;
        array[i] = PyUnicode_AsUTF8AndSize(item, &length);
        if (!array[i])
          goto error;
        if ((size_t)length != strlen(array[i]))
        {
          PyErr_Format(PyExc_ValueError, "Tuple item %lld string has embedded nulls and can't be used with carray",
                       i + start);
          goto error;
        }
        continue;
      }

      assert(flags == SQLITE_CARRAY_BLOB);
      if (!self->aData)
      {
        self->aData = PyMem_Malloc(sizeof(struct iovec) * nitems);
        self->free_aData = 1;
        if (!self->aData)
          goto error;
      }

      if (!self->views)
      {
        self->views = PyMem_Calloc(sizeof(Py_buffer), nitems);
        if (!self->views)
          goto error;
      }

      if (PyObject_GetBuffer(item, &self->views[i], PyBUF_SIMPLE))
        goto error;
      self->views_active++;
      assert(self->views_active == i + 1);

      struct iovec *array = self->aData;
      array[i].iov_base = self->views[i].buf;
      array[i].iov_len = self->views[i].len;
    }

    self->tuple = Py_NewRef(object);
    self->mFlags = flags;
    self->nData = nitems;
  }
  else
  {
    if (0 != PyObject_GetBuffer(object, &self->view, PyBUF_FORMAT | PyBUF_ANY_CONTIGUOUS))
      goto error;

    self->view_active = 1;

    if (flags == -1)
    {
      /* try to auto-detect format */
      if (0 == strcmp(self->view.format, "i") || 0 == strcmp(self->view.format, "l")
          || 0 == strcmp(self->view.format, "q"))
      {
        switch (self->view.itemsize)
        {
        case 8:
          flags = SQLITE_CARRAY_INT64;
          break;
        case 4:
          flags = SQLITE_CARRAY_INT32;
          break;
        default:
          PyErr_Format(PyExc_ValueError, "int size %d not supported", self->view.itemsize);
          goto error;
        }
      }
      else if (0 == strcmp(self->view.format, "d") && self->view.itemsize == 8)
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
      PyErr_Format(PyExc_ValueError, "Unsupported flags value %d for numbers", flags);
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

    if ((size_t)start > nitems)
    {
      PyErr_Format(PyExc_ValueError, "Start %lld is beyond end of %lld item array", start, nitems);
      goto error;
    }

    if (stop < 0)
      stop = nitems;

    if ((size_t)stop > nitems)
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
  }
  return 0;

error:
  /* dealloc takes care of cleanup */
  return -1;
}

static void
CArrayBind_dealloc(PyObject *self_)
{

  CArrayBind *self = (CArrayBind *)self_;

  if (self->view_active)
    PyBuffer_Release(&self->view);

  if (self->free_aData)
    PyMem_Free(self->aData);

  for (size_t i = 0; i < self->views_active; i++)
    PyBuffer_Release(&self->views[i]);

  PyMem_Free(self->views);

  Py_XDECREF(self->tuple);

  Py_TpFree(self_);
}

#ifdef APSW_MODIFIED_CARRAY
static void
CArrayBind_bind_destructor(void *pCtx)
{
  Py_DECREF((PyObject *)pCtx);
}
#endif

static PyTypeObject CArrayBindType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.carray",
  .tp_basicsize = sizeof(CArrayBind),
  .tp_flags = Py_TPFLAGS_DEFAULT,
  .tp_init = CArrayBind_init,
  .tp_new = PyType_GenericNew,
  .tp_dealloc = CArrayBind_dealloc,
  .tp_doc = Apsw_carray_DOC,
};