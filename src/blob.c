/*
  Blob and ZeroBlob code

  See the accompanying LICENSE file.
*/

/**
.. _blobio:

Blob Input/Output
*****************

A `blob <https://en.wikipedia.org/wiki/Binary_large_object>`_ is a
SQLite `datatype <https://sqlite.org/datatype3.html>`_ representing
a sequence of bytes.  It can be zero or more bytes in size.  Blobs
cannot be resized, but you can read and overwrite parts of them.

SQLite blobs have an absolute maximum size of 2GB and a `default
maximum size <https://sqlite.org/c3ref/c_limit_attached.html>`_ of
1GB.

An alternate approach to using blobs is to store the data in files and
store the filename in the database.  Doing so loses the `ACID
<https://sqlite.org/transactional.html>`_ properties of SQLite.
There are `benchmarks <https://www.sqlite.org/fasterthanfs.html>`__.

*/

/* ZEROBLOB CODE */

/** .. class:: zeroblob

  If you want to insert a blob into a row, you need to
  supply the entire blob in one go.  Using this class or
  `function <https://www.sqlite.org/lang_corefunc.html#zeroblob>`__
  allocates the space in the database filling it with zeroes.

  You can then overwrite parts in smaller chunks, without having
  to do it all at once.  The :ref:`example <example_blob_io>` shows
  how to use it.
*/

/* ZeroBlobBind is defined in apsw.c because of forward references */

static PyObject *
ZeroBlobBind_new(PyTypeObject *type, PyObject *Py_UNUSED(args), PyObject *Py_UNUSED(kwargs))
{
  ZeroBlobBind *self;
  self = (ZeroBlobBind *)type->tp_alloc(type, 0);
  if (self)
  {
    self->blobsize = 0;
    self->init_was_called = 0;
  }
  return (PyObject *)self;
}

/** .. method:: __init__(size: int)

  :param size: Number of zeroed bytes to create
*/
static int
ZeroBlobBind_init(ZeroBlobBind *self, PyObject *args, PyObject *kwargs)
{
  long long size;

  {
    Zeroblob_init_CHECK;
    PREVENT_INIT_MULTIPLE_CALLS;
    ARG_CONVERT_VARARGS_TO_FASTCALL;
    ARG_PROLOG(1, Zeroblob_init_KWNAMES);
    ARG_MANDATORY ARG_int64(size);
    ARG_EPILOG(-1, Zeroblob_init_USAGE, Py_XDECREF(fast_kwnames));
  }
  if (size < 0)
  {
    PyErr_Format(PyExc_TypeError, "zeroblob size must be >= 0");
    return -1;
  }
  self->blobsize = size;

  return 0;
}

/** .. method:: length() -> int

  Size of zero blob in bytes.
*/
static PyObject *
ZeroBlobBind_len(ZeroBlobBind *self)
{
  return PyLong_FromLong(self->blobsize);
}

static PyObject *
ZeroBlobBind_tp_str(ZeroBlobBind *self)
{
  return PyUnicode_FromFormat("<apsw.zeroblob object size %lld at %p>",
                              self->blobsize,
                              self);
}

static PyMethodDef ZeroBlobBind_methods[] = {
    {"length", (PyCFunction)ZeroBlobBind_len, METH_NOARGS,
     Zeroblob_length_DOC},
    {0, 0, 0, 0}};

static PyTypeObject ZeroBlobBindType = {
    PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "apsw.zeroblob",
    .tp_basicsize = sizeof(ZeroBlobBind),
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_doc = Zeroblob_class_DOC,
    .tp_methods = ZeroBlobBind_methods,
    .tp_init = (initproc)ZeroBlobBind_init,
    .tp_new = ZeroBlobBind_new,
    .tp_str = (reprfunc)ZeroBlobBind_tp_str,
};

/* BLOB TYPE */
struct APSWBlob
{
  PyObject_HEAD
      Connection *connection;
  sqlite3_blob *pBlob;
  unsigned inuse;        /* track if we are in use preventing concurrent thread mangling */
  int curoffset;         /* SQLite only supports 32 bit signed int offsets */
  PyObject *weakreflist; /* weak reference tracking */
};

typedef struct APSWBlob APSWBlob;

static PyTypeObject APSWBlobType;

/* BLOB CODE */

/** .. class:: Blob

  This object is created by :meth:`Connection.blob_open` and provides
  access to a blob in the database.  It behaves like a Python file.
  It wraps a `sqlite3_blob
  <https://sqlite.org/c3ref/blob.html>`_.

  .. note::

    You cannot change the size of a blob using this object. You should
    create it with the correct size in advance either by using
    :class:`zeroblob` or the `zeroblob()
    <https://sqlite.org/lang_corefunc.html>`_ function.

  See the :ref:`example <example_blob_io>`.
*/

static void
APSWBlob_init(APSWBlob *self, Connection *connection, sqlite3_blob *blob)
{
  self->connection = (Connection *)Py_NewRef((PyObject *)connection);
  self->pBlob = blob;
  self->curoffset = 0;
  self->inuse = 0;
  self->weakreflist = NULL;
}

static int
APSWBlob_close_internal(APSWBlob *self, int force)
{
  int setexc = 0;

  PY_ERR_FETCH_IF(force == 2, exc_save);

  /* note that sqlite3_blob_close always works even if an error is
     returned */

  if (self->pBlob)
  {
    int res;
    PYSQLITE_BLOB_CALL(res = sqlite3_blob_close(self->pBlob));
    if (res != SQLITE_OK)
    {
      switch (force)
      {
      case 0:
        SET_EXC(res, self->connection->db);
        setexc = 1;
        break;
      case 1:
        break;
      case 2:
        SET_EXC(res, self->connection->db);
        apsw_write_unraisable(NULL);
      }
    }
    self->pBlob = 0;
  }

  /* Remove from connection dependents list.  Has to be done before we
     decref self->connection otherwise connection could dealloc and
     we'd still be in list */
  if (self->connection)
    Connection_remove_dependent(self->connection, (PyObject *)self);

  Py_CLEAR(self->connection);

  if (force == 2)
    PY_ERR_RESTORE(exc_save);

  return setexc;
}

static void
APSWBlob_dealloc(APSWBlob *self)
{
  APSW_CLEAR_WEAKREFS;

  APSWBlob_close_internal(self, 2);

  Py_TpFree((PyObject *)self);
}

/* If the blob is closed, we return the same error as normal python files */
#define CHECK_BLOB_CLOSED                                                    \
  do                                                                         \
  {                                                                          \
    if (!self->pBlob)                                                        \
      return PyErr_Format(PyExc_ValueError, "I/O operation on closed blob"); \
  } while (0)

/** .. method:: length() -> int

  Returns the size of the blob in bytes.

  -* sqlite3_blob_bytes
*/

static PyObject *
APSWBlob_length(APSWBlob *self)
{
  CHECK_USE(NULL);
  CHECK_BLOB_CLOSED;
  return PyLong_FromLong(sqlite3_blob_bytes(self->pBlob));
}

/** .. method:: read(length: int = -1) -> bytes

  Reads amount of data requested, or till end of file, whichever is
  earlier. Attempting to read beyond the end of the blob returns an
  empty bytes in the same manner as end of file on normal file
  objects.  Negative numbers read all remaining data.

  -* sqlite3_blob_read
*/

static PyObject *
APSWBlob_read(APSWBlob *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int length = -1;
  int res;
  PyObject *buffy = 0;
  char *thebuffer;

  CHECK_USE(NULL);
  CHECK_BLOB_CLOSED;

  /* The python file read routine treats negative numbers as read till
     end of file, which I think is rather silly.  (Try reading -3
     bytes from /dev/zero on a 64 bit machine with lots of swap to see
     why).  In any event we remain consistent with Python file
     objects */
  {
    Blob_read_CHECK;
    ARG_PROLOG(1, Blob_read_KWNAMES);
    ARG_OPTIONAL ARG_int(length);
    ARG_EPILOG(NULL, Blob_read_USAGE, );
  }

  if (
      (self->curoffset == sqlite3_blob_bytes(self->pBlob)) /* eof */
      ||
      (length == 0))
    return PyBytes_FromStringAndSize(NULL, 0);

  if (length < 0)
    length = sqlite3_blob_bytes(self->pBlob) - self->curoffset;

  /* trying to read more than is in the blob? */
  if ((sqlite3_int64)self->curoffset + (sqlite3_int64)length > sqlite3_blob_bytes(self->pBlob))
    length = sqlite3_blob_bytes(self->pBlob) - self->curoffset;

  buffy = PyBytes_FromStringAndSize(NULL, length);

  if (!buffy)
    return NULL;

  thebuffer = PyBytes_AS_STRING(buffy);
  PYSQLITE_BLOB_CALL(res = sqlite3_blob_read(self->pBlob, thebuffer, length, self->curoffset));

  MakeExistingException(); /* this could happen if there were issues in the vfs */

  if (PyErr_Occurred())
    return NULL;

  if (res != SQLITE_OK)
  {
    Py_DECREF(buffy);
    SET_EXC(res, self->connection->db);
    return NULL;
  }
  else
    self->curoffset += length;
  assert(self->curoffset <= sqlite3_blob_bytes(self->pBlob));
  return buffy;
}

/** .. method:: read_into(buffer: bytearray |  array.array[Any] | memoryview, offset: int = 0, length: int = -1) -> None

  Reads from the blob into a buffer you have supplied.  This method is
  useful if you already have a buffer like object that data is being
  assembled in, and avoids allocating results in :meth:`Blob.read` and
  then copying into buffer.

  :param buffer: A writable buffer like object.
                 There is a :class:`bytearray` type that is very useful.
                 :mod:`Arrays <array>` also work.

  :param offset: The position to start writing into the buffer
                 defaulting to the beginning.

  :param length: How much of the blob to read.  The default is the
                 remaining space left in the buffer.  Note that if
                 there is more space available than blob left then you
                 will get a *ValueError* exception.

  -* sqlite3_blob_read
*/

static PyObject *
APSWBlob_read_into(APSWBlob *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int res = SQLITE_OK;
  long long offset = 0, length = -1;
  PyObject *buffer = NULL;

  int aswb;

  int bloblen;
  Py_buffer py3buffer;

  CHECK_USE(NULL);
  CHECK_BLOB_CLOSED;
  {
    Blob_read_into_CHECK;
    ARG_PROLOG(3, Blob_read_into_KWNAMES);
    ARG_MANDATORY ARG_pyobject(buffer);
    ARG_OPTIONAL ARG_int64(offset);
    ARG_OPTIONAL ARG_int64(length);
    ARG_EPILOG(NULL, Blob_read_into_USAGE, );
  }

#define ERREXIT(x)  \
  do                \
  {                 \
    x;              \
    goto errorexit; \
  } while (0)

  memset(&py3buffer, 0, sizeof(py3buffer));
  aswb = PyObject_GetBufferContiguous(buffer, &py3buffer, PyBUF_WRITABLE | PyBUF_SIMPLE);
  if (aswb)
    return NULL;

  bloblen = sqlite3_blob_bytes(self->pBlob);

  if (length < 0)
    length = py3buffer.len - offset;

  if (offset < 0 || offset > py3buffer.len)
    ERREXIT(PyErr_Format(PyExc_ValueError, "offset is less than zero or beyond end of buffer"));

  if (offset + length > py3buffer.len)
    ERREXIT(PyErr_Format(PyExc_ValueError, "Data would go beyond end of buffer"));

  if (length > bloblen - self->curoffset)
    ERREXIT(PyErr_Format(PyExc_ValueError, "More data requested than blob length"));

  PYSQLITE_BLOB_CALL(res = sqlite3_blob_read(self->pBlob, (char *)(py3buffer.buf) + offset, length, self->curoffset));

  MakeExistingException(); /* vfs errors could cause this */

  if (PyErr_Occurred())
    ERREXIT(NULL);

  if (res != SQLITE_OK)
  {
    SET_EXC(res, self->connection->db);
    ERREXIT(NULL);
  }
  self->curoffset += length;

  PyBuffer_Release(&py3buffer);
  Py_RETURN_NONE;

errorexit:
  PyBuffer_Release(&py3buffer);
  return NULL;
#undef ERREXIT
}

/** .. method:: seek(offset: int, whence: int = 0) -> None

  Changes current position to *offset* biased by *whence*.

  :param offset: New position to seek to.  Can be positive or negative number.
  :param whence: Use 0 if *offset* is relative to the beginning of the blob,
                 1 if *offset* is relative to the current position,
                 and 2 if *offset* is relative to the end of the blob.
  :raises ValueError: If the resulting offset is before the beginning (less than zero) or beyond the end of the blob.
*/

static PyObject *
APSWBlob_seek(APSWBlob *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int offset, whence = 0;
  CHECK_USE(NULL);
  CHECK_BLOB_CLOSED;

  {
    Blob_seek_CHECK;
    ARG_PROLOG(2, Blob_seek_KWNAMES);
    ARG_MANDATORY ARG_int(offset);
    ARG_OPTIONAL ARG_int(whence);
    ARG_EPILOG(NULL, Blob_seek_USAGE, );
  }
  switch (whence)
  {
  default:
    return PyErr_Format(PyExc_ValueError, "whence parameter should be 0, 1 or 2");
  case 0: /* relative to beginning of file */
    if (offset < 0 || offset > sqlite3_blob_bytes(self->pBlob))
      goto out_of_range;
    self->curoffset = offset;
    break;
  case 1: /* relative to current position */
    if (self->curoffset + offset < 0 || self->curoffset + offset > sqlite3_blob_bytes(self->pBlob))
      goto out_of_range;
    self->curoffset += offset;
    break;
  case 2: /* relative to end of file */
    if (sqlite3_blob_bytes(self->pBlob) + offset < 0 || sqlite3_blob_bytes(self->pBlob) + offset > sqlite3_blob_bytes(self->pBlob))
      goto out_of_range;
    self->curoffset = sqlite3_blob_bytes(self->pBlob) + offset;
    break;
  }
  Py_RETURN_NONE;
out_of_range:
  return PyErr_Format(PyExc_ValueError, "The resulting offset would be less than zero or past the end of the blob");
}

/** .. method:: tell() -> int

  Returns the current offset.
*/

static PyObject *
APSWBlob_tell(APSWBlob *self)
{
  CHECK_USE(NULL);
  CHECK_BLOB_CLOSED;
  return PyLong_FromLong(self->curoffset);
}

/** .. method:: write(data: bytes) -> None

  Writes the data to the blob.

  :param data: bytes to write

  :raises TypeError: Wrong data type

  :raises ValueError: If the data would go beyond the end of the blob.
      You cannot increase the size of a blob by writing beyond the end.
      You need to use :class:`zeroblob` to set the desired size first when
      inserting the blob.

  -* sqlite3_blob_write
*/
static PyObject *
APSWBlob_write(APSWBlob *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int ok = 0, res = SQLITE_OK;
  Py_buffer data_buffer;
  PyObject *data;

  CHECK_USE(NULL);
  CHECK_BLOB_CLOSED;

  {
    Blob_write_CHECK;
    ARG_PROLOG(1, Blob_write_KWNAMES);
    ARG_MANDATORY ARG_py_buffer(data);
    ARG_EPILOG(NULL, Blob_write_USAGE, );
  }

  if (0 != PyObject_GetBufferContiguous(data, &data_buffer, PyBUF_SIMPLE))
  {
    assert(PyErr_Occurred());
    return NULL;
  }

  Py_ssize_t calc_end = data_buffer.len + self->curoffset;

  APSW_FAULT_INJECT(BlobWriteTooBig, , calc_end = (Py_ssize_t)0x7FFFFFFF * (Py_ssize_t)0x1000);

  if ((int)calc_end < 0)
  {
    PyErr_Format(PyExc_ValueError, "Data is too large (integer overflow)");
    goto finally;
  }

  if (calc_end > sqlite3_blob_bytes(self->pBlob))
  {
    PyErr_Format(PyExc_ValueError, "Data would go beyond end of blob");
    goto finally;
  }

  PYSQLITE_BLOB_CALL(res = sqlite3_blob_write(self->pBlob, data_buffer.buf, data_buffer.len, self->curoffset));
  assert(!PyErr_Occurred());

  if (res != SQLITE_OK)
  {
    SET_EXC(res, self->connection->db);
    goto finally;
  }
  self->curoffset += data_buffer.len;
  assert(self->curoffset <= sqlite3_blob_bytes(self->pBlob));
  ok = 1;

finally:
  PyBuffer_Release(&data_buffer);
  if (ok)
    Py_RETURN_NONE;
  else
    return NULL;
}

/** .. method:: close(force: bool = False) -> None

  Closes the blob.  Note that even if an error occurs the blob is
  still closed.

  .. note::

     In some cases errors that technically occurred in the
     :meth:`~Blob.read` and :meth:`~Blob.write` routines may not be
     reported until close is called.  Similarly errors that occurred
     in those methods (eg calling :meth:`~Blob.write` on a read-only
     blob) may also be re-reported in :meth:`~Blob.close`.  (This
     behaviour is what the underlying SQLite APIs do - it is not APSW
     doing it.)

  It is okay to call :meth:`~Blob.close` multiple times.

  :param force: Ignores any errors during close.

  -* sqlite3_blob_close
*/

static PyObject *
APSWBlob_close(APSWBlob *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int setexc;
  int force = 0;

  CHECK_USE(NULL);

  {
    Blob_close_CHECK;
    ARG_PROLOG(1, Blob_close_KWNAMES);
    ARG_OPTIONAL ARG_bool(force);
    ARG_EPILOG(NULL, Blob_close_USAGE, );
  }
  setexc = APSWBlob_close_internal(self, !!force);

  if (setexc)
    return NULL;

  Py_RETURN_NONE;
}

/** .. method:: __enter__() -> Blob

  You can use a blob as a `context manager
  <https://docs.python.org/3/reference/datamodel.html#with-statement-context-managers>`_
  as defined in :pep:`0343`.  When you use *with* statement,
  the blob is always :meth:`closed <Blob.close>` on exit from the block, even if an
  exception occurred in the block.

  For example::

    with connection.blob_open() as blob:
        blob.write("...")
        res=blob.read(1024)

*/

static PyObject *
APSWBlob_enter(APSWBlob *self)
{
  CHECK_USE(NULL);
  CHECK_BLOB_CLOSED;

  return Py_NewRef((PyObject *)self);
}

/** .. method:: __exit__(etype: Optional[type[BaseException]], evalue: Optional[BaseException], etraceback: Optional[types.TracebackType]) -> Optional[bool]

  Implements context manager in conjunction with
  :meth:`~Blob.__enter__`.  Any exception that happened in the
  *with* block is raised after closing the blob.
*/

static PyObject *
APSWBlob_exit(APSWBlob *self, PyObject *Py_UNUSED(args))
{
  int setexc;
  CHECK_USE(NULL);
  CHECK_BLOB_CLOSED;

  setexc = APSWBlob_close_internal(self, 0);
  if (setexc)
    return NULL;

  Py_RETURN_FALSE;
}

/** .. method:: reopen(rowid: int) -> None

  Change this blob object to point to a different row.  It can be
  faster than closing an existing blob an opening a new one.

  -* sqlite3_blob_reopen
*/

static PyObject *
APSWBlob_reopen(APSWBlob *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int res;
  long long rowid;

  CHECK_USE(NULL);
  CHECK_BLOB_CLOSED;

  {
    Blob_reopen_CHECK;
    ARG_PROLOG(1, Blob_reopen_KWNAMES);
    ARG_MANDATORY ARG_int64(rowid);
    ARG_EPILOG(NULL, Blob_reopen_USAGE, );
  }
  /* no matter what happens we always reset current offset */
  self->curoffset = 0;

  PYSQLITE_BLOB_CALL(res = sqlite3_blob_reopen(self->pBlob, rowid));

  MakeExistingException(); /* a vfs error could cause this */

  if (PyErr_Occurred())
    return NULL;

  if (res != SQLITE_OK)
  {
    SET_EXC(res, self->connection->db);
    return NULL;
  }
  Py_RETURN_NONE;
}

static PyObject *
APSWBlob_tp_str(APSWBlob *self)
{
  return PyUnicode_FromFormat("<apsw.Blob object from %S at %p>",
                              self->connection ? (PyObject *)self->connection : apst.closed,
                              self);
}

static PyMethodDef APSWBlob_methods[] = {
    {"length", (PyCFunction)APSWBlob_length, METH_NOARGS,
     Blob_length_DOC},
    {"read", (PyCFunction)APSWBlob_read, METH_FASTCALL | METH_KEYWORDS,
     Blob_read_DOC},
    {"read_into", (PyCFunction)APSWBlob_read_into, METH_FASTCALL | METH_KEYWORDS,
     Blob_read_into_DOC},
    {"seek", (PyCFunction)APSWBlob_seek, METH_FASTCALL | METH_KEYWORDS,
     Blob_seek_DOC},
    {"tell", (PyCFunction)APSWBlob_tell, METH_NOARGS,
     Blob_tell_DOC},
    {"write", (PyCFunction)APSWBlob_write, METH_FASTCALL | METH_KEYWORDS,
     Blob_write_DOC},
    {"reopen", (PyCFunction)APSWBlob_reopen, METH_FASTCALL | METH_KEYWORDS,
     Blob_reopen_DOC},
    {"close", (PyCFunction)APSWBlob_close, METH_FASTCALL | METH_KEYWORDS,
     Blob_close_DOC},
    {"__enter__", (PyCFunction)APSWBlob_enter, METH_NOARGS,
     Blob_enter_DOC},
    {"__exit__", (PyCFunction)APSWBlob_exit, METH_VARARGS,
     Blob_exit_DOC},
#ifndef APSW_OMIT_OLD_NAMES
    {Blob_read_into_OLDNAME, (PyCFunction)APSWBlob_read_into, METH_FASTCALL | METH_KEYWORDS,
     Blob_read_into_OLDDOC},
#endif
    {0, 0, 0, 0} /* Sentinel */
};

static PyTypeObject APSWBlobType = {
    PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "apsw.Blob",
    .tp_basicsize = sizeof(APSWBlob),
    .tp_dealloc = (destructor)APSWBlob_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = Blob_class_DOC,
    .tp_weaklistoffset = offsetof(APSWBlob, weakreflist),
    .tp_methods = APSWBlob_methods,
    .tp_str = (reprfunc)APSWBlob_tp_str,
};
