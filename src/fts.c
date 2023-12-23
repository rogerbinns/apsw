

static fts5_api *
Connection_fts5_api(Connection *self)
{
  CHECK_USE(NULL);
  CHECK_CLOSED(self, NULL);

  if (self->fts5_api_cached)
    return self->fts5_api_cached;

  int res;
  sqlite3_stmt *stmt = NULL;

  PYSQLITE_VOID_CALL(res = sqlite3_prepare(self->db, "select fts5(?1)", -1, &stmt, NULL));
  if (res != SQLITE_OK)
    goto finally;
  /* ::TODO:: fix this mess - INUSE the whole thing.  because the GIL is released above, when this thread resumes the db could have been closed */
  CHECK_CLOSED(self, NULL);
  PYSQLITE_VOID_CALL(res = sqlite3_bind_pointer(stmt, 1, &self->fts5_api_cached, "fts5_api_ptr", NULL));
  if (res != SQLITE_OK)
    goto finally;
  CHECK_CLOSED(self, NULL);
  PYSQLITE_VOID_CALL(sqlite3_step(stmt));
  CHECK_CLOSED(self, NULL);
finally:
  if (stmt)
    PYSQLITE_VOID_CALL(sqlite3_finalize(stmt));
  if (!self->fts5_api_cached)
    PyErr_Format(ExcNoFTS5, "Getting the FTS5 API failed");
  return self->fts5_api_cached;
}

/* Python instance */
typedef struct APSWFTS5Tokenizer
{
  PyObject_HEAD
  Connection *db;
  const char *name;
  PyObject *args;
  Fts5Tokenizer *tokenizer_instance;
  vectorcallfunc vectorcall;

  /* from fts5_tokenizer structure */
  int (*xTokenize)(Fts5Tokenizer *, void *pCtx, int flags, const char *pText, int nText,
                   int (*xToken)(void *pCtx, int tflags, const char *pToken, int nToken, int iStart, int iEnd));
  void (*xDelete)(Fts5Tokenizer *);
} APSWFTS5Tokenizer;

/** .. class:: FTS5Tokenizer

  Wraps a registered tokenizer.  Returned by :meth:`Connection.fts5_tokenizer`.
*/

/* State during tokenization run */
typedef struct
{
  /* result being built up */
  PyObject *the_list;
  /* current last item - colocated tokens get added to it and we need
     to call _PyTuple_Resize so it can't be added to list until no more
     colocated tokens are possible  */
  PyObject *last_item;
  int include_offsets;
  int include_colocated;
  /* bounds checking */
  int buffer_len;
} TokenizingContext;

static int
xTokenizer_Callback(void *pCtx, int iflags, const char *pToken, int nToken, int iStart, int iEnd)
{
  assert(!PyErr_Occurred());
  TokenizingContext *our_context = pCtx;

  PyObject *token = NULL;
  PyObject *start = NULL, *end = NULL;

  if (iflags != 0 && iflags != FTS5_TOKEN_COLOCATED)
  {
    PyErr_Format(PyExc_ValueError, "Invalid tokenize flags (%d)", iflags);
    goto error;
  }

  if (iStart < 0 || iEnd > our_context->buffer_len)
  {
    PyErr_Format(PyExc_ValueError, "Invalid start (%d) or end of token (%d) for input buffer size (%d)", iStart, iEnd,
                 our_context->buffer_len);
    goto error;
  }

  /* fast exit for colocated */
  if (iflags == FTS5_TOKEN_COLOCATED && !our_context->include_colocated)
    return SQLITE_OK;

  token = PyUnicode_DecodeUTF8(pToken, nToken, "replace");
  if (!token)
    goto error;

  if (iflags == FTS5_TOKEN_COLOCATED)
  {
    if (!our_context->last_item)
    {
      PyErr_Format(PyExc_ValueError, "FTS5_TOKEN_COLOCATED set when there is no previous token");
      goto error;
    }
    assert(PyTuple_Check(our_context->last_item));

    if (0 != _PyTuple_Resize(&our_context->last_item, 1 + PyTuple_GET_SIZE(our_context->last_item)))
      goto error;
    PyTuple_SET_ITEM(our_context->last_item, PyTuple_GET_SIZE(our_context->last_item) - 1, token);
    token = NULL; /* set item took the reference */
  }
  else
  {
    if (our_context->last_item)
    {
      if (0 != PyList_Append(our_context->the_list, our_context->last_item))
        goto error;
      Py_CLEAR(our_context->last_item);
    }

    if (our_context->include_offsets)
    {
      start = PyLong_FromLong(iStart);
      end = PyLong_FromLong(iEnd);
      if (!start || !end)
        goto error;
      our_context->last_item = PyTuple_Pack(3, start, end, token);
      Py_CLEAR(start);
      Py_CLEAR(end);
      Py_CLEAR(token);
    }
    else if (our_context->include_colocated)
    {
      our_context->last_item = PyTuple_Pack(1, token);
      Py_CLEAR(token);
    }
    else
    {
      if (0 != PyList_Append(our_context->the_list, token))
        goto error;
      Py_CLEAR(token);
    }
  }
  assert(!token); /* it should have been stashed somewhere */
  return SQLITE_OK;

error:
  Py_XDECREF(token);
  Py_XDECREF(start);
  Py_XDECREF(end);
  return SQLITE_ERROR;
}

/** .. method:: __call__(utf8: bytes, reason: int,  *, include_offsets: bool = True, include_colocated: bool = True) -> list

  Does a tokenization, returning a list of the results.  If you have no
  interest in token offsets or colocated tokens then they can be omitted from
  the results.

  :param utf8: Input bytes
  :param reason: :data:`Reason <apsw.mapping_fts5_tokenize_reason>` flag
  :param include_offsets: Returned list includes offsets into utf8 for each token
  :param include_colocated: Returned list can include colocated tokens

  Example outputs
  ---------------

  Tokenizing ``first place`` where ``1st`` has been provided as a
  colocated token for ``first``.

  (**Default**) include_offsets **True**, include_colocated **True**

    .. code-block:: python

          [
            (0, 5, "first", "1st"),
            (6, 11, "place"),
          ]

  include_offsets **False**, include_colocated **True**

    .. code-block:: python

          [
            ("first", "1st"),
            ("place", ),
          ]

  include_offsets **True**, include_colocated **False**

    .. code-block:: python

          [
            (0, 5, "first"),
            (6, 11, "place"),
          ]

  include_offsets **False**, include_colocated **False**

    .. code-block:: python

          [
            "first",
            "place",
          ]

*/
static PyObject *
APSWFTS5Tokenizer_call(APSWFTS5Tokenizer *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                       PyObject *fast_kwnames)
{
  Py_buffer utf8_buffer;
  PyObject *utf8;
  int include_offsets = 1, include_colocated = 1, reason;
  int rc = SQLITE_OK;

  {
    FTS5Tokenizer_call_CHECK;
    ARG_PROLOG(2, FTS5Tokenizer_call_KWNAMES);
    ARG_MANDATORY ARG_py_buffer(utf8);
    ARG_MANDATORY ARG_int(reason);
    ARG_OPTIONAL ARG_bool(include_offsets);
    ARG_OPTIONAL ARG_bool(include_colocated);
    ARG_EPILOG(NULL, FTS5Tokenizer_call_USAGE, );
  }

  if (reason != FTS5_TOKENIZE_DOCUMENT && reason != FTS5_TOKENIZE_QUERY
      && reason != (FTS5_TOKENIZE_QUERY | FTS5_TOKENIZE_PREFIX) && reason != FTS5_TOKENIZE_AUX)
  {
    PyErr_Format(PyExc_ValueError, "reason is not an allowed value (%d)", reason);
    return NULL;
  }

  if (0 != PyObject_GetBufferContiguous(utf8, &utf8_buffer, PyBUF_SIMPLE))
  {
    assert(PyErr_Occurred());
    return NULL;
  }

  TokenizingContext our_context = {
    .the_list = PyList_New(0),
    .buffer_len = (int)utf8_buffer.len,
    .include_colocated = include_colocated,
    .include_offsets = include_offsets,
  };

  if (!our_context.the_list)
    goto finally;

  if (utf8_buffer.len >= INT_MAX)
  {
    PyErr_Format(PyExc_ValueError, "utf8 byres is too large (%zd)", utf8_buffer.len);
    goto finally;
  }

  /* ::TODO:: should we release gil around this? */
  rc = self->xTokenize(self->tokenizer_instance, &our_context, reason, utf8_buffer.buf, utf8_buffer.len,
                       xTokenizer_Callback);
  if (rc != SQLITE_OK)
  {
    SET_EXC(rc, NULL);
    AddTraceBackHere(__FILE__, __LINE__, "FTS5Tokenizer_call.xTokenize", "{s:i,s:O}", "reason", reason, "utf8", utf8);
    goto finally;
  }

finally:
  PyBuffer_Release(&utf8_buffer);

  if (rc == SQLITE_OK && our_context.last_item)
  {
    if (0 != PyList_Append(our_context.the_list, our_context.last_item))
      rc = SQLITE_ERROR;
  }
  if (rc != SQLITE_OK)
  {
    assert(PyErr_Occurred());
    Py_CLEAR(our_context.the_list);
  }
  Py_CLEAR(our_context.last_item);
  return our_context.the_list;
}

/** .. attribute:: connection
  :type: Connection

  The :class:`Connection` this tokenizer is registered with.
*/
static PyObject *
APSWFTS5Tokenizer_connection(APSWFTS5Tokenizer *self)
{
  return Py_NewRef((PyObject *)self->db);
}

/** .. attribute:: args
  :type: tuple

  The arguments the tokenizer was created with.
*/
static PyObject *
APSWFTS5Tokenizer_args(APSWFTS5Tokenizer *self)
{
  return Py_NewRef(self->args);
}

/** .. attribute:: name
  :type: str

  Tokenizer name
*/
static PyObject *
APSWFTS5Tokenizer_name(APSWFTS5Tokenizer *self)
{
  return PyUnicode_FromString(self->name);
}

static PyObject *
APSWFTS5Tokenizer_tp_str(APSWFTS5Tokenizer *self)
{
  return PyUnicode_FromFormat("<apsw.FTS5Tokenizer object \"%s\" args %S at %p>", self->name, self->args, self);
}

static void
APSWFTS5Tokenizer_dealloc(APSWFTS5Tokenizer *self)
{
  Py_XDECREF(self->db);
  Py_XDECREF(self->args);
  PyMem_Free((void *)self->name);
  if (self->tokenizer_instance)
    self->xDelete(self->tokenizer_instance);
  Py_TpFree((PyObject *)self);
}

static PyGetSetDef APSWFTS5Tokenizer_getset[] = {
  { "connection", (getter)APSWFTS5Tokenizer_connection, NULL, FTS5Tokenizer_connection_DOC },
  { "args", (getter)APSWFTS5Tokenizer_args, NULL, FTS5Tokenizer_args_DOC },
  { "name", (getter)APSWFTS5Tokenizer_name, NULL, FTS5Tokenizer_name_DOC },
  { 0 },
};

static PyTypeObject APSWFTS5TokenizerType = {
  /* clang-format off */
  PyVarObject_HEAD_INIT(NULL, 0)
  .tp_name = "apsw.FTS5Tokenizer",
  /* clang-format on */
  .tp_doc = FTS5Tokenizer_class_DOC,
  .tp_basicsize = sizeof(APSWFTS5Tokenizer),
  .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_VECTORCALL,
  .tp_dealloc = (destructor)APSWFTS5Tokenizer_dealloc,
  .tp_call = PyVectorcall_Call,
  .tp_vectorcall_offset = offsetof(APSWFTS5Tokenizer, vectorcall),
  .tp_getset = APSWFTS5Tokenizer_getset,
  .tp_str = (reprfunc)APSWFTS5Tokenizer_tp_str,
};

typedef struct
{
  PyObject *factory_func;
  PyObject *connection;
} TokenizerFactoryData;

static void
APSWPythonTokenizerFactoryDelete(void *factory_data)
{
  PyGILState_STATE gilstate = PyGILState_Ensure();
  TokenizerFactoryData *tfd = (TokenizerFactoryData *)factory_data;
  Py_DECREF(tfd->factory_func);
  Py_DECREF(tfd->connection);
  PyMem_Free(tfd);
  PyGILState_Release(gilstate);
}

static int
APSWPythonTokenizerCreate(void *factory_data, const char **argv, int argc, Fts5Tokenizer **ppOut)
{
  PyGILState_STATE gilstate = PyGILState_Ensure();
  int i, res = SQLITE_NOMEM;
  TokenizerFactoryData *tfd = (TokenizerFactoryData *)factory_data;

  PyObject *args = PyList_New(argc);
  if (!args)
    goto finally;

  for (i = 0; i < argc; i++)
  {
    PyObject *arg = PyUnicode_FromString(argv[i]);
    if (!arg)
      goto finally;
    PyList_SET_ITEM(args, i, arg);
  }

  PyObject *vargs[] = { NULL, tfd->connection, args };

  PyObject *pyres = PyObject_Vectorcall(tfd->factory_func, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  if (!pyres)
  {
    res = SQLITE_ERROR;
    goto finally;
  }

  *ppOut = (Fts5Tokenizer *)pyres;
  res = SQLITE_OK;

finally:
  Py_XDECREF(args);

  assert((res == SQLITE_OK && !PyErr_Occurred()) || (res != SQLITE_OK && PyErr_Occurred()));
  PyGILState_Release(gilstate);
  return res;
}

static const char *
get_token_value(PyObject *s, int *size)
{
  Py_ssize_t ssize;
  const char *address = PyUnicode_AsUTF8AndSize(s, &ssize);
  if (!address)
    return NULL;
  if (ssize >= INT_MAX)
  {
    PyErr_Format(PyExc_ValueError, "Token is too long (%zd)", ssize);
    return NULL;
  }
  *size = (int)ssize;
  return address;
}

static int
APSWPythonTokenizerTokenize(Fts5Tokenizer *our_context, void *their_context, int flags, const char *pText, int nText,
                            int (*xToken)(void *pCtx, int tflags, const char *pToken, int nToken, int iStart, int iEnd))
{
  PyGILState_STATE gilstate = PyGILState_Ensure();
  int rc = SQLITE_OK;
  PyObject *bytes = NULL, *pyflags = NULL, *iterator = NULL, *item = NULL, *object = NULL;

  bytes = PyBytes_FromStringAndSize(pText, nText);
  if (!bytes)
    goto finally;
  pyflags = PyLong_FromLong(flags);
  if (!pyflags)
    goto finally;

  PyObject *vargs[] = { NULL, bytes, pyflags };
  object = PyObject_Vectorcall((PyObject *)our_context, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  if (!object)
    goto finally;

  iterator = PyObject_GetIter(object);
  if (!iterator)
    goto finally;
  while ((item = PyIter_Next(iterator)))
  {
    /* single string */
    if (PyUnicode_Check(item))
    {
      int size;
      const char *addr = get_token_value(item, &size);
      if (!addr)
        goto finally;
      rc = xToken(their_context, 0, addr, size, 0, 0);
      Py_CLEAR(item);
      if (rc != SQLITE_OK)
        goto finally;
      continue;
    }
    if (!PyTuple_Check(item))
    {
      PyErr_Format(PyExc_ValueError, "Expected a str or a tuple, not %s", Py_TypeName(item));
      goto finally;
    }
    Py_ssize_t tuple_len = PyTuple_GET_SIZE(item);
    if (tuple_len < 1)
    {
      PyErr_Format(PyExc_ValueError, "tuple is empty");
      goto finally;
    }

    Py_ssize_t string_offset = 0;
    int iStart = 0, iEnd = 0;
    if (PyLong_Check(PyTuple_GET_ITEM(item, 0)))
    {
      if (tuple_len < 3)
      {
        PyErr_Format(PyExc_ValueError,
                     "Tuple isn't long enough (%zd).  Should be at "
                     "least two integers and a string.",
                     tuple_len);
        goto finally;
      }
      string_offset = 2;
      if (!PyLong_Check(PyTuple_GET_ITEM(item, 1)))
      {
        PyErr_Format(PyExc_ValueError, "Second tuple element should also be an integer");
        goto finally;
      }
      iStart = PyLong_AsInt(PyTuple_GET_ITEM(item, 0));
      iEnd = PyLong_AsInt(PyTuple_GET_ITEM(item, 1));
      if (PyErr_Occurred())
        goto finally;
      if (iStart < 0 || iEnd < 0 || iStart > iEnd || iEnd > nText)
      {
        PyErr_Format(PyExc_ValueError,
                     "start (%d) and end (%d) must be positive, within "
                     "the utf8 length (%d) and start before end",
                     iStart, iEnd, nText);
        goto finally;
      }
    }

    int first = 1;
    for (; string_offset < tuple_len; string_offset++, first = 0)
    {
      PyObject *str = PyTuple_GET_ITEM(item, string_offset);
      if (!PyUnicode_Check(str))
      {
        PyErr_Format(PyExc_ValueError, "Expected tuple item %zd to be a str, not %s", string_offset, Py_TypeName(str));
        goto finally;
      }
      int str_size;
      const char *str_addr = get_token_value(str, &str_size);
      if (!str_addr)
        goto finally;
      rc = xToken(their_context, first ? 0 : FTS5_TOKEN_COLOCATED, str_addr, str_size, iStart, iEnd);
    }
    Py_CLEAR(item);
  }

finally:
  if (PyErr_Occurred())
  {
    if (item)
      AddTraceBackHere(__FILE__, __LINE__, "xTokenize.iterator", "{s:O}", "item", item);
    AddTraceBackHere(__FILE__, __LINE__, "xTokenize", "{s:O,s:O,s:i}", "self", (PyObject *)our_context, "bytes",
                     OBJ(bytes), "flags", flags);
  }

  Py_XDECREF(bytes);
  Py_XDECREF(pyflags);
  Py_XDECREF(iterator);
  Py_XDECREF(object);
  Py_XDECREF(item);
  int res = PyErr_Occurred() ? SQLITE_ERROR : rc;
  PyGILState_Release(gilstate);
  return res;
}

static void
APSWPythonTokenizerDelete(Fts5Tokenizer *ptr)
{
  PyGILState_STATE gilstate = PyGILState_Ensure();
  Py_DECREF((PyObject *)ptr);
  PyGILState_Release(gilstate);
}

static fts5_tokenizer APSWPythonTokenizer = {
  .xCreate = APSWPythonTokenizerCreate,
  .xDelete = APSWPythonTokenizerDelete,
  .xTokenize = APSWPythonTokenizerTokenize,
};

/** .. class:: FTS5ExtensionApi

  Wraps the `auxiliary functions API
  <https://www.sqlite.org/fts5.html#_custom_auxiliary_functions_api_reference_>`__
*/

typedef struct APSWFTS5ExtensionApi
{
  PyObject_HEAD
  const Fts5ExtensionApi *pApi;
  Fts5Context *pFts;
} APSWFTS5ExtensionApi;

#define FTSEXT_CHECK(v)                                                                                                \
  do                                                                                                                   \
  {                                                                                                                    \
    if (!self->pApi)                                                                                                   \
    {                                                                                                                  \
      PyErr_Format(ExcInvalidContext, "apsw.FTS5ExtensionApi is being used outside of the callback it was valid in");  \
      return v;                                                                                                        \
    }                                                                                                                  \
  } while (0)

/** .. attribute:: column_count
  :type: int

  Returns the `number of columns in the table
  <https://www.sqlite.org/fts5.html#xColumnCount>`__
*/
static PyObject *
APSWFTS5ExtensionApi_xColumnCount(APSWFTS5ExtensionApi *self)
{
  FTSEXT_CHECK(NULL);
  return PyLong_FromLong(self->pApi->xColumnCount(self->pFts));
}

/** .. attribute:: row_count
  :type: int

  Returns the `number of rows in the table
  <https://www.sqlite.org/fts5.html#xRowCount>`__
*/
static PyObject *
APSWFTS5ExtensionApi_xRowCount(APSWFTS5ExtensionApi *self)
{
  FTSEXT_CHECK(NULL);
  sqlite3_int64 row_count;
  int rc = self->pApi->xRowCount(self->pFts, &row_count);
  if (rc != SQLITE_OK)
  {
    SET_EXC(rc, NULL);
    return NULL;
  }
  return PyLong_FromLongLong(row_count);
}

/** .. attribute:: rowid
  :type: int

  Rowid of the `current row <https://www.sqlite.org/fts5.html#xGetAuxdata>`__
*/
static PyObject *
APSWFTS5ExtensionApi_xRowid(APSWFTS5ExtensionApi *self)
{
  FTSEXT_CHECK(NULL);
  return PyLong_FromLong(self->pApi->xRowid(self->pFts));
}

/** .. attribute:: aux_data
  :type: Any

  You can store an object as `auxiliary data <https://www.sqlite.org/fts5.html#xSetAuxdata>`__
  which is available across rows and :meth:`query_phrase`.
*/

static void
auxdata_xdelete(void *auxdata)
{
  PyGILState_STATE gilstate = PyGILState_Ensure();
  Py_DECREF((PyObject *)auxdata);
  PyGILState_Release(gilstate);
}

static PyObject *
APSWFTS5ExtensionApi_xGetAuxdata(APSWFTS5ExtensionApi *self)
{
  FTSEXT_CHECK(NULL);

  PyObject *data = self->pApi->xGetAuxdata(self->pFts, 0);
  if (!data)
    data = Py_None;
  return Py_NewRef(data);
}

static int
APSWFTS5ExtensionApi_xSetAuxdata(APSWFTS5ExtensionApi *self, PyObject *value)
{
  FTSEXT_CHECK(-1);

  int rc = self->pApi->xSetAuxdata(self->pFts, value, auxdata_xdelete);
  if (rc != SQLITE_OK)
  {
    SET_EXC(rc, NULL);
    return -1;
  }
  Py_IncRef(value);
  return 0;
}

/** .. method:: column_total_size(col: int = -1) -> int

  Returns the `total number of tokens in the table
  <https://www.sqlite.org/fts5.html#xColumnTotalSize>`__ for a specific
  column, of if `col` is negative then for all columns.
*/
static PyObject *
APSWFTS5ExtensionApi_xColumnTotalSize(APSWFTS5ExtensionApi *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                                      PyObject *fast_kwnames)
{
  FTSEXT_CHECK(NULL);

  int col = -1;

  {
    FTS5ExtensionApi_column_total_size_CHECK;
    ARG_PROLOG(1, FTS5ExtensionApi_column_total_size_KWNAMES);
    ARG_OPTIONAL ARG_int(col);
    ARG_EPILOG(NULL, FTS5ExtensionApi_column_total_size_USAGE, );
  }
  sqlite3_int64 nToken;
  int rc = self->pApi->xColumnTotalSize(self->pFts, col, &nToken);
  if (rc != SQLITE_OK)
  {
    SET_EXC(rc, NULL);
    return NULL;
  }
  return PyLong_FromLongLong(nToken);
}

static PyGetSetDef APSWFTS5ExtensionApi_getset[] = {
  { "column_count", (getter)APSWFTS5ExtensionApi_xColumnCount, NULL, FTS5ExtensionApi_column_count_DOC },
  { "row_count", (getter)APSWFTS5ExtensionApi_xRowCount, NULL, FTS5ExtensionApi_row_count_DOC },
  { "aux_data", (getter)APSWFTS5ExtensionApi_xGetAuxdata, (setter)APSWFTS5ExtensionApi_xSetAuxdata,
    FTS5ExtensionApi_aux_data_DOC },
  { "rowid", (getter)APSWFTS5ExtensionApi_xRowid, NULL, FTS5ExtensionApi_rowid_DOC },
  { 0 },
};

static PyMethodDef APSWFTS5ExtensionApi_methods[] = {
  { "column_total_size", (PyCFunction)APSWFTS5ExtensionApi_xColumnTotalSize, METH_FASTCALL | METH_KEYWORDS,
    FTS5ExtensionApi_column_total_size_DOC },
  { 0 },
};

static PyTypeObject APSWFTS5ExtensionAPIType = {
  /* clang-format off */
  PyVarObject_HEAD_INIT(NULL, 0)
  .tp_name = "apsw.FTS5ExtensionApi",
  /* clang-format on */
  .tp_doc = FTS5ExtensionApi_class_DOC,
  .tp_basicsize = sizeof(APSWFTS5ExtensionApi),
  .tp_flags = Py_TPFLAGS_DEFAULT,
  .tp_getset = APSWFTS5ExtensionApi_getset,
  .tp_methods = APSWFTS5ExtensionApi_methods,
};

struct fts5aux_cbinfo
{
  PyObject *callback;
  const char *name;
};

static void
apsw_fts5_extension_function_destroy(void *pUserData)
{
  struct fts5aux_cbinfo *cbinfo = (struct fts5aux_cbinfo *)pUserData;
  Py_DECREF(cbinfo->callback);
  PyMem_Free((void *)cbinfo->name);
  PyMem_Free(cbinfo);
}

static void
apsw_fts5_extension_function(const Fts5ExtensionApi *pApi, /* API offered by current FTS version */
                             Fts5Context *pFts,            /* First arg to pass to pApi functions */
                             sqlite3_context *pCtx,        /* Context for returning result/error */
                             int nVal,                     /* Number of values in apVal[] array */
                             sqlite3_value **apVal         /* Array of trailing arguments */
)
{
  fprintf(stderr, "apsw_fts5_extension_function called %p %p %d\n", pApi, pFts, nVal);
  PyGILState_STATE gilstate = PyGILState_Ensure();
  PyObject *retval = NULL;

  VLA_PYO(vargs, 2 + nVal);

  APSWFTS5ExtensionApi *extapi = (APSWFTS5ExtensionApi *)_PyObject_New(&APSWFTS5ExtensionAPIType);
  if (!extapi)
  {
    sqlite3_result_error_nomem(pCtx);
    goto finally;
  }

  struct fts5aux_cbinfo *cbinfo = (struct fts5aux_cbinfo *)pApi->xUserData(pFts);

  extapi->pApi = pApi;
  extapi->pFts = pFts;

  vargs[1] = (PyObject *)extapi;
  if (getfunctionargs(vargs + 2, pCtx, nVal, apVal))
    goto finally;

  retval = PyObject_Vectorcall(cbinfo->callback, vargs + 1, (1 + nVal) | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);

  Py_DECREF_ARRAY(vargs + 2, nVal);
  if (retval)
    set_context_result(pCtx, retval);
  else
  {
    char *errmsg = NULL;
    sqlite3_result_error_code(pCtx, MakeSqliteMsgFromPyException(&errmsg));
    sqlite3_result_error(pCtx, errmsg, -1);
    AddTraceBackHere(__FILE__, __LINE__, "apsw_fts5_extension_function", "{s: s, s: i, s: s}", "name", cbinfo->name,
                     "nargs", nVal, "message", errmsg);
    sqlite3_free(errmsg);
  }

finally:
  if (extapi)
  {
    extapi->pApi = NULL;
    extapi->pFts = NULL;
    Py_DECREF(extapi);
  }
  Py_XDECREF(retval);
  PyGILState_Release(gilstate);
}
