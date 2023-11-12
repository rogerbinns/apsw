
/**

Full text search
****************

TODO wraps https://www.sqlite.org/fts5.html

lmglk jdlkj fsgdsfgdsfkj dfjkshg jkdsfhglkjhdfs


*/

static fts5_api *
Connection_fts5_api(Connection *self)
{
  CHECK_USE(NULL);
  CHECK_CLOSED(self, NULL);

  if (self->fts5_api_cached)
    return self->fts5_api_cached;

  int res;
  sqlite3_stmt *stmt = NULL;

  res = sqlite3_prepare(self->db, "select fts5(?1)", -1, &stmt, NULL); /* No PYSQLITE_CALL needed */
  if (res != SQLITE_OK)
    goto finally;
  res = sqlite3_bind_pointer(stmt, 1, &self->fts5_api_cached, "fts5_api_ptr", NULL); /* No PYSQLITE_CALL needed */
  if (res != SQLITE_OK)
    goto finally;
  sqlite3_step(stmt); /* No PYSQLITE_CALL needed */
finally:
  if (stmt)
    sqlite3_finalize(stmt); /* No PYSQLITE_CALL needed */
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
  fts5_tokenizer tokenizer;
  void *userdata;
  int tokenizer_serial;
  vectorcallfunc vectorcall;
} APSWFTS5Tokenizer;

/* Another tokenizer of the same name could have been registered which
   makes any current pointers potentially invalid.
   Connection->tokenizer_serial changes on each registration, so we use
   that to revalidate our pointers
*/
static int
Connection_tokenizer_refresh(APSWFTS5Tokenizer *self)
{
  CHECK_CLOSED(self->db, -1);
  /* CHECK_USE not needed */

  if (self->tokenizer_serial == self->db->tokenizer_serial)
  {
    assert(self->tokenizer);
    return 0;
  }
  fts5_api *api = Connection_fts5_api(self->db);
  if (!api)
    return -1;

  fts5_tokenizer tokenizer = {};
  void *userdata = NULL;
  int res = api->xFindTokenizer(
      api,
      self->name,
      &userdata,
      &tokenizer);

  /* existing tokenizer did not change */
  if (res == SQLITE_OK && 0 == memcmp(&self->tokenizer, &tokenizer, sizeof(tokenizer)) && self->userdata == userdata)
  {
    self->tokenizer_serial = self->db->tokenizer_serial;
    return 0;
  }

  if (self->tokenizer_serial == 0)
  {
    /* currently returns SQLITE_ERROR for not found */
    if (res != SQLITE_OK)
    {
      PyErr_Format(PyExc_ValueError, "No tokenizer named \"%s\"", self->name);
      return -1;
    }
    self->tokenizer_serial = self->db->tokenizer_serial;
    self->tokenizer = tokenizer;
    self->userdata = userdata;
    return 0;
  }

  if (res != SQLITE_OK)
  {
    PyErr_Format(PyExc_RuntimeError, "Tokenizer \"%s\" has been deleted", self->name);
    return -1;
  }

  assert(!(self->tokenizer == tokenizer && self->userdata == userdata));
  PyErr_Format(PyExc_RuntimeError, "Tokenizer \"%s\" has been changed", self->name);
  return -1;
}

/** .. class:: FTS5Tokenizer

  Wraps a registered tokenizer.
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
    PyErr_Format(PyExc_ValueError, "Invalid start (%d) or end of token (%d) for input buffer size (%d)", iStart, iEnd, our_context->buffer_len);
    goto error;
  }

  /* fast exit for colocated */
  if (iflags == FTS5_TOKEN_COLOCATED && !our_context->include_colocated && PyList_GET_SIZE(our_context->the_list))
    return SQLITE_OK;

  token = PyUnicode_FromStringAndSize(pToken, nToken);
  if (!token)
    goto error;

  if (iflags == FTS5_TOKEN_COLOCATED)
  {
    if (!our_context->last_item)
    {
      PyErr_Format(PyExc_ValueError, "FTS5_TOKEN_COLOCATED set when there is no previous token");
      goto error;
    }
    assert(PyUnicode_Check(our_context->last_item) || PyTuple_Check(our_context->last_item));
    if (PyTuple_Check(our_context->last_item))
    {
      if (0 != _PyTuple_Resize(&our_context->last_item, 1 + PyTuple_GET_SIZE(our_context->last_item)))
        goto error;
      PyTuple_SET_ITEM(our_context->last_item, PyTuple_GET_SIZE(our_context->last_item) - 1, token);
    }
    else
    {
      PyObject *newlast = PyTuple_Pack(2, our_context->last_item, token);
      if (!newlast)
        goto error;
      Py_DECREF(token);
      Py_DECREF(our_context->last_item);
      our_context->last_item = newlast;
    }
    return SQLITE_OK;
  }

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
  else
  {
    if (0 != PyList_Append(our_context->the_list, token))
      goto error;
    Py_CLEAR(token);
  }

  assert(!token); /* it should have been stashed somewhere */
  return SQLITE_OK;

error:
  Py_XDECREF(token);
  Py_XDECREF(start);
  Py_XDECREF(end);
  return SQLITE_ERROR;
}

/** .. method:: __call__(utf8: bytes, reason: int, args: list[str] | None = None, *, include_offsets: bool = True, include_colocated: bool = True) -> list

  Does a tokenization, returning a list of the results.  If you have no interest in
  token offsets or colocated tokens then they can be omitted from the results.

  :param utf8: Input bytes
  :param reason: :data:`Reason <apsw.mapping_fts5_tokenize_reason>` flag
  :param args: Arguments to the tokenizer
  :param include_offsets: Returned list includes offsets into utf8 for each token
  :param include_colocated: Returned list can include colocated tokens

  Example outputs
  ---------------

  Tokenizing :code:`"first place"` where :code:`1st` has been provided as a colocated
  token for :code:`first`.

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
            "place",
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
APSWFTS5Tokenizer_call(APSWFTS5Tokenizer *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  Py_buffer utf8_buffer;
  PyObject *utf8, *args = NULL;
  int include_offsets = 1, include_colocated = 1, reason;
  int rc = SQLITE_OK;

  Fts5Tokenizer *their_context = NULL;

  {
    FTS5Tokenizer_call_CHECK;
    ARG_PROLOG(3, FTS5Tokenizer_call_KWNAMES);
    ARG_MANDATORY ARG_py_buffer(utf8);
    ARG_MANDATORY ARG_int(reason);
    ARG_OPTIONAL ARG_optional_list_str(args);
    ARG_OPTIONAL ARG_bool(include_offsets);
    ARG_OPTIONAL ARG_bool(include_colocated);
    ARG_EPILOG(NULL, FTS5Tokenizer_call_USAGE, );
  }

  if (reason != FTS5_TOKENIZE_DOCUMENT && reason != FTS5_TOKENIZE_QUERY && reason != (FTS5_TOKENIZE_QUERY | FTS5_TOKENIZE_PREFIX) && reason != FTS5_TOKENIZE_AUX)
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
      .include_offsets = include_offsets};

  if (!our_context.the_list)
    goto finally;

  if (utf8_buffer.len > INT_MAX)
  {
    PyErr_Format(PyExc_ValueError, "utf8 byres is too large");
    goto finally;
  }

  Py_ssize_t argc = args ? PyList_GET_SIZE(args) : 0;
  /* arbitrary but reasonable maximum */
  if (argc > 128)
  {
    PyErr_Format(PyExc_ValueError, "Too many args");
    goto finally;
  }

  {
    VLA(argv, argc, const char *);
    for (int i = 0; i < argc; i++)
    {
      argv[i] = PyUnicode_AsUTF8(PyList_GET_ITEM(args, i));
      if (!argv[i])
        goto finally;
    }

    rc = self->tokenizer.xCreate(self->userdata, argv, argc, &their_context);
    if (rc != SQLITE_OK)
    {
      SET_EXC(rc, NULL);
      AddTraceBackHere(__FILE__, __LINE__, "FTS5Tokenizer_call.xCreate", "{s:O}", "args", OBJ(args));
      goto finally;
    }

    rc = self->tokenizer.xTokenize(their_context, &our_context, reason, utf8_buffer.buf, utf8_buffer.len, xTokenizer_Callback);
    if (rc != SQLITE_OK)
    {
      SET_EXC(rc, NULL);
      AddTraceBackHere(__FILE__, __LINE__, "FTS5Tokenizer_call.xTokenize", "{s:O,s:i,s:O}", "args", OBJ(args), "reason", reason, "utf8", utf8);
      goto finally;
    }
  }

finally:
  if (their_context)
    self->tokenizer.xDelete(their_context);
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

static PyObject *
APSWFTS5Tokenizer_str(APSWFTS5Tokenizer *self)
{
  return PyUnicode_FromFormat("<apsw.FTS5Tokenizer object \"%s\" at %p on %S>", self->name, self, self->db);
}

static void
APSWFTS5Tokenizer_dealloc(APSWFTS5Tokenizer *self)
{
  Py_DECREF(self->db);
  PyMem_Free((void *)self->name);
  Py_TpFree((PyObject *)self);
}

static PyTypeObject APSWFTS5TokenizerType = {
    PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "apsw.FTS5Tokenizer",
    .tp_doc = FTS5Tokenizer_class_DOC,
    .tp_basicsize = sizeof(APSWFTS5Tokenizer),
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_VECTORCALL,
    .tp_dealloc = (destructor)APSWFTS5Tokenizer_dealloc,
    .tp_str = (reprfunc)APSWFTS5Tokenizer_str,
    .tp_call = PyVectorcall_Call,
    .tp_vectorcall_offset = offsetof(APSWFTS5Tokenizer, vectorcall)};
