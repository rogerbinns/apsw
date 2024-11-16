

#undef Connection_fts5_api
static fts5_api *
Connection_fts5_api(Connection *self)
{
#include "faultinject.h"

  CHECK_USE(NULL);
  CHECK_CLOSED(self, NULL);

  if (self->fts5_api_cached)
    return self->fts5_api_cached;

  int res;
  sqlite3_stmt *stmt = NULL;

  fts5_api *api = NULL;

  /* this prevents any other thread from messing with our work.  The
     PYSQLITE_CALL are to let the source checker know those calls are ok */
  INUSE_CALL({
    Py_BEGIN_ALLOW_THREADS;
    res = sqlite3_prepare(self->db, "select fts5(?1)", -1, &stmt, NULL); /* PYSQLITE_CALL */
    if (res == SQLITE_OK)
      res = sqlite3_bind_pointer(stmt, 1, &api, "fts5_api_ptr", NULL); /* PYSQLITE_CALL */
    if (res == SQLITE_OK)
    {
      res = sqlite3_step(stmt); /* PYSQLITE_CALL */
      if (res == SQLITE_ROW)
        res = SQLITE_OK;
    }
    if (stmt)
      sqlite3_finalize(stmt); /* PYSQLITE_CALL */
    Py_END_ALLOW_THREADS;
  });

  if (res == SQLITE_OK)
  {
    if (api->iVersion < 3)
    {
      PyErr_Format(ExcNoFTS5, "FTS5 API iVersion %d is lower than expected 3.", api->iVersion);
      return NULL;
    }
    self->fts5_api_cached = api;
    return api;
  }

  PyErr_Format(ExcNoFTS5, "Getting the FTS5 API failed.  Is the extension included in SQLite?");
  return NULL;
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
  /* see https://sqlite.org/forum/forumpost/4dd2087c1f for why we
     store the pointers and not the underlying fts5_tokenizer_v2 */
  void (*xDelete)(Fts5Tokenizer *);
  int (*xTokenize)(Fts5Tokenizer *, void *pCtx, int flags, const char *pText, int nText, const char *pLocale,
                   int nLocale,
                   int (*xToken)(void *pCtx, int tflags, const char *pToken, int nToken, int iStart, int iEnd));
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
  /* it could be called from a third party tokenizer that has gil released */
  PyGILState_STATE gilstate = PyGILState_Ensure();

  assert(!PyErr_Occurred());
  TokenizingContext *our_context = pCtx;

  PyObject *token = NULL;
  PyObject *start = NULL, *end = NULL;

  APSW_FAULT_INJECT(xTokenCBFlagsBad, , iflags = 77);

  if (iflags != 0 && iflags != FTS5_TOKEN_COLOCATED)
  {
    PyErr_Format(PyExc_ValueError, "Invalid tokenize flags (%d)", iflags);
    goto error;
  }

  APSW_FAULT_INJECT(xTokenCBOffsetsBad, , iEnd = 9999999);
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

  APSW_FAULT_INJECT(xTokenCBColocatedBad, , iflags = FTS5_TOKEN_COLOCATED);

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
      if (!our_context->last_item)
        goto error;
      Py_CLEAR(start);
      Py_CLEAR(end);
      Py_CLEAR(token);
    }
    else if (our_context->include_colocated)
    {
      our_context->last_item = PyTuple_Pack(1, token);
      if (!our_context->last_item)
        goto error;
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
  PyGILState_Release(gilstate);
  return SQLITE_OK;

error:
  Py_XDECREF(token);
  Py_XDECREF(start);
  Py_XDECREF(end);
  PyGILState_Release(gilstate);
  return SQLITE_ERROR;
}

/** .. method:: __call__(utf8: bytes, flags: int,  locale: Optional[str], *, include_offsets: bool = True, include_colocated: bool = True) -> list[tuple[int, int, *tuple[str, ...]]]

  Does a tokenization, returning a list of the results.  If you have no
  interest in token offsets or colocated tokens then they can be omitted from
  the results.

  :param utf8: Input bytes
  :param reason: :data:`Reason <apsw.mapping_fts5_tokenize_reason>` flag
  :param include_offsets: Returned list includes offsets into utf8 for each token
  :param include_colocated: Returned list can include colocated tokens

  Example outputs
  ---------------

  Tokenizing ``b"first place"`` where ``1st`` has been provided as a
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
  const char *locale = NULL;
  Py_ssize_t locale_size = 0;
  int include_offsets = 1, include_colocated = 1, flags;
  int rc = SQLITE_OK;

  {
    FTS5Tokenizer_call_CHECK;
    ARG_PROLOG(3, FTS5Tokenizer_call_KWNAMES);
    ARG_MANDATORY ARG_py_buffer(utf8);
    ARG_MANDATORY ARG_int(flags);
    ARG_MANDATORY ARG_optional_UTF8AndSize(locale);
    ARG_OPTIONAL ARG_bool(include_offsets);
    ARG_OPTIONAL ARG_bool(include_colocated);
    ARG_EPILOG(NULL, FTS5Tokenizer_call_USAGE, );
  }

  if (flags != FTS5_TOKENIZE_DOCUMENT && flags != FTS5_TOKENIZE_QUERY
      && flags != (FTS5_TOKENIZE_QUERY | FTS5_TOKENIZE_PREFIX) && flags != FTS5_TOKENIZE_AUX)
    return PyErr_Format(PyExc_ValueError, "flags is not an allowed value (%d)", flags);

  if (0 != PyObject_GetBufferContiguous(utf8, &utf8_buffer, PyBUF_SIMPLE))
  {
    assert(PyErr_Occurred());
    return NULL;
  }

  if (locale_size >= INT32_MAX)
    return PyErr_Format(PyExc_ValueError, "locale is too large - limit is 2GB");

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

  rc = self->xTokenize(self->tokenizer_instance, &our_context, flags, utf8_buffer.buf, utf8_buffer.len, locale,
                       (int)locale_size, xTokenizer_Callback);
  if (rc != SQLITE_OK)
  {
    SET_EXC(rc, NULL);
    AddTraceBackHere(__FILE__, __LINE__, "FTS5Tokenizer_call.xTokenize", "{s:i,s:s,s:O}", "flags", flags, "locale",
                     locale, "utf8", utf8);
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
  :type: tuple[str]

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

  if (!PyCallable_Check(pyres))
  {
    PyErr_Format(PyExc_TypeError, "Expected a callable returned from FTS5 Tokenizer create, not %s",
                 Py_TypeName(pyres));
    AddTraceBackHere(__FILE__, __LINE__, "FTS5Tokenizer.xCreate", "{s:O,s:O,s:O}", "tokenizer", tfd->factory_func,
                     "args", args, "returned", pyres);
    Py_DECREF(pyres);
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

#undef get_token_value
static const char *
get_token_value(PyObject *s, int *size)
{
#include "faultinject.h"
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
                            const char *pLocale, int nLocale,
                            int (*xToken)(void *pCtx, int tflags, const char *pToken, int nToken, int iStart, int iEnd))
{
  PyGILState_STATE gilstate = PyGILState_Ensure();
  int rc = SQLITE_OK;
  PyObject *bytes = NULL, *locale = NULL, *pyflags = NULL, *iterator = NULL, *item = NULL, *object = NULL;

  bytes = PyBytes_FromStringAndSize(pText, nText);
  if (!bytes)
    goto finally;

  if (pLocale && nLocale)
  {
    locale = PyUnicode_FromStringAndSize(pLocale, nLocale);
    if (!locale)
      goto finally;
  }
  else
    locale = Py_NewRef(Py_None);

  pyflags = PyLong_FromLong(flags);
  if (!pyflags)
    goto finally;

  PyObject *vargs[] = { NULL, bytes, pyflags, locale };
  object = PyObject_Vectorcall((PyObject *)our_context, vargs + 1, 3 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  if (!object)
    goto finally;

  iterator = PyObject_GetIter(object);
  if (!iterator)
    goto finally;

  while (rc == SQLITE_OK && (item = PyIter_Next(iterator)))
  {
    /* single string */
    if (PyUnicode_Check(item))
    {
      int size;
      const char *addr = get_token_value(item, &size);
      if (!addr)
        goto finally;
      rc = xToken(their_context, 0, addr, size, 0, 0);
      APSW_FAULT_INJECT(TokenizeRC, , rc = SQLITE_NOMEM);
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
      APSW_FAULT_INJECT(TokenizeRC2, , rc = SQLITE_NOMEM);
      if (rc != SQLITE_OK)
      {
        if (!PyErr_Occurred())
          SET_EXC(rc, NULL);
        break;
      }
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
  Py_XDECREF(locale);
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

static fts5_tokenizer_v2 APSWPythonTokenizer = {
  .iVersion = 2,
  .xCreate = APSWPythonTokenizerCreate,
  .xDelete = APSWPythonTokenizerDelete,
  .xTokenize = APSWPythonTokenizerTokenize,
};

/** .. class:: FTS5ExtensionApi

`Auxiliary functions
<https://www.sqlite.org/fts5.html#_auxiliary_functions_>`__  run in
the context of a FTS5 search, and can be used for ranking,
highlighting, and similar operations.  Auxiliary functions are
registered via :meth:`Connection.register_fts5_function`.  This wraps
the `auxiliary functions API
<https://www.sqlite.org/fts5.html#custom_auxiliary_functions>`__
passed as the first parameter to auxiliary functions.

See :ref:`the example <example_fts5_auxfunc>`.
*/

typedef struct APSWFTS5ExtensionApi
{
  PyObject_HEAD
  const Fts5ExtensionApi *pApi;
  Fts5Context *pFts;
} APSWFTS5ExtensionApi;

static PyTypeObject APSWFTS5ExtensionAPIType;

/* ::TODO:: some sort of recycling for these */
#undef fts5extensionapi_acquire
static APSWFTS5ExtensionApi *
fts5extensionapi_acquire(void)
{
#include "faultinject.h"
  APSWFTS5ExtensionApi *res = (APSWFTS5ExtensionApi *)_PyObject_New(&APSWFTS5ExtensionAPIType);
  if (res)
  {
    res->pApi = 0;
    res->pFts = 0;
  }
  return res;
}

static void
fts5extensionapi_release(APSWFTS5ExtensionApi *extapi)
{
  if (extapi)
  {
    extapi->pApi = NULL;
    extapi->pFts = NULL;
    /* if ref count is 1 then this could be recycled */
    Py_DECREF(extapi);
  }
}

#define FTSEXT_CHECK(v)                                                                                                \
  do                                                                                                                   \
  {                                                                                                                    \
    if (!self->pApi)                                                                                                   \
    {                                                                                                                  \
      PyErr_Format(ExcInvalidContext, "apsw.FTS5ExtensionApi is being used outside of the callback it was valid in");  \
      return v;                                                                                                        \
    }                                                                                                                  \
  } while (0)

/** .. attribute:: phrase_count
  :type: int

  Returns the `number of phrases in the query
  <https://www.sqlite.org/fts5.html#xPhraseCount>`__
*/
static PyObject *
APSWFTS5ExtensionApi_xPhraseCount(APSWFTS5ExtensionApi *self)
{
  FTSEXT_CHECK(NULL);
  return PyLong_FromLong(self->pApi->xPhraseCount(self->pFts));
}

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
  APSW_FAULT_INJECT(xRowCountErr, , rc = SQLITE_NOMEM);
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
  return PyLong_FromLongLong(self->pApi->xRowid(self->pFts));
}

/** .. attribute:: aux_data
  :type: Any

  You can store an object as `auxiliary data <https://www.sqlite.org/fts5.html#xSetAuxdata>`__
  which is available across matching rows.  It starts out as :class:`None`.

  An example use is to do up front calculations once, rather than on
  every matched row, such as
  :func:`fts5aux.inverse_document_frequency`.
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

  int rc;
  APSW_FAULT_INJECT(xSetAuxDataErr, rc = self->pApi->xSetAuxdata(self->pFts, value, auxdata_xdelete),
                    rc = SQLITE_NOMEM);
  if (rc != SQLITE_OK)
  {
    SET_EXC(rc, NULL);
    return -1;
  }
  Py_IncRef(value);
  return 0;
}

/** .. attribute:: phrases
  :type: tuple[tuple[str | None, ...], ...]

  A tuple where each member is a phrase from the query.  Each phrase is a tuple
  of str (or None when not available) per token of the phrase.

  This combines the results of `xPhraseCount <https://www.sqlite.org/fts5.html#xPhraseCount>`__,
  `xPhraseSize <https://www.sqlite.org/fts5.html#xPhraseSize>`__ and
  `xQueryToken <https://www.sqlite.org/fts5.html#xQueryToken>`__
*/
static PyObject *
APSWFTS5ExtensionApi_phrases(APSWFTS5ExtensionApi *self)
{
  FTSEXT_CHECK(NULL);
  PyObject *outside = NULL, *phrase = NULL;
  int phrase_num, token_num;

  int nphrases = self->pApi->xPhraseCount(self->pFts);

  outside = PyTuple_New(nphrases);
  if (!outside)
    goto error;
  for (phrase_num = 0; phrase_num < nphrases; phrase_num++)
  {
    int ntokens = self->pApi->xPhraseSize(self->pFts, phrase_num);
    phrase = PyTuple_New(ntokens);
    if (!phrase)
      goto error;
    for (token_num = 0; token_num < ntokens; token_num++)
    {
      const char *pToken = NULL;
      int nToken = 0;
      if (self->pApi->iVersion >= 3)
      {
        int rc = self->pApi->xQueryToken(self->pFts, phrase_num, token_num, &pToken, &nToken);
        APSW_FAULT_INJECT(xQueryTokenErr, , rc = SQLITE_NOMEM);
        if (rc != SQLITE_OK)
        {
          SET_EXC(rc, NULL);
          goto error;
        }
      }
      if (pToken)
      {
        PyObject *tmpstr = PyUnicode_FromStringAndSize(pToken, nToken);
        if (!tmpstr)
          goto error;
        PyTuple_SET_ITEM(phrase, token_num, tmpstr);
      }
      else
        PyTuple_SET_ITEM(phrase, token_num, Py_NewRef(Py_None));
    }
    PyTuple_SET_ITEM(outside, phrase_num, phrase);
    phrase = NULL;
  }

  return outside;
error:
  Py_XDECREF(outside);
  Py_XDECREF(phrase);
  return NULL;
}

/** .. attribute:: inst_count
  :type: int

  Returns the `number of hits in the current row
  <https://www.sqlite.org/fts5.html#xInstCount>`__
*/
static PyObject *
APSWFTS5ExtensionApi_xInstCount(APSWFTS5ExtensionApi *self)
{
  FTSEXT_CHECK(NULL);
  int inst_count;
  int rc = self->pApi->xInstCount(self->pFts, &inst_count);
  APSW_FAULT_INJECT(xInstCountErr, , rc = SQLITE_NOMEM);
  if (rc != SQLITE_OK)
  {
    SET_EXC(rc, NULL);
    return NULL;
  }
  return PyLong_FromLong(inst_count);
}

/** .. method:: inst_tokens(inst: int) -> tuple[str, ...] | None

  `Access tokens of hit inst in current row <https://www.sqlite.org/fts5.html#xInstToken>`__.
  None is returned if the call is not supported.
*/
static PyObject *
APSWFTS5ExtensionApi_xInstToken(APSWFTS5ExtensionApi *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                                PyObject *fast_kwnames)
{
  FTSEXT_CHECK(NULL);

  int inst;

  {
    FTS5ExtensionApi_inst_tokens_CHECK;
    ARG_PROLOG(1, FTS5ExtensionApi_inst_tokens_KWNAMES);
    ARG_MANDATORY ARG_int(inst);
    ARG_EPILOG(NULL, FTS5ExtensionApi_inst_tokens_USAGE, );
  }

  PyObject *retval = NULL;

  for (int token = 0;; token++)
  {
    const char *bytes = NULL;
    int size = 0, rc = SQLITE_OK;

    rc = self->pApi->xInstToken(self->pFts, inst, token, &bytes, &size);
    if (rc == SQLITE_RANGE && retval)
      break;
    if (rc != SQLITE_OK)
    {
      SET_EXC(rc, NULL);
      goto error;
    }
    if (!retval)
    {
      retval = PyTuple_New(0);
      if (!retval)
        goto error;
    }
    if (0 != _PyTuple_Resize(&retval, 1 + PyTuple_GET_SIZE(retval)))
      goto error;
    PyObject *str = PyUnicode_FromStringAndSize(bytes, size);
    if (!str)
      goto error;
    PyTuple_SET_ITEM(retval, PyTuple_GET_SIZE(retval) - 1, str);
  }

  return retval;

error:
  Py_XDECREF(retval);
  return NULL;
}

/** .. method:: phrase_columns(phrase: int) -> tuple[int]

 Returns `which columns the phrase number occurs in <https://www.sqlite.org/fts5.html#xPhraseFirstColumn>`__
*/
static PyObject *
APSWFTS5ExtensionApi_phrase_columns(APSWFTS5ExtensionApi *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                                    PyObject *fast_kwnames)
{

  FTSEXT_CHECK(NULL);

  int phrase;

  {
    FTS5ExtensionApi_phrase_columns_CHECK;
    ARG_PROLOG(1, FTS5ExtensionApi_phrase_columns_KWNAMES);
    ARG_MANDATORY ARG_int(phrase);
    ARG_EPILOG(NULL, FTS5ExtensionApi_phrase_columns_USAGE, );
  }

  Fts5PhraseIter iter;
  int iCol = -1;

  /* the loop is done differently than the doc so we can check this return */
  int rc = self->pApi->xPhraseFirstColumn(self->pFts, phrase, &iter, &iCol);
  if (rc != SQLITE_OK)
  {
    SET_EXC(rc, NULL);
    return NULL;
  }

  PyObject *retval = PyTuple_New(0);
  if (!retval)
    return NULL;
  while (iCol >= 0)
  {
    if (0 != _PyTuple_Resize(&retval, 1 + PyTuple_GET_SIZE(retval)))
      goto error;
    PyObject *tmp = PyLong_FromLong(iCol);
    if (!tmp)
      goto error;
    PyTuple_SET_ITEM(retval, PyTuple_GET_SIZE(retval) - 1, tmp);
    self->pApi->xPhraseNextColumn(self->pFts, &iter, &iCol);
  }

  return retval;
error:
  Py_DECREF(retval);
  return NULL;
}

/** .. method:: phrase_locations(phrase: int) -> list[list[int]]

 Returns `which columns and token offsets  the phrase number occurs in
 <https://www.sqlite.org/fts5.html#xPhraseFirst>`__.

 The returned list is the same length as the number of columns.  Each
 member is a list of token offsets in that column, and will be empty
 if the phrase is not in that column.
*/
static PyObject *
APSWFTS5ExtensionApi_phrase_locations(APSWFTS5ExtensionApi *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                                      PyObject *fast_kwnames)
{

  FTSEXT_CHECK(NULL);

  int phrase;

  {
    FTS5ExtensionApi_phrase_locations_CHECK;
    ARG_PROLOG(1, FTS5ExtensionApi_phrase_locations_KWNAMES);
    ARG_MANDATORY ARG_int(phrase);
    ARG_EPILOG(NULL, FTS5ExtensionApi_phrase_locations_USAGE, );
  }

  Fts5PhraseIter iter;
  int iCol = -1, iOff = -1;

  /* the loop is done differently than the doc so we can check this return */
  int rc = self->pApi->xPhraseFirst(self->pFts, phrase, &iter, &iCol, &iOff);
  if (rc != SQLITE_OK)
  {
    SET_EXC(rc, NULL);
    return NULL;
  }

  int ncols = self->pApi->xColumnCount(self->pFts);

  PyObject *retval = PyList_New(ncols);
  if (!retval)
    return NULL;

  for (int i = 0; i < ncols; i++)
  {
    PyObject *tmp = PyList_New(0);
    if (!tmp)
      goto error;
    PyList_SET_ITEM(retval, i, tmp);
  }

  while (iCol >= 0)
  {
    PyObject *tmp = PyLong_FromLong(iOff);
    if (!tmp)
      goto error;
    if (0 != PyList_Append(PyList_GET_ITEM(retval, iCol), tmp))
    {
      Py_DECREF(tmp);
      goto error;
    }
    Py_DECREF(tmp);
    self->pApi->xPhraseNext(self->pFts, &iter, &iCol, &iOff);
  }

  return retval;
error:
  Py_DECREF(retval);
  return NULL;
}

/** .. method:: phrase_column_offsets(phrase: int, column: int) -> list[int]

 Returns `token offsets the phrase number occurs in
 <https://www.sqlite.org/fts5.html#xPhraseFirst>`__  in the specified
 column.

*/
static PyObject *
APSWFTS5ExtensionApi_phrase_column_offsets(APSWFTS5ExtensionApi *self, PyObject *const *fast_args,
                                           Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{

  FTSEXT_CHECK(NULL);

  int phrase;
  int column;

  {
    FTS5ExtensionApi_phrase_column_offsets_CHECK;
    ARG_PROLOG(2, FTS5ExtensionApi_phrase_column_offsets_KWNAMES);
    ARG_MANDATORY ARG_int(phrase);
    ARG_MANDATORY ARG_int(column);
    ARG_EPILOG(NULL, FTS5ExtensionApi_phrase_column_offsets_USAGE, );
  }

  Fts5PhraseIter iter;
  int iCol = -1, iOff = -1;

  /* the loop is done differently than the doc so we can check this return */
  int rc = self->pApi->xPhraseFirst(self->pFts, phrase, &iter, &iCol, &iOff);
  if (rc != SQLITE_OK)
  {
    SET_EXC(rc, NULL);
    return NULL;
  }

  int ncols = self->pApi->xColumnCount(self->pFts);
  if (column < 0 || column >= ncols)
  {
    SET_EXC(SQLITE_RANGE, NULL);
    return NULL;
  }

  PyObject *retval = PyList_New(0);
  if (!retval)
    return NULL;

  while (iCol >= 0)
  {
    if (iCol < column)
      goto next;
    if (iCol > column)
      break;
    PyObject *tmp = PyLong_FromLong(iOff);
    if (!tmp)
      goto error;
    if (0 != PyList_Append(retval, tmp))
    {
      Py_DECREF(tmp);
      goto error;
    }
    Py_DECREF(tmp);
  next:
    self->pApi->xPhraseNext(self->pFts, &iter, &iCol, &iOff);
  }

  return retval;
error:
  Py_DECREF(retval);
  return NULL;
}

/** .. method:: column_total_size(col: int = -1) -> int

  Returns the `total number of tokens in the table
  <https://www.sqlite.org/fts5.html#xColumnTotalSize>`__ for a specific
  column, or if ``col`` is negative then for all columns.
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

/** .. method:: column_size(col: int = -1) -> int

  Returns the `total number of tokens in the current row
  <https://www.sqlite.org/fts5.html#xColumnSize>`__ for a specific
  column, or if ``col`` is negative then for all columns.
*/
static PyObject *
APSWFTS5ExtensionApi_xColumnSize(APSWFTS5ExtensionApi *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                                 PyObject *fast_kwnames)
{
  FTSEXT_CHECK(NULL);

  int col = -1;

  {
    FTS5ExtensionApi_column_size_CHECK;
    ARG_PROLOG(1, FTS5ExtensionApi_column_size_KWNAMES);
    ARG_OPTIONAL ARG_int(col);
    ARG_EPILOG(NULL, FTS5ExtensionApi_column_size_USAGE, );
  }
  int nToken;
  int rc = self->pApi->xColumnSize(self->pFts, col, &nToken);
  if (rc != SQLITE_OK)
  {
    SET_EXC(rc, NULL);
    return NULL;
  }
  return PyLong_FromLong(nToken);
}

/** .. method:: column_text(col: int) -> bytes

  Returns the `utf8 bytes for the column of the current row <https://www.sqlite.org/fts5.html#xColumnText>`__.

*/
static PyObject *
APSWFTS5ExtensionApi_xColumnText(APSWFTS5ExtensionApi *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                                 PyObject *fast_kwnames)
{
  FTSEXT_CHECK(NULL);

  int col;

  {
    FTS5ExtensionApi_column_text_CHECK;
    ARG_PROLOG(1, FTS5ExtensionApi_column_text_KWNAMES);
    ARG_MANDATORY ARG_int(col);
    ARG_EPILOG(NULL, FTS5ExtensionApi_column_text_USAGE, );
  }

  const char *bytes = NULL;
  int size = 0;

  int rc = self->pApi->xColumnText(self->pFts, col, &bytes, &size);
  if (rc != SQLITE_OK)
  {
    SET_EXC(rc, NULL);
    return NULL;
  }

  return PyBytes_FromStringAndSize(bytes, size);
}

/** .. method:: tokenize(utf8: bytes, locale: Optional[str], *, include_offsets: bool = True, include_colocated: bool = True) -> list

  `Tokenizes the utf8 <https://www.sqlite.org/fts5.html#xTokenize_v2>`__.  FTS5 sets the reason to ``FTS5_TOKENIZE_AUX``.
  See :meth:`apsw.FTS5Tokenizer.__call__` for details.

*/
static PyObject *
APSWFTS5ExtensionApi_xTokenize(APSWFTS5ExtensionApi *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                               PyObject *fast_kwnames)
{
  FTSEXT_CHECK(NULL);

  Py_buffer utf8_buffer;
  PyObject *utf8;
  const char *locale = NULL;
  Py_ssize_t locale_size = 0;

  int include_offsets = 1, include_colocated = 1;
  int rc = SQLITE_OK;

  {
    FTS5ExtensionApi_tokenize_CHECK;
    ARG_PROLOG(2, FTS5ExtensionApi_tokenize_KWNAMES);
    ARG_MANDATORY ARG_py_buffer(utf8);
    ARG_MANDATORY ARG_optional_UTF8AndSize(locale);
    ARG_OPTIONAL ARG_bool(include_offsets);
    ARG_OPTIONAL ARG_bool(include_colocated);
    ARG_EPILOG(NULL, FTS5ExtensionApi_tokenize_USAGE, );
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

  if (locale_size >= INT_MAX)
  {
    PyErr_Format(PyExc_ValueError, "locale too large (%zd)", locale_size);
    goto finally;
  }

  rc = self->pApi->xTokenize_v2(self->pFts, utf8_buffer.buf, utf8_buffer.len, locale, locale_size, &our_context,
                                xTokenizer_Callback);
  APSW_FAULT_INJECT(xTokenizeErr, , rc = SQLITE_NOMEM);
  if (rc != SQLITE_OK)
  {
    if (!PyErr_Occurred())
      SET_EXC(rc, NULL);
    AddTraceBackHere(__FILE__, __LINE__, "FTS5ExtensionApi.tokenize", "{s:O}", "utf8", utf8);
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

/** .. method:: column_locale(column: int) -> str | None

  `Retrieves the locale for a column  <https://www.sqlite.org/fts5.html#xColumnLocale>`__ on
  this row.

*/
static PyObject *
APSWFTS5ExtensionApi_xColumnLocale(APSWFTS5ExtensionApi *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                                   PyObject *fast_kwnames)
{
  FTSEXT_CHECK(NULL);

  int column;

  {
    FTS5ExtensionApi_column_locale_CHECK;
    ARG_PROLOG(1, FTS5ExtensionApi_column_locale_KWNAMES);
    ARG_MANDATORY ARG_int(column);
    ARG_EPILOG(NULL, FTS5ExtensionApi_column_locale_USAGE, );
  }

  const char *pLocale = NULL;
  int nLocale = 0;

  int rc = self->pApi->xColumnLocale(self->pFts, column, &pLocale, &nLocale);
  if (rc != SQLITE_OK)
  {
    SET_EXC(rc, NULL);
    return NULL;
  }
  if (!pLocale || !nLocale)
    Py_RETURN_NONE;
  return PyUnicode_FromStringAndSize(pLocale, nLocale);
}

struct query_phrase_context
{
  APSWFTS5ExtensionApi *extapi;
  PyObject *callable;
  PyObject *closure;
};

static int
apsw_fts_query_phrase_callback(const Fts5ExtensionApi *pApi, Fts5Context *pFts, void *userData)
{
  struct query_phrase_context *qpc = (struct query_phrase_context *)userData;
  qpc->extapi->pApi = pApi;
  qpc->extapi->pFts = pFts;
  PyObject *vargs[] = { NULL, (PyObject *)qpc->extapi, qpc->closure };
  PyObject *ret = PyObject_Vectorcall(qpc->callable, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  qpc->extapi->pApi = NULL;
  qpc->extapi->pFts = NULL;
  if (ret)
  {
    Py_DECREF(ret);
    return SQLITE_OK;
  }
  return SQLITE_ERROR;
}

/** .. method:: query_phrase(phrase: int, callback: FTS5QueryPhrase, closure: Any) -> None

  Searches the table for the `numbered query <https://www.sqlite.org/fts5.html#xQueryPhrase>`__.
  The callback takes two parameters - a different :class:`apsw.FTS5ExtensionApi` and closure.

  An example usage for this method is to see how often the phrases occur in the table.  Setup a
  tracking counter here, and then in the callback you can update it on each visited row.  This
  is shown in :ref:`the example <example_fts5_auxfunc>`.
*/
static PyObject *
APSWFTS5ExtensionApi_xQueryPhrase(APSWFTS5ExtensionApi *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                                  PyObject *fast_kwnames)
{
  FTSEXT_CHECK(NULL);

  PyObject *callback, *closure;
  int phrase;

  {
    FTS5ExtensionApi_query_phrase_CHECK;
    ARG_PROLOG(3, FTS5ExtensionApi_query_phrase_KWNAMES);
    ARG_MANDATORY ARG_int(phrase);
    ARG_MANDATORY ARG_Callable(callback);
    ARG_MANDATORY ARG_pyobject(closure);
    ARG_EPILOG(NULL, FTS5ExtensionApi_query_phrase_USAGE, );
  }

  APSWFTS5ExtensionApi *qpcapi = fts5extensionapi_acquire();
  if (!qpcapi)
    return NULL;

  struct query_phrase_context context = {
    .extapi = qpcapi,
    .callable = Py_NewRef(callback),
    .closure = Py_NewRef(closure),
  };

  int rc = self->pApi->xQueryPhrase(self->pFts, phrase, &context, apsw_fts_query_phrase_callback);
  fts5extensionapi_release(context.extapi);
  Py_DECREF(context.callable);
  Py_DECREF(context.closure);
  if (rc != SQLITE_OK)
  {
    if (!PyErr_Occurred())
      SET_EXC(rc, NULL);
    AddTraceBackHere(__FILE__, __LINE__, "FTS5ExtensionApi.query_phrase", "{s: i, s:O, s: O}", "phrase", phrase,
                     "callback", callback, "closure", closure);
    return NULL;
  }
  Py_RETURN_NONE;
}

static PyGetSetDef APSWFTS5ExtensionApi_getset[] = {
  { "phrase_count", (getter)APSWFTS5ExtensionApi_xPhraseCount, NULL, FTS5ExtensionApi_phrase_count_DOC },
  { "column_count", (getter)APSWFTS5ExtensionApi_xColumnCount, NULL, FTS5ExtensionApi_column_count_DOC },
  { "row_count", (getter)APSWFTS5ExtensionApi_xRowCount, NULL, FTS5ExtensionApi_row_count_DOC },
  { "aux_data", (getter)APSWFTS5ExtensionApi_xGetAuxdata, (setter)APSWFTS5ExtensionApi_xSetAuxdata,
    FTS5ExtensionApi_aux_data_DOC },
  { "rowid", (getter)APSWFTS5ExtensionApi_xRowid, NULL, FTS5ExtensionApi_rowid_DOC },
  { "phrases", (getter)APSWFTS5ExtensionApi_phrases, NULL, FTS5ExtensionApi_phrases_DOC },
  { "inst_count", (getter)APSWFTS5ExtensionApi_xInstCount, NULL, FTS5ExtensionApi_inst_count_DOC },
  { 0 },
};

static PyMethodDef APSWFTS5ExtensionApi_methods[] = {
  { "column_total_size", (PyCFunction)APSWFTS5ExtensionApi_xColumnTotalSize, METH_FASTCALL | METH_KEYWORDS,
    FTS5ExtensionApi_column_total_size_DOC },
  { "column_size", (PyCFunction)APSWFTS5ExtensionApi_xColumnSize, METH_FASTCALL | METH_KEYWORDS,
    FTS5ExtensionApi_column_size_DOC },
  { "tokenize", (PyCFunction)APSWFTS5ExtensionApi_xTokenize, METH_FASTCALL | METH_KEYWORDS,
    FTS5ExtensionApi_tokenize_DOC },
  { "column_text", (PyCFunction)APSWFTS5ExtensionApi_xColumnText, METH_FASTCALL | METH_KEYWORDS,
    FTS5ExtensionApi_column_text_DOC },
  { "phrase_columns", (PyCFunction)APSWFTS5ExtensionApi_phrase_columns, METH_FASTCALL | METH_KEYWORDS,
    FTS5ExtensionApi_phrase_columns_DOC },
  { "phrase_locations", (PyCFunction)APSWFTS5ExtensionApi_phrase_locations, METH_FASTCALL | METH_KEYWORDS,
    FTS5ExtensionApi_phrase_locations_DOC },
  { "phrase_column_offsets", (PyCFunction)APSWFTS5ExtensionApi_phrase_column_offsets, METH_FASTCALL | METH_KEYWORDS,
    FTS5ExtensionApi_phrase_column_offsets_DOC },
  { "query_phrase", (PyCFunction)APSWFTS5ExtensionApi_xQueryPhrase, METH_FASTCALL | METH_KEYWORDS,
    FTS5ExtensionApi_query_phrase_DOC },
  { "inst_tokens", (PyCFunction)APSWFTS5ExtensionApi_xInstToken, METH_FASTCALL | METH_KEYWORDS,
    FTS5ExtensionApi_inst_tokens_DOC },
  { "column_locale", (PyCFunction)APSWFTS5ExtensionApi_xColumnLocale, METH_FASTCALL | METH_KEYWORDS,
    FTS5ExtensionApi_column_locale_DOC },
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
  PyGILState_STATE gilstate = PyGILState_Ensure();
  struct fts5aux_cbinfo *cbinfo = (struct fts5aux_cbinfo *)pUserData;
  Py_DECREF(cbinfo->callback);
  PyMem_Free((void *)cbinfo->name);
  PyMem_Free(cbinfo);
  PyGILState_Release(gilstate);
}

static void
apsw_fts5_extension_function(const Fts5ExtensionApi *pApi, /* API offered by current FTS version */
                             Fts5Context *pFts,            /* First arg to pass to pApi functions */
                             sqlite3_context *pCtx,        /* Context for returning result/error */
                             int nVal,                     /* Number of values in apVal[] array */
                             sqlite3_value **apVal         /* Array of trailing arguments */
)
{
  PyGILState_STATE gilstate = PyGILState_Ensure();
  PyObject *retval = NULL;

  VLA_PYO(vargs, 2 + nVal);

  APSWFTS5ExtensionApi *extapi = fts5extensionapi_acquire();
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
  fts5extensionapi_release(extapi);
  Py_XDECREF(retval);
  PyGILState_Release(gilstate);
}
