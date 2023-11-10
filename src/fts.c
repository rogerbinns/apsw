
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

/** .. class:: FTS5Tokenizer


 Wraps a tokenizer
*/
typedef struct APSWFTS5Tokenizer
{
  PyObject_HEAD
      Connection *db;
  const char *name;
  fts5_tokenizer tokenizer;
  void *userdata;
} APSWFTS5Tokenizer;


/** .. method:: __call__(utf8: bytes, *, include_offsets: bool = True) -> list

  Do the tokenization
*/
static PyObject *
APSWFTS5Tokenizer_call(APSWFTS5Tokenizer *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  Py_buffer utf8_buffer;
  PyObject *utf8;
  int include_offsets = 1;
  {
    FTS5Tokenizer_call_CHECK;
    ARG_PROLOG(1, FTS5Tokenizer_call_KWNAMES);
    ARG_MANDATORY ARG_py_buffer(utf8);
    ARG_OPTIONAL ARG_bool(include_offsets);
    ARG_EPILOG(NULL, FTS5Tokenizer_call_USAGE, );
  }

  if (0 != PyObject_GetBufferContiguous(utf8, &utf8_buffer, PyBUF_SIMPLE))
  {
    assert(PyErr_Occurred());
    return NULL;
  }

  PyErr_Format(PyExc_NotImplementedError, "not implemented yet");
  return NULL;
}

static void
APSWFTS5Tokenizer_dealloc(APSWFTS5Tokenizer *self)
{
  fprintf(stderr, "dealloc %p\n", self);
  Py_DECREF(self->db);
  PyMem_Free((void *)self->name);
  Py_TpFree((PyObject *)self);
}

static PyTypeObject APSWFTS5TokenizerType = {
    PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "apsw.FTS5Tokenizer",
    .tp_doc = FTS5Tokenizer_class_DOC,
    .tp_basicsize = sizeof(APSWFTS5Tokenizer),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_dealloc = (destructor)APSWFTS5Tokenizer_dealloc,
    .tp_call = PyVectorcall_Call
};
