/*
  Cursor handling code

 See the accompanying LICENSE file.
*/

/**

.. _cursors:

Cursors (executing SQL)
***********************

A cursor encapsulates a SQL query and returning results.  You only need an
explicit cursor if you want more information or control over execution.  Using
:meth:`Connection.execute` or :meth:`Connection.executemany` will automatically
obtain a cursor behind the scenes.

If you need a cursor you should call :meth:`~Connection.cursor` on your
database::

  db = apsw.Connection("databasefilename")
  cursor = db.cursor()

The :ref:`example <example_executing_sql>` shows how to execute SQL and
how to provide values used in queries (bindings).

Cursors are cheap.  Use as many as you need. Behind the scenes a
:class:`Cursor` maps to a `SQLite statement <https://sqlite.org/c3ref/stmt.html>`_.
APSW maintains a :ref:`cache <statementcache>` so that the mapping is very fast, and the
SQLite objects are reused when possible.

.. note::

  Cursors on the same :ref:`Connection <connections>` are not isolated
  from each other.  Anything done on one cursor is immediately visible
  to all other Cursors on the same connection.  This still applies if
  you start transactions.  Connections are isolated from each other
  with cursors on other connections not seeing changes until they are
  committed.

.. seealso::

  * `SQLite transactions <https://sqlite.org/lang_transaction.html>`_
  * `Atomic commit <https://sqlite.org/atomiccommit.html>`_
  * :ref:`Benchmarking`

*/

/* secret backdoor to allow bindings all as null */
static PyObject *apsw_cursor_null_bindings;

/** .. class:: Cursor
*/

/* CURSOR TYPE */

struct APSWCursor
{
  PyObject_HEAD
      Connection *connection; /* pointer to parent connection */

  unsigned inuse;                  /* track if we are in use preventing concurrent thread mangling */
  struct APSWStatement *statement; /* statement we are currently using */

  /* what state we are in */
  enum
  {
    C_BEGIN,
    C_ROW,
    C_DONE
  } status;

  /* bindings for query */
  PyObject *bindings;        /* dict or sequence */
  Py_ssize_t bindingsoffset; /* for sequence tracks how far along we are when dealing with multiple statements */

  /* iterator for executemany, original query string, prepare options */
  PyObject *emiter;
  PyObject *emoriginalquery;
  APSWStatementOptions emoptions;

  /* tracing functions */
  PyObject *exectrace;
  PyObject *rowtrace;

  /* weak reference support */
  PyObject *weakreflist;

  PyObject *description_cache[3];

  int init_was_called;
};

typedef struct APSWCursor APSWCursor;
static PyTypeObject APSWCursorType;

static PyObject *collections_abc_Mapping;

/* CURSOR CODE */

/* Macro for getting a tracer.  If our tracer is NULL then return connection tracer */

#define ROWTRACE (self->rowtrace ? self->rowtrace : self->connection->rowtrace)

#define EXECTRACE (self->exectrace ? self->exectrace : self->connection->exectrace)

/* Do finalization and free resources.  Returns the SQLITE error code.  If force is 2 then don't raise any exceptions */
static int
resetcursor(APSWCursor *self, int force)
{
  int res = SQLITE_OK;
  int hasmore = statementcache_hasmore(self->statement);

  Py_CLEAR(self->description_cache[0]);
  Py_CLEAR(self->description_cache[1]);
  Py_CLEAR(self->description_cache[2]);

  PY_ERR_FETCH_IF(force, exc_save);

  if (self->statement)
  {
    INUSE_CALL(res = statementcache_finalize(self->connection->stmtcache, self->statement));
    if (res == SQLITE_OK && PyErr_Occurred())
      res = SQLITE_ERROR;
    if (res)
    {
      if (force && PyErr_Occurred())
        apsw_write_unraisable(NULL);
      else
        SET_EXC(res, self->connection->db);
    }
    self->statement = 0;
  }

  Py_CLEAR(self->bindings);
  self->bindingsoffset = -1;

  if (!force && self->status != C_DONE && hasmore)
  {
    if (res == SQLITE_OK)
    {
      /* We still have more, so this is actually an abort. */
      res = SQLITE_ERROR;
      if (!PyErr_Occurred())
      {
        PyErr_Format(ExcIncomplete, "Error: there are still remaining sql statements to execute");
      }
    }
  }

  if (!force && self->status != C_DONE && self->emiter)
  {
    PyObject *next;
    INUSE_CALL(next = PyIter_Next(self->emiter));
    if (next)
    {
      Py_DECREF(next);
      res = SQLITE_ERROR;
      assert(PyErr_Occurred());
    }
  }

  Py_CLEAR(self->emiter);
  Py_CLEAR(self->emoriginalquery);

  self->status = C_DONE;

  if (PyErr_Occurred())
  {
    assert(res);
    AddTraceBackHere(__FILE__, __LINE__, "resetcursor", "{s: i}", "res", res);
  }

  if (force)
    PY_ERR_RESTORE(exc_save);

  return res;
}

static int
APSWCursor_close_internal(APSWCursor *self, int force)
{
  int res;

  PY_ERR_FETCH_IF(force == 2, exc_save);

  res = resetcursor(self, force);

  if (force == 2)
    PY_ERR_RESTORE(exc_save);
  else
  {
    if (res)
    {
      assert(PyErr_Occurred());
      return 1;
    }
    assert(!PyErr_Occurred());
  }

  /* Remove from connection dependents list.  Has to be done before we decref self->connection
     otherwise connection could dealloc and we'd still be in list */
  if (self->connection)
    Connection_remove_dependent(self->connection, (PyObject *)self);

  /* executemany iterator */
  Py_CLEAR(self->emiter);

  /* no need for tracing */
  Py_CLEAR(self->exectrace);
  Py_CLEAR(self->rowtrace);

  /* we no longer need connection */
  Py_CLEAR(self->connection);

  Py_CLEAR(self->description_cache[0]);
  Py_CLEAR(self->description_cache[1]);
  Py_CLEAR(self->description_cache[2]);

  return 0;
}

static void
APSWCursor_dealloc(APSWCursor *self)
{
  /* dealloc is not allowed to return an exception or
     clear the current exception */
  PY_ERR_FETCH(exc_save);

  PyObject_GC_UnTrack(self);
  APSW_CLEAR_WEAKREFS;

  APSWCursor_close_internal(self, 2);

  if (PyErr_Occurred())
    apsw_write_unraisable(NULL);

  PY_ERR_RESTORE(exc_save);
  Py_TpFree((PyObject *)self);
}

static PyObject *
APSWCursor_new(PyTypeObject *type, PyObject *Py_UNUSED(args), PyObject *Py_UNUSED(kwds))
{
  APSWCursor *self;

  self = (APSWCursor *)type->tp_alloc(type, 0);
  if (self != NULL)
  {
    self->connection = NULL;
    self->statement = 0;
    self->status = C_DONE;
    self->bindings = 0;
    self->bindingsoffset = 0;
    self->emiter = 0;
    self->emoriginalquery = 0;
    self->exectrace = 0;
    self->rowtrace = 0;
    self->inuse = 0;
    self->weakreflist = NULL;
    self->description_cache[0] = 0;
    self->description_cache[1] = 0;
    self->description_cache[2] = 0;
    self->init_was_called = 0;
  }

  return (PyObject *)self;
}

/** .. method:: __init__(connection: Connection)

 Use :meth:`Connection.cursor` to make a new cursor.

*/

static int
APSWCursor_init(APSWCursor *self, PyObject *args, PyObject *kwargs)
{
  Connection *connection = NULL;

  {
    Cursor_init_CHECK;
    PREVENT_INIT_MULTIPLE_CALLS;
    ARG_CONVERT_VARARGS_TO_FASTCALL;
    ARG_PROLOG(1, Cursor_init_KWNAMES);
    ARG_MANDATORY ARG_Connection(connection);
    ARG_EPILOG(-1, Cursor_init_USAGE, Py_XDECREF(fast_kwnames));
  }

  self->connection = (Connection *)Py_NewRef((PyObject *)connection);

  return 0;
}

static int
APSWCursor_tp_traverse(APSWCursor *self, visitproc visit, void *arg)
{
  Py_VISIT(self->connection);
  Py_VISIT(self->exectrace);
  Py_VISIT(self->rowtrace);
  return 0;
}

static const char *description_formats[] = {
    "(ss)",
    "(ssOOOOO)",
    "(sssss)"};

static PyObject *
APSWCursor_internal_get_description(APSWCursor *self, int fmtnum)
{
  int ncols, i;
  PyObject *result = NULL;
  PyObject *column = NULL;

  assert(sizeof(description_formats) == sizeof(self->description_cache));

  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  if (!self->statement)
  {
    assert(self->description_cache[0] == 0);
    assert(self->description_cache[1] == 0);
    assert(self->description_cache[2] == 0);
    return PyErr_Format(ExcComplete, "Can't get description for statements that have completed execution");
  }

  if (self->description_cache[fmtnum])
    return Py_NewRef(self->description_cache[fmtnum]);

  ncols = sqlite3_column_count(self->statement->vdbestatement);
  result = PyTuple_New(ncols);
  if (!result)
    goto error;

  for (i = 0; i < ncols; i++)
  {

#define INDEX self->statement->vdbestatement, i
/* this is needed because msvc chokes with the ifdef inside */
#ifdef SQLITE_ENABLE_COLUMN_METADATA
#define DESCFMT2 Py_BuildValue(description_formats[fmtnum],         \
                               column_name,                         \
                               sqlite3_column_decltype(INDEX),      \
                               sqlite3_column_database_name(INDEX), \
                               sqlite3_column_table_name(INDEX),    \
                               sqlite3_column_origin_name(INDEX))
#else
#define DESCFMT2 NULL
#endif
    /* only column_name is described as returning NULL on error */
    const char *column_name = sqlite3_column_name(INDEX);
    if (!column_name)
    {
      PyErr_Format(PyExc_MemoryError, "SQLite call sqlite3_column_name ran out of memory");
      goto error;
    }
    INUSE_CALL(
        column = (fmtnum < 2) ? Py_BuildValue(description_formats[fmtnum],
                                              column_name,
                                              sqlite3_column_decltype(INDEX),
                                              Py_None,
                                              Py_None,
                                              Py_None,
                                              Py_None,
                                              Py_None)
                              : DESCFMT2);
#undef INDEX
    if (!column)
      goto error;
    assert(!PyErr_Occurred());
    PyTuple_SET_ITEM(result, i, column);
    /* owned by result now */
    column = 0;
  }

  self->description_cache[fmtnum] = Py_NewRef(result);
  return result;

error:
  Py_XDECREF(result);
  Py_XDECREF(column);
  return NULL;
}

/** .. method:: get_description() -> tuple[tuple[str, str], ...]

   If you are trying to get information about a table or view,
   then `pragma table_info <https://sqlite.org/pragma.html#pragma_table_info>`__
   is better.  If you want to know up front what columns and other
   details a query does then :func:`apsw.ext.query_info` is useful.

   Returns a tuple describing each column in the result row.  The
   return is identical for every row of the results.

   The information about each column is a tuple of ``(column_name,
   declared_column_type)``.  The type is what was declared in the
   ``CREATE TABLE`` statement - the value returned in the row will be
   whatever type you put in for that row and column.

   See the :ref:`query_info example <example_query_details>`.

   -* sqlite3_column_name sqlite3_column_decltype

*/
static PyObject *APSWCursor_get_description(APSWCursor *self)
{
  return APSWCursor_internal_get_description(self, 0);
}

/** .. attribute:: description
    :type: tuple[tuple[str, str, None, None, None, None, None], ...]

    Based on the `DB-API cursor property
    <https://www.python.org/dev/peps/pep-0249/>`__, this returns the
    same as :meth:`get_description` but with 5 Nones appended because
    SQLite does not have the information.
*/

static PyObject *APSWCursor_getdescription_dbapi(APSWCursor *self)
{
  return APSWCursor_internal_get_description(self, 1);
}

/** .. attribute:: description_full
  :type: tuple[tuple[str, str, str, str, str], ...]

Only present if SQLITE_ENABLE_COLUMN_METADATA was defined at
compile time.

Returns all information about the query result columns. In
addition to the name and declared type, you also get the database
name, table name, and origin name.

-* sqlite3_column_name sqlite3_column_decltype sqlite3_column_database_name sqlite3_column_table_name sqlite3_column_origin_name

*/
#ifdef SQLITE_ENABLE_COLUMN_METADATA
static PyObject *APSWCursor_get_description_full(APSWCursor *self)
{
  return APSWCursor_internal_get_description(self, 2);
}
#endif

static int
APSWCursor_is_dict_binding(PyObject *obj)
{
  /* See https://github.com/rogerbinns/apsw/issues/373 for why this function exists */
  assert(obj);

  /* check the most common cases first */
  if (PyDict_CheckExact(obj))
    return 1;
  if (PyList_CheckExact(obj) || PyTuple_CheckExact(obj))
    return 0;

  /* possible but less likely */
  if (PyDict_Check(obj))
    return 1;
  if (PyList_Check(obj) || PyTuple_Check(obj))
    return 0;

  /* abstract base classes final answer */
  if (collections_abc_Mapping && PyObject_IsInstance(obj, collections_abc_Mapping) == 1)
    return 1;

  return 0;
}

/* internal function - returns SQLite error code (ie SQLITE_OK if all is well) */
static int
APSWCursor_dobinding(APSWCursor *self, int arg, PyObject *obj)
{

  /* DUPLICATE(ish) code: this is substantially similar to the code in
     set_context_result.  If you fix anything here then do it there as
     well. */

  int res = SQLITE_OK;

  assert(!PyErr_Occurred());

  if (Py_IsNone(obj))
    PYSQLITE_CUR_CALL(res = sqlite3_bind_null(self->statement->vdbestatement, arg));
  else if (PyLong_Check(obj))
  {
    /* nb: PyLong_AsLongLong can cause Python level error */
    long long v = PyLong_AsLongLong(obj);
    PYSQLITE_CUR_CALL(res = sqlite3_bind_int64(self->statement->vdbestatement, arg, v));
  }
  else if (PyFloat_Check(obj))
  {
    double v = PyFloat_AS_DOUBLE(obj);
    PYSQLITE_CUR_CALL(res = sqlite3_bind_double(self->statement->vdbestatement, arg, v));
  }
  else if (PyUnicode_Check(obj))
  {
    const char *strdata = NULL;
    Py_ssize_t strbytes = 0;
    strdata = PyUnicode_AsUTF8AndSize(obj, &strbytes);
    if (strdata)
    {
      PYSQLITE_CUR_CALL(res = sqlite3_bind_text64(self->statement->vdbestatement, arg, strdata, strbytes, SQLITE_TRANSIENT, SQLITE_UTF8));
    }
    else
    {
      assert(PyErr_Occurred());
      return -1;
    }
  }
  else if (PyObject_CheckBuffer(obj))
  {
    int asrb;
    Py_buffer py3buffer;

    asrb = PyObject_GetBufferContiguous(obj, &py3buffer, PyBUF_SIMPLE);
    if (asrb != 0)
      return -1;

    PYSQLITE_CUR_CALL(res = sqlite3_bind_blob64(self->statement->vdbestatement, arg, py3buffer.buf, py3buffer.len, SQLITE_TRANSIENT));
    PyBuffer_Release(&py3buffer);
  }
  else if (PyObject_TypeCheck(obj, &ZeroBlobBindType) == 1)
  {
    PYSQLITE_CUR_CALL(res = sqlite3_bind_zeroblob64(self->statement->vdbestatement, arg, ((ZeroBlobBind *)obj)->blobsize));
  }
  else
  {
    PyErr_Format(PyExc_TypeError, "Bad binding argument type supplied - argument #%d: type %s", (int)(arg + self->bindingsoffset), Py_TypeName(obj));
    AddTraceBackHere(__FILE__, __LINE__, "Cursor.dobinding", "{s: i, s: O}", "number", arg, "value", obj);
    return -1;
  }
  if (res != SQLITE_OK)
  {
    SET_EXC(res, self->connection->db);
    return -1;
  }
  if (PyErr_Occurred())
    return -1;
  return 0;
}

/* internal function */
static int
APSWCursor_dobindings(APSWCursor *self)
{
  int nargs, arg, sz = 0;
  PyObject *obj;

  assert(!PyErr_Occurred());
  assert(self->bindingsoffset >= 0);

  /* skip for null bindings */
  if (Py_Is(self->bindings, apsw_cursor_null_bindings))
    return 0;

  nargs = sqlite3_bind_parameter_count(self->statement->vdbestatement);
  if (nargs == 0 && !self->bindings)
    return 0; /* common case, no bindings needed or supplied */

  if (nargs > 0 && !self->bindings)
  {
    PyErr_Format(ExcBindings, "Statement has %d bindings but you didn't supply any!", nargs);
    return -1;
  }

  /* a dictionary? */
  if (self->bindings && APSWCursor_is_dict_binding(self->bindings))
  {
    for (arg = 1; arg <= nargs; arg++)
    {
      const char *key;

      key = sqlite3_bind_parameter_name(self->statement->vdbestatement, arg);

      if (!key)
      {
        PyErr_Format(ExcBindings, "Binding %d has no name, but you supplied a dict (which only has names).", arg - 1);
        return -1;
      }

      assert(*key == ':' || *key == '$' || *key == '@');
      key++; /* first char is a colon / dollar / at which we skip */

      if (PyDict_Check(self->bindings) && allow_missing_dict_bindings)
      {
        obj = PyDict_GetItemString(self->bindings, key);
        /* it returns a borrowed reference */
        Py_XINCREF(obj);
      }
      else
        obj = PyMapping_GetItemString(self->bindings, key);
      if (PyErr_Occurred())
      {
        Py_XDECREF(obj);
        return -1;
      }
      if (!obj)
      {
        /* missing keys allowed */
        assert(allow_missing_dict_bindings);
        continue;
      }
      if (APSWCursor_dobinding(self, arg, obj) != SQLITE_OK)
      {
        assert(PyErr_Occurred());
        Py_DECREF(obj);
        return -1;
      }
      Py_DECREF(obj);
    }

    return 0;
  }

  /* it must be a fast sequence */
  /* verify the number of args supplied */
  if (self->bindings)
    sz = PySequence_Fast_GET_SIZE(self->bindings);
  /* there is another statement after this one ... */
  if (statementcache_hasmore(self->statement) && sz - self->bindingsoffset < nargs)
  {
    PyErr_Format(ExcBindings, "Incorrect number of bindings supplied.  The current statement uses %d and there are only %d left.  Current offset is %d",
                 nargs, (self->bindings) ? sz : 0, (int)(self->bindingsoffset));
    return -1;
  }
  /* no more statements */
  if (!statementcache_hasmore(self->statement) && sz - self->bindingsoffset != nargs)
  {
    PyErr_Format(ExcBindings, "Incorrect number of bindings supplied.  The current statement uses %d and there are %d supplied.  Current offset is %d",
                 nargs, (self->bindings) ? sz : 0, (int)(self->bindingsoffset));
    return -1;
  }

  /* nb sqlite starts bind args at one not zero */
  for (arg = 1; arg <= nargs; arg++)
  {
    obj = PySequence_Fast_GET_ITEM(self->bindings, arg - 1 + self->bindingsoffset);
    if (APSWCursor_dobinding(self, arg, obj))
    {
      assert(PyErr_Occurred());
      return -1;
    }
  }

  self->bindingsoffset += nargs;
  return 0;
}

static int
APSWCursor_do_exec_trace(APSWCursor *self, Py_ssize_t savedbindingsoffset)
{
  PyObject *retval = NULL;
  PyObject *sqlcmd = NULL;
  PyObject *bindings = NULL;
  PyObject *exectrace;
  int result;

  exectrace = EXECTRACE;
  assert(exectrace);
  assert(self->statement);

  /* make a string of the command */
  sqlcmd = PyUnicode_FromStringAndSize(self->statement->utf8 ? self->statement->utf8 : "", self->statement->query_size);

  if (!sqlcmd)
    return -1;

  /* now deal with the bindings */
  if (self->bindings)
  {
    if (APSWCursor_is_dict_binding(self->bindings))
    {
      bindings = Py_NewRef(self->bindings);
    }
    else if (Py_Is(self->bindings, apsw_cursor_null_bindings))
    {
      bindings = Py_NewRef(Py_None);
    }
    else
    {
      bindings = PySequence_GetSlice(self->bindings, savedbindingsoffset, self->bindingsoffset);

      if (!bindings)
      {
        Py_DECREF(sqlcmd);
        return -1;
      }
    }
  }
  else
  {
    bindings = Py_NewRef(Py_None);
  }

  PyObject *vargs[] = {NULL, (PyObject *)self, sqlcmd, bindings};
  retval = PyObject_Vectorcall(exectrace, vargs + 1, 3 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_DECREF(sqlcmd);
  Py_DECREF(bindings);

  if (!retval)
  {
    assert(PyErr_Occurred());
    return -1;
  }
  result = PyObject_IsTrueStrict(retval);
  Py_DECREF(retval);
  assert(result == -1 || result == 0 || result == 1);
  if (result == -1)
  {
    assert(PyErr_Occurred());
    return -1;
  }
  if (result)
    return 0;

  /* callback didn't want us to continue */
  PyErr_Format(ExcTraceAbort, "Aborted by false/null return value of exec tracer");
  return -1;
}

static PyObject *
APSWCursor_do_row_trace(APSWCursor *self, PyObject *retval)
{
  PyObject *rowtrace = ROWTRACE;

  assert(rowtrace);

  PyObject *vargs[] = {NULL, (PyObject *)self, retval};
  return PyObject_Vectorcall(rowtrace, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
}

/* Returns a borrowed reference to self if all is ok, else NULL on error */
static PyObject *
APSWCursor_step(APSWCursor *self)
{
  int res;
  int savedbindingsoffset = 0; /* initialised to stop stupid compiler from whining */

  for (;;)
  {
    assert(!PyErr_Occurred());
    PYSQLITE_CUR_CALL(res = (self->statement->vdbestatement) ? (sqlite3_step(self->statement->vdbestatement)) : (SQLITE_DONE));

    switch (res & 0xff)
    {
    case SQLITE_ROW:
      self->status = C_ROW;
      return (PyErr_Occurred()) ? (NULL) : ((PyObject *)self);

    case SQLITE_DONE:
      if (PyErr_Occurred())
      {
        self->status = C_DONE;
        return NULL;
      }
      break;

    default:
      /* FALLTHRU */
    case SQLITE_ERROR: /* SQLITE_BUSY is handled here as well */
      /* there was an error - we need to get actual error code from sqlite3_finalize */
      self->status = C_DONE;
      if (PyErr_Occurred())
        /* we don't care about further errors from the sql */
        resetcursor(self, 1);
      else
      {
        res = resetcursor(self, 0); /* this will get the error code for us */
        assert(res != SQLITE_OK);
      }
      return NULL;
    }
    assert(res == SQLITE_DONE);

    /* done with that statement, are there any more? */
    self->status = C_DONE;
    if (!statementcache_hasmore(self->statement))
    {
      PyObject *next;

      /* in executemany mode ?*/
      if (!self->emiter)
      {
        /* no more so we finalize */
        res = resetcursor(self, 0);
        assert(res == SQLITE_OK);
        return (PyObject *)self;
      }

      /* we are in executemany mode */
      INUSE_CALL(next = PyIter_Next(self->emiter));
      if (PyErr_Occurred())
      {
        assert(!next);
        return NULL;
      }

      if (!next)
      {
        res = resetcursor(self, 0);
        assert(res == SQLITE_OK);
        return (PyObject *)self;
      }

      /* we need to clear just completed and restart original executemany statement */
      INUSE_CALL(statementcache_finalize(self->connection->stmtcache, self->statement));
      self->statement = NULL;
      /* don't need bindings from last round if emiter.next() */
      Py_CLEAR(self->bindings);
      self->bindingsoffset = 0;
      /* verify type of next before putting in bindings */
      if (APSWCursor_is_dict_binding(next))
        self->bindings = next;
      else
      {
        self->bindings = PySequence_Fast(next, "You must supply a dict or a sequence for bindings");
        /* we no longer need next irrespective of what happens in line above */
        Py_DECREF(next);
        if (!self->bindings)
          return NULL;
      }
      assert(self->bindings);
    }

    /* finalise and go again */
    if (!self->statement)
    {
      /* we are going again in executemany mode */
      assert(self->emiter);
      INUSE_CALL(self->statement = statementcache_prepare(self->connection->stmtcache, self->emoriginalquery, &self->emoptions));
      res = (self->statement) ? SQLITE_OK : SQLITE_ERROR;
    }
    else
    {
      /* next sql statement */
      INUSE_CALL(res = statementcache_next(self->connection->stmtcache, &self->statement));
      SET_EXC(res, self->connection->db);
    }

    if (res != SQLITE_OK)
    {
      assert((res & 0xff) != SQLITE_BUSY); /* finalize shouldn't be returning busy, only step */
      assert(!self->statement);
      return NULL;
    }

    assert(self->statement);
    savedbindingsoffset = self->bindingsoffset;

    assert(!PyErr_Occurred());

    Py_CLEAR(self->description_cache[0]);
    Py_CLEAR(self->description_cache[1]);
    Py_CLEAR(self->description_cache[2]);

    if (APSWCursor_dobindings(self))
    {
      assert(PyErr_Occurred());
      return NULL;
    }

    if (EXECTRACE)
    {
      if (APSWCursor_do_exec_trace(self, savedbindingsoffset))
      {
        assert(self->status == C_DONE);
        assert(PyErr_Occurred());
        return NULL;
      }
    }
    assert(self->status == C_DONE);
    self->status = C_BEGIN;
  }

  /* you can't actually get here */
  assert(0);
  return NULL;
}

/** .. method:: execute(statements: str, bindings: Optional[Bindings] = None, *, can_cache: bool = True, prepare_flags: int = 0, explain: int = -1) -> Cursor

    Executes the statements using the supplied bindings.  Execution
    returns when the first row is available or all statements have
    completed.

    :param statements: One or more SQL statements such as ``select *
      from books`` or ``begin; insert into books ...; select
      last_insert_rowid(); end``.
    :param bindings: If supplied should either be a sequence or a dictionary.  Each item must be one of the :ref:`supported types <types>`
    :param can_cache: If False then the statement cache will not be used to find an already prepared query, nor will it be
      placed in the cache after execution
    :param prepare_flags: `flags <https://sqlite.org/c3ref/c_prepare_normalize.html>`__ passed to
      `sqlite_prepare_v3 <https://sqlite.org/c3ref/prepare.html>`__
    :param explain: If 0 or greater then the statement is passed to `sqlite3_stmt_explain <https://sqlite.org/c3ref/stmt_explain.html>`__
       where you can force it to not be an explain, or force explain or explain query plan.

    :raises TypeError: The bindings supplied were neither a dict nor a sequence
    :raises BindingsError: You supplied too many or too few bindings for the statements
    :raises IncompleteExecutionError: There are remaining unexecuted queries from your last execute

    -* sqlite3_prepare_v3 sqlite3_step sqlite3_bind_int64 sqlite3_bind_null sqlite3_bind_text64 sqlite3_bind_double sqlite3_bind_blob64 sqlite3_bind_zeroblob

    .. seealso::

       * :ref:`Example <example_executing_sql>` showing how to use bindings
       * :ref:`executionmodel`

*/
static PyObject *
APSWCursor_execute(APSWCursor *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int res;
  int savedbindingsoffset = -1;
  int prepare_flags = 0;
  int can_cache = 1;
  int explain = -1;
  PyObject *retval = NULL;
  PyObject *statements, *bindings = NULL;
  APSWStatementOptions options;

  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  res = resetcursor(self, /* force= */ 0);
  if (res != SQLITE_OK)
  {
    assert(PyErr_Occurred());
    return NULL;
  }

  assert(!self->bindings);
  {
    Cursor_execute_CHECK;
    ARG_PROLOG(2, Cursor_execute_KWNAMES);
    ARG_MANDATORY ARG_PyUnicode(statements);
    ARG_OPTIONAL ARG_optional_Bindings(bindings);
    ARG_OPTIONAL ARG_bool(can_cache);
    ARG_OPTIONAL ARG_int(prepare_flags);
    ARG_OPTIONAL ARG_int(explain);
    ARG_EPILOG(NULL, Cursor_execute_USAGE, );
  }
  self->bindings = bindings;

  options.can_cache = can_cache;
  options.prepare_flags = prepare_flags;
  options.explain = explain;

  if (self->bindings)
  {
    if (APSWCursor_is_dict_binding(self->bindings) || Py_Is(self->bindings, apsw_cursor_null_bindings))
      Py_INCREF(self->bindings);
    else
    {
      self->bindings = PySequence_Fast(self->bindings, "You must supply a dict or a sequence for execute");
      if (!self->bindings)
        return NULL;
    }
  }

  assert(!self->statement);
  assert(!PyErr_Occurred());
  INUSE_CALL(self->statement = statementcache_prepare(self->connection->stmtcache, statements, &options));
  if (!self->statement)
  {
    AddTraceBackHere(__FILE__, __LINE__, "APSWCursor_execute.sqlite3_prepare_v3", "{s: O, s: O}",
                     "Connection", self->connection,
                     "statement", OBJ(statements));
    return NULL;
  }
  assert(!PyErr_Occurred());

  self->bindingsoffset = 0;
  savedbindingsoffset = 0;

  if (APSWCursor_dobindings(self))
  {
    assert(PyErr_Occurred());
    return NULL;
  }

  if (EXECTRACE)
  {
    if (APSWCursor_do_exec_trace(self, savedbindingsoffset))
    {
      assert(PyErr_Occurred());
      return NULL;
    }
  }

  self->status = C_BEGIN;

  retval = APSWCursor_step(self);
  if (!retval)
  {
    assert(PyErr_Occurred());
    return NULL;
  }
  return Py_NewRef(retval);
}

/** .. method:: executemany(statements: str, sequenceofbindings: Iterable[Bindings], *, can_cache: bool = True, prepare_flags: int = 0, explain: int = -1) -> Cursor

  This method is for when you want to execute the same statements over
  a sequence of bindings.  Conceptually it does this::

    for binding in sequenceofbindings:
        cursor.execute(statements, binding)

  The return is the cursor itself which acts as an iterator.  Your
  statements can return data.  See :meth:`~Cursor.execute` for more
  information, and the :ref:`example <example_executemany>`.

*/

static PyObject *
APSWCursor_executemany(APSWCursor *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int res;
  PyObject *retval = NULL;
  PyObject *sequenceofbindings = NULL;
  PyObject *next = NULL;
  PyObject *statements = NULL;
  int savedbindingsoffset = -1;
  int can_cache = 1;
  int prepare_flags = 0;
  int explain = -1;

  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  res = resetcursor(self, /* force= */ 0);
  if (res != SQLITE_OK)
  {
    assert(PyErr_Occurred());
    return NULL;
  }

  assert(!self->bindings);
  assert(!self->emiter);
  assert(!self->emoriginalquery);
  assert(self->status == C_DONE);
  {
    Cursor_executemany_CHECK;
    ARG_PROLOG(2, Cursor_executemany_KWNAMES);
    ARG_MANDATORY ARG_PyUnicode(statements);
    ARG_MANDATORY ARG_pyobject(sequenceofbindings);
    ARG_OPTIONAL ARG_bool(can_cache);
    ARG_OPTIONAL ARG_int(prepare_flags);
    ARG_OPTIONAL ARG_int(explain);
    ARG_EPILOG(NULL, Cursor_executemany_USAGE, );
  }
  self->emiter = PyObject_GetIter(sequenceofbindings);
  if (!self->emiter)
  {
    assert(PyErr_Occurred());
    return NULL;
  }

  INUSE_CALL(next = PyIter_Next(self->emiter));
  if (!next && PyErr_Occurred())
    return NULL;
  if (!next)
  {
    /* empty list */
    return Py_NewRef((PyObject *)self);
  }

  if (APSWCursor_is_dict_binding(next))
    self->bindings = next;
  else
  {
    self->bindings = PySequence_Fast(next, "You must supply a dict or a sequence for executemany");
    Py_DECREF(next); /* _Fast makes new reference */
    if (!self->bindings)
      return NULL;
  }

  self->emoptions.can_cache = can_cache;
  self->emoptions.prepare_flags = prepare_flags;
  self->emoptions.explain = explain;

  assert(!self->statement);
  assert(!PyErr_Occurred());
  assert(!self->statement);
  INUSE_CALL(self->statement = statementcache_prepare(self->connection->stmtcache, statements, &self->emoptions));
  if (!self->statement)
  {
    AddTraceBackHere(__FILE__, __LINE__, "APSWCursor_executemany.sqlite3_prepare_v3", "{s: O, s: O}",
                     "Connection", self->connection,
                     "statements", OBJ(statements));
    return NULL;
  }
  assert(!PyErr_Occurred());

  self->emoriginalquery = Py_NewRef(statements);

  self->bindingsoffset = 0;
  savedbindingsoffset = 0;

  if (APSWCursor_dobindings(self))
  {
    assert(PyErr_Occurred());
    return NULL;
  }

  if (EXECTRACE)
  {
    if (APSWCursor_do_exec_trace(self, savedbindingsoffset))
    {
      assert(PyErr_Occurred());
      return NULL;
    }
  }

  self->status = C_BEGIN;

  retval = APSWCursor_step(self);
  if (!retval)
  {
    assert(PyErr_Occurred());
    return NULL;
  }
  return Py_NewRef(retval);
}

/** .. method:: close(force: bool = False) -> None

  It is very unlikely you will need to call this method.
  Cursors are automatically garbage collected and when there
  are none left will allow the connection to be garbage collected if
  it has no other references.

  A cursor is open if there are remaining statements to execute (if
  your query included multiple statements), or if you called
  :meth:`~Cursor.executemany` and not all of the sequence of bindings
  have been used yet.

  :param force: If False then you will get exceptions if there is
   remaining work to do be in the Cursor such as more statements to
   execute, more data from the executemany binding sequence etc. If
   force is True then all remaining work and state information will be
   silently discarded.

*/

static PyObject *
APSWCursor_close(APSWCursor *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int force = 0;

  CHECK_USE(NULL);
  if (!self->connection)
    Py_RETURN_NONE;

  {
    Cursor_close_CHECK;
    ARG_PROLOG(1, Cursor_close_KWNAMES);
    ARG_OPTIONAL ARG_bool(force);
    ARG_EPILOG(NULL, Cursor_close_USAGE, );
  }
  APSWCursor_close_internal(self, !!force);

  if (PyErr_Occurred())
    return NULL;

  Py_RETURN_NONE;
}

/** .. method:: __next__(self: Cursor) -> Any

    Cursors are iterators
*/
static PyObject *
APSWCursor_next(APSWCursor *self)
{
  PyObject *retval;
  PyObject *item;
  int numcols = -1;
  int i;

  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

again:
  if (self->status == C_BEGIN)
    if (!APSWCursor_step(self))
    {
      assert(PyErr_Occurred());
      return NULL;
    }
  if (self->status == C_DONE)
    return NULL;

  assert(self->status == C_ROW);

  self->status = C_BEGIN;

  /* return the row of data */
  numcols = sqlite3_data_count(self->statement->vdbestatement);
  retval = PyTuple_New(numcols);
  if (!retval)
    goto error;

  for (i = 0; i < numcols; i++)
  {
    INUSE_CALL(item = convert_column_to_pyobject(self->statement->vdbestatement, i));
    if (!item)
      goto error;
    PyTuple_SET_ITEM(retval, i, item);
  }
  if (ROWTRACE)
  {
    PyObject *r2 = APSWCursor_do_row_trace(self, retval);
    Py_DECREF(retval);
    if (!r2)
      return NULL;
    if (Py_IsNone(r2))
    {
      Py_DECREF(r2);
      goto again;
    }
    return r2;
  }
  return retval;
error:
  Py_XDECREF(retval);
  return NULL;
}

/** .. method:: __iter__(self: Cursor) -> Cursor

    Cursors are iterators
*/

static PyObject *
APSWCursor_iter(APSWCursor *self)
{
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  return Py_NewRef((PyObject *)self);
}

/** .. method:: set_exec_trace(callable: Optional[ExecTracer]) -> None

  Sets the :attr:`execution tracer <Cursor.exec_trace>`
*/
static PyObject *
APSWCursor_set_exec_trace(APSWCursor *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *callable = NULL;
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  {
    Cursor_set_exec_trace_CHECK;
    ARG_PROLOG(1, Cursor_set_exec_trace_KWNAMES);
    ARG_MANDATORY ARG_optional_Callable(callable);
    ARG_EPILOG(NULL, Cursor_set_exec_trace_USAGE, );
  }

  Py_XINCREF(callable);
  Py_XDECREF(self->exectrace);
  self->exectrace = callable;

  Py_RETURN_NONE;
}

/** .. method:: set_row_trace(callable: Optional[RowTracer]) -> None

  Sets the :attr:`row tracer <Cursor.row_trace>`
*/

static PyObject *
APSWCursor_set_row_trace(APSWCursor *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *callable = NULL;
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  {
    Cursor_set_row_trace_CHECK;
    ARG_PROLOG(1, Cursor_set_row_trace_KWNAMES);
    ARG_MANDATORY ARG_optional_Callable(callable);
    ARG_EPILOG(NULL, Cursor_set_row_trace_USAGE, );
  }

  Py_XINCREF(callable);
  Py_XDECREF(self->rowtrace);
  self->rowtrace = callable;

  Py_RETURN_NONE;
}

/** .. method:: get_exec_trace() -> Optional[ExecTracer]

  Returns the currently installed :attr:`execution tracer
  <Cursor.exec_trace>`

  .. seealso::

    * :ref:`tracing`
*/
static PyObject *
APSWCursor_get_exec_trace(APSWCursor *self)
{
  PyObject *ret;

  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  ret = (self->exectrace) ? (self->exectrace) : Py_None;
  return Py_NewRef(ret);
}

/** .. method:: get_row_trace() -> Optional[RowTracer]

  Returns the currently installed (via :meth:`~Cursor.set_row_trace`)
  row tracer.

  .. seealso::

    * :ref:`tracing`
*/
static PyObject *
APSWCursor_get_row_trace(APSWCursor *self)
{
  PyObject *ret;
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);
  ret = (self->rowtrace) ? (self->rowtrace) : Py_None;
  return Py_NewRef(ret);
}

/** .. method:: get_connection() -> Connection

  Returns the :attr:`connection` this cursor is part of
*/

static PyObject *
APSWCursor_get_connection(APSWCursor *self)
{
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  return Py_NewRef((PyObject *)self->connection);
}

/** .. method:: fetchall() -> list[tuple[SQLiteValue, ...]]

  Returns all remaining result rows as a list.  This method is defined
  in DBAPI.  See :meth:`get` which does the same thing, but with the least
  amount of structure to unpack.
*/
static PyObject *
APSWCursor_fetchall(APSWCursor *self)
{
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  return PySequence_List((PyObject *)self);
}

/** .. method:: fetchone() -> Optional[Any]

  Returns the next row of data or None if there are no more rows.
*/

static PyObject *
APSWCursor_fetchone(APSWCursor *self)
{
  PyObject *res;

  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  res = APSWCursor_next(self);

  if (res == NULL && !PyErr_Occurred())
    Py_RETURN_NONE;

  return res;
}

/** .. attribute:: exec_trace
  :type: Optional[ExecTracer]

  Called with the cursor, statement and bindings for
  each :meth:`~Cursor.execute` or :meth:`~Cursor.executemany` on this
  cursor.

  If *callable* is *None* then any existing execution tracer is
  unregistered.

  .. seealso::

    * :ref:`tracing`
    * :ref:`executiontracer`
    * :attr:`Connection.exec_trace`

*/
static PyObject *
APSWCursor_get_exec_trace_attr(APSWCursor *self)
{
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  if (self->exectrace)
    return Py_NewRef(self->exectrace);
  Py_RETURN_NONE;
}

static int
APSWCursor_set_exec_trace_attr(APSWCursor *self, PyObject *value)
{
  CHECK_USE(-1);
  CHECK_CURSOR_CLOSED(-1);

  if (!Py_IsNone(value) && !PyCallable_Check(value))
  {
    PyErr_Format(PyExc_TypeError, "exec_trace expected a Callable");
    return -1;
  }
  Py_CLEAR(self->exectrace);
  if (!Py_IsNone(value))
    self->exectrace = Py_NewRef(value);
  return 0;
}

/** .. attribute:: row_trace
  :type: Optional[RowTracer]

  Called with cursor and row being returned.  You can
  change the data that is returned or cause the row to be skipped
  altogether.

  If *callable* is *None* then any existing row tracer is
  unregistered.

  .. seealso::

    * :ref:`tracing`
    * :ref:`rowtracer`
    * :attr:`Connection.row_trace`

*/
static PyObject *
APSWCursor_get_row_trace_attr(APSWCursor *self)
{
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  if (self->rowtrace)
    return Py_NewRef(self->rowtrace);
  Py_RETURN_NONE;
}

static int
APSWCursor_set_row_trace_attr(APSWCursor *self, PyObject *value)
{
  CHECK_USE(-1);
  CHECK_CURSOR_CLOSED(-1);

  if (!Py_IsNone(value) && !PyCallable_Check(value))
  {
    PyErr_Format(PyExc_TypeError, "rowtrace expected a Callable");
    return -1;
  }
  Py_CLEAR(self->rowtrace);
  if (!Py_IsNone(value))
    self->rowtrace = Py_NewRef(value);
  return 0;
}

/** .. attribute:: connection
  :type: Connection

  :class:`Connection` this cursor is using
*/
static PyObject *
APSWCursor_get_connection_attr(APSWCursor *self)
{
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  return Py_NewRef((PyObject *)self->connection);
}

/** .. attribute:: bindings_count
  :type: int

  How many bindings are in the statement.  The ``?`` form
  results in the largest number.  For example you could do
  ``SELECT ?123``` in which case the count will be ``123``.

  -* sqlite3_bind_parameter_count
*/
static PyObject *
APSWCursor_bindings_count(APSWCursor *self)
{
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  return PyLong_FromLong((self->statement) ? sqlite3_bind_parameter_count(self->statement->vdbestatement) : 0);
}

/** .. attribute:: bindings_names
  :type: tuple[str | None]

  A tuple of the name of each bind parameter, or None for no name.  The
  leading marker (``?:@$``) is omitted

  .. note::

    SQLite parameter numbering starts at ``1``, while Python
    indexing starts at ``0``.

  -* sqlite3_bind_parameter_name
*/
static PyObject *
APSWCursor_bindings_names(APSWCursor *self)
{
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  int count = (self->statement) ? sqlite3_bind_parameter_count(self->statement->vdbestatement) : 0;

  PyObject *res = PyTuple_New(count);
  if (!res)
    goto error;

  for (int i = 1; i <= count; i++)
  {
    const char *name = sqlite3_bind_parameter_name(self->statement->vdbestatement, i);

    PyObject *val = name ? PyUnicode_FromString(name + 1) : Py_NewRef(Py_None);
    if (!val)
      goto error;
    PyTuple_SET_ITEM(res, i - 1, val);
  }

  return res;
error:
  Py_XDECREF(res);
  return NULL;
}

/** .. attribute:: is_explain
  :type: int

  Returns 0 if executing a normal query, 1 if it is an EXPLAIN query,
  and 2 if an EXPLAIN QUERY PLAN query.

  -* sqlite3_stmt_isexplain
*/
static PyObject *
APSWCursor_is_explain(APSWCursor *self)
{
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  return PyLong_FromLong((self->statement) ? sqlite3_stmt_isexplain(self->statement->vdbestatement) : 0);
}

/** .. attribute:: is_readonly
  :type: bool

  Returns True if the current query does not change the database.

  Note that called functions, virtual tables etc could make changes though.

  -* sqlite3_stmt_readonly
*/
static PyObject *
APSWCursor_is_readonly(APSWCursor *self)
{
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  if (!self->statement || sqlite3_stmt_readonly(self->statement->vdbestatement))
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

/** .. attribute:: has_vdbe
  :type: bool

  ``True`` if the SQL does anything.  Comments have nothing to
  evaluate, and so are ``False``.
*/
static PyObject *
APSWCursor_has_vdbe(APSWCursor *self)
{
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  return Py_NewRef(
      (self->statement && self->statement->vdbestatement)
          ? Py_True
          : Py_False);
}

/** .. attribute:: expanded_sql
  :type: str

  The SQL text with bound parameters expanded.  For example::

     execute("select ?, ?", (3, "three"))

  would return::

     select 3, 'three'

  Note that while SQLite supports nulls in strings, their implementation
  of sqlite3_expanded_sql stops at the first null.

  You will get :exc:`MemoryError` if SQLite ran out of memory, or if
  the expanded string would exceed `SQLITE_LIMIT_LENGTH
  <https://www.sqlite.org/c3ref/c_limit_attached.html>`__.

  -* sqlite3_expanded_sql
*/
static PyObject *
APSWCursor_expanded_sql(APSWCursor *self)
{
  PyObject *res;
  const char *es;
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  if (!self->statement)
    Py_RETURN_NONE;

  PYSQLITE_VOID_CALL(es = sqlite3_expanded_sql(self->statement->vdbestatement));
  if (!es)
    return PyErr_NoMemory();
  res = convertutf8string(es);
  sqlite3_free((void *)es);
  return res;
}

/** .. attribute:: get
 :type: Any

 Like :meth:`fetchall` but returns the data with the least amount of structure
 possible.

 .. list-table:: Some examples
    :header-rows: 1
    :widths: auto

    * - Query
      - Result
    * - select 3
      - 3
    * - select 3,4
      - (3, 4)
    * - select 3; select 4
      - [3, 4]
    * - select 3,4; select 4,5
      - [(3, 4), (4, 5)]
    * - select 3,4; select 5
      - [(3, 4), 5]

 Row tracers are not called when using this method.
*/

static PyObject *
APSWCursor_get(APSWCursor *self)
{
  PyObject *the_list = NULL, *the_row = NULL;
  PyObject *step, *item;
  int numcols, i;

  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  if (self->status == C_DONE)
    Py_RETURN_NONE;

  do
  {
    assert(self->status == C_ROW);
    if (the_row)
    {
      assert(!the_list);
      the_list = PyList_New(0);
      if (!the_list)
        goto error;
      if (0 != PyList_Append(the_list, the_row))
        goto error;
      Py_CLEAR(the_row);
    }
    numcols = sqlite3_data_count(self->statement->vdbestatement);
    if (numcols == 1)
    {
      INUSE_CALL(the_row = convert_column_to_pyobject(self->statement->vdbestatement, 0));
      if (!the_row)
        goto error;
    }
    else
    {
      the_row = PyTuple_New(numcols);
      if (!the_row)
        goto error;
      for (i = 0; i < numcols; i++)
      {
        INUSE_CALL(item = convert_column_to_pyobject(self->statement->vdbestatement, i));
        if (!item)
          goto error;
        PyTuple_SET_ITEM(the_row, i, item);
      }
    }
    if (the_list)
    {
      if (0 != PyList_Append(the_list, the_row))
        goto error;
      Py_CLEAR(the_row);
    }
    step = APSWCursor_step(self);
    if (step == NULL)
      goto error;
  } while (self->status != C_DONE);

  if (the_list)
    return the_list;
  assert(the_row);
  return the_row;

error:
  assert(PyErr_Occurred());
  Py_CLEAR(the_list);
  Py_CLEAR(the_row);
  return NULL;
}

static PyObject *
APSWCursor_tp_str(APSWCursor *self)
{
  return PyUnicode_FromFormat("<apsw.Cursor object from %S at %p>",
                              self->connection ? (PyObject *)self->connection : apst.closed,
                              self);
}

static PyMethodDef APSWCursor_methods[] = {
    {"execute", (PyCFunction)APSWCursor_execute, METH_FASTCALL | METH_KEYWORDS,
     Cursor_execute_DOC},
    {"executemany", (PyCFunction)APSWCursor_executemany, METH_FASTCALL | METH_KEYWORDS,
     Cursor_executemany_DOC},
    {"set_exec_trace", (PyCFunction)APSWCursor_set_exec_trace, METH_FASTCALL | METH_KEYWORDS,
     Cursor_set_exec_trace_DOC},
    {"set_row_trace", (PyCFunction)APSWCursor_set_row_trace, METH_FASTCALL | METH_KEYWORDS,
     Cursor_set_row_trace_DOC},
    {"get_exec_trace", (PyCFunction)APSWCursor_get_exec_trace, METH_NOARGS,
     Cursor_get_exec_trace_DOC},
    {"get_row_trace", (PyCFunction)APSWCursor_get_row_trace, METH_NOARGS,
     Cursor_get_row_trace_DOC},
    {"get_connection", (PyCFunction)APSWCursor_get_connection, METH_NOARGS,
     Cursor_get_connection_DOC},
    {"get_description", (PyCFunction)APSWCursor_get_description, METH_NOARGS,
     Cursor_get_description_DOC},
    {"close", (PyCFunction)APSWCursor_close, METH_FASTCALL | METH_KEYWORDS,
     Cursor_close_DOC},
    {"fetchall", (PyCFunction)APSWCursor_fetchall, METH_NOARGS,
     Cursor_fetchall_DOC},
    {"fetchone", (PyCFunction)APSWCursor_fetchone, METH_NOARGS,
     Cursor_fetchone_DOC},
#ifndef APSW_OMIT_OLD_NAMES
    {Cursor_set_exec_trace_OLDNAME, (PyCFunction)APSWCursor_set_exec_trace, METH_FASTCALL | METH_KEYWORDS,
     Cursor_set_exec_trace_OLDDOC},
    {Cursor_set_row_trace_OLDNAME, (PyCFunction)APSWCursor_set_row_trace, METH_FASTCALL | METH_KEYWORDS,
     Cursor_set_row_trace_OLDDOC},
    {Cursor_get_exec_trace_OLDNAME, (PyCFunction)APSWCursor_get_exec_trace, METH_NOARGS,
     Cursor_get_exec_trace_OLDDOC},
    {Cursor_get_row_trace_OLDNAME, (PyCFunction)APSWCursor_get_row_trace, METH_NOARGS,
     Cursor_get_row_trace_OLDDOC},
    {Cursor_get_connection_OLDNAME, (PyCFunction)APSWCursor_get_connection, METH_NOARGS,
     Cursor_get_connection_OLDDOC},
    {Cursor_get_description_OLDNAME, (PyCFunction)APSWCursor_get_description, METH_NOARGS,
     Cursor_get_description_OLDDOC},
#endif
    {0, 0, 0, 0} /* Sentinel */
};

static PyGetSetDef APSWCursor_getset[] = {
    {"description", (getter)APSWCursor_getdescription_dbapi, NULL, Cursor_description_DOC, NULL},
#ifdef SQLITE_ENABLE_COLUMN_METADATA
    {"description_full", (getter)APSWCursor_get_description_full, NULL, Cursor_description_full_DOC, NULL},
#endif
    {"is_explain", (getter)APSWCursor_is_explain, NULL, Cursor_is_explain_DOC, NULL},
    {"is_readonly", (getter)APSWCursor_is_readonly, NULL, Cursor_is_readonly_DOC, NULL},
    {"has_vdbe", (getter)APSWCursor_has_vdbe, NULL, Cursor_has_vdbe_DOC, NULL},
    {"bindings_count", (getter)APSWCursor_bindings_count, NULL, Cursor_bindings_count_DOC, NULL},
    {"bindings_names", (getter)APSWCursor_bindings_names, NULL, Cursor_bindings_names_DOC, NULL},
    {"expanded_sql", (getter)APSWCursor_expanded_sql, NULL, Cursor_expanded_sql_DOC, NULL},
    {"exec_trace", (getter)APSWCursor_get_exec_trace_attr, (setter)APSWCursor_set_exec_trace_attr, Cursor_exec_trace_DOC},
    {Cursor_exec_trace_OLDNAME, (getter)APSWCursor_get_exec_trace_attr, (setter)APSWCursor_set_exec_trace_attr, Cursor_exec_trace_OLDDOC},
    {"row_trace", (getter)APSWCursor_get_row_trace_attr, (setter)APSWCursor_set_row_trace_attr, Cursor_row_trace_DOC},
    {Cursor_row_trace_OLDNAME, (getter)APSWCursor_get_row_trace_attr, (setter)APSWCursor_set_row_trace_attr, Cursor_row_trace_OLDDOC},
    {"connection", (getter)APSWCursor_get_connection_attr, NULL, Cursor_connection_DOC},
    {"get", (getter)APSWCursor_get, NULL, Cursor_get_DOC},
    {NULL, NULL, NULL, NULL, NULL}};

static PyTypeObject APSWCursorType = {
    PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "apsw.Cursor",
    .tp_basicsize = sizeof(APSWCursor),
    .tp_dealloc = (destructor)APSWCursor_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC,
    .tp_doc = Cursor_class_DOC,
    .tp_traverse = (traverseproc)APSWCursor_tp_traverse,
    .tp_weaklistoffset = offsetof(APSWCursor, weakreflist),
    .tp_iter = (getiterfunc)APSWCursor_iter,
    .tp_iternext = (iternextfunc)APSWCursor_next,
    .tp_methods = APSWCursor_methods,
    .tp_getset = APSWCursor_getset,
    .tp_init = (initproc)APSWCursor_init,
    .tp_new = APSWCursor_new,
    .tp_str = (reprfunc)APSWCursor_tp_str,
};
