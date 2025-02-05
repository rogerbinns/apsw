/**

Session extension
*****************

APSW provides access to all stable session functionality.

The `session extension <https://www.sqlite.org/sessionintro.html>`__
allows recording changes to a database, and later replaying them on
another database, or undoing them.  This allows offline syncing, as
well as collaboration.  It is also useful for debugging, development,
and testing.

Notable features include:

* You can choose which tables have changes recorded (or all), and
  pause / resume recording at any time

* The recorded change set includes the row values before and after a
  change.  This allows comprehensive conflict detection, and inverting
  (undoing the change),  Optionally you can use patch sets (a subset of
  change sets) which do not have the before values, consuming less
  space but have less ability to detect conflicts or be inverted.

* When applying changes you can supply a conflict handler to choose
  what happens on each conflict, including aborting, skipping,
  applying anyway, applying your own change, and can record the
  conflicting operation to another change set for later.

* You can iterate over a change set to see what it contains

* Using the change set builder, you can accumulate multiple change
  sets, and add changes from an iterator or conflict handler.

.. important::

    By default Session can only record and replay changes that have an
    explicit `primary key <https://www.sqlite.org/lang_createtable.html#the_primary_key>`__
    defined (ie ``PRIMARY KEY`` must be present in the table definition).
    It doesn't matter what type or how many columns make up the primary key.
    This provides a stable way to identify rows for insertion, changes, and
    deletion.

    You can use :meth:`Session.config` with `SQLITE_SESSION_OBJCONFIG_ROWID
    <https://www.sqlite.org/session/c_session_objconfig_rowid.html>`__
    to enable recording of tables without an explicit primary key.

Availability
============

The session extension and APSW support for it have to be enabled at
compile time for each.  APSW builds from PyPI include session support.

Most platform provided SQLite are configured with session support, and
APSW should end up with it too.

The methods and classes documented here are only present if session
support was enabled.

Extension configuration
=======================

 */

/** .. method:: session_config(op: int, *args: Any) -> Any

 :param op: One of the `sqlite3session options <https://www.sqlite.org/session/c_session_config_strmsize.html>`__
 :param args: Zero or more arguments as appropriate for *op*

  -* sqlite3session_config

 */

static PyObject *
apsw_session_config(PyObject *Py_UNUSED(self), PyObject *args)
{
  int res, opt;
  if (PyTuple_GET_SIZE(args) < 1 || !PyLong_Check(PyTuple_GET_ITEM(args, 0)))
    return PyErr_Format(PyExc_TypeError, "There should be at least one argument with the first being a number");

  opt = PyLong_AsInt(PyTuple_GET_ITEM(args, 0));
  if (PyErr_Occurred())
    return NULL;

  switch (opt)
  {
  case SQLITE_SESSION_CONFIG_STRMSIZE: {
    int optdup, stream_size;
    if (!PyArg_ParseTuple(args, "ii", &optdup, &stream_size))
      return NULL;
    res = sqlite3session_config(opt, &stream_size);
    SET_EXC(res, NULL);
    if (PyErr_Occurred())
      return NULL;
    return PyLong_FromLong(stream_size);
  }
  default:
    return PyErr_Format(PyExc_ValueError, "Unknown config option %d", (int)opt);
  }
}

/** .. class:: Session

  This object wraps a `sqlite3_session
  <https://www.sqlite.org/session/session.html>`__ object.

*/

#define CHECK_SESSION_CLOSED(e)                                                                                        \
  do                                                                                                                   \
  {                                                                                                                    \
    if (!self->session)                                                                                                \
    {                                                                                                                  \
      PyErr_Format(ExcSessionClosed, "The session has been closed");                                                   \
      return e;                                                                                                        \
    }                                                                                                                  \
  } while (0)

typedef struct APSWSession
{
  PyObject_HEAD

  sqlite3_session *session;
  Connection *connection;
  PyObject *table_filter;

  int init_was_called;

  PyObject *weakreflist;
} APSWSession;

static PyTypeObject APSWSessionType;

typedef struct APSWChangeset
{
  PyObject_HEAD
} APSWChangeset;

static PyTypeObject APSWChangesetType;

typedef struct APSWChangesetBuilder
{
  PyObject_HEAD
  sqlite3_changegroup *group;
} APSWChangesetBuilder;

static PyTypeObject APSWChangesetBuilderType;

typedef struct APSWTableChange
{
  PyObject_HEAD
  sqlite3_changeset_iter *iter;
} APSWTableChange;

static PyTypeObject APSWTableChangeType;

/** .. method:: __init__(db: Connection, schema: str)

  Starts a new session.

  :param connection: Which database to operate on
  :param schema: `main`, `temp`, the name in `ATTACH <https://sqlite.org/lang_attach.html>`__
*/
static int
APSWSession_init(APSWSession *self, PyObject *args, PyObject *kwargs)
{
  Connection *db = NULL;
  const char *schema = NULL;

  {
    Session_init_CHECK;
    PREVENT_INIT_MULTIPLE_CALLS;
    ARG_CONVERT_VARARGS_TO_FASTCALL;
    ARG_PROLOG(2, Session_init_KWNAMES);
    ARG_MANDATORY ARG_Connection(db);
    ARG_MANDATORY ARG_str(schema);
    ARG_EPILOG(-1, Session_init_USAGE, Py_XDECREF(fast_kwnames));
  }

  CHECK_CLOSED(db, -1);

  int rc = sqlite3session_create(db->db, schema, &self->session);
  if (rc != SQLITE_OK)
  {
    SET_EXC(rc, NULL);
    return -1;
  }

  self->init_was_called = 1;

  self->connection = db;
  Py_INCREF(self->connection);

  PyObject *weakref = NULL;

  weakref = PyWeakref_NewRef((PyObject *)self, NULL);
  if (!weakref)
    goto error;
  if (PyList_Append(db->dependents, weakref))
    goto error;

  Py_DECREF(weakref);

  assert(!PyErr_Occurred());

  return 0;

error:
  assert(PyErr_Occurred());
  Py_XDECREF(weakref);
  return -1;
}

static void
APSWSession_close_internal(APSWSession *self)
{
  if (self->session)
  {
    sqlite3session_delete(self->session);
    self->session = NULL;
  }

  Py_CLEAR(self->table_filter);

  if (self->connection)
    Connection_remove_dependent(self->connection, (PyObject *)self);
  Py_CLEAR(self->connection);
}

/** .. method:: close() -> None

  Ends the session object.  APSW ensures that all
  Session objects are closed before the database is closed
  so there is no need to manually call this.

  -* sqlite3session_delete
*/
static PyObject *
APSWSession_close(APSWSession *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  {
    Session_close_CHECK;
    ARG_PROLOG(0, Session_close_KWNAMES);
    ARG_EPILOG(NULL, Session_close_USAGE, );
  }

  APSWSession_close_internal(self);

  if (PyErr_Occurred())
    return NULL;

  Py_RETURN_NONE;
}

/** .. method:: attach(name: Optional[str] = None) -> None

 Attach to a specific table, or all tables if no name is provided.  The
 table does not need to exist at the time of the call.  You can call
 this multiple times.

 .. seealso::

    :meth:`table_filter`

 -* sqlite3session_attach
*/
static PyObject *
APSWSession_attach(APSWSession *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *name = NULL;
  CHECK_SESSION_CLOSED(NULL);
  {
    Session_attach_CHECK;
    ARG_PROLOG(1, Session_attach_KWNAMES);
    ARG_OPTIONAL ARG_optional_str(name);
    ARG_EPILOG(NULL, Session_attach_USAGE, );
  }

  int rc = sqlite3session_attach(self->session, name);
  SET_EXC(rc, NULL);
  if (PyErr_Occurred())
    return NULL;
  Py_RETURN_NONE;
}

/** .. method:: diff(from_schema: str, table: str) -> None

  Loads the changes necessary to update the named ``table`` in the attached database
  ``from_schema`` to match the same named table in the database this session is
  attached to.

  -* sqlite3session_diff
*/
static PyObject *
APSWSession_diff(APSWSession *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  CHECK_SESSION_CLOSED(NULL);
  const char *from_schema = NULL;
  const char *table = NULL;

  {
    Session_diff_CHECK;
    ARG_PROLOG(2, Session_diff_KWNAMES);
    ARG_MANDATORY ARG_str(from_schema);
    ARG_MANDATORY ARG_str(table);
    ARG_EPILOG(NULL, Session_diff_USAGE, );
  }

  char *pErrMsg = NULL;
  int rc = sqlite3session_diff(self->session, from_schema, table, &pErrMsg);

  /* a vfs could have errored */
  if (PyErr_Occurred())
    return NULL;

  if (rc != SQLITE_OK)
  {
    make_exception_with_message(rc, pErrMsg, -1);
    sqlite3_free(pErrMsg);
    return NULL;
  }

  Py_RETURN_NONE;
}

static PyObject *
APSWSession_get_change_patch_set(APSWSession *self, int changeset)
{
  int nChangeset = 0;
  void *pChangeset = NULL;

  /* ::TODO:: release GIL around this call? */

  int rc = changeset ? sqlite3session_changeset(self->session, &nChangeset, &pChangeset)
                     : sqlite3session_patchset(self->session, &nChangeset, &pChangeset);

  PyObject *result = NULL;
  if (rc != SQLITE_OK)
    SET_EXC(rc, NULL);
  else
    result = PyBytes_FromStringAndSize((const char *)pChangeset, nChangeset);
  if (pChangeset)
    sqlite3_free(pChangeset);
  return result;
}

/** .. method:: changeset() -> bytes

  Produces a changeset of the session so far.

  -* sqlite3session_changeset
*/
static PyObject *
APSWSession_changeset(APSWSession *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  CHECK_SESSION_CLOSED(NULL);

  return APSWSession_get_change_patch_set(self, 1);
}

/** .. method:: patchset() -> bytes

  Produces a patchset of the session so far.  Patchsets do not include
  before values of changes, making them smaller, but also harder to detect
  conflicts.

  -* sqlite3session_patchset
*/
static PyObject *
APSWSession_patchset(APSWSession *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  CHECK_SESSION_CLOSED(NULL);

  return APSWSession_get_change_patch_set(self, 0);
}

static int
APSWSession_xOutput(void *pOut, const void *pData, int nData)
{
  assert(!PyErr_Occurred());
  PyObject *result = NULL, *result2 = NULL;
  PyObject *vargs[] = { NULL, PyMemoryView_FromMemory((char *)pData, nData, PyBUF_READ) };
  if (vargs[1])
  {
    result = PyObject_Vectorcall((PyObject *)pOut, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
    CHAIN_EXC_BEGIN
    result2 = PyObject_CallMethodNoArgs(vargs[1], apst.release);
    CHAIN_EXC_END;
  }
  Py_XDECREF(vargs[1]);
  Py_XDECREF(result);
  Py_XDECREF(result2);
  return PyErr_Occurred() ? SQLITE_ERROR : SQLITE_OK;
}

static PyObject *
APSWSession_get_change_patch_set_stream(APSWSession *self, int changeset, PyObject *xOutput)
{
  /* ::TODO:: release GIL around this call? */
  int rc = changeset ? sqlite3session_changeset_strm(self->session, APSWSession_xOutput, xOutput)
                     : sqlite3session_patchset_strm(self->session, APSWSession_xOutput, xOutput);
  SET_EXC(rc, NULL);

  if (PyErr_Occurred())
    return NULL;
  Py_RETURN_NONE;
}

/** .. method:: changeset_stream(output: Callable[[memoryview], None]) -> None

  Produces a changeset of the session so far in a stream

  -* sqlite3session_changeset_strm
*/
static PyObject *
APSWSession_changeset_stream(APSWSession *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                             PyObject *fast_kwnames)
{
  CHECK_SESSION_CLOSED(NULL);
  PyObject *output;
  {
    Session_changeset_stream_CHECK;
    ARG_PROLOG(1, Session_changeset_stream_KWNAMES);
    ARG_MANDATORY ARG_Callable(output);
    ARG_EPILOG(NULL, Session_changeset_stream_USAGE, );
  }

  return APSWSession_get_change_patch_set_stream(self, 1, output);
}

/** .. method:: patchset_stream(output: Callable[[memoryview], None]) -> None

  Produces a patchset of the session so far in a stream

  -* sqlite3session_patchset_strm
*/
static PyObject *
APSWSession_patchset_stream(APSWSession *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                            PyObject *fast_kwnames)
{
  CHECK_SESSION_CLOSED(NULL);
  PyObject *output;
  {
    Session_patchset_stream_CHECK;
    ARG_PROLOG(1, Session_patchset_stream_KWNAMES);
    ARG_MANDATORY ARG_Callable(output);
    ARG_EPILOG(NULL, Session_patchset_stream_USAGE, );
  }

  return APSWSession_get_change_patch_set_stream(self, 0, output);
}

/** .. method:: table_filter(callback: Callable[[str], bool]) -> None

  Register a callback that says if changes to the named table should be
  recorded.  If your callback has an exception then ``False`` is
  returned.

  .. seealso::

    :meth:`attach`

  -* sqlite3session_table_filter
*/

static int
session_table_filter_cb(void *pCtx, const char *name)
{
  int result = 0;
  PyGILState_STATE gilstate = PyGILState_Ensure();

  if (!PyErr_Occurred())
  {
    PyObject *vargs[] = { NULL, PyUnicode_FromString(name) };
    if (vargs[1])
    {
      PyObject *retval = PyObject_Vectorcall(pCtx, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
      if (retval)
        result = PyObject_IsTrueStrict(retval);
    }
    Py_XDECREF(vargs[1]);
  }
  if (PyErr_Occurred())
    result = 0;

  PyGILState_Release(gilstate);
  return result;
}

static PyObject *
APSWSession_table_filter(APSWSession *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *callback = NULL;
  CHECK_SESSION_CLOSED(NULL);
  {
    Session_table_filter_CHECK;
    ARG_PROLOG(1, Session_table_filter_KWNAMES);
    ARG_MANDATORY ARG_Callable(callback);
    ARG_EPILOG(NULL, Session_table_filter_USAGE, );
  }

  Py_CLEAR(self->table_filter);
  self->table_filter = Py_NewRef(callback);
  sqlite3session_table_filter(self->session, session_table_filter_cb, callback);

  Py_RETURN_NONE;
}

/** .. method:: config(op: int, *args) -> Any

  Set or get `configuration values <https://www.sqlite.org/session/c_session_objconfig_rowid.html>`__

  For example :code:`session.config(apsw.SQLITE_SESSION_OBJCONFIG_SIZE, -1)` tells you
  if size information is enabled.

  -* sqlite3session_object_config
*/

static PyObject *
APSWSession_config(APSWSession *self, PyObject *args)
{
  CHECK_SESSION_CLOSED(NULL);
  if (PyTuple_GET_SIZE(args) < 1 || !PyLong_Check(PyTuple_GET_ITEM(args, 0)))
    return PyErr_Format(PyExc_TypeError, "There should be at least one argument with the first being a number");

  int opt = PyLong_AsInt(PyTuple_GET_ITEM(args, 0));
  if (PyErr_Occurred())
    return NULL;

  switch (opt)
  {
  case SQLITE_SESSION_OBJCONFIG_SIZE:
  case SQLITE_SESSION_OBJCONFIG_ROWID: {
    int optdup, val;
    if (!PyArg_ParseTuple(args, "ii", &optdup, &val))
      return NULL;
    int res = sqlite3session_object_config(self->session, opt, &val);
    SET_EXC(res, NULL);
    if (PyErr_Occurred())
      return NULL;
    return PyLong_FromLong(val);
  }

  default:
    return PyErr_Format(PyExc_ValueError, "Unknown config value %d", opt);
  }
}

/** .. attribute:: enabled
    :type: bool

    Get or change if this session is recording changes.

    -* sqlite3session_enable
*/
static PyObject *
APSWSession_get_enabled(APSWSession *self)
{
  CHECK_SESSION_CLOSED(NULL);

  int res = sqlite3session_enable(self->session, -1);
  if (res)
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

static int
APSWSession_set_enabled(APSWSession *self, PyObject *value)
{
  CHECK_SESSION_CLOSED(-1);

  int enabled = PyObject_IsTrueStrict(value);
  if (enabled == -1)
    return -1;
  sqlite3session_enable(self->session, enabled);
  return 0;
}

/** .. attribute:: indirect
    :type: bool

    Get or change if this session is in indirect mode

    -* sqlite3session_indirect
*/
static PyObject *
APSWSession_get_indirect(APSWSession *self)
{
  CHECK_SESSION_CLOSED(NULL);

  int res = sqlite3session_indirect(self->session, -1);
  if (res)
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

static int
APSWSession_set_indirect(APSWSession *self, PyObject *value)
{
  CHECK_SESSION_CLOSED(-1);

  int enabled = PyObject_IsTrueStrict(value);
  if (enabled == -1)
    return -1;
  sqlite3session_indirect(self->session, enabled);
  return 0;
}

/** .. attribute:: is_empty
    :type: bool

    True if no changes have been recorded.

    -* sqlite3session_isempty
*/
static PyObject *
APSWSession_get_empty(APSWSession *self)
{
  CHECK_SESSION_CLOSED(NULL);

  int res = sqlite3session_isempty(self->session);
  if (res)
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

/** .. attribute:: memory_used
    :type: int

    How many bytes of memory have been used to record session changes.

    -* sqlite3session_memory_used
*/
static PyObject *
APSWSession_get_memory_used(APSWSession *self)
{
  CHECK_SESSION_CLOSED(NULL);

  sqlite3_int64 res = sqlite3session_memory_used(self->session);

  return PyLong_FromLongLong(res);
}

/** .. attribute:: changeset_size
    :type: int

    Returns upper limit on changeset size, but only if :meth:`Session.config`
    was used to enable it.  Otherwise it will be zero.

    -* sqlite3session_changeset_size
*/
static PyObject *
APSWSession_get_changeset_size(APSWSession *self)
{
  CHECK_SESSION_CLOSED(NULL);

  sqlite3_int64 res = sqlite3session_changeset_size(self->session);

  return PyLong_FromLongLong(res);
}

/** .. class:: Changeset

 Provides changeset (including patchset) related methods.
*/

/** .. method:: invert(changeset: bytes) -> bytes

  Produces a changeset that reverses the effect of
  the supplied changeset.

  -* sqlite3changeset_invert
*/

static PyObject *
APSWChangeset_invert(void *Py_UNUSED(static_method), PyObject *const *fast_args, Py_ssize_t fast_nargs,
                     PyObject *fast_kwnames)
{
  PyObject *changeset;
  Py_buffer changeset_buffer;

  {
    Changeset_invert_CHECK;
    ARG_PROLOG(1, Changeset_invert_KWNAMES);
    ARG_MANDATORY ARG_py_buffer(changeset);
    ARG_EPILOG(NULL, Changeset_invert_USAGE, );
  }

  if (0 != PyObject_GetBufferContiguous(changeset, &changeset_buffer, PyBUF_SIMPLE))
  {
    assert(PyErr_Occurred());
    return NULL;
  }

  PyObject *result = NULL;

    /* ::TODO:: turn this into a function that can be fault injected and used in other places */
  if (changeset_buffer.len > 0x7fffffff)
    SET_EXC(SQLITE_TOOBIG, NULL);
  else
  {
    int nOut;
    void *pOut = NULL;

    int rc = sqlite3changeset_invert(changeset_buffer.len, changeset_buffer.buf, &nOut, &pOut);
    if (rc == SQLITE_OK)
      result = PyBytes_FromStringAndSize((char *)pOut, nOut);
    sqlite3_free(pOut);
  }
  PyBuffer_Release(&changeset_buffer);
  assert((PyErr_Occurred() && !result) || (result && !PyErr_Occurred()));
  return result;
}

static PyMethodDef APSWSession_methods[] = {
  { "close", (PyCFunction)APSWSession_close, METH_FASTCALL | METH_KEYWORDS, Session_close_DOC },
  { "attach", (PyCFunction)APSWSession_attach, METH_FASTCALL | METH_KEYWORDS, Session_attach_DOC },
  { "diff", (PyCFunction)APSWSession_diff, METH_FASTCALL | METH_KEYWORDS, Session_diff_DOC },
  { "table_filter", (PyCFunction)APSWSession_table_filter, METH_FASTCALL | METH_KEYWORDS, Session_table_filter_DOC },
  { "changeset", (PyCFunction)APSWSession_changeset, METH_FASTCALL | METH_KEYWORDS, Session_changeset_DOC },
  { "patchset", (PyCFunction)APSWSession_patchset, METH_FASTCALL | METH_KEYWORDS, Session_patchset_DOC },
  { "changeset_stream", (PyCFunction)APSWSession_changeset_stream, METH_FASTCALL | METH_KEYWORDS,
    Session_changeset_stream_DOC },
  { "patchset_stream", (PyCFunction)APSWSession_patchset_stream, METH_FASTCALL | METH_KEYWORDS,
    Session_patchset_stream_DOC },
  { "config", (PyCFunction)APSWSession_config, METH_VARARGS, Session_config_DOC },
  { 0 },
};

static PyGetSetDef APSWSession_getset[] = {
  { "enabled", (getter)APSWSession_get_enabled, (setter)APSWSession_set_enabled, Session_enabled_DOC },
  { "indirect", (getter)APSWSession_get_indirect, (setter)APSWSession_set_indirect, Session_indirect_DOC },
  { "is_empty", (getter)APSWSession_get_empty, NULL, Session_is_empty_DOC },
  { "memory_used", (getter)APSWSession_get_memory_used, NULL, Session_memory_used_DOC },
  { "changeset_size", (getter)APSWSession_get_changeset_size, NULL, Session_changeset_size_DOC },
  { 0 },
};

static PyTypeObject APSWSessionType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.Session",
  .tp_basicsize = sizeof(APSWSession),
  .tp_doc = Session_class_DOC,
  .tp_new = PyType_GenericNew,
  .tp_init = (initproc)APSWSession_init,
  .tp_finalize = (destructor)APSWSession_close_internal,
  .tp_methods = APSWSession_methods,
  .tp_getset = APSWSession_getset,
  .tp_flags = Py_TPFLAGS_BASETYPE | Py_TPFLAGS_DEFAULT,
  .tp_weaklistoffset = offsetof(APSWSession, weakreflist),
};

static PyMethodDef APSWChangeset_methods[] = {
    { "invert", (PyCFunction)APSWChangeset_invert, METH_STATIC | METH_FASTCALL | METH_KEYWORDS, Changeset_invert_DOC },
    {0},
};

static PyTypeObject APSWChangesetType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.Changeset",
  .tp_doc = Changeset_class_DOC,
  .tp_basicsize = sizeof(APSWChangeset),
  .tp_methods = APSWChangeset_methods,
};

static PyTypeObject APSWChangesetBuilderType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.ChangesetBuilder",
  .tp_basicsize = sizeof(APSWChangesetBuilder),
};

static PyTypeObject APSWTableChangeType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.TableChange",
  .tp_basicsize = sizeof(APSWTableChange),
};
