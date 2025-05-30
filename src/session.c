/**

Session extension
*****************

APSW provides access to all session functionality (including
experimental).  See the :doc:`example-session`.

The `session extension <https://www.sqlite.org/sessionintro.html>`__
allows recording changes to a database, and later replaying them on
another database, or undoing them.  This allows offline syncing, as
well as collaboration.  It is also useful for debugging, development,
and testing.  Note that it records the added, modified, and deleted
row values - it does **not** record or replay the queries that
resulted in those changes.

* You can choose which tables have changes recorded (or all), and
  pause / resume recording at any time

* The recorded change set includes the row values before and after a
  change.  This allows comprehensive conflict detection, and inverting
  (undoing the change),  Optionally you can use patch sets (a subset of
  change sets) which do not have the before values, consuming less
  space but have less ability to detect conflicts, or be inverted.

* The recorded changes includes indirect changes made such as by triggers
  and foreign keys.

* When applying changes you can supply a conflict handler to choose
  what happens on each conflicting row, including aborting, skipping,
  applying anyway, applying your own change, and can record the
  conflicting operation to another change set for later.

* You are responsible for :ref:`managing your schema <schema_upgrade>`
  - the extension will not create, update, or delete tables for you.
  When applying changesets, if a corresponding table does not already
  exist then those changes are ignored.  This means that you do not
  need all tables present on all databases.

* It is efficient only storing enough to make the semantic change.
  For example if multiple changes are made to the same row, then
  they can be accumulated into one change record, not many.

* You can iterate over a change set to see what it contains

* Changesets do not contain the changes in the order made

* Using :class:`ChangesetBuilder`, you can accumulate multiple change
  sets, and add changes from an iterator or conflict handler.

* Using :class:`Rebaser` you can merge conflict resolutions made when
  applying a changeset into a later changeset, so those conflict
  resolutions do not have to be redone on each database where they are
  applied.

* Doing multi-way synchronization across multiple databases changed
  separately `is hard
  <https://en.wikipedia.org/wiki/Eventual_consistency>`__.  A common
  approach to conflicts is to use timestamps with the most recent
  change "winning".  Changesets do not include timestamps, and are not
  time ordered.  You should carefully design your schema and
  synchronization to ensure the needed levels of data integrity,
  consistency, and meeting user goals up front.  Adding it later is
  painful.

* Most APIs produce and consume changesets as bytes (or :class:`bytes
  like <collections.abc.Buffer>`). That limits the changeset size to
  2GB - the limit is in the SQLite code and also the limit for `blobs
  <https://www.sqlite.org/limits.html>`__.  To produce or consume
  larger changesets, or to not have an entire changeset in memory,
  there are streaming versions of most APIs where you need to provide
  to provide a :class:`block input <SessionStreamInput>` or
  :class:`block output <SessionStreamOutput>` callback.

.. important::

    By default Session can only record and replay changes that have an
    explicit `primary key <https://www.sqlite.org/lang_createtable.html#the_primary_key>`__
    defined (ie ``PRIMARY KEY`` must be present in the table definition).
    It doesn't matter what type or how many columns make up the primary key.
    This provides a stable way to identify rows for insertion, changes, and
    deletion.

    You can use :meth:`Session.config` with `SQLITE_SESSION_OBJCONFIG_ROWID
    <https://www.sqlite.org/session/c_session_objconfig_rowid.html>`__
    to enable recording of tables without an explicit primary key, but
    it is strongly advised to have deterministic primary keys so that
    changes made independently can be reconciled.  The changesets will
    also contain wrong operations if the table has a column named
    `_rowid_`.

Availability
============

The session extension and APSW support for it have to be enabled at
compile time for each.  APSW builds from PyPI include session support.

Most platform provided SQLite are configured with session support, and
APSW should end up with it too.

The methods and classes documented here are only present if session
support was enabled.

Usage Overview
==============

The session extension does not do table creation (or deletion).  When applying
a changeset, it will only do so if a same named table exists, with the same number
of columns, and same primary key.  If no such table exists, the change is silently
ignored.  (Tip for :ref:`managing your schema <schema_upgrade>`)

To record changes:

* Use a :class:`Session` with the relevant database.  You can
  have multiple on the same database.
* Use :meth:`Session.attach` to determine which tables
  to record
* You can use :attr:`Session.enabled` to turn recording off or
  on (it is on by default)
* Use :meth:`Session.changeset` to get the changeset for later use.
* If you have two databases, you can use :meth:`Session.diff` to get
  the changes necessary to turn one into the other without having to
  record changes as they happen

To see what your changeset contains:

* Use :meth:`Changeset.iter`

To apply a changeset:

* Use :meth:`Changeset.apply`

To manipulate changesets:

* Use :class:`ChangesetBuilder`
* You can add multiple changesets together
* You can add :class:`individual changes <TableChange>` from
  :meth:`Changeset.iter` or from your conflict handler in
  :meth:`Changeset.apply`
* Use :class:`Rebaser` to incorporate conflict resolutions into a
  changeset

.. tip::

  The session extension rarely raises exceptions, instead just doing
  nothing.  For example if tables don't exist, don't have a primary key,
  attached databases don't exist, and similar scenarios where typos
  could happen, you won't get an error, just no action.

Extension configuration
=======================

 */

/*
Notes on releasing the GIL

This code currently doesn't release the GIL anywhere.  The various
objects are not thread safe nor re-entrant in the SQLite code, so that
would have to be added.  Some of them lock the database mutex which is
a suitable alternative but there are still race conditions with close
being called in a different thread being an easy way to crash things.

The apply and changeset generate methods are the best candidates for
GIL release, so we'll wait until someone requests it to have a nice
test case.  I believe that most of the time the data will be so small
that it won't have any benefit.

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

typedef struct APSWChangesetIterator
{
  PyObject_HEAD
  sqlite3_changeset_iter *iter;
  PyObject *xInput;
  PyObject *buffer_source;
  Py_buffer buffer_buffer;
  struct APSWTableChange *last_table_change;
} APSWChangesetIterator;

static PyTypeObject APSWChangesetIteratorType;

typedef struct APSWChangesetBuilder
{
  PyObject_HEAD
  sqlite3_changegroup *group;
  int init_was_called;

  /* needed by changegroup_schema */
  Connection *connection;
  PyObject *weakreflist;
} APSWChangesetBuilder;

static PyTypeObject APSWChangesetBuilderType;

typedef struct APSWTableChange
{
  PyObject_HEAD
  /* the iter field is used to mark this change as still in scope and
     valid, plus to get the fields other than those from
     sqlite3changeset_op */
  sqlite3_changeset_iter *iter;
  const char *table_name;
  int table_column_count;
  int operation;
  int indirect;
} APSWTableChange;

static PyTypeObject APSWTableChangeType;

typedef struct APSWRebaser
{
  PyObject_HEAD
  sqlite3_rebaser *rebaser;
  int init_was_called;
} APSWRebaser;

static PyTypeObject APSWRebaserType;

/** .. class:: Session

  This object wraps a `sqlite3_session
  <https://www.sqlite.org/session/session.html>`__ object.

*/

#define CHECK_SESSION_CLOSED(e)                                                                                        \
  do                                                                                                                   \
  {                                                                                                                    \
    if (!self->session)                                                                                                \
    {                                                                                                                  \
      PyErr_Format(PyExc_ValueError, "The session has been closed");                                                   \
      return e;                                                                                                        \
    }                                                                                                                  \
  } while (0)

/** .. method:: __init__(db: Connection, schema: str)

  Starts a new session.

  :param connection: Which database to operate on
  :param schema: `main`, `temp`, the name in `ATTACH <https://sqlite.org/lang_attach.html>`__

  -* sqlite3session_create
*/
static int
APSWSession_init(PyObject *self_, PyObject *args, PyObject *kwargs)
{
  APSWSession *self = (APSWSession *)self_;
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

static void
APSWSession_dealloc(PyObject *self_)
{
  APSWSession *self = (APSWSession *)self_;
  APSWSession_close_internal(self);
  Py_TpFree(self_);
}

/** .. method:: close() -> None

  Ends the session object.  APSW ensures that all
  Session objects are closed before the database is closed
  so there is no need to manually call this.

  -* sqlite3session_delete
*/
static PyObject *
APSWSession_close(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  APSWSession *self = (APSWSession *)self_;
  {
    Session_close_CHECK;
    ARG_PROLOG(0, Session_close_KWNAMES);
    ARG_EPILOG(NULL, Session_close_USAGE, );
  }

  APSWSession_close_internal(self);
  MakeExistingException();

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
APSWSession_attach(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  APSWSession *self = (APSWSession *)self_;
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

  See the :ref:`example <example_session_diff>`.

  .. note::

    You must use :meth:`attach` (or use :meth:`table_filter`) to attach to
    the table before running this method otherwise nothing is recorded.

  -* sqlite3session_diff
*/
static PyObject *
APSWSession_diff(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  APSWSession *self = (APSWSession *)self_;
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

  assert((rc == SQLITE_OK && !pErrMsg) || (rc != SQLITE_OK));

  MakeExistingException();

  /* a vfs could have errored */
  if (PyErr_Occurred())
  {
    sqlite3_free(pErrMsg);
    return NULL;
  }

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
APSWSession_changeset(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  APSWSession *self = (APSWSession *)self_;
  CHECK_SESSION_CLOSED(NULL);

  {
    Session_changeset_CHECK;
    ARG_PROLOG(0, Session_changeset_KWNAMES);
    ARG_EPILOG(NULL, Session_changeset_USAGE, );
  }

  return APSWSession_get_change_patch_set(self, 1);
}

/** .. method:: patchset() -> bytes

  Produces a patchset of the session so far.  Patchsets do not include
  before values of changes, making them smaller, but also harder to detect
  conflicts.

  -* sqlite3session_patchset
*/
static PyObject *
APSWSession_patchset(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  APSWSession *self = (APSWSession *)self_;
  CHECK_SESSION_CLOSED(NULL);

  {
    Session_patchset_CHECK;
    ARG_PROLOG(0, Session_patchset_KWNAMES);
    ARG_EPILOG(NULL, Session_patchset_USAGE, );
  }

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

static int
APSWSession_xInput(void *pIn, void *pData, int *pnData)
{
  assert(!PyErr_Occurred());
  PyObject *result = NULL;
  PyObject *vargs[] = { NULL, PyLong_FromLong(*pnData) };
  if (vargs[1])
  {
    result = PyObject_Vectorcall((PyObject *)pIn, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
    Py_DECREF(vargs[1]);
  }
  if (result)
  {
    Py_buffer result_buffer;
    if (0 == PyObject_GetBufferContiguousBounded(result, &result_buffer, PyBUF_SIMPLE, *pnData))
    {
      memcpy(pData, result_buffer.buf, result_buffer.len);
      *pnData = (int)result_buffer.len;

      PyBuffer_Release(&result_buffer);
    }
  }
  if (PyErr_Occurred())
  {
    AddTraceBackHere(__FILE__, __LINE__, "SessionStreamInput", "{s: O, s: O, s: i}", "xInput", OBJ(pIn), "provided",
                     OBJ(result), "amount_requested", *pnData);
    Py_XDECREF(result);
    return MakeSqliteMsgFromPyException(NULL);
  }
  Py_XDECREF(result);

  return SQLITE_OK;
}

static PyObject *
APSWSession_get_change_patch_set_stream(APSWSession *self, int changeset, PyObject *xOutput)
{
  int rc = changeset ? sqlite3session_changeset_strm(self->session, APSWSession_xOutput, xOutput)
                     : sqlite3session_patchset_strm(self->session, APSWSession_xOutput, xOutput);
  SET_EXC(rc, NULL);

  if (PyErr_Occurred())
    return NULL;
  Py_RETURN_NONE;
}

/** .. method:: changeset_stream(output: SessionStreamOutput) -> None

  Produces a changeset of the session so far in a stream

  -* sqlite3session_changeset_strm
*/
static PyObject *
APSWSession_changeset_stream(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  APSWSession *self = (APSWSession *)self_;
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

/** .. method:: patchset_stream(output: SessionStreamOutput) -> None

  Produces a patchset of the session so far in a stream

  -* sqlite3session_patchset_strm
*/
static PyObject *
APSWSession_patchset_stream(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  APSWSession *self = (APSWSession *)self_;
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
      if (PyErr_Occurred())
        AddTraceBackHere(__FILE__, __LINE__, "session.table_filter.callback", "{s: s, s: O}", "name", name, "returned",
                         OBJ(retval));
      Py_XDECREF(retval);
    }
    Py_XDECREF(vargs[1]);
  }
  if (PyErr_Occurred())
    result = 0;

  PyGILState_Release(gilstate);
  return result;
}

static PyObject *
APSWSession_table_filter(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  APSWSession *self = (APSWSession *)self_;
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

/** .. method:: config(op: int, *args: Any) -> Any

  Set or get `configuration values <https://www.sqlite.org/session/c_session_objconfig_rowid.html>`__

  For example :code:`session.config(apsw.SQLITE_SESSION_OBJCONFIG_SIZE, -1)` tells you
  if size information is enabled.

  -* sqlite3session_object_config
*/

static PyObject *
APSWSession_config(PyObject *self_, PyObject *args)
{
  APSWSession *self = (APSWSession *)self_;
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

    Get or change if this session is recording changes.  Disabling only
    stops recording rows not already part of the changeset.

    -* sqlite3session_enable
*/
static PyObject *
APSWSession_get_enabled(PyObject *self_, void *Py_UNUSED(unused))
{
  APSWSession *self = (APSWSession *)self_;
  CHECK_SESSION_CLOSED(NULL);

  int res = sqlite3session_enable(self->session, -1);
  if (res)
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

static int
APSWSession_set_enabled(PyObject *self_, PyObject *value, void *Py_UNUSED(unused))
{
  APSWSession *self = (APSWSession *)self_;
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
APSWSession_get_indirect(PyObject *self_, void *Py_UNUSED(unused))
{
  APSWSession *self = (APSWSession *)self_;
  CHECK_SESSION_CLOSED(NULL);

  int res = sqlite3session_indirect(self->session, -1);
  if (res)
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

static int
APSWSession_set_indirect(PyObject *self_, PyObject *value, void *Py_UNUSED(unused))
{
  APSWSession *self = (APSWSession *)self_;
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
APSWSession_get_empty(PyObject *self_, void *Py_UNUSED(unused))
{
  APSWSession *self = (APSWSession *)self_;
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
APSWSession_get_memory_used(PyObject *self_, void *Py_UNUSED(unused))
{
  APSWSession *self = (APSWSession *)self_;
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
APSWSession_get_changeset_size(PyObject *self_, void *Py_UNUSED(unused))
{
  APSWSession *self = (APSWSession *)self_;
  CHECK_SESSION_CLOSED(NULL);

  sqlite3_int64 res = sqlite3session_changeset_size(self->session);

  return PyLong_FromLongLong(res);
}

static int
APSWSession_tp_traverse(PyObject *self_, visitproc visit, void *arg)
{
  APSWSession *self = (APSWSession *)self_;
  Py_VISIT(self->connection);
  Py_VISIT(self->table_filter);
  return 0;
}

/** .. class:: TableChange

  Represents a `changed row
  <https://sqlite.org/session/changeset_iter.html>`__.  They come from
  :meth:`changeset iteration <Changeset.iter>` and from the
  :meth:`conflict handler in apply <Changeset.apply>`.

  A TableChange is only valid when your conflict handler is active, or
  has just been provided by a changeset iterator.  It goes out of scope
  after your conflict handler returns, or the iterator moves to the next
  entry.  You will get :exc:`~apsw.InvalidContextError` if you try to
  access fields when out of scope.  This means you can't save
  TableChanges for later, and need to copy out any information you need.

 */

#define CHECK_TABLE_SCOPE                                                                                              \
  do                                                                                                                   \
  {                                                                                                                    \
    if (!self->iter)                                                                                                   \
      return PyErr_Format(ExcInvalidContext, "The table change has gone out of scope");                                \
  } while (0)

#undef MakeTableChange
static APSWTableChange *
MakeTableChange(sqlite3_changeset_iter *iter)
{
#include "faultinject.h"

  APSWTableChange *tc = (APSWTableChange *)_PyObject_New(&APSWTableChangeType);
  if (!tc)
    return NULL;
  tc->iter = NULL;

  int rc = sqlite3changeset_op(iter, &tc->table_name, &tc->table_column_count, &tc->operation, &tc->indirect);
  if (rc != SQLITE_OK)
  {
    Py_DECREF(tc);
    SET_EXC(rc, NULL);
    return NULL;
  }

  tc->iter = iter;

  return tc;
}

/** .. attribute:: name
  :type: str

   Name of the affected table
*/
static PyObject *
APSWTableChange_name(PyObject *self_, void *Py_UNUSED(unused))
{
  APSWTableChange *self = (APSWTableChange *)self_;
  CHECK_TABLE_SCOPE;

  return PyUnicode_FromString(self->table_name);
}

/** .. attribute:: column_count
  :type: int

   Number of columns in the affected table
*/
static PyObject *
APSWTableChange_column_count(PyObject *self_, void *Py_UNUSED(unused))
{
  APSWTableChange *self = (APSWTableChange *)self_;
  CHECK_TABLE_SCOPE;

  return PyLong_FromLong(self->table_column_count);
}

/** .. attribute:: opcode
  :type: int

   The operation code - ``apsw.SQLITE_INSERT``,
   ``apsw.SQLITE_DELETE``, or ``apsw.SQLITE_UPDATE``.
   See :attr:`op` for this as a string.
*/

static PyObject *
APSWTableChange_opcode(PyObject *self_, void *Py_UNUSED(unused))
{
  APSWTableChange *self = (APSWTableChange *)self_;
  CHECK_TABLE_SCOPE;

  return PyLong_FromLong(self->operation);
}

/** .. attribute:: op
  :type: str

   The operation code as a string  ``INSERT``,
   ``DELETE``, or ``UPDATE``.  See :attr:`opcode`
   for this as a number.
*/
static PyObject *
APSWTableChange_op(PyObject *self_, void *Py_UNUSED(unused))
{
  APSWTableChange *self = (APSWTableChange *)self_;
  CHECK_TABLE_SCOPE;

  if (self->operation == SQLITE_INSERT)
    return Py_NewRef(apst.INSERT);
  if (self->operation == SQLITE_DELETE)
    return Py_NewRef(apst.DELETE);
  if (self->operation == SQLITE_UPDATE)
    return Py_NewRef(apst.UPDATE);
  /* https://sqlite.org/forum/forumpost/09c94dfb08 */
  return PyUnicode_FromFormat("Undocumented op %d", self->operation);
}

/** .. attribute:: indirect
  :type: bool

  ``True`` if this is an `indirect <https://sqlite.org/session/sqlite3session_indirect.html>`__
  change - for example made by triggers or foreign keys.
*/
static PyObject *
APSWTableChange_indirect(PyObject *self_, void *Py_UNUSED(unused))
{
  APSWTableChange *self = (APSWTableChange *)self_;
  CHECK_TABLE_SCOPE;
  if (self->indirect)
    Py_RETURN_TRUE;

  Py_RETURN_FALSE;
}

/** .. attribute:: new
  :type: tuple[SQLiteValue | Literal[no_change], ...] | None

  :class:`None` if not applicable (like a DELETE).  Otherwise a
  tuple of the new values for the row, with :attr:`apsw.no_change`
  if no value was provided for that column.

  -* sqlite3changeset_new
 */
static PyObject *
APSWTableChange_new(PyObject *self_, void *Py_UNUSED(unused))
{
  APSWTableChange *self = (APSWTableChange *)self_;
  CHECK_TABLE_SCOPE;

  sqlite3_value *value, *misuse_check;
  if (SQLITE_MISUSE == sqlite3changeset_new(self->iter, 0, &misuse_check))
    Py_RETURN_NONE;

  PyObject *tuple = PyTuple_New(self->table_column_count);
  if (!tuple)
    goto error;

  for (int i = 0; i < self->table_column_count; i++)
  {
    int res = sqlite3changeset_new(self->iter, i, &value);
    if (res != SQLITE_OK)
    {
      SET_EXC(res, NULL);
      goto error;
    }
    if (value == NULL)
      PyTuple_SET_ITEM(tuple, i, Py_NewRef((PyObject *)&apsw_no_change_object));
    else
    {
      PyObject *pyvalue = convert_value_to_pyobject(value, 0, 0);
      if (!pyvalue)
        goto error;
      PyTuple_SET_ITEM(tuple, i, pyvalue);
    }
  }
  return tuple;

error:
  assert(PyErr_Occurred());
  Py_XDECREF(tuple);
  return NULL;
}

/** .. attribute:: old
  :type: tuple[SQLiteValue | Literal[no_change], ...] | None

  :class:`None` if not applicable (like an INSERT).  Otherwise a tuple
  of the old values for the row before this change, with
  :attr:`apsw.no_change` if no value was provided for that column,

  -* sqlite3changeset_old
 */
static PyObject *
APSWTableChange_old(PyObject *self_, void *Py_UNUSED(unused))
{
  APSWTableChange *self = (APSWTableChange *)self_;
  CHECK_TABLE_SCOPE;

  sqlite3_value *value, *misuse_check;
  if (SQLITE_MISUSE == sqlite3changeset_old(self->iter, 0, &misuse_check))
    Py_RETURN_NONE;

  PyObject *tuple = PyTuple_New(self->table_column_count);
  if (!tuple)
    goto error;

  for (int i = 0; i < self->table_column_count; i++)
  {
    int res = sqlite3changeset_old(self->iter, i, &value);
    if (res != SQLITE_OK)
    {
      SET_EXC(res, NULL);
      goto error;
    }
    if (value == NULL)
      PyTuple_SET_ITEM(tuple, i, Py_NewRef((PyObject *)&apsw_no_change_object));
    else
    {
      PyObject *pyvalue = convert_value_to_pyobject(value, 0, 0);
      if (!pyvalue)
        goto error;
      PyTuple_SET_ITEM(tuple, i, pyvalue);
    }
  }
  return tuple;

error:
  assert(PyErr_Occurred());
  Py_XDECREF(tuple);
  return NULL;
}

/** .. attribute:: conflict
  :type: tuple[SQLiteValue, ...] | None

  :class:`None` if not applicable (not in a conflict).  Otherwise a
  tuple of values for the conflicting row.

  -* sqlite3changeset_conflict
 */
static PyObject *
APSWTableChange_conflict(PyObject *self_, void *Py_UNUSED(unused))
{
  APSWTableChange *self = (APSWTableChange *)self_;
  CHECK_TABLE_SCOPE;
  sqlite3_value *value;
  int res = sqlite3changeset_conflict(self->iter, 0, &value);
  if (res == SQLITE_MISUSE)
    Py_RETURN_NONE;

  PyObject *tuple = NULL;

  if (res != SQLITE_OK)
  {
    SET_EXC(res, NULL);
    goto error;
  }

  tuple = PyTuple_New(self->table_column_count);
  if (!tuple)
    goto error;

  for (int i = 0; i < self->table_column_count; i++)
  {
    int res = sqlite3changeset_conflict(self->iter, i, &value);
    if (res != SQLITE_OK)
    {
      SET_EXC(res, NULL);
      goto error;
    }
    PyObject *pyvalue = convert_value_to_pyobject(value, 0, 0);
    if (!pyvalue)
      goto error;
    PyTuple_SET_ITEM(tuple, i, pyvalue);
  }
  return tuple;

error:
  assert(PyErr_Occurred());
  Py_XDECREF(tuple);
  return NULL;
}

/** .. attribute:: fk_conflicts
  :type: int | None

  The number of known foreign key conflicts, or :class:`None` if not in a
  conflict handler.

  -* sqlite3changeset_fk_conflicts
*/

static PyObject *
APSWTableChange_fk_conflicts(PyObject *self_, void *Py_UNUSED(unused))
{
  APSWTableChange *self = (APSWTableChange *)self_;
  CHECK_TABLE_SCOPE;

  int nOut;

  int res = sqlite3changeset_fk_conflicts(self->iter, &nOut);
  if (res == SQLITE_MISUSE)
    Py_RETURN_NONE;
  if (res == SQLITE_OK)
    return PyLong_FromLong(nOut);
  SET_EXC(res, NULL);
  return NULL;
}

/** .. attribute:: pk_columns
  :type: set[int]

  Which columns make up the primary key for this table

  -* sqlite3changeset_pk
*/
static PyObject *
APSWTableChange_pk_columns(PyObject *self_, void *Py_UNUSED(unused))
{
  APSWTableChange *self = (APSWTableChange *)self_;
  CHECK_TABLE_SCOPE;

  unsigned char *abPK;
  int nCol;

  int res = sqlite3changeset_pk(self->iter, &abPK, &nCol);

  if (res != SQLITE_OK)
  {
    SET_EXC(res, NULL);
    return NULL;
  }

  PyObject *value = NULL, *set = PySet_New(NULL);
  if (!set)
    goto error;
  /*  the abPK test is because of https://sqlite.org/forum/forumpost/09c94dfb08 */
  for (int i = 0; i < nCol && abPK; i++)
  {
    if (abPK[i])
    {
      value = PyLong_FromLong(i);
      if (!value)
        goto error;
      if (0 != PySet_Add(set, value))
        goto error;
      Py_CLEAR(value);
    }
  }

  return set;
error:
  assert(PyErr_Occurred());
  Py_XDECREF(set);
  Py_XDECREF(value);
  return NULL;
}

static PyObject *
APSWTableChange_tp_str(PyObject *self_)
{
  APSWTableChange *self = (APSWTableChange *)self_;
  if (!self->iter)
    return PyUnicode_FromFormat("<apsw.TableChange out of scope, at %p>", self);

  PyObject *op = NULL, *old = NULL, *new_vals = NULL, *conflict = NULL, *pk_columns = NULL, *fk_conflicts = NULL;

  op = APSWTableChange_op(self_, NULL);
  if (op)
    old = APSWTableChange_old(self_, NULL);
  if (old)
    new_vals = APSWTableChange_new(self_, NULL);
  if (new_vals)
    conflict = APSWTableChange_conflict(self_, NULL);
  if (conflict)
    pk_columns = APSWTableChange_pk_columns(self_, NULL);
  if (pk_columns)
    fk_conflicts = APSWTableChange_fk_conflicts(self_, NULL);

  PyObject *res = NULL;

  if (fk_conflicts)
    res = PyUnicode_FromFormat("<apsw.TableChange name=\"%s\", column_count=%d, pk_columns=%S, operation=%U, "
                               "indirect=%S, old=%S, new=%S, conflict=%S, fk_conflicts=%S, at %p>",
                               self->table_name ? self->table_name : "(NULL)", self->table_column_count, pk_columns, op,
                               (self->indirect) ? Py_True : Py_False, old, new_vals, conflict, fk_conflicts, self);

  Py_XDECREF(op);
  Py_XDECREF(old);
  Py_XDECREF(new_vals);
  Py_XDECREF(conflict);
  Py_XDECREF(pk_columns);
  Py_XDECREF(fk_conflicts);

  return res;
}

static void
APSWTableChange_dealloc(PyObject *self)
{
  Py_TpFree(self);
}

/** .. class:: Changeset

  Provides changeset (including patchset) related methods.  Note that
  all methods are static (belong to the class).  There is no Changeset
  object.   On input Changesets can be a :class:`collections.abc.Buffer`
  (anything that resembles a sequence of bytes), or
  :class:`SessionStreamInput` which provides the bytes in chunks from a
  callback.

  Output is bytes, or :class:`SessionStreamOutput` (chunks in a callback).

  The streaming versions are useful when you are concerned about memory
  usage, or where changesets are larger than 2GB (the SQLite limit).
*/

/** .. method:: invert(changeset: Buffer) -> bytes

  Produces a changeset that reverses the effect of
  the supplied changeset.

  -* sqlite3changeset_invert
*/

static PyObject *
APSWChangeset_invert(PyObject *Py_UNUSED(static_method), PyObject *const *fast_args, Py_ssize_t fast_nargs,
                     PyObject *fast_kwnames)
{
  PyObject *changeset;
  Py_buffer changeset_buffer;

  {
    Changeset_invert_CHECK;
    ARG_PROLOG(1, Changeset_invert_KWNAMES);
    ARG_MANDATORY ARG_Buffer(changeset);
    ARG_EPILOG(NULL, Changeset_invert_USAGE, );
  }

  if (0 != PyObject_GetBufferContiguousBounded(changeset, &changeset_buffer, PyBUF_SIMPLE, INT32_MAX))
  {
    assert(PyErr_Occurred());
    return NULL;
  }

  PyObject *result = NULL;
  int nOut;
  void *pOut = NULL;

  int rc = sqlite3changeset_invert(changeset_buffer.len, changeset_buffer.buf, &nOut, &pOut);
  if (rc == SQLITE_OK)
    result = PyBytes_FromStringAndSize((char *)pOut, nOut);
  else
    SET_EXC(rc, NULL);
  sqlite3_free(pOut);

  PyBuffer_Release(&changeset_buffer);
  assert((PyErr_Occurred() && !result) || (result && !PyErr_Occurred()));
  return result;
}

/** .. method:: invert_stream(changeset: SessionStreamInput, output: SessionStreamOutput) -> None

  Streaming reverses the effect of the supplied changeset.

  -* sqlite3changeset_invert_strm
*/
static PyObject *
APSWChangeset_invert_stream(PyObject *Py_UNUSED(static_method), PyObject *const *fast_args, Py_ssize_t fast_nargs,
                            PyObject *fast_kwnames)
{

  PyObject *changeset = NULL;
  PyObject *output = NULL;

  {
    Changeset_invert_stream_CHECK;
    ARG_PROLOG(2, Changeset_invert_stream_KWNAMES);
    ARG_MANDATORY ARG_Callable(changeset);
    ARG_MANDATORY ARG_Callable(output);
    ARG_EPILOG(NULL, Changeset_invert_stream_USAGE, );
  }

  int rc = sqlite3changeset_invert_strm(APSWSession_xInput, changeset, APSWSession_xOutput, output);
  SET_EXC(rc, NULL);
  if (PyErr_Occurred())
    return NULL;
  Py_RETURN_NONE;
}

/** .. method:: concat(A: Buffer, B: Buffer) -> bytes

  Returns combined changesets

  -* sqlite3changeset_concat
*/

static PyObject *
APSWChangeset_concat(PyObject *Py_UNUSED(static_method), PyObject *const *fast_args, Py_ssize_t fast_nargs,
                     PyObject *fast_kwnames)
{
  PyObject *A = NULL;
  Py_buffer A_buffer;
  PyObject *B = NULL;
  Py_buffer B_buffer;

  {
    Changeset_concat_CHECK;
    ARG_PROLOG(2, Changeset_concat_KWNAMES);
    ARG_MANDATORY ARG_Buffer(A);
    ARG_MANDATORY ARG_Buffer(B);
    ARG_EPILOG(NULL, Changeset_concat_USAGE, );
  }

  if (0 != PyObject_GetBufferContiguousBounded(A, &A_buffer, PyBUF_SIMPLE, INT32_MAX))
  {
    assert(PyErr_Occurred());
    return NULL;
  }

  if (0 != PyObject_GetBufferContiguousBounded(B, &B_buffer, PyBUF_SIMPLE, INT32_MAX))
  {
    assert(PyErr_Occurred());
    PyBuffer_Release(&A_buffer);
    return NULL;
  }

  PyObject *result = NULL;

  int nOut;
  void *pOut = NULL;

  int rc = sqlite3changeset_concat(A_buffer.len, A_buffer.buf, B_buffer.len, B_buffer.buf, &nOut, &pOut);

  if (rc == SQLITE_OK)
    result = PyBytes_FromStringAndSize((char *)pOut, nOut);
  else
    SET_EXC(rc, NULL);

  sqlite3_free(pOut);

  PyBuffer_Release(&A_buffer);
  PyBuffer_Release(&B_buffer);
  assert((PyErr_Occurred() && !result) || (result && !PyErr_Occurred()));
  return result;
}

/** .. method:: concat_stream(A: SessionStreamInput, B: SessionStreamInput, output: SessionStreamOutput) -> None

  Streaming concatenate two changesets

  -* sqlite3changeset_concat_strm
*/
static PyObject *
APSWChangeset_concat_stream(PyObject *Py_UNUSED(static_method), PyObject *const *fast_args, Py_ssize_t fast_nargs,
                            PyObject *fast_kwnames)
{

  PyObject *A = NULL, *B = NULL;
  PyObject *output = NULL;

  {
    Changeset_concat_stream_CHECK;
    ARG_PROLOG(3, Changeset_concat_stream_KWNAMES);
    ARG_MANDATORY ARG_Callable(A);
    ARG_MANDATORY ARG_Callable(B);
    ARG_MANDATORY ARG_Callable(output);
    ARG_EPILOG(NULL, Changeset_concat_stream_USAGE, );
  }

  int rc = sqlite3changeset_concat_strm(APSWSession_xInput, A, APSWSession_xInput, B, APSWSession_xOutput, output);
  SET_EXC(rc, NULL);
  if (PyErr_Occurred())
    return NULL;
  Py_RETURN_NONE;
}

/** .. method:: iter(changeset: ChangesetInput, *, flags: int = 0) -> Iterator[TableChange]

   Provides an iterator over a changeset.  You can supply the changeset as
   the bytes, or streamed via a callable.

   If flags is non-zero them the ``v2`` API is used (marked as experimental)

  -* sqlite3changeset_start sqlite3changeset_start_v2 sqlite3changeset_start_strm sqlite3changeset_start_v2_strm
*/

static PyObject *
APSWChangeset_iter(PyObject *Py_UNUSED(static_method), PyObject *const *fast_args, Py_ssize_t fast_nargs,
                   PyObject *fast_kwnames)
{
  PyObject *changeset = NULL;
  int flags = 0;
  {
    Changeset_iter_CHECK;
    ARG_PROLOG(1, Changeset_iter_KWNAMES);
    ARG_MANDATORY ARG_ChangesetInput(changeset);
    ARG_OPTIONAL ARG_int(flags);
    ARG_EPILOG(NULL, Changeset_iter_USAGE, );
  }

  APSWChangesetIterator *iterator = (APSWChangesetIterator *)_PyObject_New(&APSWChangesetIteratorType);
  if (!iterator)
    return NULL;

  iterator->iter = NULL;
  iterator->xInput = NULL;
  iterator->buffer_source = NULL;
  iterator->last_table_change = NULL;

  /* streaming? */
  if (PyCallable_Check(changeset))
  {
    int rc = flags ? sqlite3changeset_start_v2_strm(&iterator->iter, APSWSession_xInput, changeset, flags)
                   : sqlite3changeset_start_strm(&iterator->iter, APSWSession_xInput, changeset);
    if (rc != SQLITE_OK)
    {
      SET_EXC(rc, NULL);
      goto error;
    }
    iterator->xInput = Py_NewRef(changeset);
  }
  else
  {
    if (0 != PyObject_GetBufferContiguousBounded(changeset, &iterator->buffer_buffer, PyBUF_SIMPLE, INT32_MAX))
      goto error;
    iterator->buffer_source = Py_NewRef(changeset);

    int rc = flags ? sqlite3changeset_start_v2(&iterator->iter, (int)iterator->buffer_buffer.len,
                                               iterator->buffer_buffer.buf, flags)
                   : sqlite3changeset_start(&iterator->iter, (int)iterator->buffer_buffer.len,
                                            iterator->buffer_buffer.buf);
    if (rc != SQLITE_OK)
    {
      SET_EXC(rc, NULL);
      goto error;
    }
  }

  return (PyObject *)iterator;

error:
  Py_DECREF(iterator);
  assert(PyErr_Occurred());
  return NULL;
}

/** .. method:: apply(changeset: ChangesetInput, db: Connection, *, filter: Optional[Callable[[str], bool]] = None, conflict: Optional[Callable[[int,TableChange], int]] = None, flags: int = 0, rebase: bool = False) -> bytes | None

  Applies a changeset to a database.

  :param source: The changeset either as the bytes, or a stream
  :param db: The connection to make the change on
  :param filter: Callback to determine if changes to a table are done
  :param conflict: Callback to handle a change that cannot be applied
  :param flags: `v2 API flags <https://www.sqlite.org/session/c_changesetapply_fknoaction.html>`__.
  :param rebase: If ``True`` then return :class:`rebase <Rebaser>` information, else :class:`None`.

  Filter
  ------

  Callback called with a table name, once per table that has a change.  It should return ``True``
  if changes to that table should be applied, or ``False`` to ignore them.  If not supplied then
  all tables have changes applied.

  Conflict
  --------

  When a change cannot be applied the conflict handler determines what
  to do.  It is called with a `conflict reason
  <https://www.sqlite.org/session/c_changeset_conflict.html>`__ as the
  first parameter, and a :class:`TableChange` as the second.  Possible
  conflicts are `described here
  <https://sqlite.org/sessionintro.html#conflicts>`__.

  It should return the `action to take <https://www.sqlite.org/session/c_changeset_abort.html>`__.

  If not supplied or on error, ``SQLITE_CHANGESET_ABORT`` is returned.

  See the :ref:`example <example_applying>`.

  -* sqlite3changeset_apply_v2 sqlite3changeset_apply_v2_strm

*/

/* this is needed because xFilter and xCallback share a context */
struct applyInfoContext
{
  PyObject *xFilter;
  PyObject *xConflict;
};

static int
applyFilter(void *pCtx, const char *zTab)
{
  /* previous filter could cause this */
  MakeExistingException();

  if (PyErr_Occurred())
    return 0;

  struct applyInfoContext *aic = (struct applyInfoContext *)pCtx;

  PyObject *vargs[] = { NULL, PyUnicode_FromString(zTab) };
  PyObject *result = NULL;
  if (vargs[1])
    result = PyObject_Vectorcall(aic->xFilter, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[1]);
  if (!result)
    return 0;
  int ret = PyObject_IsTrueStrict(result);
  Py_DECREF(result);

  return PyErr_Occurred() ? 0 : ret;
}

static int
applyConflict(void *pCtx, int eConflict, sqlite3_changeset_iter *p)
{
  /* previous filter could cause this */
  MakeExistingException();

  if (PyErr_Occurred())
    return SQLITE_CHANGESET_ABORT;

  struct applyInfoContext *aic = (struct applyInfoContext *)pCtx;

  int val = SQLITE_CHANGESET_ABORT;
  PyObject *py_eConflict = NULL, *result = NULL;
  APSWTableChange *table_change = MakeTableChange(p);

  if (!table_change)
    goto exit;

  py_eConflict = PyLong_FromLong(eConflict);
  if (!py_eConflict)
    goto exit;

  PyObject *vargs[] = { NULL, py_eConflict, (PyObject *)table_change };
  result = PyObject_Vectorcall(aic->xConflict, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  if (result)
  {

    val = PyLong_AsInt(result);
    if (!PyErr_Occurred())
    {
      switch (val)
      {
      case SQLITE_CHANGESET_OMIT:
      case SQLITE_CHANGESET_REPLACE:
      case SQLITE_CHANGESET_ABORT:
        break;
      default:
        PyErr_Format(PyExc_ValueError, "Conflict return %d is not valid SQLITE_CHANGESET_ value", val);
      }
    }
  }

exit:
  if (PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "session.apply.xConflict", "{s: i, s: O}", "eConflict", eConflict, "return",
                     OBJ(result));

  Py_XDECREF(py_eConflict);
  Py_XDECREF(result);
  if (table_change)
  {
    table_change->iter = NULL;
    Py_DECREF((PyObject *)table_change);
  }

  return PyErr_Occurred() ? SQLITE_CHANGESET_ABORT : val;
}

static int
conflictReject(void *pCtx, int eConflict, sqlite3_changeset_iter *p)
{
  return SQLITE_CHANGESET_ABORT;
}

static PyObject *
APSWChangeset_apply(PyObject *Py_UNUSED(static_method), PyObject *const *fast_args, Py_ssize_t fast_nargs,
                    PyObject *fast_kwnames)
{
  PyObject *changeset;
  Connection *db = NULL;

  PyObject *filter = NULL;
  PyObject *conflict = NULL;

  int flags = 0;
  int rebase = 0;

  {
    Changeset_apply_CHECK;
    ARG_PROLOG(2, Changeset_apply_KWNAMES);
    ARG_MANDATORY ARG_ChangesetInput(changeset);
    ARG_MANDATORY ARG_Connection(db);
    ARG_OPTIONAL ARG_optional_Callable(filter);
    ARG_OPTIONAL ARG_optional_Callable(conflict);
    ARG_OPTIONAL ARG_int(flags);
    ARG_OPTIONAL ARG_bool(rebase);
    ARG_EPILOG(NULL, Changeset_apply_USAGE, );
  }

  CHECK_CLOSED(db, NULL);

  struct applyInfoContext aic = { .xFilter = filter, .xConflict = conflict };

  int res = SQLITE_ERROR;

  void *pRebase = NULL;
  int nRebase = 0;

  /* streaming? */
  if (PyCallable_Check(changeset))
  {
    res = sqlite3changeset_apply_v2_strm(db->db, APSWSession_xInput, changeset, filter ? applyFilter : NULL,
                                         conflict ? applyConflict : conflictReject, &aic, rebase ? &pRebase : NULL,
                                         rebase ? &nRebase : NULL, flags);
  }
  else
  {
    Py_buffer changeset_buffer;
    if (0 != PyObject_GetBufferContiguousBounded(changeset, &changeset_buffer, PyBUF_SIMPLE, INT32_MAX))
    {
      assert(PyErr_Occurred());
      return NULL;
    }
    res = sqlite3changeset_apply_v2(db->db, changeset_buffer.len, changeset_buffer.buf, filter ? applyFilter : NULL,
                                    conflict ? applyConflict : conflictReject, &aic, rebase ? &pRebase : NULL,
                                    rebase ? &nRebase : NULL, flags);
    PyBuffer_Release(&changeset_buffer);
  }

  if (res != SQLITE_OK)
  {
    assert(pRebase == NULL);
    SET_EXC(res, NULL);
    return NULL;
  }

  if (PyErr_Occurred())
  {
    if (res == SQLITE_OK)
      sqlite3_log(SQLITE_ERROR, "An error occurred at the Python level but could not be reported to the session "
                                "extension, so SQLite considered the session apply successful");
  }

  if (rebase)
  {
    PyObject *retval = PyBytes_FromStringAndSize(pRebase, nRebase);
    if (retval)
    {
      sqlite3_free(pRebase);
      return retval;
    }
  }

  sqlite3_free(pRebase);

  if (PyErr_Occurred())
    return NULL;

  Py_RETURN_NONE;
}

static PyObject *
APSWChangesetIterator_next(PyObject *self_)
{
  APSWChangesetIterator *self = (APSWChangesetIterator *)self_;
  /* invalidate what we previous made */
  if (self->last_table_change)
  {
    self->last_table_change->iter = NULL;
    self->last_table_change = NULL;
  }

  int rc = sqlite3changeset_next(self->iter);
  if (rc == SQLITE_DONE)
    return NULL;

  if (rc != SQLITE_ROW)
  {
    SET_EXC(rc, NULL);
    return NULL;
  }

  self->last_table_change = MakeTableChange(self->iter);

  assert((self->last_table_change == NULL && PyErr_Occurred())
         || (self->last_table_change != NULL && !PyErr_Occurred()));
  return self->last_table_change ? (PyObject *)self->last_table_change : NULL;
}

static PyObject *
APSWChangesetIterator_iter(PyObject *self)
{
  return Py_NewRef(self);
}

static void
APSWChangesetIterator_dealloc(PyObject *self_)
{
  APSWChangesetIterator *self = (APSWChangesetIterator *)self_;
  if (self->iter)
  {
    sqlite3changeset_finalize(self->iter);
    self->iter = NULL;
  }
  Py_CLEAR(self->xInput);
  if (self->buffer_source)
  {
    PyBuffer_Release(&self->buffer_buffer);
    Py_CLEAR(self->buffer_source);
  }
  Py_TpFree(self_);
}

/** .. class:: ChangesetBuilder

  This object wraps a `sqlite3_changegroup <https://sqlite.org/session/changegroup.html>`__
  letting you concatenate changesets and individual :class:`TableChange` into one larger
  changeset.

 */

#define CHECK_BUILDER_CLOSED(e)                                                                                        \
  do                                                                                                                   \
  {                                                                                                                    \
    if (!self->group)                                                                                                  \
    {                                                                                                                  \
      PyErr_Format(PyExc_ValueError, "The ChangesetBuilder has been closed");                                          \
      return e;                                                                                                        \
    }                                                                                                                  \
  } while (0)

/** .. method:: __init__()

 Creates a new empty builder.

 -* sqlite3changegroup_new

 */
static int
APSWChangesetBuilder_init(PyObject *self_, PyObject *args, PyObject *kwargs)
{
  APSWChangesetBuilder *self = (APSWChangesetBuilder *)self_;
  {
    ChangesetBuilder_init_CHECK;
    PREVENT_INIT_MULTIPLE_CALLS;
    ARG_CONVERT_VARARGS_TO_FASTCALL;
    ARG_PROLOG(0, ChangesetBuilder_init_KWNAMES);
    ARG_EPILOG(-1, ChangesetBuilder_init_USAGE, Py_XDECREF(fast_kwnames));
  }

  int rc = sqlite3changegroup_new(&self->group);
  SET_EXC(rc, NULL);
  return (rc == SQLITE_OK) ? 0 : -1;
}

static void
APSWChangesetBuilder_close_internal(APSWChangesetBuilder *self)
{
  if (self->group)
  {
    sqlite3changegroup_delete(self->group);
    self->group = NULL;
  }
  if (self->connection)
  {
    Connection_remove_dependent(self->connection, (PyObject *)self);
    Py_CLEAR(self->connection);
  }
}

static void
APSWChangesetBuilder_dealloc(PyObject *self_)
{
  APSWChangesetBuilder *self = (APSWChangesetBuilder *)self_;
  APSWChangesetBuilder_close_internal(self);
  Py_TpFree(self_);
}

/** .. method:: close() -> None

  Releases the builder

  -* sqlite3changegroup_delete
*/
static PyObject *
APSWChangesetBuilder_close(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  APSWChangesetBuilder *self = (APSWChangesetBuilder *)self_;
  {
    ChangesetBuilder_close_CHECK;
    ARG_PROLOG(0, ChangesetBuilder_close_KWNAMES);
    ARG_EPILOG(NULL, ChangesetBuilder_close_USAGE, );
  }

  APSWChangesetBuilder_close_internal(self);
  MakeExistingException();

  if (PyErr_Occurred())
    return NULL;

  Py_RETURN_NONE;
}

/** .. method:: add(changeset: ChangesetInput) -> None

  :param changeset: The changeset as the bytes, or a stream

  Adds the changeset to the builder

  -* sqlite3changegroup_add sqlite3changegroup_add_strm
 */
static PyObject *
APSWChangesetBuilder_add(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  APSWChangesetBuilder *self = (APSWChangesetBuilder *)self_;
  PyObject *changeset = NULL;
  {
    ChangesetBuilder_add_CHECK;
    ARG_PROLOG(1, ChangesetBuilder_add_KWNAMES);
    ARG_MANDATORY ARG_ChangesetInput(changeset);
    ARG_EPILOG(NULL, ChangesetBuilder_add_USAGE, );
  }

  CHECK_BUILDER_CLOSED(NULL);

  int res = SQLITE_ERROR;

  if (PyCallable_Check(changeset))
    res = sqlite3changegroup_add_strm(self->group, APSWSession_xInput, changeset);
  else
  {
    Py_buffer changeset_buffer;
    if (0 != PyObject_GetBufferContiguousBounded(changeset, &changeset_buffer, PyBUF_SIMPLE, INT32_MAX))
      return NULL;
    res = sqlite3changegroup_add(self->group, changeset_buffer.len, changeset_buffer.buf);
    PyBuffer_Release(&changeset_buffer);
  }
  SET_EXC(res, NULL);
  if (PyErr_Occurred())
    return NULL;
  Py_RETURN_NONE;
}

/** .. method:: add_change(change: TableChange) -> None

  :param change: An individual change to add.

  You can obtain :class:`TableChange` from :meth:`Changeset.iter` or from the conflict callback
  of :meth:`Changeset.apply`.

  -* sqlite3changegroup_add_change
 */
static PyObject *
APSWChangesetBuilder_add_change(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                                PyObject *fast_kwnames)
{
  APSWChangesetBuilder *self = (APSWChangesetBuilder *)self_;
  APSWTableChange *change = NULL;

  {
    ChangesetBuilder_add_change_CHECK;
    ARG_PROLOG(1, ChangesetBuilder_add_change_KWNAMES);
    ARG_MANDATORY ARG_TableChange(change);
    ARG_EPILOG(NULL, ChangesetBuilder_add_change_USAGE, );
  }

  CHECK_BUILDER_CLOSED(NULL);

  if (!change->iter)
    return PyErr_Format(ExcInvalidContext, "The table change has gone out of scope");

  int rc = sqlite3changegroup_add_change(self->group, change->iter);
  SET_EXC(rc, NULL);

  if (PyErr_Occurred())
    return NULL;
  Py_RETURN_NONE;
}

/** .. method:: schema(db: Connection, schema: str) -> None

  Ensures the changesets comply with the tables in the database

  :param db: Connection to consult
  :param schema: `main`, `temp`, the name in `ATTACH <https://sqlite.org/lang_attach.html>`__

  You will get :exc:`MisuseError` if changes have already been added, or this method has
  already been called.

  -* sqlite3changegroup_schema
 */
static PyObject *
APSWChangesetBuilder_schema(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  APSWChangesetBuilder *self = (APSWChangesetBuilder *)self_;
  Connection *db = NULL;
  const char *schema = NULL;

  {
    ChangesetBuilder_schema_CHECK;
    ARG_PROLOG(2, ChangesetBuilder_schema_KWNAMES);
    ARG_MANDATORY ARG_Connection(db);
    ARG_MANDATORY ARG_str(schema);
    ARG_EPILOG(NULL, ChangesetBuilder_schema_USAGE, );
  }

  CHECK_BUILDER_CLOSED(NULL);
  CHECK_CLOSED(db, NULL);

  int rc = sqlite3changegroup_schema(self->group, db->db, schema);
  SET_EXC(rc, NULL);
  if (PyErr_Occurred())
    return NULL;

  /* from this point on, the schema has been set, but we could
     fail at the Python level.  There is nothing we can do about
     that, and it is unlikely in practise. */

  self->connection = db;
  Py_INCREF(self->connection);

  PyObject *weakref = NULL;

  weakref = PyWeakref_NewRef((PyObject *)self, NULL);
  if (!weakref)
    return NULL;
  int append = PyList_Append(db->dependents, weakref);
  Py_DECREF(weakref);
  if (append)
    return NULL;

  assert(!PyErr_Occurred());

  Py_RETURN_NONE;
}

/** .. method:: output() -> bytes

  Produces a changeset of what was built so far

  -* sqlite3changegroup_output
 */
static PyObject *
APSWChangesetBuilder_output(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  APSWChangesetBuilder *self = (APSWChangesetBuilder *)self_;
  {
    ChangesetBuilder_output_CHECK;
    ARG_PROLOG(0, ChangesetBuilder_output_KWNAMES);
    ARG_EPILOG(NULL, ChangesetBuilder_output_USAGE, );
  }

  CHECK_BUILDER_CLOSED(NULL);

  int nData;
  void *pData = NULL;

  int rc = sqlite3changegroup_output(self->group, &nData, &pData);
  PyObject *result = NULL;
  if (rc != SQLITE_OK)
    SET_EXC(rc, NULL);
  else
    result = PyBytes_FromStringAndSize((const char *)pData, nData);
  if (pData)
    sqlite3_free(pData);
  return result;
}

/** .. method:: output_stream(output: SessionStreamOutput) -> None

  Produces a streaming changeset of what was built so far

  -* sqlite3changegroup_output_strm
 */
static PyObject *
APSWChangesetBuilder_output_stream(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                                   PyObject *fast_kwnames)
{
  APSWChangesetBuilder *self = (APSWChangesetBuilder *)self_;
  PyObject *output = NULL;
  {
    ChangesetBuilder_output_stream_CHECK;
    ARG_PROLOG(1, ChangesetBuilder_output_stream_KWNAMES);
    ARG_MANDATORY ARG_Callable(output);
    ARG_EPILOG(NULL, ChangesetBuilder_output_stream_USAGE, );
  }

  CHECK_BUILDER_CLOSED(NULL);

  int rc = sqlite3changegroup_output_strm(self->group, APSWSession_xOutput, output);
  SET_EXC(rc, NULL);

  if (PyErr_Occurred())
    return NULL;
  Py_RETURN_NONE;
}

static int
APSWChangesetBuilder_tp_traverse(PyObject *self_, visitproc visit, void *arg)
{
  APSWChangesetBuilder *self = (APSWChangesetBuilder *)self_;
  Py_VISIT(self->connection);
  return 0;
}


/** .. class:: Rebaser

  This object wraps a `sqlite3_rebaser
  <https://www.sqlite.org/session/rebaser.html>`__ object.

*/

#define CHECK_REBASER_CLOSED(e)                                                                                        \
  do                                                                                                                   \
  {                                                                                                                    \
    if (!self->rebaser)                                                                                                \
    {                                                                                                                  \
      PyErr_Format(PyExc_ValueError, "The rebaser has been closed");                                                   \
      return e;                                                                                                        \
    }                                                                                                                  \
  } while (0)

/** .. method:: __init__()

  Starts a new rebaser.

  -* sqlite3rebaser_create
 */
static int
APSWRebaser_init(PyObject *self_, PyObject *args, PyObject *kwargs)
{
  APSWRebaser *self = (APSWRebaser *)self_;
  {
    Rebaser_init_CHECK;
    PREVENT_INIT_MULTIPLE_CALLS;
    ARG_CONVERT_VARARGS_TO_FASTCALL;
    ARG_PROLOG(0, Rebaser_init_KWNAMES);
    ARG_EPILOG(-1, Rebaser_init_USAGE, Py_XDECREF(fast_kwnames));
  }

  int rc = sqlite3rebaser_create(&self->rebaser);
  if (rc != SQLITE_OK)
  {
    SET_EXC(rc, NULL);
    return -1;
  }

  self->init_was_called = 1;
  return 0;
}

/** .. method:: configure(cr: Buffer) -> None

  Tells the rebaser about conflict resolutions made in an earlier
  :meth:`Changeset.apply`.

  -* sqlite3rebaser_configure
 */
static PyObject *
APSWRebaser_configure(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  APSWRebaser *self = (APSWRebaser *)self_;
  CHECK_REBASER_CLOSED(NULL);
  PyObject *cr = NULL;
  {
    Rebaser_configure_CHECK;
    ARG_PROLOG(1, Rebaser_configure_KWNAMES);
    ARG_MANDATORY ARG_Buffer(cr);
    ARG_EPILOG(NULL, Rebaser_configure_USAGE, );
  }

  Py_buffer cr_buffer;

  if (0 != PyObject_GetBufferContiguousBounded(cr, &cr_buffer, PyBUF_SIMPLE, INT32_MAX))
  {
    assert(PyErr_Occurred());
    return NULL;
  }

  int rc = sqlite3rebaser_configure(self->rebaser, cr_buffer.len, cr_buffer.buf);
  PyBuffer_Release(&cr_buffer);
  SET_EXC(rc, NULL);

  if (PyErr_Occurred())
    return NULL;
  Py_RETURN_NONE;
}

/** .. method:: rebase(changeset: Buffer) -> bytes

  Produces a new changeset rebased according to :meth:`configure` calls made.

  -* sqlite3rebaser_rebase
 */
static PyObject *
APSWRebaser_rebase(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  APSWRebaser *self = (APSWRebaser *)self_;
  CHECK_REBASER_CLOSED(NULL);

  PyObject *changeset = NULL;
  Py_buffer changeset_buffer;
  {
    Rebaser_rebase_CHECK;
    ARG_PROLOG(1, Rebaser_rebase_KWNAMES);
    ARG_MANDATORY ARG_Buffer(changeset);
    ARG_EPILOG(NULL, Rebaser_rebase_USAGE, );
  }

  if (0 != PyObject_GetBufferContiguousBounded(changeset, &changeset_buffer, PyBUF_SIMPLE, INT32_MAX))
  {
    assert(PyErr_Occurred());
    return NULL;
  }

  PyObject *result = NULL;

  int nOut;
  void *pOut = NULL;

  int rc = sqlite3rebaser_rebase(self->rebaser, changeset_buffer.len, changeset_buffer.buf, &nOut, &pOut);
  if (rc == SQLITE_OK)
    result = PyBytes_FromStringAndSize((char *)pOut, nOut);
  else
    SET_EXC(rc, NULL);
  sqlite3_free(pOut);

  PyBuffer_Release(&changeset_buffer);
  assert((PyErr_Occurred() && !result) || (result && !PyErr_Occurred()));
  return result;
}

/** .. method:: rebase_stream(changeset: SessionStreamInput, output: SessionStreamOutput) -> None

  Produces a new changeset rebased according to :meth:`configure` calls made, using streaming
  input and output.

  -* sqlite3rebaser_rebase_strm
 */
static PyObject *
APSWRebaser_rebase_stream(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  APSWRebaser *self = (APSWRebaser *)self_;
  CHECK_REBASER_CLOSED(NULL);
  PyObject *changeset = NULL;
  PyObject *output = NULL;
  {
    Rebaser_rebase_stream_CHECK;
    ARG_PROLOG(2, Rebaser_rebase_stream_KWNAMES);
    ARG_MANDATORY ARG_Callable(changeset);
    ARG_MANDATORY ARG_Callable(output);
    ARG_EPILOG(NULL, Rebaser_rebase_stream_USAGE, );
  }

  int rc = sqlite3rebaser_rebase_strm(self->rebaser, APSWSession_xInput, changeset, APSWSession_xOutput, output);
  SET_EXC(rc, NULL);
  if (PyErr_Occurred())
    return NULL;
  Py_RETURN_NONE;
}

static void
APSWRebaser_dealloc(PyObject *self_)
{
  APSWRebaser *self = (APSWRebaser *)self_;
  if (self->rebaser)
  {
    sqlite3rebaser_delete(self->rebaser);
    self->rebaser = NULL;
  }
  Py_TpFree(self_);
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
  { "enabled", APSWSession_get_enabled, APSWSession_set_enabled, Session_enabled_DOC },
  { "indirect", APSWSession_get_indirect, APSWSession_set_indirect, Session_indirect_DOC },
  { "is_empty", APSWSession_get_empty, NULL, Session_is_empty_DOC },
  { "memory_used", APSWSession_get_memory_used, NULL, Session_memory_used_DOC },
  { "changeset_size", APSWSession_get_changeset_size, NULL, Session_changeset_size_DOC },
  { 0 },
};

static PyTypeObject APSWSessionType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.Session",
  .tp_basicsize = sizeof(APSWSession),
  .tp_doc = Session_class_DOC,
  .tp_new = PyType_GenericNew,
  .tp_init = APSWSession_init,
  .tp_dealloc = APSWSession_dealloc,
  .tp_methods = APSWSession_methods,
  .tp_getset = APSWSession_getset,
  .tp_flags = Py_TPFLAGS_BASETYPE | Py_TPFLAGS_DEFAULT,
  .tp_weaklistoffset = offsetof(APSWSession, weakreflist),
  .tp_traverse = APSWSession_tp_traverse,
};

static PyMethodDef APSWChangeset_methods[] = {
  { "invert", (PyCFunction)APSWChangeset_invert, METH_STATIC | METH_FASTCALL | METH_KEYWORDS, Changeset_invert_DOC },
  { "invert_stream", (PyCFunction)APSWChangeset_invert_stream, METH_STATIC | METH_FASTCALL | METH_KEYWORDS,
    Changeset_invert_stream_DOC },
  { "concat", (PyCFunction)APSWChangeset_concat, METH_STATIC | METH_FASTCALL | METH_KEYWORDS, Changeset_concat_DOC },
  { "concat_stream", (PyCFunction)APSWChangeset_concat_stream, METH_STATIC | METH_FASTCALL | METH_KEYWORDS,
    Changeset_concat_stream_DOC },
  { "iter", (PyCFunction)APSWChangeset_iter, METH_STATIC | METH_FASTCALL | METH_KEYWORDS, Changeset_iter_DOC },
  { "apply", (PyCFunction)APSWChangeset_apply, METH_STATIC | METH_FASTCALL | METH_KEYWORDS, Changeset_apply_DOC },
  { 0 },
};

static PyTypeObject APSWChangesetType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.Changeset",
  .tp_doc = Changeset_class_DOC,
  .tp_basicsize = sizeof(APSWChangeset),
  .tp_methods = APSWChangeset_methods,
};

static PyTypeObject APSWChangesetIteratorType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.ChangesetIterator", .tp_basicsize = sizeof(APSWChangesetIterator),
  .tp_iternext = APSWChangesetIterator_next,           .tp_iter = APSWChangesetIterator_iter,
  .tp_dealloc = APSWChangesetIterator_dealloc,
};

static PyMethodDef APSWChangesetBuilder_methods[] = {
  { "close", (PyCFunction)APSWChangesetBuilder_close, METH_FASTCALL | METH_KEYWORDS, ChangesetBuilder_close_DOC },
  { "output", (PyCFunction)APSWChangesetBuilder_output, METH_FASTCALL | METH_KEYWORDS, ChangesetBuilder_output_DOC },
  { "output_stream", (PyCFunction)APSWChangesetBuilder_output_stream, METH_FASTCALL | METH_KEYWORDS,
    ChangesetBuilder_output_stream_DOC },
  { "add", (PyCFunction)APSWChangesetBuilder_add, METH_FASTCALL | METH_KEYWORDS, ChangesetBuilder_add_DOC },
  { "add_change", (PyCFunction)APSWChangesetBuilder_add_change, METH_FASTCALL | METH_KEYWORDS,
    ChangesetBuilder_add_change_DOC },
  { "schema", (PyCFunction)APSWChangesetBuilder_schema, METH_FASTCALL | METH_KEYWORDS, ChangesetBuilder_schema_DOC },
  { 0 },
};

static PyTypeObject APSWChangesetBuilderType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.ChangesetBuilder",
  .tp_basicsize = sizeof(APSWChangesetBuilder),
  .tp_methods = APSWChangesetBuilder_methods,
  .tp_new = PyType_GenericNew,
  .tp_init = APSWChangesetBuilder_init,
  .tp_dealloc = APSWChangesetBuilder_dealloc,
  .tp_doc = ChangesetBuilder_class_DOC,
  .tp_weaklistoffset = offsetof(APSWChangesetBuilder, weakreflist),
  .tp_traverse = APSWChangesetBuilder_tp_traverse,
};

static PyGetSetDef APSWTableChange_getset[] = {
  { "name", APSWTableChange_name, NULL, TableChange_name_DOC },
  { "column_count", APSWTableChange_column_count, NULL, TableChange_column_count_DOC },
  { "op", APSWTableChange_op, NULL, TableChange_op_DOC },
  { "opcode", APSWTableChange_opcode, NULL, TableChange_opcode_DOC },
  { "indirect", APSWTableChange_indirect, NULL, TableChange_indirect_DOC },
  { "old", APSWTableChange_old, NULL, TableChange_old_DOC },
  { "new", APSWTableChange_new, NULL, TableChange_new_DOC },
  { "conflict", APSWTableChange_conflict, NULL, TableChange_conflict_DOC },
  { "fk_conflicts", APSWTableChange_fk_conflicts, NULL, TableChange_fk_conflicts_DOC },
  { "pk_columns", APSWTableChange_pk_columns, NULL, TableChange_pk_columns_DOC },
  { 0 },
};

static PyTypeObject APSWTableChangeType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.TableChange",
  .tp_basicsize = sizeof(APSWTableChange),
  .tp_getset = APSWTableChange_getset,
  .tp_doc = TableChange_class_DOC,
  .tp_dealloc = APSWTableChange_dealloc,
  .tp_str = APSWTableChange_tp_str,
};

static PyMethodDef APSWRebaser_methods[] = {
  { "configure", (PyCFunction)APSWRebaser_configure, METH_FASTCALL | METH_KEYWORDS, Rebaser_configure_DOC },
  { "rebase", (PyCFunction)APSWRebaser_rebase, METH_FASTCALL | METH_KEYWORDS, Rebaser_rebase_DOC },
  { "rebase_stream", (PyCFunction)APSWRebaser_rebase_stream, METH_FASTCALL | METH_KEYWORDS, Rebaser_rebase_stream_DOC },

  { 0 },
};

static PyTypeObject APSWRebaserType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.Rebaser",
  .tp_basicsize = sizeof(APSWRebaser),
  .tp_doc = Rebaser_class_DOC,
  .tp_methods = APSWRebaser_methods,
  .tp_new = PyType_GenericNew,
  .tp_init = APSWRebaser_init,
  .tp_dealloc = APSWRebaser_dealloc,
};
