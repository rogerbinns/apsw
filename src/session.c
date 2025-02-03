/**

Session extension
*****************

The `session extension <https://www.sqlite.org/sessionintro.html>`
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

Availability
============

The session extension and APSW support for it have to be enabled at
compile time for each.  APSW builds from PyPI include session support.

Most platform provided SQLite are configured with session support, and
APSW should end up with it too.

The methods and classes documented here are only present if session
support was enabled.

API
===

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
  PyObject *connection;
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

  self->connection = Py_NewRef((PyObject *)db);

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

static PyMethodDef APSWSession_methods[] = {
  { "close", (PyCFunction)APSWSession_close, METH_FASTCALL | METH_KEYWORDS, Session_close_DOC },
  { 0 },
};

static PyGetSetDef APSWSession_getset[] = {
  { "enabled", (getter)APSWSession_get_enabled, (setter)APSWSession_set_enabled, Session_enabled_DOC },
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

static PyTypeObject APSWChangesetType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.Changeset",
  .tp_basicsize = sizeof(APSWChangeset),
};

static PyTypeObject APSWChangesetBuilderType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.ChangesetBuilder",
  .tp_basicsize = sizeof(APSWChangesetBuilder),
};

static PyTypeObject APSWTableChangeType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.TableChange",
  .tp_basicsize = sizeof(APSWTableChange),
};
