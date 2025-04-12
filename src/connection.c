/*
  Connection handling code

  See the accompanying LICENSE file.
*/

/**

.. _connections:

Connections to a database
*************************

A :class:`Connection` encapsulates access to a database.  You can have
multiple :class:`Connections <Connection>` open against the same
database file in the same process, across threads and in other
processes.

*/

/* CALLBACK INFO */

/* details of a registered function passed as user data to sqlite3_create_function_v2 */
typedef struct
{
  PyObject_HEAD
  const char *name;           /* utf8 function name */
  PyObject *scalarfunc;       /* the function to call for stepping */
  PyObject *aggregatefactory; /* factory for aggregate functions */
  PyObject *windowfactory;    /* factory for window functions */
} FunctionCBInfo;

/* a particular aggregate function instance used as sqlite3_aggregate_context */
typedef struct
{
  enum
  {
    afcOK = 1,
    afcUNINIT = 0,
    afcERROR = -1
  } state;
  PyObject *aggvalue;  /* the aggregation value passed as first parameter */
  PyObject *stepfunc;  /* step function */
  PyObject *finalfunc; /* final function */
} aggregatefunctioncontext;

/* a particular window function instance used as sqlite3_aggregate_context */
typedef struct
{
  enum
  {
    wfcOK = 1,
    wfcUNINIT = 0,
    wfcERROR = -1
  } state;
  PyObject *aggvalue;    /* the aggregation value passed as first parameter */
  PyObject *stepfunc;    /* step function */
  PyObject *finalfunc;   /* final function */
  PyObject *valuefunc;   /* value function */
  PyObject *inversefunc; /* inverse function */
} windowfunctioncontext;

struct tracehook
{
  unsigned mask;
  PyObject *callback;
  PyObject *id;
};

struct progresshandler
{
  int nsteps;
  PyObject *callback;
  PyObject *id;
};

/* CONNECTION TYPE */

struct Connection
{
  PyObject_HEAD
  sqlite3 *db;                      /* the actual database connection */
  sqlite3_mutex *dbmutex;           /* what we lock */
  struct StatementCache *stmtcache; /* prepared statement cache */

  fts5_api *fts5_api_cached;

  PyObject *dependents; /* tracking cursors & blobs etc as weakrefs belonging to this connection */

  PyObject *cursor_factory;

  /* registered hooks/handlers (NULL or callable) */
  PyObject *busyhandler;
  PyObject *rollbackhook;
  PyObject *updatehook;
  PyObject *commithook;
  PyObject *walhook;
  PyObject *authorizer;
  PyObject *collationneeded;
  PyObject *exectrace;
  PyObject *rowtrace;
  /* Array of tracehook.  Entry 0 is reserved for the set_profile
     callback. */
  struct tracehook *tracehooks;
  unsigned tracehooks_count;

  struct progresshandler *progresshandler;
  unsigned progresshandler_count;

  /* if we are using one of our VFS since sqlite doesn't reference count them */
  PyObject *vfs;

  /* used for nested with (contextmanager) statements */
  long savepointlevel;

  /* informational attributes */
  PyObject *open_flags;
  PyObject *open_vfs;

  /* weak reference support */
  PyObject *weakreflist;

  /* limit calls to callbacks */
  CALL_TRACK(xConnect);
  CALL_TRACK(xUpdate);

  int init_was_called;
};

typedef struct Connection Connection;

static PyTypeObject ConnectionType;

typedef struct _vtableinfo
{
  PyObject *datasource;   /* object with create/connect methods */
  Connection *connection; /* the Connection this is registered against so we don't
                             have to have a global table mapping sqlite3_db* to
                             Connection* */
  int bestindex_object;   /* 0: tuples are passed to xBestIndex, 1: object is */
  int use_no_change;
  struct sqlite3_module *sqlite3_module_def;
} vtableinfo;

/* forward declarations */
struct APSWBlob;
static void APSWBlob_init(struct APSWBlob *self, Connection *connection, sqlite3_blob *blob);
static PyTypeObject APSWBlobType;

struct APSWBackup;
static void APSWBackup_init(struct APSWBackup *self, Connection *dest, Connection *source, sqlite3_backup *backup);
static PyTypeObject APSWBackupType;

static PyTypeObject APSWCursorType;

struct ZeroBlobBind;
static PyTypeObject ZeroBlobBindType;

static void apsw_connection_remove(Connection *con);

static int apsw_connection_add(Connection *con);

static void
FunctionCBInfo_dealloc(FunctionCBInfo *self)
{
  if (self->name)
    PyMem_Free((void *)(self->name));
  Py_CLEAR(self->scalarfunc);
  Py_CLEAR(self->aggregatefactory);
  Py_CLEAR(self->windowfactory);
  Py_TpFree((PyObject *)self);
}

/** .. class:: Connection


  This object wraps a `sqlite3 pointer
  <https://sqlite.org/c3ref/sqlite3.html>`_.
*/

/* CONNECTION CODE */

static void
Connection_internal_cleanup(Connection *self)
{
  Py_CLEAR(self->cursor_factory);
  Py_CLEAR(self->busyhandler);
  Py_CLEAR(self->rollbackhook);
  Py_CLEAR(self->updatehook);
  Py_CLEAR(self->commithook);
  Py_CLEAR(self->walhook);
  Py_CLEAR(self->authorizer);
  Py_CLEAR(self->collationneeded);
  Py_CLEAR(self->exectrace);
  Py_CLEAR(self->rowtrace);
  Py_CLEAR(self->vfs);
  Py_CLEAR(self->open_flags);
  Py_CLEAR(self->open_vfs);
  for (unsigned i = 0; i < self->tracehooks_count; i++)
  {
    Py_CLEAR(self->tracehooks[i].callback);
    Py_CLEAR(self->tracehooks[i].id);
  }
  PyMem_Free(self->tracehooks);

  self->tracehooks = 0;
  self->tracehooks_count = 0;

  for (unsigned i = 0; i < self->progresshandler_count; i++)
  {
    Py_CLEAR(self->progresshandler[i].callback);
    Py_CLEAR(self->progresshandler[i].id);
  }
  PyMem_Free(self->progresshandler);

  self->progresshandler = 0;
  self->progresshandler_count = 0;
}

static void
Connection_remove_dependent(Connection *self, PyObject *o)
{
  /* in addition to removing the dependent, we also remove any dead
     weakrefs */
  Py_ssize_t i;

  for (i = 0; i < PyList_GET_SIZE(self->dependents);)
  {
    PyObject *wr = PyList_GET_ITEM(self->dependents, i);
    PyObject *wo = NULL;
    if (PyWeakref_GetRef(wr, &wo) < 0)
    {
      apsw_write_unraisable(NULL);
      continue;
    }
    if (!wo || Py_Is(wo, o))
    {
      PyList_SetSlice(self->dependents, i, i + 1, NULL);
      if (!wo)
        continue;
      Py_DECREF(wo);
      return;
    }
    Py_XDECREF(wo);
    i++;
  }
}

/* returns zero on success, non-zero on error */
static int
Connection_close_internal(Connection *self, int force)
{
  int res;

  PY_ERR_FETCH_IF(force == 2, exc_save);

  /* close out dependents by repeatedly processing first item until
     list is empty.  note that closing an item will cause the list to
     be perturbed as a side effect */
  while (self->dependents && PyList_GET_SIZE(self->dependents))
  {
    PyObject *closeres = NULL, *item = NULL, *wr = PyList_GET_ITEM(self->dependents, 0);
    if (PyWeakref_GetRef(wr, &item) < 0)
      return 1;
    if (!item)
    {
      Connection_remove_dependent(self, item);
      continue;
    }

    PyObject *vargs[] = { NULL, item, PyBool_FromLong(force) };
    if (vargs[2])
      closeres = PyObject_VectorcallMethod(apst.close, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
    Py_XDECREF(vargs[2]);
    Py_XDECREF(vargs[1]);
    Py_XDECREF(closeres);
    if (!closeres)
    {
      assert(PyErr_Occurred());
      if (force == 2)
        apsw_write_unraisable(NULL);
      else
      {
        sqlite3_mutex_leave(self->dbmutex);
        return 1;
      }
    }
  }

  if (self->stmtcache)
    statementcache_free(self->stmtcache);
  self->stmtcache = 0;

  apsw_connection_remove(self);

  /* This ensures any SQLITE_TRACE_CLOSE callbacks see a closed
     database */
  sqlite3 *tmp_db = self->db;
  sqlite3_mutex *tmp_mutex = self->dbmutex;
  self->db = 0;
  self->dbmutex = 0;
  /* caller should have acquired */
  sqlite3_mutex_leave(tmp_mutex);
  res = sqlite3_close(tmp_db);

  if (res != SQLITE_OK)
  {
    SET_EXC(res, NULL);
    if (force == 2)
    {
      PyErr_Format(ExcConnectionNotClosed,
                   "apsw.Connection at address %p. The destructor "
                   "has encountered an error %d closing the connection, but cannot raise an exception.",
                   self, res);
      apsw_write_unraisable(NULL);
    }
  }

  Connection_internal_cleanup(self);

  if (PyErr_Occurred() && force != 2)
  {
    AddTraceBackHere(__FILE__, __LINE__, "Connection.close", NULL);
    return 1;
  }

  if (force == 2)
    PY_ERR_RESTORE(exc_save);
  return 0;
}

/** .. method:: close(force: bool = False) -> None

  Closes the database.  If there are any outstanding :class:`cursors
  <Cursor>`, :class:`blobs <Blob>` or :class:`backups <Backup>` then
  they are closed too.  It is normally not necessary to call this
  method as the database is automatically closed when there are no
  more references.  It is ok to call the method multiple times.

  If your user defined functions or collations have direct or indirect
  references to the Connection then it won't be automatically garbage
  collected because of circular referencing that can't be
  automatically broken.  Calling *close* will free all those objects
  and what they reference.

  SQLite is designed to survive power failures at even the most
  awkward moments.  Consequently it doesn't matter if it is closed
  when the process is exited, or even if the exit is graceful or
  abrupt.  In the worst case of having a transaction in progress, that
  transaction will be rolled back by the next program to open the
  database, reverting the database to a know good state.

  If *force* is *True* then any exceptions are ignored.

  -* sqlite3_close
*/

/* Closes cursors and blobs belonging to this connection */
static PyObject *
Connection_close(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int force = 0;

  assert(!PyErr_Occurred());
  {
    Connection_close_CHECK;
    ARG_PROLOG(1, Connection_close_KWNAMES);
    ARG_OPTIONAL ARG_bool(force);
    ARG_EPILOG(NULL, Connection_close_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  if (Connection_close_internal(self, force))
  {
    assert(PyErr_Occurred());
    return NULL;
  }

  Py_RETURN_NONE;
}

static void
Connection_dealloc(Connection *self)
{
  PyObject_GC_UnTrack(self);
  APSW_CLEAR_WEAKREFS;

  DBMUTEX_FORCE(self->dbmutex);
  Connection_close_internal(self, 2);

  /* Our dependents all hold a refcount on us, so they must have all
      released before this destructor could be called */
  assert(!self->dependents || PyList_GET_SIZE(self->dependents) == 0);
  Py_CLEAR(self->dependents);

  Py_TpFree((PyObject *)self);
}

static PyObject *
Connection_new(PyTypeObject *type, PyObject *Py_UNUSED(args), PyObject *Py_UNUSED(kwds))
{
  Connection *self;

  self = (Connection *)type->tp_alloc(type, 0);
  if (self != NULL)
  {
    self->db = 0;
    self->dbmutex = 0;
    self->cursor_factory = Py_NewRef((PyObject *)&APSWCursorType);
    self->dependents = PyList_New(0);
    self->stmtcache = 0;
    self->fts5_api_cached = 0;
    self->busyhandler = 0;
    self->rollbackhook = 0;
    self->updatehook = 0;
    self->commithook = 0;
    self->walhook = 0;
    self->authorizer = 0;
    self->collationneeded = 0;
    self->exectrace = 0;
    self->rowtrace = 0;
    self->vfs = 0;
    self->savepointlevel = 0;
    self->open_flags = 0;
    self->open_vfs = 0;
    self->weakreflist = 0;
    self->tracehooks = PyMem_Malloc(sizeof(struct tracehook) * 1);
    self->tracehooks_count = 0;
    if (self->tracehooks)
    {
      self->tracehooks[0].callback = 0;
      self->tracehooks[0].id = 0;
      self->tracehooks[0].mask = 0;
      self->tracehooks_count = 1;
    }
    self->progresshandler = 0;
    self->progresshandler_count = 0;
    CALL_TRACK_INIT(xConnect);
    if (self->dependents && self->tracehooks)
      return (PyObject *)self;
  }

  return NULL;
}

/** .. method:: __init__(filename: str, flags: int = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, vfs: Optional[str] = None, statementcachesize: int = 100)

  Opens the named database.  You can use ``:memory:`` to get a private temporary
  in-memory database that is not shared with any other connections.

  :param flags: One or more of the `open flags <https://sqlite.org/c3ref/c_open_autoproxy.html>`_ orred together
  :param vfs: The name of the `vfs <https://sqlite.org/c3ref/vfs.html>`_ to use.  If *None* then the default
     vfs will be used.

  :param statementcachesize: Use zero to disable the statement cache,
    or a number larger than the total distinct SQL statements you
    execute frequently.

  -* sqlite3_open_v2

  .. seealso::

    * :attr:`apsw.connection_hooks`
    * :ref:`statementcache`
    * :ref:`vfs`

*/
/* forward declaration so we can tell if it is one of ours */
static int is_apsw_vfs(sqlite3_vfs *vfs);

static int
Connection_init(Connection *self, PyObject *args, PyObject *kwargs)
{
  PyObject *hooks = NULL, *hook = NULL, *iterator = NULL, *hookresult = NULL;
  const char *filename = NULL;
  int res = 0;
  int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
  const char *vfs = 0;
  int statementcachesize = 100;
  sqlite3_vfs *vfsused = 0;

  {
    Connection_init_CHECK;
    PREVENT_INIT_MULTIPLE_CALLS;
    ARG_CONVERT_VARARGS_TO_FASTCALL;
    ARG_PROLOG(4, Connection_init_KWNAMES);
    ARG_MANDATORY ARG_str(filename);
    ARG_OPTIONAL ARG_int(flags);
    ARG_OPTIONAL ARG_optional_str(vfs);
    ARG_OPTIONAL ARG_int(statementcachesize);
    ARG_EPILOG(-1, Connection_init_USAGE, Py_XDECREF(fast_kwnames));
  }
  flags |= SQLITE_OPEN_EXRESCODE;

  /* clamp cache size */
  if (statementcachesize < 0)
    statementcachesize = 0;
  if (statementcachesize > 512)
    statementcachesize = 512;

  /* Technically there is a race condition as a vfs of the same name
     could be registered between our find and the open starting.
     Don't do that!  We also have to manage the error message thread
     safety manually as self->db is null on entry. */
  vfsused = sqlite3_vfs_find(vfs);
  Py_BEGIN_ALLOW_THREADS
  {
    /* Real SQLite always creates a self->db so you can get the error
       code etc.  Fault injection leaves it NULL hence the checks for
       self->db */
    res = sqlite3_open_v2(filename, &self->db, flags, vfs);
    /* get detailed error codes */
    if (self->db)
      sqlite3_extended_result_codes(self->db, 1);
  }
  Py_END_ALLOW_THREADS;

  if (res != SQLITE_OK && !PyErr_Occurred())
  {
    if (self->db)
    {
      /* we have to hold the dbmutex around this */
      int acquired = sqlite3_mutex_try(sqlite3_db_mutex(self->db));
      /* there is no reason it could fail */
      assert(acquired == SQLITE_OK);
      (void)acquired;
    }
    make_exception(res, self->db);
    if (self->db)
      sqlite3_mutex_leave(sqlite3_db_mutex(self->db));
  }

  /* normally sqlite will have an error code but some internal vfs
     error codes aren't propagated so PyErr_Occurred will be set*/
  if (res != SQLITE_OK || PyErr_Occurred())
    goto pyexception;

  self->dbmutex = sqlite3_db_mutex(self->db);

  if (vfsused && is_apsw_vfs(vfsused))
    self->vfs = Py_NewRef((PyObject *)(vfsused->pAppData));

  /* record information */
  self->open_flags = PyLong_FromLong(flags);
  if (!self->open_flags)
    goto pyexception;
  if (vfsused)
  {
    self->open_vfs = convertutf8string(vfsused->zName);
    if (!self->open_vfs)
      goto pyexception;
  }

  /* call connection hooks */
  hooks = PyObject_GetAttr(apswmodule, apst.connection_hooks);
  if (!hooks)
    goto pyexception;

  iterator = PyObject_GetIter(hooks);
  if (!iterator)
  {
    AddTraceBackHere(__FILE__, __LINE__, "Connection.__init__", "{s: O}", "connection_hooks", OBJ(hooks));
    goto pyexception;
  }

  self->stmtcache = statementcache_init(self->db, statementcachesize);
  if (!self->stmtcache)
    goto pyexception;

  while ((hook = PyIter_Next(iterator)))
  {
    PyObject *vargs[] = { NULL, (PyObject *)self };
    hookresult = PyObject_Vectorcall(hook, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
    if (!hookresult)
      goto pyexception;
    Py_DECREF(hook);
    hook = NULL;
    Py_DECREF(hookresult);
  }

  if (!PyErr_Occurred())
  {
    res = 0;
    goto finally;
  }

pyexception:
  /* clean up db since it is useless - no need for user to call close */
  assert(PyErr_Occurred());
  res = -1;
  DBMUTEX_FORCE(self->dbmutex);
  Connection_close_internal(self, 2);
  assert(PyErr_Occurred());

finally:
  Py_XDECREF(iterator);
  Py_XDECREF(hooks);
  Py_XDECREF(hook);
  if (res == 0)
  {
    res = apsw_connection_add(self);
    if (res)
    {
      DBMUTEX_FORCE(self->dbmutex);
      Connection_close_internal(self, 2);
    }
  }
  assert((PyErr_Occurred() && res != 0) || (res == 0 && !PyErr_Occurred()));
  return res;
}

/** .. method:: blob_open(database: str, table: str, column: str, rowid: int, writeable: bool)  -> Blob

   Opens a blob for :ref:`incremental I/O <blobio>`.

   :param database: Name of the database.  `main`, `temp`, the name in `ATTACH <https://sqlite.org/lang_attach.html>`__.
   :param table: The name of the table
   :param column: The name of the column
   :param rowid: The id that uniquely identifies the row.
   :param writeable: If True then you can read and write the blob.  If False then you can only read it.

   :rtype: :class:`Blob`

   .. seealso::

     * :ref:`Blob I/O example <example_blob_io>`
     * `SQLite row ids <https://sqlite.org/autoinc.html>`_

   -* sqlite3_blob_open
*/
static PyObject *
Connection_blob_open(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  struct APSWBlob *apswblob = 0;
  sqlite3_blob *blob = 0;
  const char *database, *table, *column;
  long long rowid;
  int writeable = 0;
  int res;
  PyObject *weakref = NULL;

  CHECK_CLOSED(self, NULL);

  {
    Connection_blob_open_CHECK;
    ARG_PROLOG(5, Connection_blob_open_KWNAMES);
    ARG_MANDATORY ARG_str(database);
    ARG_MANDATORY ARG_str(table);
    ARG_MANDATORY ARG_str(column);
    ARG_MANDATORY ARG_int64(rowid);
    ARG_MANDATORY ARG_bool(writeable);
    ARG_EPILOG(NULL, Connection_blob_open_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  res = sqlite3_blob_open(self->db, database, table, column, rowid, writeable, &blob);

  SET_EXC(res, self->db);
  sqlite3_mutex_leave(self->dbmutex);

  if (PyErr_Occurred())
    return NULL;

  apswblob = (struct APSWBlob *)_PyObject_New(&APSWBlobType);
  if (!apswblob)
    goto error;

  APSWBlob_init(apswblob, self, blob);
  blob = NULL;
  weakref = PyWeakref_NewRef((PyObject *)apswblob, NULL);
  if (!weakref)
    goto error;
  if (0 == PyList_Append(self->dependents, weakref))
    return (PyObject *)apswblob;
error:
  if (blob)
    sqlite3_blob_close(blob);
  Py_XDECREF(weakref);
  Py_XDECREF(apswblob);
  return NULL;
}

/** .. method:: backup(databasename: str, sourceconnection: Connection, sourcedatabasename: str)  -> Backup

   Opens a :ref:`backup object <Backup>`.  All data will be copied from source
   database to this database.

   :param databasename: Name of the database. `main`, `temp`, the name in `ATTACH <https://sqlite.org/lang_attach.html>`__
   :param sourceconnection: The :class:`Connection` to copy a database from.
   :param sourcedatabasename: Name of the database in the source (eg ``main``).

   :rtype: :class:`Backup`

   .. seealso::

     * :doc:`Backup reference <backup>`
     * :ref:`Backup example <example_backup>`

   -* sqlite3_backup_init
*/
static PyObject *
Connection_backup(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  struct APSWBackup *apswbackup = 0;
  sqlite3_backup *backup = 0;
  int res = SQLITE_OK;
  PyObject *result = NULL;
  PyObject *weakref = NULL;
  Connection *sourceconnection = NULL;
  const char *databasename = NULL;
  const char *sourcedatabasename = NULL;

  CHECK_CLOSED(self, NULL);

  {
    Connection_backup_CHECK;
    ARG_PROLOG(3, Connection_backup_KWNAMES);
    ARG_MANDATORY ARG_str(databasename);
    ARG_MANDATORY ARG_Connection(sourceconnection);
    ARG_MANDATORY ARG_str(sourcedatabasename);
    ARG_EPILOG(NULL, Connection_backup_USAGE, );
  }
  if (!sourceconnection->db)
    return PyErr_Format(PyExc_ValueError, "source connection is closed!");

  if (sourceconnection->db == self->db)
    return PyErr_Format(PyExc_ValueError, "source and destination are the same");

  DBMUTEXES_ENSURE(sourceconnection->dbmutex, "Backup source Connection is busy in another thread", self->dbmutex,
                   "Backup destination Connection is busy in another thread");

  backup = sqlite3_backup_init(self->db, databasename, sourceconnection->db, sourcedatabasename);

  if (!backup)
  {
    res = sqlite3_extended_errcode(self->db);
    if (res == SQLITE_OK) /* this happens when doing fault injection */
      res = SQLITE_ERROR;
    SET_EXC(res, self->db);
  }

  if (res != SQLITE_OK)
    goto finally;

  apswbackup = (struct APSWBackup *)_PyObject_New(&APSWBackupType);
  if (!apswbackup)
    goto finally;

  APSWBackup_init(apswbackup, (Connection *)Py_NewRef((PyObject *)self),
                  (Connection *)Py_NewRef((PyObject *)sourceconnection), backup);
  backup = NULL;

  /* add to dependent lists */
  weakref = PyWeakref_NewRef((PyObject *)apswbackup, NULL);
  if (!weakref)
    goto finally;
  res = PyList_Append(self->dependents, weakref);
  if (res)
    goto finally;
  Py_DECREF(weakref);
  weakref = PyWeakref_NewRef((PyObject *)apswbackup, NULL);
  if (!weakref)
    goto finally;
  res = PyList_Append(sourceconnection->dependents, weakref);
  if (res)
    goto finally;
  Py_DECREF(weakref);
  weakref = 0;

  result = (PyObject *)apswbackup;
  apswbackup = NULL;

finally:
  /* check errors occurred vs result */
  assert(result ? (PyErr_Occurred() == NULL) : (PyErr_Occurred() != NULL));
  assert(result ? (backup == NULL) : 1);
  if (backup)
    sqlite3_backup_finish(backup);

  sqlite3_mutex_leave(sourceconnection->dbmutex);
  sqlite3_mutex_leave(self->dbmutex);

  Py_XDECREF((PyObject *)apswbackup);
  Py_XDECREF(weakref);

  return result;
}

/** .. method:: cursor() -> Cursor

  Creates a new :class:`Cursor` object on this database.

  :rtype: :class:`Cursor`
*/
static PyObject *
Connection_cursor(Connection *self)
{
  PyObject *cursor = NULL;
  PyObject *weakref;

  CHECK_CLOSED(self, NULL);

  PyObject *vargs[] = { NULL, (PyObject *)self };
  cursor = PyObject_Vectorcall(self->cursor_factory, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  if (!cursor)
  {
    AddTraceBackHere(__FILE__, __LINE__, "Connection.cursor", "{s: O}", "cursor_factory", OBJ(self->cursor_factory));
    return NULL;
  }

  weakref = PyWeakref_NewRef((PyObject *)cursor, NULL);
  if (!weakref)
  {
    assert(PyErr_Occurred());
    AddTraceBackHere(__FILE__, __LINE__, "Connection.cursor", "{s: O}", "cursor", OBJ(cursor));
    Py_DECREF(cursor);
    return NULL;
  }
  if (PyList_Append(self->dependents, weakref))
    cursor = NULL;
  Py_DECREF(weakref);

  return (PyObject *)cursor;
}

/** .. method:: set_busy_timeout(milliseconds: int) -> None

  If the database is locked such as when another connection is making
  changes, SQLite will keep retrying.  This sets the maximum amount of
  time SQLite will keep retrying before giving up.  If the database is
  still busy then :class:`apsw.BusyError` will be returned.

  :param milliseconds: Maximum thousandths of a second to wait.

  If you previously called :meth:`~Connection.set_busy_handler` then
  calling this overrides that.

  .. seealso::

     * :meth:`Connection.set_busy_handler`
     * :ref:`Busy handling <busyhandling>`

  -* sqlite3_busy_timeout
*/
static PyObject *
Connection_set_busy_timeout(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int milliseconds = 0;
  int res;

  CHECK_CLOSED(self, NULL);

  {
    Connection_set_busy_timeout_CHECK;
    ARG_PROLOG(1, Connection_set_busy_timeout_KWNAMES);
    ARG_MANDATORY ARG_int(milliseconds);
    ARG_EPILOG(NULL, Connection_set_busy_timeout_USAGE, );
  }
  DBMUTEX_ENSURE(self->dbmutex);
  res = sqlite3_busy_timeout(self->db, milliseconds);
  SET_EXC(res, self->db);
  sqlite3_mutex_leave(self->dbmutex);

  if (PyErr_Occurred())
    return NULL;

  /* free any explicit busyhandler we may have had */
  Py_XDECREF(self->busyhandler);
  self->busyhandler = 0;

  Py_RETURN_NONE;
}

/** .. method:: changes() -> int

  Returns the number of database rows that were changed (or inserted
  or deleted) by the most recently completed INSERT, UPDATE, or DELETE
  statement.

  -* sqlite3_changes64
*/
static PyObject *
Connection_changes(Connection *self)
{

  CHECK_CLOSED(self, NULL);

  return PyLong_FromLongLong(sqlite3_changes64(self->db));
}

/** .. method:: total_changes() -> int

  Returns the total number of database rows that have be modified,
  inserted, or deleted since the database connection was opened.

  -* sqlite3_total_changes64
*/
static PyObject *
Connection_total_changes(Connection *self)
{

  CHECK_CLOSED(self, NULL);

  return PyLong_FromLongLong(sqlite3_total_changes64(self->db));
}

/** .. method:: get_autocommit() -> bool

  Returns if the Connection is in auto commit mode (ie not in a transaction).

  -* sqlite3_get_autocommit
*/
static PyObject *
Connection_get_autocommit(Connection *self)
{

  CHECK_CLOSED(self, NULL);
  if (sqlite3_get_autocommit(self->db))
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

/** .. method:: db_names() -> list[str]

 Returns the list of database names.  For example the first database
 is named 'main', the next 'temp', and the rest with the name provided
 in `ATTACH <https://www.sqlite.org/lang_attach.html>`__

 -* sqlite3_db_name
*/
static PyObject *
Connection_db_names(Connection *self)
{
  PyObject *res = NULL, *str = NULL;
  int i;

  CHECK_CLOSED(self, NULL);

  DBMUTEX_ENSURE(self->dbmutex);
  res = PyList_New(0);
  if (!res)
    goto error;

  for (i = 0;; i++)
  {
    int appendres;

    const char *s = sqlite3_db_name(self->db, i);
    if (!s)
      break;
    str = convertutf8string(s);
    if (!str)
      goto error;
    appendres = PyList_Append(res, str);
    if (0 != appendres)
      goto error;
    Py_CLEAR(str);
  }

  sqlite3_mutex_leave(self->dbmutex);
  return res;
error:
  sqlite3_mutex_leave(self->dbmutex);
  assert(PyErr_Occurred());
  Py_XDECREF(res);
  Py_XDECREF(str);

  return NULL;
}

/** .. method:: last_insert_rowid() -> int

  Returns the integer key of the most recent insert in the database.

  -* sqlite3_last_insert_rowid
*/
static PyObject *
Connection_last_insert_rowid(Connection *self)
{

  CHECK_CLOSED(self, NULL);

  return PyLong_FromLongLong(sqlite3_last_insert_rowid(self->db));
}

/** .. method:: set_last_insert_rowid(rowid: int) -> None

  Sets the value calls to :meth:`last_insert_rowid` will return.

  -* sqlite3_set_last_insert_rowid
*/
static PyObject *
Connection_set_last_insert_rowid(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                                 PyObject *fast_kwnames)
{
  sqlite3_int64 rowid;

  CHECK_CLOSED(self, NULL);

  {
    Connection_set_last_insert_rowid_CHECK;
    ARG_PROLOG(1, Connection_set_last_insert_rowid_KWNAMES);
    ARG_MANDATORY ARG_int64(rowid);
    ARG_EPILOG(NULL, Connection_set_last_insert_rowid_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  sqlite3_set_last_insert_rowid(self->db, rowid);
  sqlite3_mutex_leave(self->dbmutex);

  Py_RETURN_NONE;
}

/** .. method:: interrupt() -> None

  Causes all pending operations on the database to abort at the
  earliest opportunity. You can call this from any thread.  For
  example you may have a long running query when the user presses the
  stop button in your user interface.  :exc:`InterruptError`
  will be raised in the queries that got interrupted.

  -* sqlite3_interrupt
*/
static PyObject *
Connection_interrupt(Connection *self)
{
  CHECK_CLOSED(self, NULL);

  sqlite3_interrupt(self->db); /* no return value */
  Py_RETURN_NONE;
}

/** .. method:: limit(id: int, newval: int = -1) -> int

  If called with one parameter then the current limit for that *id* is
  returned.  If called with two then the limit is set to *newval*.


  :param id: One of the `runtime limit ids <https://sqlite.org/c3ref/c_limit_attached.html>`_
  :param newval: The new limit.  This is a 32 bit signed integer even on 64 bit platforms.

  :returns: The limit in place on entry to the call.

  -* sqlite3_limit

  .. seealso::

    * :ref:`Example <example_limits>`

*/
static PyObject *
Connection_limit(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int newval = -1, res, id;

  CHECK_CLOSED(self, NULL);
  {
    Connection_limit_CHECK;
    ARG_PROLOG(2, Connection_limit_KWNAMES);
    ARG_MANDATORY ARG_int(id);
    ARG_OPTIONAL ARG_int(newval);
    ARG_EPILOG(NULL, Connection_limit_USAGE, );
  }
  res = sqlite3_limit(self->db, id, newval);

  return PyLong_FromLong(res);
}

static void
updatecb(void *context, int updatetype, char const *databasename, char const *tablename, sqlite3_int64 rowid)
{
  /* The hook returns void. That makes it impossible for us to
     abort immediately due to an error in the callback */

  PyGILState_STATE gilstate;
  PyObject *retval = NULL;
  Connection *self = (Connection *)context;

  assert(self);
  assert(self->updatehook);
  assert(!Py_IsNone(self->updatehook));

  gilstate = PyGILState_Ensure();

  MakeExistingException();

  if (PyErr_Occurred())
    goto finally; /* abort hook due to outstanding exception */

  PyObject *vargs[] = { NULL, PyLong_FromLong(updatetype), PyUnicode_FromString(databasename),
                        PyUnicode_FromString(tablename), PyLong_FromLongLong(rowid) };
  if (vargs[1] && vargs[2] && vargs[3] && vargs[4])
    retval = PyObject_Vectorcall(self->updatehook, vargs + 1, 4 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[1]);
  Py_XDECREF(vargs[2]);
  Py_XDECREF(vargs[3]);
  Py_XDECREF(vargs[4]);
finally:
  Py_XDECREF(retval);
  PyGILState_Release(gilstate);
}

/** .. method:: set_update_hook(callable: Optional[Callable[[int, str, str, int], None]]) -> None

  Calls *callable* whenever a row is updated, deleted or inserted.  If
  *callable* is *None* then any existing update hook is
  unregistered.  The update hook cannot make changes to the database while
  the query is still executing, but can record them for later use or
  apply them in a different connection.

  The update hook is called with 4 parameters:

    type (int)
      *SQLITE_INSERT*, *SQLITE_DELETE* or *SQLITE_UPDATE*
    database name (str)
      `main`, `temp`, the name in `ATTACH <https://sqlite.org/lang_attach.html>`__
    table name (str)
      The table on which the update happened
    rowid (int)
      The affected row

  .. seealso::

      * :ref:`Example <example_update_hook>`

  -* sqlite3_update_hook
*/
static PyObject *
Connection_set_update_hook(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  /* sqlite3_update_hook doesn't return an error code */
  PyObject *callable;

  CHECK_CLOSED(self, NULL);

  {
    Connection_set_update_hook_CHECK;
    ARG_PROLOG(1, Connection_set_update_hook_KWNAMES);
    ARG_MANDATORY ARG_optional_Callable(callable);
    ARG_EPILOG(NULL, Connection_set_update_hook_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  if (!callable)
    sqlite3_update_hook(self->db, NULL, NULL);
  else
    sqlite3_update_hook(self->db, updatecb, self);
  sqlite3_mutex_leave(self->dbmutex);

  Py_CLEAR(self->updatehook);
  if (callable)
    self->updatehook = Py_NewRef(callable);

  Py_RETURN_NONE;
}

static void
rollbackhookcb(void *context)
{
  /* The hook returns void. That makes it impossible for us to
     abort immediately due to an error in the callback */

  PyGILState_STATE gilstate;
  PyObject *retval = NULL;
  Connection *self = (Connection *)context;

  assert(self);
  assert(self->rollbackhook);
  assert(!Py_IsNone(self->rollbackhook));

  gilstate = PyGILState_Ensure();

  MakeExistingException();

  if (PyErr_Occurred())
    apsw_write_unraisable(NULL);
  else
  {
    PyObject *vargs[] = { NULL };
    retval = PyObject_Vectorcall(self->rollbackhook, vargs + 1, 0 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  }

  Py_XDECREF(retval);
  PyGILState_Release(gilstate);
}

/** .. method:: set_rollback_hook(callable: Optional[Callable[[], None]]) -> None

  Sets a callable which is invoked during a rollback.  If *callable*
  is *None* then any existing rollback hook is unregistered.

  The *callable* is called with no parameters and the return value is ignored.

  -* sqlite3_rollback_hook
*/
static PyObject *
Connection_set_rollback_hook(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                             PyObject *fast_kwnames)
{
  /* sqlite3_rollback_hook doesn't return an error code */
  PyObject *callable;

  CHECK_CLOSED(self, NULL);

  {
    Connection_set_rollback_hook_CHECK;
    ARG_PROLOG(1, Connection_set_rollback_hook_KWNAMES);
    ARG_MANDATORY ARG_optional_Callable(callable);
    ARG_EPILOG(NULL, Connection_set_rollback_hook_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);

  if (!callable)
    sqlite3_rollback_hook(self->db, NULL, NULL);
  else
    sqlite3_rollback_hook(self->db, rollbackhookcb, self);

  sqlite3_mutex_leave(self->dbmutex);

  Py_CLEAR(self->rollbackhook);
  if (callable)
    self->rollbackhook = Py_NewRef(callable);

  Py_RETURN_NONE;
}

static int
tracehook_cb(unsigned code, void *vconnection, void *one, void *two)
{
  PyGILState_STATE gilstate;
  Connection *connection = (Connection *)vconnection;
  PyObject *param = NULL, *res = NULL;
  sqlite3_stmt *stmt = NULL;
  sqlite3_int64 *nanoseconds = NULL;

  gilstate = PyGILState_Ensure();

  MakeExistingException();

  CHAIN_EXC_BEGIN

  switch (code)
  {

  case SQLITE_TRACE_STMT:
    stmt = (sqlite3_stmt *)one;
    const char *sql = (const char *)two;
    int trigger = sql[0] == '-' && sql[1] == '-' && sql[2] == ' ';

    if (!trigger)
    {
#define V(x) sqlite3_stmt_status(stmt, x, 1)
      /* reset all the counters */
      V(SQLITE_STMTSTATUS_FULLSCAN_STEP);
      V(SQLITE_STMTSTATUS_SORT);
      V(SQLITE_STMTSTATUS_AUTOINDEX);
      V(SQLITE_STMTSTATUS_VM_STEP);
      V(SQLITE_STMTSTATUS_REPREPARE);
      V(SQLITE_STMTSTATUS_RUN);
      V(SQLITE_STMTSTATUS_FILTER_MISS);
      V(SQLITE_STMTSTATUS_FILTER_HIT);
#undef V
    }
    for (unsigned i = 1; i < connection->tracehooks_count; i++)
    {
      /* only calculate this if needed */
      if (connection->tracehooks[i].mask & SQLITE_TRACE_STMT)
      {

        param = Py_BuildValue("{s: i, s: N, s: s, s: O, s: O, s: L}", "code", code, "id", PyLong_FromVoidPtr(one),
                              "sql", trigger ? sql + 3 : sql, "trigger", trigger ? Py_True : Py_False, "connection",
                              connection, "total_changes", sqlite3_total_changes64(connection->db));
        break;
      }
    }
    break;

  case SQLITE_TRACE_ROW:
    param = Py_BuildValue("{s: i, s: N, s: O}", "code", code, "id", PyLong_FromVoidPtr(one), "connection", connection);
    break;

  case SQLITE_TRACE_CLOSE:
    /* Checking the refcount is subtle but important.  If the
       Connection is being closed because there are no more references to it
       then the ref count is zero when the callback fires and adding a
       reference ressurects a mostly destroyed object which then hits zero
       again and gets destroyed a second time.  Too difficult to handle. */
    param = Py_BuildValue("{s: i, s: O}", "code", code, "connection",
                          Py_REFCNT(connection) ? (PyObject *)connection : Py_None);
    break;

  case SQLITE_TRACE_PROFILE:
#define K "s: i,"
#define V(x) #x, sqlite3_stmt_status(stmt, x, 0)

    stmt = (sqlite3_stmt *)one;
    nanoseconds = (sqlite3_int64 *)two;

    for (unsigned i = 1; i < connection->tracehooks_count; i++)
    {
      /* only calculate this if needed */
      if (connection->tracehooks[i].mask & SQLITE_TRACE_PROFILE)
      {
        param = Py_BuildValue(
            "{s: i, s: O, s: N, s: s, s: L, s: L, s: {" K K K K K K K K "s: i}}", "code", code, "connection",
            connection, "id", PyLong_FromVoidPtr(one), "sql", sqlite3_sql(stmt), "nanoseconds", *nanoseconds,
            "total_changes", sqlite3_total_changes64(connection->db), "stmt_status", V(SQLITE_STMTSTATUS_FULLSCAN_STEP),
            V(SQLITE_STMTSTATUS_SORT), V(SQLITE_STMTSTATUS_AUTOINDEX), V(SQLITE_STMTSTATUS_VM_STEP),
            V(SQLITE_STMTSTATUS_REPREPARE), V(SQLITE_STMTSTATUS_RUN), V(SQLITE_STMTSTATUS_FILTER_MISS),
            V(SQLITE_STMTSTATUS_FILTER_HIT), V(SQLITE_STMTSTATUS_MEMUSED));
        break;
      }
    }
    break;
#undef K
#undef V
  }

  if (PyErr_Occurred())
    goto finally;

  /* handle sqlite3_profile compatibility */
  if (code == SQLITE_TRACE_PROFILE && connection->tracehooks[0].callback)
  {
    CHAIN_EXC_BEGIN
    PyObject *vargs[] = { NULL, PyUnicode_FromString(sqlite3_sql(stmt)), PyLong_FromLongLong(*nanoseconds) };
    if (vargs[1] && vargs[2])
      res = PyObject_Vectorcall(connection->tracehooks[0].callback, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET,
                                NULL);
    Py_XDECREF(vargs[1]);
    Py_XDECREF(vargs[2]);
    Py_CLEAR(res);
    CHAIN_EXC_END;
  }

  if (!PyErr_Occurred())
  {
    PyObject *vargs[] = { NULL, param };
    for (unsigned i = 1; i < connection->tracehooks_count; i++)
    {
      if (connection->tracehooks[i].mask & code)
      {
        CHAIN_EXC_BEGIN
        res = PyObject_Vectorcall(connection->tracehooks[i].callback, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET,
                                  NULL);
        Py_CLEAR(res);
        CHAIN_EXC_END;
      }
    }
  }

finally:
  Py_CLEAR(res);
  Py_XDECREF(param);

  CHAIN_EXC_END;

  PyGILState_Release(gilstate);
  return 0;
}

/* does sqlite3_trace_v2 call based on current tracehooks, called
   after each change */
static PyObject *
Connection_update_trace_v2(Connection *self)
{
  /* callers already do this, but what the heck */
  CHECK_CLOSED(self, NULL);

  unsigned mask = 0;
  for (unsigned i = 0; i < self->tracehooks_count; i++)
    mask |= self->tracehooks[i].mask;

  /* this ensures counters are reset on a per statement basis */
  if (mask & SQLITE_TRACE_PROFILE)
    mask |= SQLITE_TRACE_STMT;

  int res;

  DBMUTEX_ENSURE(self->dbmutex);
  res = sqlite3_trace_v2(self->db, mask, mask ? tracehook_cb : NULL, self);
  SET_EXC(res, self->db);
  sqlite3_mutex_leave(self->dbmutex);

  if (PyErr_Occurred())
    return NULL;

  Py_RETURN_NONE;
}

/** .. method:: set_profile(callable: Optional[Callable[[str, int], None]]) -> None

  Sets a callable which is invoked at the end of execution of each
  statement and passed the statement string and how long it took to
  execute. (The execution time is in nanoseconds.) Note that it is
  called only on completion. If for example you do a ``SELECT`` and only
  read the first result, then you won't reach the end of the statement.

  -* sqlite3_trace_v2
*/

static PyObject *
Connection_set_profile(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *callable;

  CHECK_CLOSED(self, NULL);

  {
    Connection_set_profile_CHECK;
    ARG_PROLOG(1, Connection_set_profile_KWNAMES);
    ARG_MANDATORY ARG_optional_Callable(callable);
    ARG_EPILOG(NULL, Connection_set_profile_USAGE, );
  }

  Py_CLEAR(self->tracehooks[0].callback);

  if (callable)
  {
    self->tracehooks[0].mask = SQLITE_TRACE_PROFILE;
    self->tracehooks[0].callback = Py_NewRef(callable);
  }
  else
    self->tracehooks[0].mask = 0;

  return Connection_update_trace_v2(self);
}

/** .. method:: trace_v2(mask: int, callback: Optional[Callable[[dict], None]] = None, *, id: Optional[Any] = None) -> None

  Registers a trace callback.  Multiple traces can be active at once
  (implemented by APSW).  A callback of :class:`None` unregisters a
  trace.  Registered callbacks are distinguished by their ``id`` - an
  equality test is done to match ids.

  The callback is called with a dict of relevant values based on the
  code.

  .. list-table::
    :header-rows: 1
    :widths: auto

    * - Key
      - Type
      - Explanation
    * - code
      - :class:`int`
      - One of the `trace event codes <https://www.sqlite.org/c3ref/c_trace.html>`__
    * - connection
      - :class:`Connection`
      - Connection this trace event belongs to
    * - sql
      - :class:`str`
      - SQL text (except SQLITE_TRACE_ROW and SQLITE_TRACE_CLOSE).
    * - id
      - :class:`int`
      - An opaque key to correlate events on the same statement.  The
        id can be reused after SQLITE_TRACE_PROFILE.
    * - trigger
      - :class:`bool`
      - If `trigger <https://www.sqlite.org/lang_createtrigger.html>`__
        SQL is executing then this is ``True`` and the SQL is of the trigger.
        Virtual table nested queries also come through as trigger activity.
    * - total_changes
      - :class:`int`
      - Value of :meth:`total_changes`  (SQLITE_TRACE_STMT and SQLITE_TRACE_PROFILE only)
    * - nanoseconds
      - :class:`int`
      - nanoseconds SQL took to execute (SQLITE_TRACE_PROFILE only)
    * - stmt_status
      - :class:`dict`
      - SQLITE_TRACE_PROFILE only: Keys are names from `status parameters
        <https://www.sqlite.org/c3ref/c_stmtstatus_counter.html>`__ - eg
        *"SQLITE_STMTSTATUS_VM_STEP"* and corresponding integer values.
        The counters are reset each time a statement
        starts execution.  This includes any changes made by triggers.

  Note that SQLite ignores any errors from the trace callbacks, so
  whatever was being traced will still proceed.  Exceptions will be
  delivered when your Python code resumes.

  If you register for all trace types, the following sequence will happen.

  * SQLITE_TRACE_STMT with `trigger` `False` and an `id` and `sql` of
    the statement.
  * Multiple times: SQLITE_TRACE_STMT with the same `id` and `trigger`
    `True` if a trigger is executed.  The first time the `sql` will be
    ``TRIGGER name`` and then subsequent calls will be lines of the
    trigger.  This also happens for virtual tables that make queries.
  * Multiple times: SQLITE_TRACE_ROW with the same `id` for each time
    execution stopped at a row. (Rows visited by triggers do not cause
    thie event)
  * SQLITE_TRACE_PROFILE with the same `id` for any virtual table
    queries - the ``sql`` will be of those queries
  * SQLITE_TRACE_PROFILE with the same `id` for the initial SQL.

  .. seealso::

    * :ref:`Example <example_trace_v2>`
    * :class:`apsw.ext.Trace`

  -* sqlite3_trace_v2 sqlite3_stmt_status
*/
static PyObject *
Connection_trace_v2(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int mask = 0;
  PyObject *callback = NULL;
  PyObject *id = NULL;

  CHECK_CLOSED(self, NULL);

  {
    Connection_trace_v2_CHECK;
    ARG_PROLOG(2, Connection_trace_v2_KWNAMES);
    ARG_MANDATORY ARG_int(mask);
    ARG_OPTIONAL ARG_optional_Callable(callback);
    ARG_OPTIONAL ARG_pyobject(id);
    ARG_EPILOG(NULL, Connection_trace_v2_USAGE, );
  }

  if (mask && !callback)
    return PyErr_Format(PyExc_ValueError, "Non-zero mask but no callback provided");
  if (mask == 0 && callback)
    return PyErr_Format(PyExc_ValueError, "mask selects no events, but callback provided");

  /* Known values only */
  if (mask & ~(SQLITE_TRACE_STMT | SQLITE_TRACE_PROFILE | SQLITE_TRACE_ROW | SQLITE_TRACE_CLOSE))
    return PyErr_Format(PyExc_ValueError, "mask includes unknown trace values");

  /* always clear out any matching id */
  for (unsigned i = 1; i < self->tracehooks_count; i++)
  {
    if (self->tracehooks[i].callback)
    {
      int eq;
      /* handle either side being NULL */
      if ((!id || !self->tracehooks[i].id) && id != self->tracehooks[i].id)
        eq = 0;
      else
        eq = PyObject_RichCompareBool(id, self->tracehooks[i].id, Py_EQ);

      if (eq == -1)
        return NULL;
      if (eq)
      {
        Py_CLEAR(self->tracehooks[i].callback);
        Py_CLEAR(self->tracehooks[i].id);
        self->tracehooks[i].mask = 0;
      }
    }
  }

  if (callback)
  {
    /* find an empty slot */
    int found = 0;
    for (unsigned i = 1; i < self->tracehooks_count; i++)
    {
      if (self->tracehooks[i].callback == 0)
      {
        self->tracehooks[i].mask = mask;
        self->tracehooks[i].id = id ? Py_NewRef(id) : NULL;
        self->tracehooks[i].callback = Py_NewRef(callback);
        found = 1;
        break;
      }
    }
    if (!found)
    {
      /* increase tracehooks size - we have an arbitrary limit which
         makes it easier to test exhaustion */
      struct tracehook *new_tracehooks
          = (self->tracehooks_count < 1024)
                ? PyMem_Realloc(self->tracehooks, sizeof(struct tracehook) * (self->tracehooks_count + 1))
                : NULL;
      if (!new_tracehooks)
      {
        /* not bothering to call update_trace - worst case there will
           be extra trace calls that are ignored. */
        return PyErr_NoMemory();
      }
      self->tracehooks = new_tracehooks;
      self->tracehooks[self->tracehooks_count].mask = mask;
      self->tracehooks[self->tracehooks_count].id = id ? Py_NewRef(id) : NULL;
      self->tracehooks[self->tracehooks_count].callback = Py_NewRef(callback);
      self->tracehooks_count++;
    }
  }

  return Connection_update_trace_v2(self);
}

static int
commithookcb(void *context)
{
  /* The hook returns 0 for commit to go ahead and non-zero to abort
     commit (turn into a rollback). We return non-zero for errors */

  PyGILState_STATE gilstate;
  PyObject *retval = NULL;
  int ok = 1; /* error state */
  Connection *self = (Connection *)context;

  assert(self);
  assert(self->commithook);
  assert(!Py_IsNone(self->commithook));

  gilstate = PyGILState_Ensure();

  MakeExistingException();

  if (PyErr_Occurred())
    goto finally; /* abort hook due to outstanding exception */

  PyObject *vargs[] = { NULL };
  retval = PyObject_Vectorcall(self->commithook, vargs + 1, 0 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);

  if (!retval)
    goto finally; /* abort hook due to exception */

  ok = PyObject_IsTrueStrict(retval);
  assert(ok == -1 || ok == 0 || ok == 1);
  if (ok == -1)
  {
    ok = 1;
    assert(PyErr_Occurred());
    goto finally; /* abort due to exception in return value */
  }

finally:
  Py_XDECREF(retval);
  PyGILState_Release(gilstate);
  return ok;
}

/** .. method:: set_commit_hook(callable: Optional[CommitHook]) -> None

  *callable* will be called just before a commit.  It should return
  False for the commit to go ahead and True for it to be turned
  into a rollback. In the case of an exception in your callable, a
  True (rollback) value is returned.  Pass None to unregister
  the existing hook.

  .. seealso::

    * :ref:`Example <example_commit_hook>`

  -* sqlite3_commit_hook

*/
static PyObject *
Connection_set_commit_hook(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  /* sqlite3_commit_hook doesn't return an error code */
  PyObject *callable;

  CHECK_CLOSED(self, NULL);

  {
    Connection_set_commit_hook_CHECK;
    ARG_PROLOG(1, Connection_set_commit_hook_KWNAMES);
    ARG_MANDATORY ARG_optional_Callable(callable);
    ARG_EPILOG(NULL, Connection_set_commit_hook_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  if (callable)
    sqlite3_commit_hook(self->db, commithookcb, self);
  else
    sqlite3_commit_hook(self->db, NULL, NULL);
  sqlite3_mutex_leave(self->dbmutex);

  Py_CLEAR(self->commithook);
  if (callable)
    self->commithook = Py_NewRef(callable);

  Py_RETURN_NONE;
}

static int
walhookcb(void *context, sqlite3 *db, const char *dbname, int npages)
{
  PyGILState_STATE gilstate;
  PyObject *retval = NULL;
  int code = SQLITE_ERROR;
  Connection *self = (Connection *)context;

  assert(self);
  assert(self->walhook);
  assert(!Py_IsNone(self->walhook));
  assert(self->db == db);

  gilstate = PyGILState_Ensure();

  MakeExistingException();

  PyObject *vargs[] = { NULL, (PyObject *)self, PyUnicode_FromString(dbname), PyLong_FromLong(npages) };
  if (vargs[2] && vargs[3])
    retval = PyObject_Vectorcall(self->walhook, vargs + 1, 3 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[2]);
  Py_XDECREF(vargs[3]);
  if (!retval)
  {
    assert(PyErr_Occurred());
    AddTraceBackHere(__FILE__, __LINE__, "walhookcallback", "{s: O, s: s, s: i}", "Connection", self, "dbname", dbname,
                     "npages", npages);
    goto finally;
  }
  if (!PyLong_Check(retval))
  {
    PyErr_Format(PyExc_TypeError, "wal hook must return a number not %s", Py_TypeName(retval));
    AddTraceBackHere(__FILE__, __LINE__, "walhookcallback", "{s: O, s: s, s: i, s: O}", "Connection", self, "dbname",
                     dbname, "npages", npages, "retval", OBJ(retval));
    goto finally;
  }
  code = PyLong_AsInt(retval);

finally:
  Py_XDECREF(retval);
  PyGILState_Release(gilstate);
  return code;
}

/** .. method:: set_wal_hook(callable: Optional[Callable[[Connection, str, int], int]]) -> None

 *callable* will be called just after data is committed in :ref:`wal`
 mode.  It should return *SQLITE_OK* or an error code.  The
 callback is called with 3 parameters:

   * The Connection
   * The database name.  `main`, `temp`, the name in `ATTACH <https://sqlite.org/lang_attach.html>`__
   * The number of pages in the wal log

 You can pass in None in order to unregister an existing hook.

 -* sqlite3_wal_hook

*/

static PyObject *
Connection_set_wal_hook(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *callable;

  CHECK_CLOSED(self, NULL);

  {
    Connection_set_wal_hook_CHECK;
    ARG_PROLOG(1, Connection_set_wal_hook_KWNAMES);
    ARG_MANDATORY ARG_optional_Callable(callable);
    ARG_EPILOG(NULL, Connection_set_wal_hook_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  if (!callable)
    sqlite3_wal_hook(self->db, NULL, NULL);
  else
    sqlite3_wal_hook(self->db, walhookcb, self);
  sqlite3_mutex_leave(self->dbmutex);

  Py_CLEAR(self->walhook);
  if (callable)
    self->walhook = Py_NewRef(callable);

  Py_RETURN_NONE;
}

static int
progresshandlercb(void *context)
{
  /* The hook returns 0 for continue and non-zero to abort (rollback).
     We return non-zero for errors */

  PyGILState_STATE gilstate;
  PyObject *retval = NULL;
  int ok = 1; /* error state */
  Connection *self = (Connection *)context;

  assert(self);
  assert(self->progresshandler);

  gilstate = PyGILState_Ensure();

  MakeExistingException();

  if (PyErr_Occurred())
    goto finally;

  for (unsigned i = 0; i < self->progresshandler_count; i++)
  {
    if (!self->progresshandler[i].callback)
      continue;

    PyObject *vargs[] = { NULL };
    retval
        = PyObject_Vectorcall(self->progresshandler[i].callback, vargs + 1, 0 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);

    if (!retval)
      goto finally; /* abort due to exception */

    ok = PyObject_IsTrueStrict(retval);

    assert(ok == -1 || ok == 0 || ok == 1);

    if (ok == 1)
      goto finally;

    if (ok == -1)
    {
      ok = 1;
      assert(PyErr_Occurred());
      goto finally; /* abort due to exception in result */
    }
    Py_CLEAR(retval);
  }

finally:
  Py_XDECREF(retval);

  PyGILState_Release(gilstate);
  return ok;
}

/** .. method:: set_progress_handler(callable: Optional[Callable[[], bool]], nsteps: int = 100, *, id: Optional[Any] = None) -> None

  Sets a callable which is invoked every *nsteps* SQLite inststructions.
  The callable should return True to abort or False to continue. (If
  there is an error in your Python *callable* then True/abort will be
  returned).  SQLite raises :exc:`InterruptError` for aborts.

  Use :class:`None` to cancel the progress handler.  Multiple handlers
  can be present at once (implemented by APSW). Registered callbacks are
  distinguished by their ``id`` - an equality test is done to match ids.

  You can use :class:`apsw.ext.Trace` to see how many steps are used for
  a representative statement, or :class:`apsw.ext.ShowResourceUsage` to
  see how many are used in a block.  It will generally be several million
  per second.

  .. seealso::

     * :ref:`Example <example_progress_handler>`

  -* sqlite3_progress_handler
*/

static PyObject *
Connection_set_progress_handler(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                                PyObject *fast_kwnames)
{
  int nsteps = 100;
  PyObject *callable = NULL;
  PyObject *id = NULL;

  CHECK_CLOSED(self, NULL);
  {
    Connection_set_progress_handler_CHECK;
    ARG_PROLOG(2, Connection_set_progress_handler_KWNAMES);
    ARG_MANDATORY ARG_optional_Callable(callable);
    ARG_OPTIONAL ARG_int(nsteps);
    ARG_OPTIONAL ARG_pyobject(id);
    ARG_EPILOG(NULL, Connection_set_progress_handler_USAGE, );
  }

  if (callable && nsteps <= 0)
    return PyErr_Format(PyExc_ValueError, "nsteps must be a positive number");

  /* clear out any matching id */
  for (unsigned i = 0; i < self->progresshandler_count; i++)
  {
    if (self->progresshandler[i].callback)
    {
      int eq;
      /* handle either side being NULL */
      if ((!id || !self->progresshandler[i].id) && id != self->progresshandler[i].id)
        eq = 0;
      else
        eq = PyObject_RichCompareBool(id, self->progresshandler[i].id, Py_EQ);

      if (eq == -1)
        return NULL;
      if (eq)
      {
        Py_CLEAR(self->progresshandler[i].callback);
        Py_CLEAR(self->progresshandler[i].id);
      }
    }
  }

  if (callable)
  {
    /* find an empty slot */
    int found = 0;
    for (unsigned i = 0; i < self->progresshandler_count; i++)
    {
      if (!self->progresshandler[i].callback)
      {
        self->progresshandler[i].nsteps = nsteps;
        self->progresshandler[i].id = id ? Py_NewRef(id) : NULL;
        self->progresshandler[i].callback = Py_NewRef(callable);
        found = 1;
        break;
      }
    }
    if (!found)
    {
      /* increase progresshandler size - we have an arbitrary limit which
         makes it easier to test exhaustion */
      struct progresshandler *new_progresshandler
          = (self->progresshandler_count < 1024)
                ? PyMem_Realloc(self->progresshandler,
                                sizeof(struct progresshandler) * (self->progresshandler_count + 1))
                : NULL;
      if (!new_progresshandler)
        return PyErr_NoMemory();
      self->progresshandler = new_progresshandler;
      self->progresshandler[self->progresshandler_count].nsteps = nsteps;
      self->progresshandler[self->progresshandler_count].id = id ? Py_NewRef(id) : NULL;
      self->progresshandler[self->progresshandler_count].callback = Py_NewRef(callable);
      self->progresshandler_count++;
    }
  }

  int min_steps = INT_MAX;
  int active = 0;
  for (unsigned i = 0; i < self->progresshandler_count; i++)
  {
    if (self->progresshandler[i].callback)
    {
      min_steps = Py_MIN(min_steps, self->progresshandler[i].nsteps);
      active += 1;
    }
  }

  DBMUTEX_ENSURE(self->dbmutex);
  if (active)
    sqlite3_progress_handler(self->db, min_steps, progresshandlercb, self);
  else
    sqlite3_progress_handler(self->db, 0, NULL, NULL);
  sqlite3_mutex_leave(self->dbmutex);

  assert(!PyErr_Occurred());

  Py_RETURN_NONE;
}

static int
authorizercb(void *context, int operation, const char *paramone, const char *paramtwo, const char *databasename,
             const char *triggerview)
{
  /* should return one of SQLITE_OK, SQLITE_DENY, or
     SQLITE_IGNORE. (0, 1 or 2 respectively) */

  PyGILState_STATE gilstate;
  PyObject *retval = NULL;
  int result = SQLITE_DENY; /* default to deny */
  Connection *self = (Connection *)context;

  assert(self);
  assert(self->authorizer);
  assert(!Py_IsNone(self->authorizer));

  gilstate = PyGILState_Ensure();

  MakeExistingException();

  if (PyErr_Occurred())
    goto finally; /* abort due to earlier exception */

  PyObject *vargs[] = { NULL,
                        PyLong_FromLong(operation),
                        convertutf8string(paramone),
                        convertutf8string(paramtwo),
                        convertutf8string(databasename),
                        convertutf8string(triggerview) };

  if (vargs[1] && vargs[2] && vargs[3] && vargs[4] && vargs[5])
    retval = PyObject_Vectorcall(self->authorizer, vargs + 1, 5 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[1]);
  Py_XDECREF(vargs[2]);
  Py_XDECREF(vargs[3]);
  Py_XDECREF(vargs[4]);
  Py_XDECREF(vargs[5]);
  if (!retval)
    goto finally; /* abort due to exception */

  if (PyLong_Check(retval))
  {
    result = PyLong_AsInt(retval);
    goto haveval;
  }

  PyErr_Format(PyExc_TypeError, "Authorizer must return a number not %s", Py_TypeName(retval));
  AddTraceBackHere(__FILE__, __LINE__, "authorizer callback", "{s: i, s: s:, s: s, s: s}", "operation", operation,
                   "paramone", paramone, "paramtwo", paramtwo, "databasename", databasename, "triggerview",
                   triggerview);

haveval:
  if (PyErr_Occurred())
    result = SQLITE_DENY;

finally:
  Py_XDECREF(retval);

  PyGILState_Release(gilstate);
  return result;
}

/* returns NULL on failure and Py_None on success */
static void *
Connection_internal_set_authorizer(Connection *self, PyObject *callable)
{
  CHECK_CLOSED(self, NULL);

  int res = SQLITE_OK;

  assert(!Py_IsNone(callable));

  DBMUTEX_ENSURE(self->dbmutex);
  res = sqlite3_set_authorizer(self->db, callable ? authorizercb : NULL, callable ? self : NULL);
  SET_EXC(res, self->db);
  sqlite3_mutex_leave(self->dbmutex);
  if (PyErr_Occurred())
    return NULL;

  Py_CLEAR(self->authorizer);
  if (callable)
    self->authorizer = Py_NewRef(callable);

  return Py_None;
}

/** .. method:: set_authorizer(callable: Optional[Authorizer]) -> None

  Sets the :attr:`authorizer`
*/

static PyObject *
Connection_set_authorizer(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *callable;

  CHECK_CLOSED(self, NULL);

  {
    Connection_set_authorizer_CHECK;
    ARG_PROLOG(1, Connection_set_authorizer_KWNAMES);
    ARG_MANDATORY ARG_optional_Callable(callable);
    ARG_EPILOG(NULL, Connection_set_authorizer_USAGE, );
  }

  void *res = Connection_internal_set_authorizer(self, callable);
  if (!res)
  {
    assert(PyErr_Occurred());
    return NULL;
  }
  Py_RETURN_NONE;
}

static void
autovacuum_pages_cleanup(void *callable)
{
  PyGILState_STATE gilstate;

  gilstate = PyGILState_Ensure();
  Py_DECREF((PyObject *)callable);
  PyGILState_Release(gilstate);
}

#define AVPCB_TB "{s: O, s: s:, s: I, s: I, s: I, s: O}"

static unsigned int
autovacuum_pages_cb(void *callable, const char *schema, unsigned int nPages, unsigned int nFreePages,
                    unsigned int nBytesPerPage)
{
  PyGILState_STATE gilstate;
  PyObject *retval = NULL;
  int res = 0;
  gilstate = PyGILState_Ensure();

  MakeExistingException();

  CHAIN_EXC_BEGIN
  PyObject *vargs[] = { NULL, PyUnicode_FromString(schema), PyLong_FromUnsignedLong(nPages),
                        PyLong_FromUnsignedLong(nFreePages), PyLong_FromUnsignedLong(nBytesPerPage) };
  if (vargs[1] && vargs[2] && vargs[3] && vargs[4])
    retval = PyObject_Vectorcall((PyObject *)callable, vargs + 1, 4 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[1]);
  Py_XDECREF(vargs[2]);
  Py_XDECREF(vargs[3]);
  Py_XDECREF(vargs[4]);
  CHAIN_EXC_END;

  if (retval && PyLong_Check(retval))
  {
    CHAIN_EXC(res = PyLong_AsInt(retval));
    if (!PyErr_Occurred())
      goto finally;
  }

  if (retval)
    CHAIN_EXC(PyErr_Format(PyExc_TypeError, "autovacuum_pages callback must return a number that fits in 'int' not %R",
                           OBJ(retval)));
  AddTraceBackHere(__FILE__, __LINE__, "autovacuum_pages_callback", AVPCB_TB, "callback", OBJ((PyObject *)callable),
                   "schema", schema, "nPages", nPages, "nFreePages", nFreePages, "nBytesPerPage", nBytesPerPage,
                   "result", OBJ(retval));

finally:
  Py_XDECREF(retval);
  PyGILState_Release(gilstate);
  return res;
}

#undef AVPCB_CAL
#undef AVPCB_TB

/** .. method:: autovacuum_pages(callable: Optional[Callable[[str, int, int, int], int]]) -> None

  Calls `callable` to find out how many pages to autovacuum.  The callback has 4 parameters:

  * Database name: str. `main`, `temp`, the name in `ATTACH <https://sqlite.org/lang_attach.html>`__
  * Database pages: int (how many pages make up the database now)
  * Free pages: int (how many pages could be freed)
  * Page size: int (page size in bytes)

  Return how many pages should be freed.  Values less than zero or more than the free pages are
  treated as zero or free page count.  On error zero is returned.

  .. warning:: READ THE NOTE IN THE SQLITE DOCUMENTATION.

    Calling back into SQLite can result in crashes, corrupt
    databases, or worse.

  -* sqlite3_autovacuum_pages
*/
static PyObject *
Connection_autovacuum_pages(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int res;
  PyObject *callable;

  CHECK_CLOSED(self, NULL);

  {
    Connection_autovacuum_pages_CHECK;
    ARG_PROLOG(1, Connection_autovacuum_pages_KWNAMES);
    ARG_MANDATORY ARG_optional_Callable(callable);
    ARG_EPILOG(NULL, Connection_autovacuum_pages_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  if (!callable)
  {
    res = sqlite3_autovacuum_pages(self->db, NULL, NULL, NULL);
  }
  else
  {
    res = sqlite3_autovacuum_pages(self->db, autovacuum_pages_cb, callable, autovacuum_pages_cleanup);
    if (res == SQLITE_OK)
      Py_INCREF(callable);
  }
  SET_EXC(res, self->db);
  sqlite3_mutex_leave(self->dbmutex);

  if (PyErr_Occurred())
    return NULL;

  Py_RETURN_NONE;
}

static void
collationneeded_cb(void *pAux, sqlite3 *Py_UNUSED(db), int eTextRep, const char *name)
{
  PyObject *res = NULL;
  Connection *self = (Connection *)pAux;
  PyGILState_STATE gilstate = PyGILState_Ensure();

  assert(self->collationneeded);

  MakeExistingException();

  if (PyErr_Occurred())
    apsw_write_unraisable(NULL);
  PyObject *vargs[] = { NULL, (PyObject *)self, PyUnicode_FromString(name) };
  if (vargs[2])
    res = PyObject_Vectorcall(self->collationneeded, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[2]);
  if (!res)
    AddTraceBackHere(__FILE__, __LINE__, "collationneeded callback", "{s: O, s: i, s: s}", "Connection", self,
                     "eTextRep", eTextRep, "name", name);
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
}

/** .. method:: collation_needed(callable: Optional[Callable[[Connection, str], None]]) -> None

  *callable* will be called if a statement requires a `collation
  <https://en.wikipedia.org/wiki/Collation>`_ that hasn't been
  registered. Your callable will be passed two parameters. The first
  is the connection object. The second is the name of the
  collation. If you have the collation code available then call
  :meth:`Connection.create_collation`.

  This is useful for creating collations on demand.  For example you
  may include the `locale <https://en.wikipedia.org/wiki/Locale>`_ in
  the collation name, but since there are thousands of locales in
  popular use it would not be useful to :meth:`prereigster
  <Connection.create_collation>` them all.  Using
  :meth:`~Connection.collation_needed` tells you when you need to
  register them.

  .. seealso::

    * :meth:`~Connection.create_collation`

  -* sqlite3_collation_needed
*/
static PyObject *
Connection_collation_needed(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int res;
  PyObject *callable;

  CHECK_CLOSED(self, NULL);

  {
    Connection_collation_needed_CHECK;
    ARG_PROLOG(1, Connection_collation_needed_KWNAMES);
    ARG_MANDATORY ARG_optional_Callable(callable);
    ARG_EPILOG(NULL, Connection_collation_needed_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  if (callable)
    res = sqlite3_collation_needed(self->db, self, collationneeded_cb);
  else
    res = sqlite3_collation_needed(self->db, NULL, NULL);
  SET_EXC(res, self->db);
  sqlite3_mutex_leave(self->dbmutex);

  if (PyErr_Occurred())
    return NULL;

  Py_CLEAR(self->collationneeded);

  if (callable)
    self->collationneeded = Py_NewRef(callable);

  Py_RETURN_NONE;
}

static int
busyhandlercb(void *context, int ncall)
{
  /* Return zero for caller to get SQLITE_BUSY error. We default to
     zero in case of error. */

  PyGILState_STATE gilstate;
  PyObject *retval = NULL;
  int result = 0; /* default to fail with SQLITE_BUSY */
  Connection *self = (Connection *)context;

  assert(self);
  assert(self->busyhandler);

  gilstate = PyGILState_Ensure();

  MakeExistingException();
  PyObject *vargs[] = { NULL, PyLong_FromLong(ncall) };
  if (vargs[1])
    retval = PyObject_Vectorcall(self->busyhandler, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[1]);
  if (!retval)
    goto finally; /* abort due to exception */

  result = PyObject_IsTrueStrict(retval);
  assert(result == -1 || result == 0 || result == 1);
  Py_DECREF(retval);

  if (result == -1)
  {
    result = 0;
    goto finally; /* abort due to exception converting retval */
  }

finally:
  PyGILState_Release(gilstate);
  return result;
}

/** .. method:: set_busy_handler(callable: Optional[Callable[[int], bool]]) -> None

   Sets the busy handler to callable. callable will be called with one
   integer argument which is the number of prior calls to the busy
   callback for the same lock. If the busy callback returns False,
   then SQLite returns *SQLITE_BUSY* to the calling code. If
   the callback returns True, then SQLite tries to open the table
   again and the cycle repeats.

   If you previously called :meth:`~Connection.set_busy_timeout` then
   calling this overrides that.

   Passing None unregisters the existing handler.

   .. seealso::

     * :meth:`Connection.set_busy_timeout`
     * :ref:`Busy handling <busyhandling>`

   -* sqlite3_busy_handler

*/
static PyObject *
Connection_set_busy_handler(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int res = SQLITE_OK;
  PyObject *callable;

  CHECK_CLOSED(self, NULL);

  {
    Connection_set_busy_handler_CHECK;
    ARG_PROLOG(1, Connection_set_busy_handler_KWNAMES);
    ARG_MANDATORY ARG_optional_Callable(callable);
    ARG_EPILOG(NULL, Connection_set_busy_handler_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);

  if (callable)
    res = sqlite3_busy_handler(self->db, busyhandlercb, self);
  else
    res = sqlite3_busy_handler(self->db, NULL, NULL);

  SET_EXC(res, self->db);

  sqlite3_mutex_leave(self->dbmutex);

  if (PyErr_Occurred())
    return NULL;

  Py_CLEAR(self->busyhandler);
  if (callable)
    self->busyhandler = Py_NewRef(callable);

  Py_RETURN_NONE;
}

#ifndef SQLITE_OMIT_DESERIALZE
/** .. method:: serialize(name: str) -> bytes

  Returns a memory copy of the database. *name* is `main`, `temp`, the name
  in `ATTACH <https://sqlite.org/lang_attach.html>`__

  The memory copy is the same as if the database was backed up to
  disk.

  If the database name doesn't exist, then None is returned, not an
  exception (this is SQLite's behaviour).  One exception is than an
  empty temp will result in a None return.

   .. seealso::

     * :meth:`Connection.deserialize`

   -* sqlite3_serialize

*/
static PyObject *
Connection_serialize(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *pyres = NULL;
  const char *name;
  sqlite3_int64 size = 0;
  unsigned char *serialization = NULL;

  CHECK_CLOSED(self, NULL);

  {
    Connection_serialize_CHECK;
    ARG_PROLOG(1, Connection_serialize_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_EPILOG(NULL, Connection_serialize_USAGE, );
  }

  /* sqlite3_serialize does not use the same error pattern as other
  SQLite APIs.  I originally coded this as though error codes/strings
  were done behind the scenes.  However that turns out not to be the
  case so this code can't do anything about errors.  See commit
  history for prior attempt */

  DBMUTEX_ENSURE(self->dbmutex);
  serialization = sqlite3_serialize(self->db, name, &size, 0);
  sqlite3_mutex_leave(self->dbmutex);

  /* pyerror could have been raised in a vfs */
  if (serialization && !PyErr_Occurred())
    pyres = PyBytes_FromStringAndSize((char *)serialization, size);

  sqlite3_free(serialization);
  if (pyres)
    return pyres;
  if (PyErr_Occurred())
    return NULL;
  Py_RETURN_NONE;
}

/** .. method:: deserialize(name: str, contents: bytes) -> None

   Replaces the named database with an in-memory copy of *contents*.
   *name* is `main`, `temp`, the name in `ATTACH
   <https://sqlite.org/lang_attach.html>`__

   The resulting database is in-memory, read-write, and the memory is
   owned, resized, and freed by SQLite.

   .. seealso::

     * :meth:`Connection.serialize`

   -* sqlite3_deserialize

*/
static PyObject *
Connection_deserialize(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *name = NULL;
  PyObject *contents;
  Py_buffer contents_buffer;

  char *newcontents = NULL;
  int res = SQLITE_OK;

  CHECK_CLOSED(self, NULL);

  {
    Connection_deserialize_CHECK;
    ARG_PROLOG(2, Connection_deserialize_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_MANDATORY ARG_py_buffer(contents);
    ARG_EPILOG(NULL, Connection_deserialize_USAGE, );
  }

  if (0 != PyObject_GetBufferContiguous(contents, &contents_buffer, PyBUF_SIMPLE))
  {
    assert(PyErr_Occurred());
    return NULL;
  }

  size_t len = contents_buffer.len;
  newcontents = sqlite3_malloc64(contents_buffer.len);
  if (newcontents)
    memcpy(newcontents, contents_buffer.buf, len);

  PyBuffer_Release(&contents_buffer);

  if (!newcontents)
  {
    res = SQLITE_NOMEM;
    PyErr_NoMemory();
  }

  DBMUTEX_ENSURE(self->dbmutex);
  if (res == SQLITE_OK)
    res = sqlite3_deserialize(self->db, name, (unsigned char *)newcontents, len, len,
                              SQLITE_DESERIALIZE_RESIZEABLE | SQLITE_DESERIALIZE_FREEONCLOSE);
  SET_EXC(res, self->db);
  sqlite3_mutex_leave(self->dbmutex);

  /* sqlite frees the buffer on error due to freeonclose flag */
  if (PyErr_Occurred())
    return NULL;
  Py_RETURN_NONE;
}
#endif /* SQLITE_OMIT_DESERIALZE */

#ifndef SQLITE_OMIT_LOAD_EXTENSION
/** .. method:: enable_load_extension(enable: bool) -> None

  Enables/disables `extension loading
  <https://www.sqlite.org/loadext.html>`_
  which is disabled by default.

  :param enable: If True then extension loading is enabled, else it is disabled.

  -* sqlite3_enable_load_extension

  .. seealso::

    * :meth:`~Connection.load_extension`
*/

static PyObject *
Connection_enable_load_extension(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                                 PyObject *fast_kwnames)
{
  int enable, res;

  CHECK_CLOSED(self, NULL);

  {
    Connection_enable_load_extension_CHECK;
    ARG_PROLOG(1, Connection_enable_load_extension_KWNAMES);
    ARG_MANDATORY ARG_bool(enable);
    ARG_EPILOG(NULL, Connection_enable_load_extension_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  res = sqlite3_enable_load_extension(self->db, enable);
  SET_EXC(res, self->db);
  sqlite3_mutex_leave(self->dbmutex);

  if (PyErr_Occurred())
    return NULL;
  Py_RETURN_NONE;
}

/** .. method:: load_extension(filename: str, entrypoint: Optional[str] = None) -> None

  Loads *filename* as an `extension <https://www.sqlite.org/loadext.html>`_

  :param filename: The file to load.

  :param entrypoint: The initialization method to call.  If this
    parameter is not supplied then the SQLite default of
    ``sqlite3_extension_init`` is used.

  :raises ExtensionLoadingError: If the extension could not be
    loaded.  The exception string includes more details.

  -* sqlite3_load_extension

  .. seealso::

    * :meth:`~Connection.enable_load_extension`
*/
static PyObject *
Connection_load_extension(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int res;
  const char *filename = NULL, *entrypoint = NULL;
  char *errmsg = NULL; /* sqlite doesn't believe in const */

  CHECK_CLOSED(self, NULL);
  {
    Connection_load_extension_CHECK;
    ARG_PROLOG(2, Connection_load_extension_KWNAMES);
    ARG_MANDATORY ARG_str(filename);
    ARG_OPTIONAL ARG_optional_str(entrypoint);
    ARG_EPILOG(NULL, Connection_load_extension_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  Py_BEGIN_ALLOW_THREADS res = sqlite3_load_extension(self->db, filename, entrypoint, &errmsg);
  Py_END_ALLOW_THREADS;
  sqlite3_mutex_leave(self->dbmutex);

  /* load_extension doesn't set the error message on the db so we have to make exception manually */
  if (res != SQLITE_OK)
  {
    if (!PyErr_Occurred())
      PyErr_Format(ExcExtensionLoading, "ExtensionLoadingError: %s", errmsg ? errmsg : "<unspecified error>");
    sqlite3_free(errmsg);
  }

  if (PyErr_Occurred())
    return NULL;

  Py_RETURN_NONE;
}
#endif

/* USER DEFINED FUNCTION CODE.*/
static PyTypeObject FunctionCBInfoType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.FunctionCBInfo",
  .tp_basicsize = sizeof(FunctionCBInfo),
  .tp_dealloc = (destructor)FunctionCBInfo_dealloc,
  .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
  .tp_doc = "FunctionCBInfo object",
};

#undef allocfunccbinfo
static FunctionCBInfo *
allocfunccbinfo(const char *name)
{
#include "faultinject.h"
  FunctionCBInfo *res = (FunctionCBInfo *)_PyObject_New(&FunctionCBInfoType);
  if (res)
  {
    res->name = apsw_strdup(name);
    res->scalarfunc = 0;
    res->aggregatefactory = 0;
    res->windowfactory = 0;
    if (!res->name)
    {
      FunctionCBInfo_dealloc(res);
      res = 0;
    }
  }
  return res;
}

/* converts a python object into a sqlite3_context result

  returns zero on failure, non-zero on success
*/
static int
set_context_result(sqlite3_context *context, PyObject *obj)
{
  assert(obj);

  /* DUPLICATE(ish) code: this is substantially similar to the code in
     APSWCursor_dobinding.  If you fix anything here then do it there as
     well. */

  if (Py_IsNone(obj))
  {
    sqlite3_result_null(context);
    return 1;
  }
  if (PyLong_Check(obj))
  {
    long long v = PyLong_AsLongLong(obj);
    if (v == -1 && PyErr_Occurred())
    {
      sqlite3_result_error(context, "python integer overflow", -1);
      return 0;
    }
    sqlite3_result_int64(context, v);
    return 1;
  }
  if (PyFloat_Check(obj))
  {
    sqlite3_result_double(context, PyFloat_AS_DOUBLE(obj));
    return 1;
  }
  if (PyUnicode_Check(obj))
  {
    const char *strdata;
    Py_ssize_t strbytes;

    strdata = PyUnicode_AsUTF8AndSize(obj, &strbytes);
    if (strdata)
    {
      sqlite3_result_text64(context, strdata, strbytes, SQLITE_TRANSIENT, SQLITE_UTF8);
      return 1;
    }
    sqlite3_result_error(context, "Unicode conversions failed", -1);
    return 0;
  }

  if (PyObject_CheckBuffer(obj))
  {
    int asrb;
    Py_buffer py3buffer;

    asrb = PyObject_GetBufferContiguous(obj, &py3buffer, PyBUF_SIMPLE);

    if (asrb != 0)
    {
      assert(PyErr_Occurred());
      sqlite3_result_error(context, "PyObject_GetBufferContiguous failed", -1);
      return 0;
    }
    sqlite3_result_blob64(context, py3buffer.buf, py3buffer.len, SQLITE_TRANSIENT);
    PyBuffer_Release(&py3buffer);
    return 1;
  }

  if (PyObject_TypeCheck(obj, &ZeroBlobBindType) == 1)
  {
    sqlite3_result_zeroblob64(context, ((ZeroBlobBind *)obj)->blobsize);
    return 1;
  }

  if (PyObject_TypeCheck(obj, &PyObjectBindType) == 1)
  {
    sqlite3_result_pointer(context, Py_NewRef(((PyObjectBind *)obj)->object), PYOBJECT_BIND_TAG,
                           pyobject_bind_destructor);
    return 1;
  }

  PyErr_Format(PyExc_TypeError,
               "Value from Python is not supported by SQLite.  It should be one of None, int, float, str, bytes, or "
               "wrapped with apsw.pyobject.  "
               "Received %s.",
               Py_TypeName(obj));
  sqlite3_result_error(context, "Bad return type from python function callback", -1);
  return 0;
}

/* returns 0 on success, non-zero on failure */
#undef getfunctionargs
static int
getfunctionargs(PyObject *vargs[], sqlite3_context *context, int argc, sqlite3_value **argv)
{
#include "faultinject.h"
  int i;
  for (i = 0; i < argc; i++)
  {
    vargs[i] = convert_value_to_pyobject(argv[i], 0, 0);
    if (!vargs[i])
      goto error;
  }
  return 0;
error:
  sqlite3_result_error(context, "convert_value_to_pyobject failed", -1);
  int j;
  for (j = 0; j < i; j++)
    Py_XDECREF(vargs[j]);
  assert(PyErr_Occurred());
  return -1;
}

/* dispatches scalar function */
static void
cbdispatch_func(sqlite3_context *context, int argc, sqlite3_value **argv)
{
  PyGILState_STATE gilstate;
  PyObject *retval = NULL;
  FunctionCBInfo *cbinfo = (FunctionCBInfo *)sqlite3_user_data(context);
  assert(cbinfo);

  VLA_PYO(vargs, 1 + argc);

  gilstate = PyGILState_Ensure();

  assert(cbinfo->scalarfunc);

  MakeExistingException();

  if (PyErr_Occurred())
  {
    sqlite3_result_error_code(context, MakeSqliteMsgFromPyException(NULL));
    sqlite3_result_error(context, "Prior Python Error", -1);
    goto finalfinally;
  }

  if (getfunctionargs(vargs + 1, context, argc, argv))
    goto finally;

  assert(!PyErr_Occurred());
  retval = PyObject_Vectorcall(cbinfo->scalarfunc, vargs + 1, argc | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_DECREF_ARRAY(vargs + 1, argc);
  if (retval)
    set_context_result(context, retval);

finally:
  if (PyErr_Occurred())
  {
    char *errmsg = NULL;
    char *funname = NULL;
    CHAIN_EXC(funname = sqlite3_mprintf("user-defined-scalar-%s", cbinfo->name); if (!funname) PyErr_NoMemory(););
    sqlite3_result_error_code(context, MakeSqliteMsgFromPyException(&errmsg));
    sqlite3_result_error(context, errmsg, -1);
    AddTraceBackHere(__FILE__, __LINE__, funname ? funname : "sqlite3_mprintf ran out of memory", "{s: i, s: s}",
                     "NumberOfArguments", argc, "message", errmsg);
    sqlite3_free(funname);
    sqlite3_free(errmsg);
  }
finalfinally:
  Py_XDECREF(retval);

  PyGILState_Release(gilstate);
}

static aggregatefunctioncontext *
getaggregatefunctioncontext(sqlite3_context *context)
{
  aggregatefunctioncontext *aggfc = sqlite3_aggregate_context(context, sizeof(aggregatefunctioncontext));
  if (!aggfc)
    return (void *)PyErr_NoMemory();

  FunctionCBInfo *cbinfo;
  PyObject *retval;
  /* have we seen it before? */
  if (aggfc->state == afcOK)
    return aggfc;
  if (aggfc->state == afcERROR)
    return NULL;
  assert(aggfc->state == afcUNINIT);

  cbinfo = (FunctionCBInfo *)sqlite3_user_data(context);
  assert(cbinfo);
  assert(cbinfo->aggregatefactory);

  aggfc->state = afcERROR;

  /* call the aggregatefactory to get our working objects */
  PyObject *vargs[] = { NULL };
  retval = PyObject_Vectorcall(cbinfo->aggregatefactory, vargs + 1, 0 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);

  if (!retval)
  {
    return NULL;
  }
  /* it should have returned object or a tuple of 3 items: object, stepfunction and finalfunction */
  if (!PyTuple_Check(retval))
  {
    aggfc->aggvalue = NULL;
    aggfc->stepfunc = PyObject_GetAttr(retval, apst.step);
    if (!aggfc->stepfunc)
      goto finally;
    if (!PyCallable_Check(aggfc->stepfunc))
    {
      PyErr_Format(PyExc_TypeError, "aggregate step function must be callable not %s", Py_TypeName(aggfc->stepfunc));
      goto finally;
    }
    aggfc->finalfunc = PyObject_GetAttr(retval, apst.final);
    if (!aggfc->finalfunc)
      goto finally;
    if (!PyCallable_Check(aggfc->finalfunc))
    {
      PyErr_Format(PyExc_TypeError, "aggregate final function must be callable not %s", Py_TypeName(aggfc->finalfunc));
      goto finally;
    }
    aggfc->state = afcOK;
  }
  else
  {

    if (PyTuple_GET_SIZE(retval) != 3)
    {
      PyErr_Format(PyExc_TypeError,
                   "Aggregate factory should return 3 item tuple of (object, stepfunction, finalfunction)");
      goto finally;
    }
    /* we don't care about the type of the zeroth item (object) ... */

    /* stepfunc */
    if (!PyCallable_Check(PyTuple_GET_ITEM(retval, 1)))
    {
      PyErr_Format(PyExc_TypeError, "stepfunction must be callable");
      goto finally;
    }

    /* finalfunc */
    if (!PyCallable_Check(PyTuple_GET_ITEM(retval, 2)))
    {
      PyErr_Format(PyExc_TypeError, "final function must be callable");
      goto finally;
    }

    aggfc->aggvalue = Py_NewRef(PyTuple_GET_ITEM(retval, 0));
    aggfc->stepfunc = Py_NewRef(PyTuple_GET_ITEM(retval, 1));
    aggfc->finalfunc = Py_NewRef(PyTuple_GET_ITEM(retval, 2));

    aggfc->state = afcOK;
  }
finally:
  if (aggfc->state != afcOK)
  {
    Py_CLEAR(aggfc->aggvalue);
    Py_CLEAR(aggfc->stepfunc);
    Py_CLEAR(aggfc->finalfunc);
  }
  assert(retval);
  Py_DECREF(retval);
  return aggfc;
}

/*
  Note that we can't call sqlite3_result_error in the step function as
  SQLite doesn't want to you to do that (and core dumps!)
  Consequently if an error is returned, we will still be repeatedly
  called.
*/

static void
cbdispatch_step(sqlite3_context *context, int argc, sqlite3_value **argv)
{
  PyGILState_STATE gilstate;
  PyObject *retval;
  aggregatefunctioncontext *aggfc = NULL;

  VLA_PYO(vargs, 2 + argc);

  gilstate = PyGILState_Ensure();

  MakeExistingException();

  if (PyErr_Occurred())
    goto finalfinally;

  aggfc = getaggregatefunctioncontext(context);

  if (!aggfc || PyErr_Occurred())
    goto finally;

  int offset = (aggfc->aggvalue) ? 1 : 0;
  vargs[1] = aggfc->aggvalue;
  if (getfunctionargs(vargs + 1 + offset, context, argc, argv))
    goto finally;

  assert(!PyErr_Occurred());
  retval = PyObject_Vectorcall(aggfc->stepfunc, vargs + 1, (argc + offset) | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_DECREF_ARRAY(vargs + 1 + offset, argc);
  Py_XDECREF(retval);

  if (!retval)
  {
    assert(PyErr_Occurred());
  }

finally:
  if (PyErr_Occurred())
  {
    char *funname = 0;
    FunctionCBInfo *cbinfo = (FunctionCBInfo *)sqlite3_user_data(context);
    assert(cbinfo);
    CHAIN_EXC(funname = sqlite3_mprintf("user-defined-aggregate-step-%s", cbinfo->name);
              if (!funname) PyErr_NoMemory(););
    AddTraceBackHere(__FILE__, __LINE__, funname ? funname : "sqlite3_mprintf ran out of memory", "{s: i}",
                     "NumberOfArguments", argc);
    sqlite3_free(funname);
  }
finalfinally:
  PyGILState_Release(gilstate);
}

/* this is somewhat similar to cbdispatch_step, except we also have to
   do some cleanup of the aggregatefunctioncontext */
static void
cbdispatch_final(sqlite3_context *context)
{
  PyGILState_STATE gilstate;
  PyObject *retval = NULL;
  aggregatefunctioncontext *aggfc = NULL;

  gilstate = PyGILState_Ensure();

  MakeExistingException();

  PY_ERR_FETCH(exc_save);

  aggfc = getaggregatefunctioncontext(context);
  if (!aggfc)
    goto finally;

  MakeExistingException();

  if (PY_ERR_NOT_NULL(exc_save) || PyErr_Occurred() || !aggfc->finalfunc)
  {
    sqlite3_result_error(context, "Prior Python Error in step function", -1);
    goto finally;
  }

  int offset = (aggfc->aggvalue) ? 1 : 0;
  PyObject *vargs[] = { NULL, aggfc->aggvalue };
  retval = PyObject_Vectorcall(aggfc->finalfunc, vargs + 1, offset | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  if (retval)
  {
    int ok = set_context_result(context, retval);
    assert(ok || PyErr_Occurred());
    (void)ok;
    Py_DECREF(retval);
  }

finally:
  /* we also free the aggregatefunctioncontext here */
  if (aggfc)
  {
    Py_CLEAR(aggfc->aggvalue);
    Py_CLEAR(aggfc->stepfunc);
    Py_CLEAR(aggfc->finalfunc);
  }

  if (PyErr_Occurred() && PY_ERR_NOT_NULL(exc_save))
    apsw_write_unraisable(NULL);

  if (PY_ERR_NOT_NULL(exc_save))
    PY_ERR_RESTORE(exc_save);

  if (PyErr_Occurred())
  {
    char *funname = 0;
    FunctionCBInfo *cbinfo = (FunctionCBInfo *)sqlite3_user_data(context);
    assert(cbinfo);
    CHAIN_EXC(funname = sqlite3_mprintf("user-defined-aggregate-final-%s", cbinfo->name);
              if (!funname) PyErr_NoMemory(););
    AddTraceBackHere(__FILE__, __LINE__, funname ? funname : "sqlite3_mprintf ran out of memory", NULL);
    sqlite3_free(funname);
  }

  /* sqlite3 frees the actual underlying memory we used (aggfc itself) */

  PyGILState_Release(gilstate);
}

static void
clear_window_function_context(windowfunctioncontext *winfc)
{
  if (winfc)
  {
    Py_CLEAR(winfc->aggvalue);
    Py_CLEAR(winfc->stepfunc);
    Py_CLEAR(winfc->finalfunc);
    Py_CLEAR(winfc->valuefunc);
    Py_CLEAR(winfc->inversefunc);
    winfc->state = wfcERROR;
  }
}

static windowfunctioncontext *
get_window_function_context_wrapped(sqlite3_context *context)
{
  windowfunctioncontext *winfc = sqlite3_aggregate_context(context, sizeof(windowfunctioncontext));
  if (!winfc)
    return (void *)PyErr_NoMemory();

  FunctionCBInfo *cbinfo;
  PyObject *retval = NULL;
  PyObject *sequence = NULL;

  /* have we seen it before? */
  if (winfc->state == wfcOK)
    return winfc;
  if (winfc->state == wfcERROR)
    return NULL;
  assert(winfc->state == wfcUNINIT);

  winfc->state = wfcERROR;

  cbinfo = (FunctionCBInfo *)sqlite3_user_data(context);
  assert(cbinfo);
  assert(cbinfo->windowfactory);

  /* call the windowfactory to get our working object(s) */
  PyObject *vargs[] = { NULL };
  retval = PyObject_Vectorcall(cbinfo->windowfactory, vargs + 1, 0 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);

  if (!retval)
    goto finally;

  /* it should have returned a sequence of object and 4 functions, or a single object */
  if (PyTuple_Check(retval) || PyList_Check(retval))
  {
    sequence = PySequence_Fast(retval, "expected a sequence");
    if (!sequence)
      goto finally;
    if (PySequence_Fast_GET_SIZE(sequence) != 5)
    {
      PyErr_Format(PyExc_TypeError, "Expected a 5 item sequence");
      goto finally;
    }
    winfc->aggvalue = Py_NewRef(PySequence_Fast_GET_ITEM(sequence, 0));

#define METH(n, i)                                                                                                     \
  winfc->n##func = Py_NewRef(PySequence_Fast_GET_ITEM(sequence, i));                                                   \
  if (!PyCallable_Check(winfc->n##func))                                                                               \
  {                                                                                                                    \
    PyErr_Format(PyExc_TypeError, "Expected item %d (%s) to be callable - got %s", i, #n,                              \
                 Py_TypeName(winfc->n##func));                                                                         \
    goto finally;                                                                                                      \
  }

    METH(step, 1);
    METH(final, 2);
    METH(value, 3);
    METH(inverse, 4);

#undef METH
  }
  else
  {
#define METH(n)                                                                                                        \
  winfc->n##func = PyObject_GetAttr(retval, apst.n);                                                                   \
  if (!winfc->n##func)                                                                                                 \
    goto finally;                                                                                                      \
  if (!PyCallable_Check(winfc->n##func))                                                                               \
  {                                                                                                                    \
    PyErr_Format(PyExc_TypeError, "Expected callable window function %s - got %s", #n, Py_TypeName(winfc->n##func));   \
    goto finally;                                                                                                      \
  }

    METH(step);
    METH(final);
    METH(value);
    METH(inverse);
#undef METH
  }

  winfc->state = wfcOK;

finally:
  if (PyErr_Occurred())
  {
    assert(winfc->state != wfcOK);
    AddTraceBackHere(__FILE__, __LINE__, "get_window_function_context", "{s: O, s: O, s: s}", "instance", OBJ(retval),
                     "as_sequence", OBJ(sequence), "name", cbinfo->name);
  }
  Py_XDECREF(retval);
  Py_XDECREF(sequence);
  if (winfc->state == wfcOK)
    return winfc;
  clear_window_function_context(winfc);
  return NULL;
}

#undef get_window_function_context
static windowfunctioncontext *
get_window_function_context(sqlite3_context *context)
{
#include "faultinject.h"
  windowfunctioncontext *res;

  CHAIN_EXC(res = get_window_function_context_wrapped(context));
  assert(res || PyErr_Occurred());
  return res;
}

/* Used for the create function v2 xDestroy callbacks.  Note this is
   called even when supplying NULL for the function implementation (ie
   deleting it), so XDECREF has to be used.
 */
static void
apsw_free_func(void *funcinfo)
{
  PyGILState_STATE gilstate;
  gilstate = PyGILState_Ensure();

  Py_XDECREF((PyObject *)funcinfo);

  PyGILState_Release(gilstate);
}

#define funcname (sqlite3_user_data(context) ? ((FunctionCBInfo *)sqlite3_user_data(context))->name : "<unknown>")

static void
cbw_step(sqlite3_context *context, int argc, sqlite3_value **argv)
{
  PyGILState_STATE gilstate;
  windowfunctioncontext *winfc = NULL;
  PyObject *retval = NULL;

  VLA_PYO(vargs, 2 + argc);

  gilstate = PyGILState_Ensure();

  MakeExistingException();

  if (PyErr_Occurred())
    goto error;

  winfc = get_window_function_context(context);
  if (!winfc)
    goto error;

  int offset = (winfc->aggvalue) ? 1 : 0;
  vargs[1] = winfc->aggvalue;
  if (getfunctionargs(vargs + 1 + offset, context, argc, argv))
    goto error;

  retval = PyObject_Vectorcall(winfc->stepfunc, vargs + 1, (offset + argc) | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_DECREF_ARRAY(vargs + 1 + offset, argc);
  if (retval)
    goto finally;

error:
  assert(PyErr_Occurred());
  sqlite3_result_error(context, "Python exception on window function 'step'", -1);
  AddTraceBackHere(__FILE__, __LINE__, "window-function-step", "{s:i, s: O, s:s}", "argc", argc, "retval", OBJ(retval),
                   "name", funcname);

finally:
  Py_XDECREF(retval);

  PyGILState_Release(gilstate);
}

static void
cbw_final(sqlite3_context *context)
{
  PyGILState_STATE gilstate;
  windowfunctioncontext *winfc;
  PyObject *retval = NULL;
  int ok;

  gilstate = PyGILState_Ensure();

  MakeExistingException();

  /* This function is always called by SQLite in the face of previous
     errors so that cleanup can be done so we always get the window
     function context before doing any error checking */
  winfc = get_window_function_context(context);
  if (!winfc || PyErr_Occurred())
    goto error;
  PyObject *vargs[] = { NULL, winfc->aggvalue };
  retval = PyObject_Vectorcall(winfc->finalfunc, vargs + 1,
                               ((winfc->aggvalue) ? 1 : 0) | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);

  if (!retval)
    goto error;

  ok = set_context_result(context, retval);
  if (ok)
    goto finally;

error:
  assert(PyErr_Occurred());
  sqlite3_result_error(context, "Python exception on window function 'final' or earlier", -1);
  AddTraceBackHere(__FILE__, __LINE__, "window-function-final", "{s:O,s:s}", "retval", OBJ(retval), "name", funcname);

finally:
  Py_XDECREF(retval);

  clear_window_function_context(winfc);

  PyGILState_Release(gilstate);
}

static void
cbw_value(sqlite3_context *context)
{
  PyGILState_STATE gilstate;
  windowfunctioncontext *winfc;
  PyObject *retval = NULL;
  int ok;

  gilstate = PyGILState_Ensure();

  MakeExistingException();

  if (PyErr_Occurred())
    goto error;

  winfc = get_window_function_context(context);
  if (!winfc)
    goto error;

  PyObject *vargs[] = { NULL, winfc->aggvalue };

  retval = PyObject_Vectorcall(winfc->valuefunc, vargs + 1, (winfc->aggvalue) ? 1 : 0 | PY_VECTORCALL_ARGUMENTS_OFFSET,
                               NULL);
  if (!retval)
    goto error;

  ok = set_context_result(context, retval);
  if (ok)
    goto finally;

error:
  assert(PyErr_Occurred());
  sqlite3_result_error(context, "Python exception on window function 'value'", -1);
  AddTraceBackHere(__FILE__, __LINE__, "window-function-final", "{s:O,s:s}", "retval", OBJ(retval), "name", funcname);
finally:
  Py_XDECREF(retval);

  PyGILState_Release(gilstate);
}

static void
cbw_inverse(sqlite3_context *context, int argc, sqlite3_value **argv)
{
  PyGILState_STATE gilstate;
  windowfunctioncontext *winfc;
  PyObject *retval = NULL;

  VLA_PYO(vargs, 2 + argc);

  gilstate = PyGILState_Ensure();

  MakeExistingException();

  if (PyErr_Occurred())
    goto error;

  winfc = get_window_function_context(context);
  if (!winfc)
    goto error;

  int offset = (winfc->aggvalue) ? 1 : 0;
  vargs[1] = winfc->aggvalue;
  if (getfunctionargs(vargs + 1 + offset, context, argc, argv))
    goto error;
  retval = PyObject_Vectorcall(winfc->inversefunc, vargs + 1, (offset + argc) | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_DECREF_ARRAY(vargs + 1 + offset, argc);
  if (!retval)
    goto error;

  goto finally;

error:
  assert(PyErr_Occurred());
  sqlite3_result_error(context, "Python exception on window function 'inverse'", -1);
  AddTraceBackHere(__FILE__, __LINE__, "window-function-inverse", "{s:i,s:O,s:s}", "argc", argc, "retval", OBJ(retval),
                   "name", funcname);

finally:
  Py_XDECREF(retval);

  PyGILState_Release(gilstate);
}

#undef funcname

/** .. method:: create_window_function(name:str, factory: Optional[WindowFactory], numargs: int =-1, *, flags: int = 0) -> None

    Registers a `window function
    <https://sqlite.org/windowfunctions.html#user_defined_aggregate_window_functions>`__

    :param name: The string name of the function.  It should be less than 255 characters
    :param factory: Called to start a new window.  Use None to delete the function.
    :param numargs: How many arguments the function takes, with -1 meaning any number
    :param flags: `Function flags <https://www.sqlite.org/c3ref/c_deterministic.html>`__

    You need to provide callbacks for the ``step``, ``final``, ``value``
    and ``inverse`` methods.  This can be done by having `factory` as a
    class, returning an instance with the corresponding method names, or by having `factory`
    return a sequence of a first parameter, and then each of the 4
    functions.

    **Debugging note** SQlite always calls the ``final`` method to allow
    for cleanup.  If you have an exception in one of the other methods, then
    ``final`` will also be called, and you may see both methods in
    tracebacks.

    .. seealso::

     * :ref:`Example <example_window>`
     * :meth:`~Connection.create_scalar_function`
     * :meth:`~Connection.create_aggregate_function`

    -* sqlite3_create_window_function
*/
static PyObject *
Connection_create_window_function(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                                  PyObject *fast_kwnames)
{
  int numargs = -1, flags = 0, res;
  const char *name = NULL;
  PyObject *factory = NULL;
  FunctionCBInfo *cbinfo;

  CHECK_CLOSED(self, NULL);

  {
    Connection_create_window_function_CHECK;
    ARG_PROLOG(3, Connection_create_window_function_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_MANDATORY ARG_optional_Callable(factory);
    ARG_OPTIONAL ARG_int(numargs);
    ARG_OPTIONAL ARG_int(flags);
    ARG_EPILOG(NULL, Connection_create_window_function_USAGE, );
  }

  if (!factory)
    cbinfo = NULL;
  else
  {
    cbinfo = allocfunccbinfo(name);
    if (!cbinfo)
      goto finally;
    cbinfo->windowfactory = Py_NewRef(factory);
  }

  DBMUTEX_ENSURE(self->dbmutex);
  res = sqlite3_create_window_function(self->db, name, numargs, SQLITE_UTF8 | flags, cbinfo, cbinfo ? cbw_step : NULL,
                                       cbinfo ? cbw_final : NULL, cbinfo ? cbw_value : NULL,
                                       cbinfo ? cbw_inverse : NULL, apsw_free_func);
  SET_EXC(res, self->db);
  sqlite3_mutex_leave(self->dbmutex);
finally:
  if (PyErr_Occurred())
  {
    apsw_free_func(cbinfo);
    return NULL;
  }
  Py_RETURN_NONE;
}

/** .. method:: create_scalar_function(name: str, callable: Optional[ScalarProtocol], numargs: int = -1, *, deterministic: bool = False, flags: int = 0) -> None

  Registers a scalar function.  Scalar functions operate on one set of parameters once.

  :param name: The string name of the function.  It should be less than 255 characters
  :param callable: The function that will be called.  Use None to unregister.
  :param numargs: How many arguments the function takes, with -1 meaning any number
  :param deterministic: When True this means the function always
           returns the same result for the same input arguments.
           SQLite's query planner can perform additional optimisations
           for deterministic functions.  For example a random()
           function is not deterministic while one that returns the
           length of a string is.
  :param flags: Additional `function flags <https://www.sqlite.org/c3ref/c_deterministic.html>`__

  .. note::

    You can register the same named function but with different
    *callable* and *numargs*.  For example::

      connection.create_scalar_function("toip", ipv4convert, 4)
      connection.create_scalar_function("toip", ipv6convert, 16)
      connection.create_scalar_function("toip", strconvert, -1)

    The one with the correct *numargs* will be called and only if that
    doesn't exist then the one with negative *numargs* will be called.

  .. seealso::

     * :ref:`Example <example_scalar>`
     * :meth:`~Connection.create_aggregate_function`
     * :meth:`~Connection.create_window_function`

  -* sqlite3_create_function_v2
*/

static PyObject *
Connection_create_scalar_function(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                                  PyObject *fast_kwnames)
{
  int numargs = -1;
  PyObject *callable = NULL;
  int deterministic = 0, flags = 0;
  const char *name = 0;
  FunctionCBInfo *cbinfo;
  int res;

  CHECK_CLOSED(self, NULL);

  {
    Connection_create_scalar_function_CHECK;
    ARG_PROLOG(3, Connection_create_scalar_function_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_MANDATORY ARG_optional_Callable(callable);
    ARG_OPTIONAL ARG_int(numargs);
    ARG_OPTIONAL ARG_bool(deterministic);
    ARG_OPTIONAL ARG_int(flags);
    ARG_EPILOG(NULL, Connection_create_scalar_function_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);

  if (!callable)
  {
    cbinfo = 0;
  }
  else
  {
    cbinfo = allocfunccbinfo(name);
    if (!cbinfo)
      goto finally;
    cbinfo->scalarfunc = Py_NewRef(callable);
  }

  flags |= (deterministic ? SQLITE_DETERMINISTIC : 0);

  res = sqlite3_create_function_v2(self->db, name, numargs, SQLITE_UTF8 | flags, cbinfo,
                                   cbinfo ? cbdispatch_func : NULL, NULL, NULL, apsw_free_func);
  /* Note: On error sqlite3_create_function_v2 calls the destructor (apsw_free_func)! */
  SET_EXC(res, self->db);

finally:
  sqlite3_mutex_leave(self->dbmutex);
  if (PyErr_Occurred())
    return NULL;
  Py_RETURN_NONE;
}

/** .. method:: create_aggregate_function(name: str, factory: Optional[AggregateFactory], numargs: int = -1, *, flags: int = 0) -> None

  Registers an aggregate function.  Aggregate functions operate on all
  the relevant rows such as counting how many there are.

  :param name: The string name of the function.  It should be less than 255 characters
  :param factory: The function that will be called.  Use None to delete the function.
  :param numargs: How many arguments the function takes, with -1 meaning any number
  :param flags: `Function flags <https://www.sqlite.org/c3ref/c_deterministic.html>`__

  When a query starts, the *factory* will be called.  It can return an object
  with a *step* function called for each matching row, and a *final* function
  to provide the final value.

  Alternatively a non-class approach can return a tuple of 3 items:

    a context object
       This can be of any type

    a step function
       This function is called once for each row.  The first parameter
       will be the context object and the remaining parameters will be
       from the SQL statement.  Any value returned will be ignored.

    a final function
       This function is called at the very end with the context object
       as a parameter.  The value returned is set as the return for
       the function. The final function is always called even if an
       exception was raised by the step function. This allows you to
       ensure any resources are cleaned up.

  .. note::

    You can register the same named function but with different
    callables and *numargs*.  See
    :meth:`~Connection.create_scalar_function` for an example.

  .. seealso::

     * :ref:`Example <example_aggregate>`
     * :meth:`~Connection.create_scalar_function`
     * :meth:`~Connection.create_window_function`

  -* sqlite3_create_function_v2
*/

static PyObject *
Connection_create_aggregate_function(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                                     PyObject *fast_kwnames)
{
  int numargs = -1;
  PyObject *factory;
  const char *name = 0;
  FunctionCBInfo *cbinfo;
  int res;
  int flags = 0;

  CHECK_CLOSED(self, NULL);

  {
    Connection_create_aggregate_function_CHECK;
    ARG_PROLOG(3, Connection_create_aggregate_function_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_MANDATORY ARG_optional_Callable(factory);
    ARG_OPTIONAL ARG_int(numargs);
    ARG_OPTIONAL ARG_int(flags);
    ARG_EPILOG(NULL, Connection_create_aggregate_function_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);

  if (!factory)
    cbinfo = 0;
  else
  {
    cbinfo = allocfunccbinfo(name);
    if (!cbinfo)
      goto finally;

    cbinfo->aggregatefactory = Py_NewRef(factory);
  }

  res = sqlite3_create_function_v2(self->db, name, numargs, SQLITE_UTF8 | flags, cbinfo, NULL,
                                   cbinfo ? cbdispatch_step : NULL, cbinfo ? cbdispatch_final : NULL, apsw_free_func);

  /* Note: On error sqlite3_create_function_v2 calls the
     destructor (apsw_free_func)! */
  SET_EXC(res, self->db);

finally:
  sqlite3_mutex_leave(self->dbmutex);
  if (PyErr_Occurred())
    return NULL;
  Py_RETURN_NONE;
}

/* USER DEFINED COLLATION CODE.*/

static int
collation_cb(void *context, int stringonelen, const void *stringonedata, int stringtwolen, const void *stringtwodata)
{
  PyGILState_STATE gilstate;
  PyObject *cbinfo = (PyObject *)context;
  PyObject *pys1 = NULL, *pys2 = NULL, *retval = NULL;
  int result = 0;

  assert(cbinfo);

  gilstate = PyGILState_Ensure();

  MakeExistingException();

  if (PyErr_Occurred())
    goto finally; /* outstanding error */

  pys1 = PyUnicode_FromStringAndSize(stringonedata, stringonelen);
  pys2 = PyUnicode_FromStringAndSize(stringtwodata, stringtwolen);

  if (!pys1 || !pys2)
    goto finally; /* failed to allocate strings */

  PyObject *vargs[] = { NULL, pys1, pys2 };
  retval = PyObject_Vectorcall(cbinfo, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);

  if (!retval)
  {
    AddTraceBackHere(__FILE__, __LINE__, "Collation_callback", "{s: O, s: O, s: O}", "callback", OBJ(cbinfo),
                     "stringone", OBJ(pys1), "stringtwo", OBJ(pys2));
    goto finally; /* execution failed */
  }

  if (PyLong_Check(retval))
  {
    result = PyLong_AsInt(retval);
    goto haveval;
  }

  PyErr_Format(PyExc_TypeError, "Collation callback must return a number not %s", Py_TypeName(retval));
  AddTraceBackHere(__FILE__, __LINE__, "collation callback", "{s: O, s: O}", "stringone", OBJ(pys1), "stringtwo",
                   OBJ(pys2));

haveval:
  if (PyErr_Occurred())
    result = 0;

finally:
  Py_XDECREF(pys1);
  Py_XDECREF(pys2);
  Py_XDECREF(retval);
  PyGILState_Release(gilstate);
  return result;
}

static void
collation_destroy(void *context)
{
  PyGILState_STATE gilstate = PyGILState_Ensure();
  Py_DECREF((PyObject *)context);
  PyGILState_Release(gilstate);
}

/** .. method:: create_collation(name: str, callback: Optional[Callable[[str, str], int]]) -> None

  You can control how SQLite sorts (termed `collation
  <https://en.wikipedia.org/wiki/Collation>`_) when giving the
  ``COLLATE`` term to a `SELECT
  <https://sqlite.org/lang_select.html>`_.  For example your
  collation could take into account locale or do numeric sorting.

  The *callback* will be called with two items.  It should return -1
  if the first is less then the second, 0 if they are equal, and 1 if
  first is greater::

     def mycollation(first: str, two: str) -> int:
         if first < second:
             return -1
         if first == second:
             return 0
         if first > second:
             return 1

  Passing None as the callback will unregister the collation.

  .. seealso::

    * :ref:`Example <example_collation>`
    * :meth:`Connection.collation_needed`

  -* sqlite3_create_collation_v2
*/

static PyObject *
Connection_create_collation(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *callback = NULL;
  const char *name = 0;
  int res;

  CHECK_CLOSED(self, NULL);

  {
    Connection_create_collation_CHECK;
    ARG_PROLOG(2, Connection_create_collation_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_MANDATORY ARG_optional_Callable(callback);
    ARG_EPILOG(NULL, Connection_create_collation_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  res = sqlite3_create_collation_v2(self->db, name, SQLITE_UTF8, callback ? callback : NULL,
                                    callback ? collation_cb : NULL, callback ? collation_destroy : NULL);

  SET_EXC(res, self->db);
  sqlite3_mutex_leave(self->dbmutex);

  if (PyErr_Occurred())
    return NULL;

  Py_XINCREF(callback);

  Py_RETURN_NONE;
}

/** .. method:: file_control(dbname: str, op: int, pointer: int) -> bool

  Calls the :meth:`~VFSFile.xFileControl` method on the :ref:`VFS`
  implementing :class:`file access <VFSFile>` for the database.

  :param dbname: The name of the database to affect.  `main`, `temp`, the name in `ATTACH <https://sqlite.org/lang_attach.html>`__
  :param op: A `numeric code
    <https://sqlite.org/c3ref/c_fcntl_lockstate.html>`_ with values less
    than 100 reserved for SQLite internal use.
  :param pointer: A number which is treated as a ``void pointer`` at the C level.

  :returns: True or False indicating if the VFS understood the op.

  The :ref:`example <example_filecontrol>` shows getting
  `SQLITE_FCNTL_DATA_VERSION
  <https://sqlite.org/c3ref/c_fcntl_begin_atomic_write.html#sqlitefcntldataversion>`__.

  If you want data returned back then the *pointer* needs to point to
  something mutable.  Here is an example using :mod:`ctypes` of
  passing a Python dictionary to :meth:`~VFSFile.xFileControl` which
  can then modify the dictionary to set return values::

    obj={"foo": 1, 2: 3}                 # object we want to pass
    objwrap=ctypes.py_object(obj)        # objwrap must live before and after the call else
                                         # it gets garbage collected
    connection.file_control(
             "main",                     # which db
             123,                        # our op code
             ctypes.addressof(objwrap))  # get pointer

  The :meth:`~VFSFile.xFileControl` method then looks like this::

    def xFileControl(self, op, pointer):
        if op==123:                      # our op code
            obj=ctypes.py_object.from_address(pointer).value
            # play with obj - you can use id() to verify it is the same
            print(obj["foo"])
            obj["result"]="it worked"
            return True
        else:
            # pass to parent/superclass
            return super().xFileControl(op, pointer)

  This is how you set the chunk size by which the database grows.  Do
  not combine it into one line as the c_int would be garbage collected
  before the file control call is made::

     chunksize=ctypes.c_int(32768)
     connection.file_control("main", apsw.SQLITE_FCNTL_CHUNK_SIZE, ctypes.addressof(chunksize))

  -* sqlite3_file_control
*/

static PyObject *
Connection_file_control(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  void *pointer;
  int res = SQLITE_ERROR, op;
  const char *dbname = NULL;

  CHECK_CLOSED(self, NULL);

  {
    Connection_file_control_CHECK;
    ARG_PROLOG(3, Connection_file_control_KWNAMES);
    ARG_MANDATORY ARG_str(dbname);
    ARG_MANDATORY ARG_int(op);
    ARG_MANDATORY ARG_pointer(pointer);
    ARG_EPILOG(NULL, Connection_file_control_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  res = sqlite3_file_control(self->db, dbname, op, pointer);

  if (res != SQLITE_OK && res != SQLITE_NOTFOUND)
    SET_EXC(res, NULL);
  sqlite3_mutex_leave(self->dbmutex);

  if (PyErr_Occurred())
    return NULL;

  if (res == SQLITE_NOTFOUND)
    Py_RETURN_FALSE;
  Py_RETURN_TRUE;
}

/** .. method:: vfsname(dbname: str) -> str | None

Issues the `SQLITE_FCNTL_VFSNAME
<https://sqlite.org/c3ref/c_fcntl_begin_atomic_write.html#sqlitefcntlvfsname>`__
file control against the named database (`main`, `temp`, attached
name).

This is useful to see which VFS is in use, and if inheritance is used
then ``/`` will separate the names.  If you have a :class:`VFSFile` in
use then its fully qualified class name will also be included.

If ``SQLITE_FCNTL_VFSNAME`` is not implemented, ``dbname`` is not a
database name, or an error occurred then ``None`` is returned.
*/
static PyObject *
Connection_vfsname(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{

  const char *dbname = NULL;

  CHECK_CLOSED(self, NULL);

  {
    Connection_vfsname_CHECK;
    ARG_PROLOG(1, Connection_vfsname_KWNAMES);
    ARG_MANDATORY ARG_str(dbname);
    ARG_EPILOG(NULL, Connection_vfsname_USAGE, );
  }

  const char *vfsname = NULL;

  /* because it is diagnostic and we can tell from vfsname changing and
  because SQLite shell itself ignores the return code, we do the same */
  DBMUTEX_ENSURE(self->dbmutex);
  sqlite3_file_control(self->db, dbname, SQLITE_FCNTL_VFSNAME, &vfsname);
  sqlite3_mutex_leave(self->dbmutex);

  PyObject *res = convertutf8string(vfsname);

  if (vfsname)
    sqlite3_free((void *)vfsname);

  return res;
}

/** .. method:: sqlite3_pointer() -> int

Returns the underlying `sqlite3 *
<https://sqlite.org/c3ref/sqlite3.html>`_ for the connection. This
method is useful if there are other C level libraries in the same
process and you want them to use the APSW connection handle. The value
is returned as a number using `PyLong_FromVoidPtr
<https://docs.python.org/3/c-api/long.html?highlight=pylong_fromvoidptr#c.PyLong_FromVoidPtr>`__
under the hood. You should also ensure that you increment the
reference count on the :class:`Connection` for as long as the other
libraries are using the pointer.  It is also a very good idea to call
:meth:`sqlite_lib_version` and ensure it is the same as the other
libraries.

*/
static PyObject *
Connection_sqlite3_pointer(Connection *self)
{

  CHECK_CLOSED(self, NULL);

  return PyLong_FromVoidPtr(self->db);
}

/** .. method:: wal_autocheckpoint(n: int) -> None

    Sets how often the :ref:`wal` checkpointing is run.

    :param n: A number representing the checkpointing interval or
      zero/negative to disable auto checkpointing.

   -* sqlite3_wal_autocheckpoint
*/
static PyObject *
Connection_wal_autocheckpoint(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                              PyObject *fast_kwnames)
{
  int n, res;

  CHECK_CLOSED(self, NULL);

  {
    Connection_wal_autocheckpoint_CHECK;
    ARG_PROLOG(1, Connection_wal_autocheckpoint_KWNAMES);
    ARG_MANDATORY ARG_int(n);
    ARG_EPILOG(NULL, Connection_wal_autocheckpoint_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  res = sqlite3_wal_autocheckpoint(self->db, n);
  SET_EXC(res, self->db);
  sqlite3_mutex_leave(self->dbmutex);

  if (PyErr_Occurred())
    return NULL;

  Py_RETURN_NONE;
}

/** .. method:: wal_checkpoint(dbname: Optional[str] = None, mode: int = apsw.SQLITE_CHECKPOINT_PASSIVE) -> tuple[int, int]

    Does a WAL checkpoint.  Has no effect if the database(s) are not in WAL mode.

    :param dbname:  The name of the database or all databases if None

    :param mode: One of the `checkpoint modes <https://sqlite.org/c3ref/wal_checkpoint_v2.html>`__.

    :return: A tuple of the size of the WAL log in frames and the
       number of frames checkpointed as described in the
       `documentation
       <https://sqlite.org/c3ref/wal_checkpoint_v2.html>`__.

  -* sqlite3_wal_checkpoint_v2
*/
static PyObject *
Connection_wal_checkpoint(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int res;
  const char *dbname = NULL;
  int mode = SQLITE_CHECKPOINT_PASSIVE;
  int nLog = 0, nCkpt = 0;

  CHECK_CLOSED(self, NULL);

  {
    Connection_wal_checkpoint_CHECK;
    ARG_PROLOG(2, Connection_wal_checkpoint_KWNAMES);
    ARG_OPTIONAL ARG_optional_str(dbname);
    ARG_OPTIONAL ARG_int(mode);
    ARG_EPILOG(NULL, Connection_wal_checkpoint_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  res = sqlite3_wal_checkpoint_v2(self->db, dbname, mode, &nLog, &nCkpt);
  SET_EXC(res, self->db);
  sqlite3_mutex_leave(self->dbmutex);

  if (!PyErr_Occurred())
    return Py_BuildValue("ii", nLog, nCkpt);
  return NULL;
}

static void apswvtabFree(void *context);
static struct sqlite3_module *apswvtabSetupModuleDef(PyObject *datasource, int iVersion, int eponymous,
                                                     int eponymous_only, int read_only);

/** .. method:: create_module(name: str, datasource: Optional[VTModule], *, use_bestindex_object: bool = False, use_no_change: bool = False, iVersion: int = 1, eponymous: bool=False, eponymous_only: bool = False, read_only: bool = False) -> None

    Registers a virtual table, or drops it if *datasource* is *None*.
    See :ref:`virtualtables` for details.

    :param name: Module name (CREATE VIRTUAL TABLE table_name USING module_name...)
    :param datasource: Provides :class:`VTModule` methods
    :param use_bestindex_object: If True then BestIndexObject is used, else BestIndex
    :param use_no_change: Turn on understanding :meth:`VTCursor.ColumnNoChange` and using :attr:`apsw.no_change` to reduce :meth:`VTTable.UpdateChangeRow` work
    :param iVersion: iVersion field in `sqlite3_module <https://www.sqlite.org/c3ref/module.html>`__
    :param eponymous: Configures module to be `eponymous <https://www.sqlite.org/vtab.html#eponymous_virtual_tables>`__
    :param eponymous_only: Configures module to be `eponymous only <https://www.sqlite.org/vtab.html#eponymous_only_virtual_tables>`__
    :param read_only: Leaves `sqlite3_module <https://www.sqlite.org/c3ref/module.html>`__ methods that involve writing and transactions as NULL

    .. seealso::

       * :ref:`Example <example_virtual_tables>`

    -* sqlite3_create_module_v2
*/
static PyObject *
Connection_create_module(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *name = NULL;
  PyObject *datasource = NULL;
  vtableinfo *vti = NULL;
  int res;
  int use_bestindex_object = 0, use_no_change = 0;

  int iVersion = 1, eponymous = 0, eponymous_only = 0, read_only = 0;

  CHECK_CLOSED(self, NULL);

  {
    Connection_create_module_CHECK;
    ARG_PROLOG(2, Connection_create_module_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_MANDATORY ARG_pyobject(datasource);
    ARG_OPTIONAL ARG_bool(use_bestindex_object);
    ARG_OPTIONAL ARG_bool(use_no_change);
    ARG_OPTIONAL ARG_int(iVersion);
    ARG_OPTIONAL ARG_bool(eponymous);
    ARG_OPTIONAL ARG_bool(eponymous_only);
    ARG_OPTIONAL ARG_bool(read_only);
    ARG_EPILOG(NULL, Connection_create_module_USAGE, );
  }

  if (!Py_IsNone(datasource))
  {
    Py_INCREF(datasource);
    vti = PyMem_Calloc(1, sizeof(vtableinfo));
    if (!vti)
      goto error;
    vti->sqlite3_module_def = apswvtabSetupModuleDef(datasource, iVersion, eponymous, eponymous_only, read_only);
    if (!vti->sqlite3_module_def)
      goto error;
    vti->connection = self;
    vti->datasource = datasource;
    vti->bestindex_object = use_bestindex_object;
    vti->use_no_change = use_no_change;
  }

  /* SQLite is really finnicky.  Note that it calls the destructor on
     failure  */

  DBMUTEX_ENSURE(self->dbmutex);
  res = sqlite3_create_module_v2(self->db, name, vti ? vti->sqlite3_module_def : NULL, vti, apswvtabFree);
  SET_EXC(res, self->db);
  sqlite3_mutex_leave(self->dbmutex);

  if (PyErr_Occurred())
  {
  error:
    if (vti)
      apswvtabFree(vti);
    return NULL;
  }

  Py_RETURN_NONE;
}

/** .. method:: vtab_config(op: int, val: int = 0) -> None

 Callable during virtual table :meth:`~VTModule.Connect`/:meth:`~VTModule.Create`.

 -* sqlite3_vtab_config

*/
static PyObject *
Connection_vtab_config(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int op, val = 0, res;

  CHECK_CLOSED(self, NULL);

  {
    Connection_vtab_config_CHECK;
    ARG_PROLOG(2, Connection_vtab_config_KWNAMES);
    ARG_MANDATORY ARG_int(op);
    ARG_OPTIONAL ARG_int(val);
    ARG_EPILOG(NULL, Connection_vtab_config_USAGE, );
  }

  if (!CALL_CHECK(xConnect))
    return PyErr_Format(ExcInvalidContext,
                        "You can only call vtab_config while in a virtual table Create/Connect call");

  switch (op)
  {
  case SQLITE_VTAB_CONSTRAINT_SUPPORT:
  case SQLITE_VTAB_INNOCUOUS:
  case SQLITE_VTAB_DIRECTONLY:
    res = sqlite3_vtab_config(self->db, op, val);
    break;
  default:
    return PyErr_Format(PyExc_ValueError, "Unknown sqlite3_vtab_config op %d", op);
  }

  SET_EXC(res, self->db);
  if (PyErr_Occurred())
    return NULL;
  Py_RETURN_NONE;
}

/** .. method:: vtab_on_conflict() -> int

 Callable during virtual table :meth:`insert <VTTable.UpdateInsertRow>` or
 :meth:`update <VTTable.UpdateChangeRow>`

 -* sqlite3_vtab_on_conflict

*/
static PyObject *
Connection_vtab_on_conflict(Connection *self)
{

  CHECK_CLOSED(self, NULL);

  if (!CALL_CHECK(xUpdate))
    return PyErr_Format(ExcInvalidContext, "You can only call vtab_on_conflict while in a virtual table Update call");

  return PyLong_FromLong(sqlite3_vtab_on_conflict(self->db));
}

/** .. method:: overload_function(name: str, nargs: int) -> None

  Registers a placeholder function so that a virtual table can provide an implementation via
  :meth:`VTTable.FindFunction`.

  :param name: Function name
  :param nargs: How many arguments the function takes

    -* sqlite3_overload_function
*/
static PyObject *
Connection_overload_function(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                             PyObject *fast_kwnames)
{
  const char *name;
  int nargs, res;

  CHECK_CLOSED(self, NULL);
  {
    Connection_overload_function_CHECK;
    ARG_PROLOG(2, Connection_overload_function_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_MANDATORY ARG_int(nargs);
    ARG_EPILOG(NULL, Connection_overload_function_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  res = sqlite3_overload_function(self->db, name, nargs);
  SET_EXC(res, self->db);
  sqlite3_mutex_leave(self->dbmutex);

  if (PyErr_Occurred())
    return NULL;

  Py_RETURN_NONE;
}

/** .. method:: set_exec_trace(callable: Optional[ExecTracer]) -> None

   Method to set :attr:`Connection.exec_trace`
*/
static PyObject *
Connection_set_exec_trace(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *callable;

  CHECK_CLOSED(self, NULL);

  {
    Connection_set_exec_trace_CHECK;
    ARG_PROLOG(1, Connection_set_exec_trace_KWNAMES);
    ARG_MANDATORY ARG_optional_Callable(callable);
    ARG_EPILOG(NULL, Connection_set_exec_trace_USAGE, );
  }

  Py_XINCREF(callable);
  Py_XDECREF(self->exectrace);
  self->exectrace = callable;

  Py_RETURN_NONE;
}

/** .. method:: set_row_trace(callable: Optional[RowTracer]) -> None

  Method to set :attr:`Connection.row_trace`
*/

static PyObject *
Connection_set_row_trace(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *callable;

  CHECK_CLOSED(self, NULL);

  {
    Connection_set_row_trace_CHECK;
    ARG_PROLOG(1, Connection_set_row_trace_KWNAMES);
    ARG_MANDATORY ARG_optional_Callable(callable);
    ARG_EPILOG(NULL, Connection_set_row_trace_USAGE, );
  }

  Py_XINCREF(callable);
  Py_XDECREF(self->rowtrace);
  self->rowtrace = callable;

  Py_RETURN_NONE;
}

/** .. method:: get_exec_trace() -> Optional[ExecTracer]

  Returns the currently installed :attr:`execution tracer
  <Connection.exec_trace>`

*/
static PyObject *
Connection_get_exec_trace(Connection *self)
{
  PyObject *ret;

  CHECK_CLOSED(self, NULL);

  ret = (self->exectrace) ? (self->exectrace) : Py_None;
  return Py_NewRef(ret);
}

/** .. method:: get_row_trace() -> Optional[RowTracer]

  Returns the currently installed :attr:`row tracer
  <Connection.row_trace>`

*/
static PyObject *
Connection_get_row_trace(Connection *self)
{
  PyObject *ret;

  CHECK_CLOSED(self, NULL);

  ret = (self->rowtrace) ? (self->rowtrace) : Py_None;
  return Py_NewRef(ret);
}

/** .. method:: __enter__() -> Connection

  You can use the database as a `context manager
  <https://docs.python.org/3/reference/datamodel.html#with-statement-context-managers>`_
  as defined in :pep:`0343`.  When you use *with* a transaction is
  started.  If the block finishes with an exception then the
  transaction is rolled back, otherwise it is committed.  For example::

    with connection:
        connection.execute("....")
        with connection:
            # nested is supported
            call_function(connection)
            connection.execute("...")
            with connection as db:
                # You can also use 'as'
                call_function2(db)
                db.execute("...")

  Behind the scenes `savepoints <https://sqlite.org/lang_savepoint.html>`__
   are used to provide nested transactions.
*/
static PyObject *
Connection_enter(Connection *self)
{
  char *sql = 0;
  int res;

  CHECK_CLOSED(self, NULL);

  DBMUTEX_ENSURE(self->dbmutex);

  sql = sqlite3_mprintf("SAVEPOINT \"_apsw-%ld\"", self->savepointlevel);
  if (!sql)
    return PyErr_NoMemory();

  /* exec tracing - we allow it to prevent */
  if (self->exectrace && !Py_IsNone(self->exectrace))
  {
    int result;
    PyObject *retval = NULL;
    PyObject *vargs[] = { NULL, (PyObject *)self, PyUnicode_FromString(sql), Py_None };
    if (vargs[2])
      retval = PyObject_Vectorcall(self->exectrace, vargs + 1, 3 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
    Py_XDECREF(vargs[2]);
    if (!retval)
      goto error;
    result = PyObject_IsTrueStrict(retval);
    Py_DECREF(retval);
    if (result == -1)
    {
      assert(PyErr_Occurred());
      goto error;
    }
    if (result == 0)
    {
      PyErr_Format(ExcTraceAbort, "Aborted by false/null return value of exec tracer");
      goto error;
    }
    assert(result == 1);
  }

  res = sqlite3_exec(self->db, sql, 0, 0, 0);
  sqlite3_free(sql);
  SET_EXC(res, self->db);
  sqlite3_mutex_leave(self->dbmutex);

  /* sqlite3_trace_v2 callback could cause Python level error */
  if (res || PyErr_Occurred())
    return NULL;

  self->savepointlevel++;
  return Py_NewRef((PyObject *)self);

error:
  sqlite3_mutex_leave(self->dbmutex);
  assert(PyErr_Occurred());
  if (sql)
    sqlite3_free(sql);
  return NULL;
}

/** .. method:: __exit__(etype: Optional[type[BaseException]], evalue: Optional[BaseException], etraceback: Optional[types.TracebackType]) -> Optional[bool]

  Implements context manager in conjunction with
  :meth:`~Connection.__enter__`.  If no exception happened then
  the pending transaction is committed, while an exception results in a
  rollback.

*/

/* A helper function.  Returns -1 on memory error, 0 on failure and 1 on success */
#undef connection_trace_and_exec
static int
connection_trace_and_exec(Connection *self, int release, int sp, int continue_on_trace_error)
{
#include "faultinject.h"
  char *sql;
  int res;

  sql = sqlite3_mprintf(release ? "RELEASE SAVEPOINT \"_apsw-%ld\"" : "ROLLBACK TO SAVEPOINT \"_apsw-%ld\"", sp);
  if (!sql)
  {
    PyErr_NoMemory();
    return -1;
  }

  if (self->exectrace && !Py_IsNone(self->exectrace))
  {
    PyObject *result = NULL;

    CHAIN_EXC_BEGIN
    PyObject *vargs[] = { NULL, (PyObject *)self, PyUnicode_FromString(sql), Py_None };
    if (vargs[2])
    {
      result = PyObject_Vectorcall(self->exectrace, vargs + 1, 3 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
      Py_DECREF(vargs[2]);
    }
    Py_XDECREF(result);
    CHAIN_EXC_END;

    if (!result && !continue_on_trace_error)
    {
      sqlite3_free(sql);
      return 0;
    }
  }

  res = sqlite3_exec(self->db, sql, 0, 0, 0);
  SET_EXC(res, self->db);
  sqlite3_free(sql);

  /* See issue 526 for why we can't trust SQLite success code */
  return PyErr_Occurred() ? 0 : (res == SQLITE_OK);
}

static PyObject *
Connection_exit(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *etype, *evalue, *etraceback;
  long sp;
  int res;
  int return_null = 0;

  CHECK_CLOSED(self, NULL);

  /* the builtin python __exit__ implementations don't error if you
     call __exit__ without corresponding enters */
  if (self->savepointlevel == 0)
    Py_RETURN_FALSE;

  /* We always pop a level, irrespective of how this function returns
     - (ie successful or error) */
  if (self->savepointlevel)
    self->savepointlevel--;
  sp = self->savepointlevel;

  {
    Connection_exit_CHECK;
    ARG_PROLOG(3, Connection_exit_KWNAMES);
    ARG_MANDATORY ARG_pyobject(etype);
    ARG_MANDATORY ARG_pyobject(evalue);
    ARG_MANDATORY ARG_pyobject(etraceback);
    ARG_EPILOG(NULL, Connection_exit_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);

  /* try the commit first because it may fail in which case we'll need
     to roll it back - see issue 98 */
  if (Py_IsNone(etype) && Py_IsNone(evalue) && Py_IsNone(etraceback))
  {
    res = connection_trace_and_exec(self, 1, sp, 0);
    if (res == -1)
    {
      sqlite3_mutex_leave(self->dbmutex);
      return NULL;
    }
    if (res == 1)
    {
      sqlite3_mutex_leave(self->dbmutex);
      Py_RETURN_FALSE;
    }
    assert(res == 0);
    assert(PyErr_Occurred());
    return_null = 1;
  }

  res = connection_trace_and_exec(self, 0, sp, 1);
  if (res == -1)
  {
    sqlite3_mutex_leave(self->dbmutex);
    return NULL;
  }
  return_null = return_null || res == 0;
  /* we have rolled back, but still need to release the savepoint */
  res = connection_trace_and_exec(self, 1, sp, 1);
  sqlite3_mutex_leave(self->dbmutex);
  return_null = return_null || res == 0 || res == -1;

  if (return_null)
  {
    assert(PyErr_Occurred());
    return NULL;
  }
  assert(!PyErr_Occurred());
  Py_RETURN_FALSE;
}

/** .. method:: config(op: int, *args: int) -> int

    :param op: A `configuration operation
      <https://sqlite.org/c3ref/c_dbconfig_enable_fkey.html>`__
    :param args: Zero or more arguments as appropriate for *op*

    This is how to get the fkey setting::

      val = db.config(apsw.SQLITE_DBCONFIG_ENABLE_FKEY, -1)

    A parameter of zero would turn it off, 1 turns on, and negative
    leaves unaltered.  The effective value is always returned.

    -* sqlite3_db_config
*/
static PyObject *
Connection_config(Connection *self, PyObject *args)
{
  int opt;
  int res;

  CHECK_CLOSED(self, NULL);

  if (PyTuple_GET_SIZE(args) < 1 || !PyLong_Check(PyTuple_GET_ITEM(args, 0)))
    return PyErr_Format(PyExc_TypeError, "There should be at least one argument with the first being a number");

  opt = PyLong_AsInt(PyTuple_GET_ITEM(args, 0));
  if (PyErr_Occurred())
    return NULL;

  switch (opt)
  {
  case SQLITE_DBCONFIG_ENABLE_FKEY:
  case SQLITE_DBCONFIG_ENABLE_TRIGGER:
  case SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER:
  case SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION:
  case SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE:
  case SQLITE_DBCONFIG_ENABLE_QPSG:
  case SQLITE_DBCONFIG_RESET_DATABASE:
  case SQLITE_DBCONFIG_DEFENSIVE:
  case SQLITE_DBCONFIG_WRITABLE_SCHEMA:
  case SQLITE_DBCONFIG_LEGACY_ALTER_TABLE:
  case SQLITE_DBCONFIG_DQS_DML:
  case SQLITE_DBCONFIG_DQS_DDL:
  case SQLITE_DBCONFIG_ENABLE_VIEW:
  case SQLITE_DBCONFIG_TRIGGER_EQP:
  case SQLITE_DBCONFIG_LEGACY_FILE_FORMAT:
  case SQLITE_DBCONFIG_TRUSTED_SCHEMA:
  case SQLITE_DBCONFIG_REVERSE_SCANORDER:
  case SQLITE_DBCONFIG_ENABLE_ATTACH_CREATE:
  case SQLITE_DBCONFIG_ENABLE_ATTACH_WRITE:
  case SQLITE_DBCONFIG_ENABLE_COMMENTS: {
    int opdup, val, current;
    if (!PyArg_ParseTuple(args, "ii", &opdup, &val))
      return NULL;

    DBMUTEX_ENSURE(self->dbmutex);
    res = sqlite3_db_config(self->db, opdup, val, &current);
    SET_EXC(res, self->db);
    sqlite3_mutex_leave(self->dbmutex);

    if (PyErr_Occurred())
      return NULL;

    return PyLong_FromLong(current);
  }
  default:
    return PyErr_Format(PyExc_ValueError, "Unknown config operation %d", (int)opt);
  }
}

/** .. method:: status(op: int, reset: bool = False) -> tuple[int, int]

  Returns current and highwater measurements for the database.

  :param op: A `status parameter <https://sqlite.org/c3ref/c_dbstatus_options.html>`_
  :param reset: If *True* then the highwater is set to the current value
  :returns: A tuple of current value and highwater value

  .. seealso::

    * :func:`apsw.status` which does the same for SQLite as a whole
    * :ref:`Example <example_status>`

  -* sqlite3_db_status

*/
static PyObject *
Connection_status(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int res, op, current = 0, highwater = 0, reset = 0;

  CHECK_CLOSED(self, NULL);

  {
    Connection_status_CHECK;
    ARG_PROLOG(2, Connection_status_KWNAMES);
    ARG_MANDATORY ARG_int(op);
    ARG_OPTIONAL ARG_bool(reset);
    ARG_EPILOG(NULL, Connection_status_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  res = sqlite3_db_status(self->db, op, &current, &highwater, reset);
  SET_EXC(res, NULL);
  sqlite3_mutex_leave(self->dbmutex);

  if (PyErr_Occurred())
    return NULL;

  return Py_BuildValue("(ii)", current, highwater);
}

/** .. method:: readonly(name: str) -> bool

  True or False if the named (attached) database was opened readonly or file
  permissions don't allow writing.  The name is `main`, `temp`, the
  name in `ATTACH <https://sqlite.org/lang_attach.html>`__

  An exception is raised if the database doesn't exist.

  -* sqlite3_db_readonly

*/
static PyObject *
Connection_readonly(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int res = -1;
  const char *name;

  CHECK_CLOSED(self, NULL);
  {
    Connection_readonly_CHECK;
    ARG_PROLOG(1, Connection_readonly_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_EPILOG(NULL, Connection_readonly_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  res = sqlite3_db_readonly(self->db, name);
  sqlite3_mutex_leave(self->dbmutex);

  if (res == 1)
    Py_RETURN_TRUE;
  if (res == 0)
    Py_RETURN_FALSE;

  return PyErr_Format(exc_descriptors[0].cls, "Unknown database name \"%s\"", name);
}

/** .. method:: db_filename(name: str) -> str

  Returns the full filename of the named (attached) database.  The
  main is `main`, `temp`, the name in `ATTACH <https://sqlite.org/lang_attach.html>`__

  -* sqlite3_db_filename
*/
static PyObject *
Connection_db_filename(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *res;
  const char *name;
  PyObject *retval = NULL;
  CHECK_CLOSED(self, NULL);

  {
    Connection_db_filename_CHECK;
    ARG_PROLOG(1, Connection_db_filename_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_EPILOG(NULL, Connection_db_filename_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  res = sqlite3_db_filename(self->db, name);
  retval = convertutf8string(res);
  sqlite3_mutex_leave(self->dbmutex);

  return retval;
}

/** .. method:: txn_state(schema: Optional[str] = None) -> int

  Returns the current transaction state of the database, or a specific schema
  if provided.  :attr:`apsw.mapping_txn_state` contains the values returned.

  -* sqlite3_txn_state
*/

static PyObject *
Connection_txn_state(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *schema = NULL;
  int res;

  CHECK_CLOSED(self, NULL);

  {
    Connection_txn_state_CHECK;
    ARG_PROLOG(1, Connection_txn_state_KWNAMES);
    ARG_OPTIONAL ARG_optional_str(schema);
    ARG_EPILOG(NULL, Connection_txn_state_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  res = sqlite3_txn_state(self->db, schema);
  sqlite3_mutex_leave(self->dbmutex);

  if (res >= 0)
    return PyLong_FromLong(res);

  return PyErr_Format(PyExc_ValueError, "unknown schema %s", schema);
}

/** .. method:: execute(statements: str, bindings: Optional[Bindings] = None, *, can_cache: bool = True, prepare_flags: int = 0, explain: int = -1) -> Cursor

    Executes the statements using the supplied bindings.  Execution
    returns when the first row is available or all statements have
    completed.  (A cursor is automatically obtained).

    For pragmas you should use :meth:`pragma` which handles quoting and
    caching correctly.

    See :meth:`Cursor.execute` for more details, and the :ref:`example <example_executing_sql>`.
*/
static PyObject *
Connection_execute(Connection *self, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames)
{
  PyObject *cursor = NULL, *method = NULL, *res = NULL;

  CHECK_CLOSED(self, NULL);

  PyObject *vargs[] = { NULL, (PyObject *)self };
  cursor = PyObject_VectorcallMethod(apst.cursor, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  if (!cursor)
  {
    AddTraceBackHere(__FILE__, __LINE__, "Connection.execute", "{s: O}", "cursor_factory", OBJ(self->cursor_factory));
    goto fail;
  }
  method = PyObject_GetAttr(cursor, apst.execute);
  if (!method)
  {
    assert(PyErr_Occurred());
    AddTraceBackHere(__FILE__, __LINE__, "Connection.execute", "{s: O}", "cursor", OBJ(cursor));
    goto fail;
  }
  res = PyObject_Vectorcall(method, args, nargs, kwnames);

fail:
  Py_XDECREF(cursor);
  Py_XDECREF(method);
  return res;
}

/** .. method:: executemany(statements: str, sequenceofbindings:Iterable[Bindings], *, can_cache: bool = True, prepare_flags: int = 0, explain: int = -1) -> Cursor

This method is for when you want to execute the same statements over a
sequence of bindings, such as inserting into a database.  (A cursor is
automatically obtained).

See :meth:`Cursor.executemany` for more details, and the :ref:`example <example_executemany>`.
*/
static PyObject *
Connection_executemany(Connection *self, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames)
{
  PyObject *cursor = NULL, *method = NULL, *res = NULL;

  CHECK_CLOSED(self, NULL);

  PyObject *vargs[] = { NULL, (PyObject *)self };
  cursor = PyObject_VectorcallMethod(apst.cursor, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  if (!cursor)
  {
    AddTraceBackHere(__FILE__, __LINE__, "Connection.executemany", "{s: O}", "cursor_factory",
                     OBJ(self->cursor_factory));
    goto fail;
  }
  method = PyObject_GetAttr(cursor, apst.executemany);
  if (!method)
  {
    assert(PyErr_Occurred());
    AddTraceBackHere(__FILE__, __LINE__, "Connection.executemany ", "{s: O}", "cursor", OBJ(cursor));
    goto fail;
  }
  res = PyObject_Vectorcall(method, args, nargs, kwnames);

fail:
  Py_XDECREF(cursor);
  Py_XDECREF(method);
  return res;
}

static PyObject *formatsqlvalue(PyObject *Py_UNUSED(self), PyObject *value);
/** .. method:: pragma(name: str, value: Optional[SQLiteValue] = None, *, schema: Optional[str] = None) -> Any

  Issues the pragma (with the value if supplied) and returns the result with
  :attr:`the least amount of structure <Cursor.get>`.  For example
  :code:`pragma("user_version")` will return just the number, while
  :code:`pragma("journal_mode", "WAL")` will return the journal mode
  now in effect.

  Pragmas do not support bindings, so this method is a convenient
  alternative to composing SQL text.  Pragmas are often executed
  while being prepared, instead of when run like regular SQL.  They
  may also contain encryption keys.  This method ensures they are
  not cached to avoid problems.

  Use the `schema` parameter to run the pragma against a different
  attached database (eg ``temp``).

  * :ref:`Example <example_pragma>`
*/
static PyObject *
Connection_pragma(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *name = NULL;
  PyObject *value = NULL;
  const char *schema = NULL;

  CHECK_CLOSED(self, NULL);

  {
    Connection_pragma_CHECK;
    ARG_PROLOG(2, Connection_pragma_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_OPTIONAL ARG_pyobject(value);
    ARG_OPTIONAL ARG_optional_str(schema);
    ARG_EPILOG(NULL, Connection_pragma_USAGE, );
  }

  PyObject *value_format = NULL, *res = NULL, *cursor = NULL, *query_py = NULL;
  const char *value_str = NULL;
  char *query = NULL;

  if (value)
  {
    value_format = formatsqlvalue(NULL, value);
    if (!value_format)
      goto error;
    value_str = PyUnicode_AsUTF8(value_format);
    if (!value_str)
      goto error;
  }

  /* the form name(value) is used not name=value because some
     pragmas like index_info only work that way, and all
     support the parenthese method */

  query = sqlite3_mprintf("pragma %s%w%s%s\"%w\"%s%s%s",
                          /* surround schema with double quotes and follow with dot if set */
                          schema ? "\"" : "", schema ? schema : "", schema ? "\"" : "", schema ? "." : "",
                          /* pragma */
                          name,
                          /* value surrounded by parens if set */
                          value_str ? "(" : "", value_str ? value_str : "", value_str ? ")" : "");

  if (!query)
  {
    PyErr_NoMemory();
    goto error;
  }

  query_py = PyUnicode_FromString(query);
  if (!query_py)
    goto error;

  PyObject *vargs[] = { NULL, query_py, Py_False };
  PyObject *kwnames = PyTuple_Pack(1, apst.can_cache);
  if (kwnames)
    cursor = Connection_execute(self, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, kwnames);
  Py_XDECREF(kwnames);
  if (!cursor || !kwnames)
    goto error;

  res = PyObject_GetAttr(cursor, apst.get);

error:
  Py_XDECREF(value_format);
  Py_XDECREF(cursor);
  Py_XDECREF(query_py);
  sqlite3_free(query);

  assert((res && !PyErr_Occurred()) || (!res && PyErr_Occurred()));
  return res;
}

/** .. method:: cache_stats(include_entries: bool = False) -> dict[str, int]

Returns information about the statement cache as dict.

.. note::

  Calling execute with "select a; select b; insert into c ..." will
  result in 3 cache entries corresponding to each of the 3 queries
  present.

The returned dictionary has the following information.

.. list-table::
  :header-rows: 1
  :widths: auto

  * - Key
    - Explanation
  * - size
    - Maximum number of entries in the cache
  * - evictions
    - How many entries were removed (expired) to make space for a newer
      entry
  * - no_cache
    - Queries that had can_cache parameter set to False
  * - hits
    - A match was found in the cache
  * - misses
    - No match was found in the cache, or the cache couldn't be used
  * - no_vdbe
    - The statement was empty (eg a comment) or SQLite took action
      during parsing (eg some pragmas).  These are not cached and also
      included in the misses count
  * - too_big
    - UTF8 query size was larger than considered for caching.  These are also included
      in the misses count.
  * - max_cacheable_bytes
    - Maximum size of query (in bytes of utf8) that will be considered for caching
  * - entries
    - (Only present if `include_entries` is True) A list of the cache entries

If `entries` is present, then each list entry is a dict with the following information.

.. list-table::
  :header-rows: 1
  :widths: auto

  * - Key
    - Explanation
  * - query
    - Text of the query itself (first statement only)
  * - prepare_flags
    - Flags passed to `sqlite3_prepare_v3 <https://sqlite.org/c3ref/prepare.html>`__
      for this query
  * - explain
    - The value passed to `sqlite3_stmt_explain <https://sqlite.org/c3ref/stmt_explain.html>`__
      if >= 0
  * - uses
    - How many times this entry has been (re)used
  * - has_more
    - Boolean indicating if there was more query text than
      the first statement

*/
static PyObject *
Connection_cache_stats(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int include_entries = 0;

  CHECK_CLOSED(self, NULL);

  {
    Connection_cache_stats_CHECK;
    ARG_PROLOG(1, Connection_cache_stats_KWNAMES);
    ARG_OPTIONAL ARG_bool(include_entries);
    ARG_EPILOG(NULL, Connection_cache_stats_USAGE, );
  }
  return statementcache_stats(self->stmtcache, include_entries);
}

/** .. method:: table_exists(dbname: Optional[str], table_name: str) -> bool

  Returns True if the named table exists, else False.

  ``dbname`` is ``main``, ``temp``, the name in `ATTACH
  <https://sqlite.org/lang_attach.html>`__, or None to search  all
  databases

  -* sqlite3_table_column_metadata
*/
static PyObject *
Connection_table_exists(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *dbname = NULL, *table_name = NULL;
  int res;

  CHECK_CLOSED(self, NULL);

  {
    Connection_table_exists_CHECK;
    ARG_PROLOG(2, Connection_table_exists_KWNAMES);
    ARG_MANDATORY ARG_optional_str(dbname);
    ARG_MANDATORY ARG_str(table_name);
    ARG_EPILOG(NULL, Connection_table_exists_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  res = sqlite3_table_column_metadata(self->db, dbname, table_name, NULL, NULL, NULL, NULL, NULL, NULL);
  if (res != SQLITE_OK && res != SQLITE_ERROR)
    SET_EXC(res, self->db);
  sqlite3_mutex_leave(self->dbmutex);

  if (PyErr_Occurred())
    return NULL;

  if (res == SQLITE_OK)
    Py_RETURN_TRUE;
  assert(res == SQLITE_ERROR);
  Py_RETURN_FALSE;
}

/** .. method:: column_metadata(dbname: Optional[str], table_name: str, column_name: str) -> tuple[str, str, bool, bool, bool]

  `dbname` is `main`, `temp`, the name in `ATTACH <https://sqlite.org/lang_attach.html>`__, or None to search
  all databases.

  The returned :class:`tuple` has these fields:

  0: str - declared data type

  1: str - name of default collation sequence

  2: bool - True if not null constraint

  3: bool - True if part of primary key

  4: bool - True if column is `autoincrement <https://www.sqlite.org/autoinc.html>`__

  -* sqlite3_table_column_metadata
*/
static PyObject *
Connection_column_metadata(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *dbname = NULL, *table_name = NULL, *column_name = NULL;
  int res;

  const char *datatype = NULL, *collseq = NULL;
  int notnull = 0, primarykey = 0, autoinc = 0;

  CHECK_CLOSED(self, NULL);

  {
    Connection_column_metadata_CHECK;
    ARG_PROLOG(3, Connection_column_metadata_KWNAMES);
    ARG_MANDATORY ARG_optional_str(dbname);
    ARG_MANDATORY ARG_str(table_name);
    ARG_MANDATORY ARG_str(column_name);
    ARG_EPILOG(NULL, Connection_column_metadata_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  res = sqlite3_table_column_metadata(self->db, dbname, table_name, column_name, &datatype, &collseq, &notnull,
                                      &primarykey, &autoinc);

  SET_EXC(res, self->db);
  sqlite3_mutex_leave(self->dbmutex);

  return PyErr_Occurred() ? NULL
                          : Py_BuildValue("(ssOOO)", datatype, collseq, notnull ? Py_True : Py_False,
                                          primarykey ? Py_True : Py_False, autoinc ? Py_True : Py_False);
}

/** .. method:: cache_flush() -> None

  Flushes caches to disk mid-transaction.

  -* sqlite3_db_cacheflush
*/
static PyObject *
Connection_cache_flush(Connection *self)
{
  int res;

  CHECK_CLOSED(self, NULL);

  DBMUTEX_ENSURE(self->dbmutex);
  res = sqlite3_db_cacheflush(self->db);
  SET_EXC(res, self->db);
  sqlite3_mutex_leave(self->dbmutex);

  if (PyErr_Occurred())
    return NULL;

  Py_RETURN_NONE;
}

/** .. method:: release_memory() -> None

  Attempts to free as much heap memory as possible used by this connection.

  -* sqlite3_db_release_memory
*/
static PyObject *
Connection_release_memory(Connection *self)
{
  int res;

  CHECK_CLOSED(self, NULL);

  DBMUTEX_ENSURE(self->dbmutex);
  res = sqlite3_db_release_memory(self->db);
  SET_EXC(res, self->db);
  sqlite3_mutex_leave(self->dbmutex);

  if (PyErr_Occurred())
    return NULL;

  Py_RETURN_NONE;
}

/** .. method:: drop_modules(keep: Optional[Iterable[str]]) -> None

  If *keep* is *None* then all registered virtual tables are dropped.

  Otherwise *keep* is a sequence of strings, naming the virtual tables that
  are kept, dropping all others.
*/
static PyObject *
Connection_drop_modules(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int res;
  PyObject *keep = NULL, *sequence = NULL;
  char *strings = NULL, *stringstmp;
  size_t strings_size = 0;
  const char **array = NULL;
  Py_ssize_t nitems = 0, i;

  CHECK_CLOSED(self, NULL);

  {
    Connection_drop_modules_CHECK;
    ARG_PROLOG(1, Connection_drop_modules_KWNAMES);
    ARG_MANDATORY ARG_pyobject(keep);
    ARG_EPILOG(NULL, Connection_drop_modules_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);

  if (keep != Py_None)
  {
    sequence = PySequence_Fast(keep, "expected a sequence for " Connection_drop_modules_USAGE);
    if (!sequence)
      goto finally;
    nitems = PySequence_Size(sequence);
    if (nitems < 0)
      goto finally;
    array = PyMem_Calloc(nitems + 1, sizeof(char *));
    if (!array)
      goto finally;
    for (i = 0; i < nitems; i++)
    {
      const char *sc;
      size_t slen;
      PyObject *s = PySequence_Fast_GET_ITEM(sequence, i);
      assert(s);
      if (!PyUnicode_Check(s))
      {
        PyErr_Format(PyExc_TypeError, "Expected sequence item #%zd to be str, not %s", i, Py_TypeName(s));
        goto finally;
      }
      sc = PyUnicode_AsUTF8(s);
      if (!sc)
        goto finally;
      slen = strlen(sc);
      stringstmp = PyMem_Realloc(strings, strings_size + slen + 1);
      if (!stringstmp)
        goto finally;
      strings = stringstmp;
      strncpy(strings + strings_size, sc, slen + 1);
      strings_size += slen + 1;
    }
    /* fill in array pointer to each string */
    stringstmp = strings;
    for (i = 0; i < nitems; i++)
    {
      array[i] = stringstmp;
      stringstmp += strlen(stringstmp) + 1;
    }
  }

  res = sqlite3_drop_modules(self->db, array);
  SET_EXC(res, self->db);

finally:
  sqlite3_mutex_leave(self->dbmutex);
  Py_CLEAR(sequence);
  PyMem_Free(strings);
  PyMem_Free((void *)array);
  if (PyErr_Occurred())
    return NULL;

  Py_RETURN_NONE;
}

/** .. method:: read(schema: str, which: int, offset: int, amount: int) -> tuple[bool, bytes]

  Invokes the underlying VFS method to read data from the database.  It
  is strongly recommended to read aligned complete pages, since that is
  only what SQLite does.

  `schema` is `main`, `temp`, the name in `ATTACH <https://sqlite.org/lang_attach.html>`__

  `which` is 0 for the database file, 1 for the journal.

  The return value is a tuple of a boolean indicating a complete read if
  True, and the bytes read which will always be the amount requested
  in size.

  `SQLITE_IOERR_SHORT_READ` will give a `False` value for the boolean,
  and there is no way of knowing how much was read.

  Implemented using `SQLITE_FCNTL_FILE_POINTER` and `SQLITE_FCNTL_JOURNAL_POINTER`.
  Errors will usually be generic `SQLITE_ERROR` with no message.

  -* sqlite3_file_control
*/
static PyObject *
Connection_read(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *schema = NULL;
  int amount, which, opcode;
  sqlite3_int64 offset;
  int res;
  sqlite3_file *fp = NULL;
  PyObject *bytes = NULL;

  CHECK_CLOSED(self, NULL);

  {
    Connection_read_CHECK;
    ARG_PROLOG(4, Connection_read_KWNAMES);
    ARG_MANDATORY ARG_str(schema);
    ARG_MANDATORY ARG_int(which);
    ARG_MANDATORY ARG_int64(offset);
    ARG_MANDATORY ARG_int(amount);
    ARG_EPILOG(NULL, Connection_read_USAGE, );
  }

  switch (which)
  {
  case 0:
    opcode = SQLITE_FCNTL_FILE_POINTER;
    break;
  case 1:
    opcode = SQLITE_FCNTL_JOURNAL_POINTER;
    break;
  default:
    return PyErr_Format(PyExc_ValueError, "Unexpected value for which %d", which);
  }

  if (amount < 1)
    return PyErr_Format(PyExc_ValueError, "amount needs to be greater than zero, not %d", amount);

  if (offset < 0)
    return PyErr_Format(PyExc_ValueError, "offset needs to non-negative, not %lld", offset);

  bytes = PyBytes_FromStringAndSize(NULL, amount);
  if (!bytes)
    return NULL;

  DBMUTEX_ENSURE(self->dbmutex);
  res = sqlite3_file_control(self->db, schema, opcode, &fp);
  if (res != SQLITE_OK || !fp || !fp->pMethods || !fp->pMethods->xRead)
  {
    if (res == SQLITE_OK)
      res = SQLITE_ERROR;
  }
  if (res == SQLITE_OK)
  {
    res = fp->pMethods->xRead(fp, PyBytes_AS_STRING(bytes), amount, offset);
    APSW_FAULT(ConnectionReadError, , res = SQLITE_IOERR_CORRUPTFS);
  }
  if (res != SQLITE_OK && res != SQLITE_IOERR_SHORT_READ)
    SET_EXC(res, NULL);

  sqlite3_mutex_leave(self->dbmutex);

  PyObject *retval = NULL;

  if (!PyErr_Occurred())
    retval = Py_BuildValue("ON", (res == SQLITE_OK) ? Py_True : Py_False, bytes);

  if (retval)
    return retval;

  Py_DECREF(bytes);

  return NULL;
}

/** .. attribute:: filename
  :type: str

  The filename of the database.

  -* sqlite3_db_filename
*/

static PyObject *
Connection_getmainfilename(Connection *self)
{

  CHECK_CLOSED(self, NULL);
  DBMUTEX_ENSURE(self->dbmutex);
  PyObject *res = convertutf8string(sqlite3_db_filename(self->db, "main"));
  sqlite3_mutex_leave(self->dbmutex);
  return res;
}

/** .. attribute:: filename_journal
  :type: str

  The journal filename of the database,

  -* sqlite3_filename_journal
*/
static PyObject *
Connection_getjournalfilename(Connection *self)
{

  CHECK_CLOSED(self, NULL);
  DBMUTEX_ENSURE(self->dbmutex);
  PyObject *res = convertutf8string(sqlite3_filename_journal(sqlite3_db_filename(self->db, "main")));
  sqlite3_mutex_leave(self->dbmutex);
  return res;
}

/** .. attribute:: filename_wal
  :type: str

  The WAL filename of the database,

  -* sqlite3_filename_wal
*/
static PyObject *
Connection_getwalfilename(Connection *self)
{

  CHECK_CLOSED(self, NULL);
  DBMUTEX_ENSURE(self->dbmutex);
  PyObject *res = convertutf8string(sqlite3_filename_wal(sqlite3_db_filename(self->db, "main")));
  sqlite3_mutex_leave(self->dbmutex);
  return res;
}

/** .. attribute:: cursor_factory
  :type: Callable[[Connection], Any]

  Defaults to :class:`Cursor`

  Called with a :class:`Connection` as the only parameter when a cursor
  is needed such as by the :meth:`cursor` method, or
  :meth:`Connection.execute`.

  Note that whatever is returned doesn't have to be an actual
  :class:`Cursor` instance, and just needs to have the methods present
  that are actually called.  These are likely to be `execute`,
  `executemany`, `close` etc.
*/

static PyObject *
Connection_get_cursor_factory(Connection *self)
{
  /* The cursor factory will be NULL if the Connection has been closed.
     That also helps with garbage collection and reference cycles.  In
     that case we return None */
  if (!self->cursor_factory)
    Py_RETURN_NONE;
  return Py_NewRef(self->cursor_factory);
}

static int
Connection_set_cursor_factory(Connection *self, PyObject *value)
{
  if (!PyCallable_Check(value))
  {
    PyErr_Format(PyExc_TypeError, "cursor_factory expected a Callable not %s", Py_TypeName(value));
    return -1;
  }
  Py_CLEAR(self->cursor_factory);
  self->cursor_factory = Py_NewRef(value);
  return 0;
}

/** .. attribute:: in_transaction
  :type: bool

  True if currently in a transaction, else False

  -* sqlite3_get_autocommit
*/
static PyObject *
Connection_get_in_transaction(Connection *self)
{

  CHECK_CLOSED(self, NULL);
  if (!sqlite3_get_autocommit(self->db))
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

/** .. attribute:: exec_trace
  :type: Optional[ExecTracer]

  Called with the cursor, statement and bindings for
  each :meth:`~Cursor.execute` or :meth:`~Cursor.executemany` on this
  Connection, unless the :class:`Cursor` installed its own
  tracer. Your execution tracer can also abort execution of a
  statement.

  If *callable* is *None* then any existing execution tracer is
  removed.

  .. seealso::

    * :ref:`tracing`
    * :ref:`rowtracer`
    * :attr:`Cursor.exec_trace`

*/
static PyObject *
Connection_get_exec_trace_attr(Connection *self)
{

  CHECK_CLOSED(self, NULL);

  return Py_NewRef(self->exectrace ? self->exectrace : Py_None);
}

static int
Connection_set_exec_trace_attr(Connection *self, PyObject *value)
{
  CHECK_CLOSED(self, -1);

  if (!Py_IsNone(value) && !PyCallable_Check(value))
  {
    PyErr_Format(PyExc_TypeError, "exec_trace expected a Callable not %s", Py_TypeName(value));
    return -1;
  }
  Py_CLEAR(self->exectrace);
  if (value != Py_None)
    self->exectrace = Py_NewRef(value);
  return 0;
}

/** .. attribute:: row_trace
  :type: Optional[RowTracer]

  Called with the cursor and row being returned for
  :class:`cursors <Cursor>` associated with this Connection, unless
  the Cursor installed its own tracer.  You can change the data that
  is returned or cause the row to be skipped altogether.

  If *callable* is *None* then any existing row tracer is
  removed.

  .. seealso::

    * :ref:`tracing`
    * :ref:`rowtracer`
    * :attr:`Cursor.exec_trace`

*/
static PyObject *
Connection_get_row_trace_attr(Connection *self)
{

  CHECK_CLOSED(self, NULL);

  if (self->rowtrace)
    return Py_NewRef(self->rowtrace);
  Py_RETURN_NONE;
}

static int
Connection_set_row_trace_attr(Connection *self, PyObject *value)
{
  CHECK_CLOSED(self, -1);

  if (!Py_IsNone(value) && !PyCallable_Check(value))
  {
    PyErr_Format(PyExc_TypeError, "row trace expected a Callable not %s", Py_TypeName(value));
    return -1;
  }
  Py_CLEAR(self->rowtrace);
  if (!Py_IsNone(value))
    self->rowtrace = Py_NewRef(value);
  return 0;
}

/** .. attribute:: authorizer
  :type: Optional[Authorizer]

  While `preparing <https://sqlite.org/c3ref/prepare.html>`_
  statements, SQLite will call any defined authorizer to see if a
  particular action is ok to be part of the statement.

  Typical usage would be if you are running user supplied SQL and want
  to prevent harmful operations.  You should also
  set the :class:`statementcachesize <Connection>` to zero.

  The authorizer callback has 5 parameters:

    * An `operation code <https://sqlite.org/c3ref/c_alter_table.html>`_
    * A string (or None) dependent on the operation `(listed as 3rd) <https://sqlite.org/c3ref/c_alter_table.html>`_
    * A string (or None) dependent on the operation `(listed as 4th) <https://sqlite.org/c3ref/c_alter_table.html>`_
    * A string name of the database (or None)
    * Name of the innermost trigger or view doing the access (or None)

  The authorizer callback should return one of *SQLITE_OK*,
  *SQLITE_DENY* or *SQLITE_IGNORE*.
  (*SQLITE_DENY* is returned if there is an error in your
  Python code).

  .. seealso::

    * :ref:`Example <example_authorizer>`
    * :ref:`statementcache`

  -* sqlite3_set_authorizer
*/
static PyObject *
Connection_get_authorizer_attr(Connection *self)
{

  CHECK_CLOSED(self, NULL);

  if (self->authorizer)
    return Py_NewRef(self->authorizer);
  Py_RETURN_NONE;
}

static int
Connection_set_authorizer_attr(Connection *self, PyObject *value)
{
  CHECK_CLOSED(self, -1);

  if (!Py_IsNone(value) && !PyCallable_Check(value))
  {
    PyErr_Format(PyExc_TypeError, "authorizer expected a Callable or None not %s", Py_TypeName(value));
    return -1;
  }
  void *res = Connection_internal_set_authorizer(self, (!Py_IsNone(value)) ? value : NULL);
  if (res)
    return 0;
  assert(PyErr_Occurred());
  return -1;
}

/** .. attribute:: system_errno
 :type: int

 The underlying system error code for the most recent I/O error.

 -* sqlite3_system_errno
*/
static PyObject *
Connection_get_system_errno(Connection *self)
{

  CHECK_CLOSED(self, NULL);

  return PyLong_FromLong(sqlite3_system_errno(self->db));
}

/** .. attribute:: is_interrupted
   :type: bool

   Indicates if this connection has been interrupted.

   -* sqlite3_is_interrupted
*/
static PyObject *
Connection_is_interrupted(Connection *self)
{

  CHECK_CLOSED(self, NULL);

  return Py_NewRef(sqlite3_is_interrupted(self->db) ? Py_True : Py_False);
}

/** .. method:: data_version(schema: Optional[str] = None) -> int

  Unlike `pragma data_version
  <https://sqlite.org/pragma.html#pragma_data_version>`__ this value
  updates when changes are made by other connections, **AND** this one.

  :param schema: `schema` is `main`, `temp`, the name in `ATTACH <https://sqlite.org/lang_attach.html>`__,
      defaulting to `main` if not supplied.

  -* sqlite3_file_control
*/
static PyObject *
Connection_data_version(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{

  CHECK_CLOSED(self, NULL);

  const char *schema = NULL;
  {
    Connection_data_version_CHECK;
    ARG_PROLOG(1, Connection_data_version_KWNAMES);
    ARG_OPTIONAL ARG_optional_str(schema);
    ARG_EPILOG(NULL, Connection_data_version_USAGE, );
  }
  int res, data_version = -1;
  res = sqlite3_file_control(self->db, schema ? schema : "main", SQLITE_FCNTL_DATA_VERSION, &data_version);

  /* errmsg is not set on failure */
  SET_EXC(res, NULL);

  return PyErr_Occurred() ? NULL : PyLong_FromLong(data_version);
}

/* done this way here to keep doc generation simple */
#include "fts.c"

/** .. method:: fts5_tokenizer(name: str, args: list[str] | None = None) -> apsw.FTS5Tokenizer

  Returns the named tokenizer initialized with ``args``.  Names are case insensitive.

  .. seealso::

      * :meth:`register_fts5_tokenizer`
      * :doc:`textsearch`

*/
static PyObject *
Connection_fts5_tokenizer(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *name = NULL;
  const char *name_dup = NULL;
  PyObject *args = NULL, *args_as_tuple = NULL, *tmptuple = NULL;

  CHECK_CLOSED(self, NULL);

  {
    Connection_fts5_tokenizer_CHECK;
    ARG_PROLOG(2, Connection_fts5_tokenizer_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_OPTIONAL ARG_optional_list_str(args);
    ARG_EPILOG(NULL, Connection_fts5_tokenizer_USAGE, );
  }

  if (args)
    tmptuple = NULL;
  else
  {
    tmptuple = PyTuple_New(0);
    if (!tmptuple)
      return NULL;
  }

  Py_ssize_t argc = args ? PyList_GET_SIZE(args) : 0;
  /* arbitrary but reasonable maximum consuming 1kb of stack */
  if (argc > 128)
  {
    PyErr_Format(PyExc_ValueError, "Too many args (%zd)", argc);
    return NULL;
  }

  DBMUTEX_ENSURE(self->dbmutex);

  /* vla can't be size zero */
  VLA(argv, argc + 1, const char *);
  for (int i = 0; i < argc; i++)
  {
    argv[i] = PyUnicode_AsUTF8(PyList_GET_ITEM(args, i));
    if (!argv[i])
      goto error;
  }

  /* force args to always be a tuple because we save it
     in returned object and don't want that to be modifyable */
  args_as_tuple = PySequence_Tuple(args ? args : tmptuple);
  if (!args_as_tuple)
    goto error;

  fts5_api *api = Connection_fts5_api(self);
  if (!api)
    goto error;

  void *userdata = NULL;
  fts5_tokenizer_v2 *tokenizer_class = NULL;

  int rc = api->xFindTokenizer_v2(api, name, &userdata, &tokenizer_class);
  if (rc != SQLITE_OK)
  {
    PyErr_Format(get_exception_for_code(rc), "No tokenizer named \"%s\"", name);
    AddTraceBackHere(__FILE__, __LINE__, "Connection.fts5_api.xFindTokenizer_v2", "{s:s}", "name", name);
    goto error;
  }

  /* no objects/memory has been allocated yet */
  name_dup = apsw_strdup(name);
  if (!name_dup)
    goto error;

  APSWFTS5Tokenizer *pytok = (APSWFTS5Tokenizer *)_PyObject_New(&APSWFTS5TokenizerType);
  if (!pytok)
    goto error;

  /* fill in fields */
  pytok->db = self;
  Py_INCREF(self);
  pytok->name = name_dup;
  name_dup = NULL;
  pytok->args = Py_NewRef(args_as_tuple);
  pytok->xDelete = tokenizer_class->xDelete;
  pytok->xTokenize = tokenizer_class->xTokenize;
  pytok->tokenizer_instance = NULL;
  pytok->vectorcall = (vectorcallfunc)APSWFTS5Tokenizer_call;

  rc = tokenizer_class->xCreate(userdata, argv, argc, &pytok->tokenizer_instance);
  if (rc != SQLITE_OK)
  {
    SET_EXC(rc, self->db);
    AddTraceBackHere(__FILE__, __LINE__, "Connection.fts5_tokenizer_v2.xCreate", "{s:s,s:i,s:O}", "name", name,
                     "len(args)", argc, "args", args_as_tuple);
    APSWFTS5TokenizerType.tp_dealloc((PyObject *)pytok);
    goto error;
  }
  Py_XDECREF(tmptuple);
  Py_DECREF(args_as_tuple);
  sqlite3_mutex_leave(self->dbmutex);
  return (PyObject *)pytok;
error:
  Py_XDECREF(tmptuple);
  Py_XDECREF(args_as_tuple);
  PyMem_Free((void *)name_dup);
  sqlite3_mutex_leave(self->dbmutex);
  return NULL;
}

/** .. method:: register_fts5_tokenizer(name: str, tokenizer_factory: FTS5TokenizerFactory) -> None

  Registers a tokenizer factory.  Names are case insensitive.  It is not possible to
  unregister a tokenizer.

  .. seealso::

      * :meth:`fts5_tokenizer`
      * :doc:`textsearch`
      * `FTS5 documentation <https://www.sqlite.org/fts5.html#custom_tokenizers>`__
*/
static PyObject *
Connection_register_fts5_tokenizer(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                                   PyObject *fast_kwnames)
{

  CHECK_CLOSED(self, NULL);
  const char *name;
  int rc = SQLITE_NOMEM;
  PyObject *tokenizer_factory;

  {
    Connection_register_fts5_tokenizer_CHECK;
    ARG_PROLOG(2, Connection_register_fts5_tokenizer_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_MANDATORY ARG_Callable(tokenizer_factory);
    ARG_EPILOG(NULL, Connection_register_fts5_tokenizer_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  fts5_api *api = Connection_fts5_api(self);
  sqlite3_mutex_leave(self->dbmutex);
  if (!api)
    return NULL;

  TokenizerFactoryData *tfd = PyMem_Calloc(1, sizeof(TokenizerFactoryData));
  if (!tfd)
    goto finally;
  tfd->factory_func = Py_NewRef(tokenizer_factory);
  tfd->connection = Py_NewRef((PyObject *)self);

  APSW_FAULT(FTS5TokenizerRegister,
             rc = api->xCreateTokenizer_v2(api, name, tfd, &APSWPythonTokenizer, APSWPythonTokenizerFactoryDelete),
             rc = SQLITE_NOMEM);

finally:
  if (rc != SQLITE_OK)
  {
    if (tfd)
      APSWPythonTokenizerFactoryDelete(tfd);
    SET_EXC(rc, NULL);
    return NULL;
  }
  Py_RETURN_NONE;
}

/** .. method:: fts5_tokenizer_available(name: str) -> bool

  Checks if the named tokenizer is registered.

  .. seealso::

      * :meth:`fts5_tokenizer`
      * :doc:`textsearch`
      * `FTS5 documentation <https://www.sqlite.org/fts5.html#custom_tokenizers>`__
*/
static PyObject *
Connection_fts5_tokenizer_available(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                                    PyObject *fast_kwnames)
{
  CHECK_CLOSED(self, NULL);
  const char *name;
  int rc = -1;

  {
    Connection_fts5_tokenizer_available_CHECK;
    ARG_PROLOG(1, Connection_fts5_tokenizer_available_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_EPILOG(NULL, Connection_fts5_tokenizer_available_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  fts5_api *api = Connection_fts5_api(self);

  if (api)
  {
    void *user_data = NULL;
    fts5_tokenizer_v2 *tokenizer_class = NULL;

    rc = api->xFindTokenizer_v2(api, name, &user_data, &tokenizer_class);
  }
  sqlite3_mutex_leave(self->dbmutex);
  if (!api)
  {
    assert(PyErr_Occurred());
    return NULL;
  }
  if (rc == SQLITE_OK)
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

/** .. method:: register_fts5_function(name: str, function: FTS5Function) -> None

  Registers the (case insensitive) named function used as an `auxiliary
  function  <https://www.sqlite.org/fts5.html#custom_auxiliary_functions>`__.

  The first parameter to the function will be :class:`FTS5ExtensionApi`
  and the rest will be the function arguments at the SQL level.
*/
static PyObject *
Connection_register_fts5_function(Connection *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                                  PyObject *fast_kwnames)
{

  CHECK_CLOSED(self, NULL);

  const char *name;
  PyObject *function = NULL;
  {
    Connection_register_fts5_function_CHECK;
    ARG_PROLOG(2, Connection_register_fts5_function_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_MANDATORY ARG_Callable(function);
    ARG_EPILOG(NULL, Connection_register_fts5_function_USAGE, );
  }

  DBMUTEX_ENSURE(self->dbmutex);
  fts5_api *api = Connection_fts5_api(self);

  if (api)
  {
    struct fts5aux_cbinfo *cbinfo = PyMem_Calloc(1, sizeof(struct fts5aux_cbinfo));
    if (!cbinfo)
      goto finally;
    cbinfo->callback = Py_NewRef(function);
    cbinfo->name = apsw_strdup(name);

    int rc = SQLITE_NOMEM;
    if (cbinfo->name)
    {
      APSW_FAULT(FTS5FunctionRegister,
                 rc = api->xCreateFunction(api, name, cbinfo, apsw_fts5_extension_function,
                                           apsw_fts5_extension_function_destroy),
                 rc = SQLITE_BUSY);
    }
    if (rc != SQLITE_OK)
    {
      if (!PyErr_Occurred())
        PyErr_Format(get_exception_for_code(rc), "Registering function named \"%s\"", name);
      AddTraceBackHere(__FILE__, __LINE__, "Connection.fts5_api.xCreateFunction", "{s:s,s:O}", "name", name, "function",
                       function);
      apsw_fts5_extension_function_destroy(cbinfo);
    }
  }
finally:
  sqlite3_mutex_leave(self->dbmutex);

  if (PyErr_Occurred())
    return NULL;

  Py_RETURN_NONE;
}

static PyGetSetDef Connection_getseters[] = {
  /* name getter setter doc closure */
  { "filename", (getter)Connection_getmainfilename, NULL, Connection_filename_DOC, NULL },
  { "filename_journal", (getter)Connection_getjournalfilename, NULL, Connection_filename_journal_DOC, NULL },
  { "filename_wal", (getter)Connection_getwalfilename, NULL, Connection_filename_wal_DOC, NULL },
  { "cursor_factory", (getter)Connection_get_cursor_factory, (setter)Connection_set_cursor_factory,
    Connection_cursor_factory_DOC, NULL },
  { "in_transaction", (getter)Connection_get_in_transaction, NULL, Connection_in_transaction_DOC },
  { "exec_trace", (getter)Connection_get_exec_trace_attr, (setter)Connection_set_exec_trace_attr,
    Connection_exec_trace_DOC },
  { "row_trace", (getter)Connection_get_row_trace_attr, (setter)Connection_set_row_trace_attr,
    Connection_row_trace_DOC },
  { "authorizer", (getter)Connection_get_authorizer_attr, (setter)Connection_set_authorizer_attr,
    Connection_authorizer_DOC },
  { "system_errno", (getter)Connection_get_system_errno, NULL, Connection_system_errno_DOC },
  { "is_interrupted", (getter)Connection_is_interrupted, NULL, Connection_is_interrupted_DOC },
#ifndef APSW_OMIT_OLD_NAMES
  { Connection_exec_trace_OLDNAME, (getter)Connection_get_exec_trace_attr, (setter)Connection_set_exec_trace_attr,
    Connection_exec_trace_OLDDOC },
  { Connection_row_trace_OLDNAME, (getter)Connection_get_row_trace_attr, (setter)Connection_set_row_trace_attr,
    Connection_row_trace_OLDDOC },
#endif
  /* Sentinel */
  { NULL, NULL, NULL, NULL, NULL }
};

/** .. attribute:: open_flags
  :type: int

  The combination of :attr:`flags <apsw.mapping_open_flags>` used to open the database.
*/

/** .. attribute:: open_vfs
  :type: str

  The string name of the vfs used to open the database.
*/

static int
Connection_tp_traverse(Connection *self, visitproc visit, void *arg)
{
  Py_VISIT(self->busyhandler);
  Py_VISIT(self->rollbackhook);
  Py_VISIT(self->updatehook);
  Py_VISIT(self->commithook);
  Py_VISIT(self->walhook);
  Py_VISIT(self->authorizer);
  Py_VISIT(self->collationneeded);
  Py_VISIT(self->exectrace);
  Py_VISIT(self->rowtrace);
  Py_VISIT(self->vfs);
  Py_VISIT(self->dependents);
  Py_VISIT(self->cursor_factory);
  for (unsigned i = 0; i < self->tracehooks_count; i++)
  {
    Py_VISIT(self->tracehooks[i].callback);
    Py_VISIT(self->tracehooks[i].id);
  }
  for (unsigned i = 0; i < self->progresshandler_count; i++)
  {
    Py_VISIT(self->progresshandler[i].callback);
    Py_VISIT(self->progresshandler[i].id);
  }
  return 0;
}

static PyObject *
Connection_tp_str(Connection *self)
{
  if (self->dbmutex)
  {
    DBMUTEX_ENSURE(self->dbmutex);
    PyObject *res
        = PyUnicode_FromFormat("<apsw.Connection object \"%s\" at %p>", sqlite3_db_filename(self->db, "main"), self);
    sqlite3_mutex_leave(self->dbmutex);
    return res;
  }
  return PyUnicode_FromFormat("<apsw.Connection object (closed) at %p>", self);
}

static PyMemberDef Connection_members[] = {
  /* name type offset flags doc */
  { "open_flags", T_OBJECT, offsetof(Connection, open_flags), READONLY, Connection_open_flags_DOC },
  { "open_vfs", T_OBJECT, offsetof(Connection, open_vfs), READONLY, Connection_open_vfs_DOC },
  { 0, 0, 0, 0, 0 }
};

static PyMethodDef Connection_methods[] = {
  { "cursor", (PyCFunction)Connection_cursor, METH_NOARGS, Connection_cursor_DOC },
  { "close", (PyCFunction)Connection_close, METH_FASTCALL | METH_KEYWORDS, Connection_close_DOC },
  { "set_busy_timeout", (PyCFunction)Connection_set_busy_timeout, METH_FASTCALL | METH_KEYWORDS,
    Connection_set_busy_timeout_DOC },
  { "interrupt", (PyCFunction)Connection_interrupt, METH_NOARGS, Connection_interrupt_DOC },
  { "create_scalar_function", (PyCFunction)Connection_create_scalar_function, METH_FASTCALL | METH_KEYWORDS,
    Connection_create_scalar_function_DOC },
  { "create_aggregate_function", (PyCFunction)Connection_create_aggregate_function, METH_FASTCALL | METH_KEYWORDS,
    Connection_create_aggregate_function_DOC },
  { "set_busy_handler", (PyCFunction)Connection_set_busy_handler, METH_FASTCALL | METH_KEYWORDS,
    Connection_set_busy_handler_DOC },
  { "changes", (PyCFunction)Connection_changes, METH_NOARGS, Connection_changes_DOC },
  { "total_changes", (PyCFunction)Connection_total_changes, METH_NOARGS, Connection_total_changes_DOC },
  { "get_autocommit", (PyCFunction)Connection_get_autocommit, METH_NOARGS, Connection_get_autocommit_DOC },
  { "create_collation", (PyCFunction)Connection_create_collation, METH_FASTCALL | METH_KEYWORDS,
    Connection_create_collation_DOC },
  { "last_insert_rowid", (PyCFunction)Connection_last_insert_rowid, METH_NOARGS, Connection_last_insert_rowid_DOC },
  { "set_last_insert_rowid", (PyCFunction)Connection_set_last_insert_rowid, METH_FASTCALL | METH_KEYWORDS,
    Connection_set_last_insert_rowid_DOC },
  { "collation_needed", (PyCFunction)Connection_collation_needed, METH_FASTCALL | METH_KEYWORDS,
    Connection_collation_needed_DOC },
  { "set_authorizer", (PyCFunction)Connection_set_authorizer, METH_FASTCALL | METH_KEYWORDS,
    Connection_set_authorizer_DOC },
  { "set_update_hook", (PyCFunction)Connection_set_update_hook, METH_FASTCALL | METH_KEYWORDS,
    Connection_set_update_hook_DOC },
  { "set_rollback_hook", (PyCFunction)Connection_set_rollback_hook, METH_FASTCALL | METH_KEYWORDS,
    Connection_set_rollback_hook_DOC },
  { "blob_open", (PyCFunction)Connection_blob_open, METH_FASTCALL | METH_KEYWORDS, Connection_blob_open_DOC },
  { "set_progress_handler", (PyCFunction)Connection_set_progress_handler, METH_FASTCALL | METH_KEYWORDS,
    Connection_set_progress_handler_DOC },
  { "set_commit_hook", (PyCFunction)Connection_set_commit_hook, METH_FASTCALL | METH_KEYWORDS,
    Connection_set_commit_hook_DOC },
  { "set_wal_hook", (PyCFunction)Connection_set_wal_hook, METH_FASTCALL | METH_KEYWORDS, Connection_set_wal_hook_DOC },
  { "limit", (PyCFunction)Connection_limit, METH_FASTCALL | METH_KEYWORDS, Connection_limit_DOC },
  { "set_profile", (PyCFunction)Connection_set_profile, METH_FASTCALL | METH_KEYWORDS, Connection_set_profile_DOC },
#ifndef SQLITE_OMIT_LOAD_EXTENSION
  { "enable_load_extension", (PyCFunction)Connection_enable_load_extension, METH_FASTCALL | METH_KEYWORDS,
    Connection_enable_load_extension_DOC },
  { "load_extension", (PyCFunction)Connection_load_extension, METH_FASTCALL | METH_KEYWORDS,
    Connection_load_extension_DOC },
#endif
  { "create_module", (PyCFunction)Connection_create_module, METH_FASTCALL | METH_KEYWORDS,
    Connection_create_module_DOC },
  { "overload_function", (PyCFunction)Connection_overload_function, METH_FASTCALL | METH_KEYWORDS,
    Connection_overload_function_DOC },
  { "backup", (PyCFunction)Connection_backup, METH_FASTCALL | METH_KEYWORDS, Connection_backup_DOC },
  { "file_control", (PyCFunction)Connection_file_control, METH_FASTCALL | METH_KEYWORDS, Connection_file_control_DOC },
  { "vfsname", (PyCFunction)Connection_vfsname, METH_FASTCALL | METH_KEYWORDS, Connection_vfsname_DOC },
  { "sqlite3_pointer", (PyCFunction)Connection_sqlite3_pointer, METH_NOARGS, Connection_sqlite3_pointer_DOC },
  { "set_exec_trace", (PyCFunction)Connection_set_exec_trace, METH_FASTCALL | METH_KEYWORDS,
    Connection_set_exec_trace_DOC },
  { "set_row_trace", (PyCFunction)Connection_set_row_trace, METH_FASTCALL | METH_KEYWORDS,
    Connection_set_row_trace_DOC },
  { "get_exec_trace", (PyCFunction)Connection_get_exec_trace, METH_NOARGS, Connection_get_exec_trace_DOC },
  { "get_row_trace", (PyCFunction)Connection_get_row_trace, METH_NOARGS, Connection_get_row_trace_DOC },
  { "__enter__", (PyCFunction)Connection_enter, METH_NOARGS, Connection_enter_DOC },
  { "__exit__", (PyCFunction)Connection_exit, METH_FASTCALL | METH_KEYWORDS, Connection_exit_DOC },
  { "wal_autocheckpoint", (PyCFunction)Connection_wal_autocheckpoint, METH_FASTCALL | METH_KEYWORDS,
    Connection_wal_autocheckpoint_DOC },
  { "wal_checkpoint", (PyCFunction)Connection_wal_checkpoint, METH_FASTCALL | METH_KEYWORDS,
    Connection_wal_checkpoint_DOC },
  { "config", (PyCFunction)Connection_config, METH_VARARGS, Connection_config_DOC },
  { "status", (PyCFunction)Connection_status, METH_FASTCALL | METH_KEYWORDS, Connection_status_DOC },
  { "readonly", (PyCFunction)Connection_readonly, METH_FASTCALL | METH_KEYWORDS, Connection_readonly_DOC },
  { "db_filename", (PyCFunction)Connection_db_filename, METH_FASTCALL | METH_KEYWORDS, Connection_db_filename_DOC },
  { "txn_state", (PyCFunction)Connection_txn_state, METH_FASTCALL | METH_KEYWORDS, Connection_txn_state_DOC },
  { "serialize", (PyCFunction)Connection_serialize, METH_FASTCALL | METH_KEYWORDS, Connection_serialize_DOC },
  { "deserialize", (PyCFunction)Connection_deserialize, METH_FASTCALL | METH_KEYWORDS, Connection_deserialize_DOC },
  { "autovacuum_pages", (PyCFunction)Connection_autovacuum_pages, METH_FASTCALL | METH_KEYWORDS,
    Connection_autovacuum_pages_DOC },
  { "db_names", (PyCFunction)Connection_db_names, METH_NOARGS, Connection_db_names_DOC },
  { "execute", (PyCFunction)Connection_execute, METH_FASTCALL | METH_KEYWORDS, Connection_execute_DOC },
  { "executemany", (PyCFunction)Connection_executemany, METH_FASTCALL | METH_KEYWORDS, Connection_executemany_DOC },
  { "cache_stats", (PyCFunction)Connection_cache_stats, METH_FASTCALL | METH_KEYWORDS, Connection_cache_stats_DOC },
  { "table_exists", (PyCFunction)Connection_table_exists, METH_FASTCALL | METH_KEYWORDS, Connection_table_exists_DOC },
  { "column_metadata", (PyCFunction)Connection_column_metadata, METH_FASTCALL | METH_KEYWORDS,
    Connection_column_metadata_DOC },
  { "trace_v2", (PyCFunction)Connection_trace_v2, METH_FASTCALL | METH_KEYWORDS, Connection_trace_v2_DOC },
  { "cache_flush", (PyCFunction)Connection_cache_flush, METH_NOARGS, Connection_cache_flush_DOC },
  { "release_memory", (PyCFunction)Connection_release_memory, METH_FASTCALL | METH_KEYWORDS,
    Connection_release_memory_DOC },
  { "drop_modules", (PyCFunction)Connection_drop_modules, METH_FASTCALL | METH_KEYWORDS, Connection_drop_modules_DOC },
  { "create_window_function", (PyCFunction)Connection_create_window_function, METH_FASTCALL | METH_KEYWORDS,
    Connection_create_window_function_DOC },
  { "vtab_config", (PyCFunction)Connection_vtab_config, METH_FASTCALL | METH_KEYWORDS, Connection_vtab_config_DOC },
  { "vtab_on_conflict", (PyCFunction)Connection_vtab_on_conflict, METH_NOARGS, Connection_vtab_on_conflict_DOC },
  { "pragma", (PyCFunction)Connection_pragma, METH_FASTCALL | METH_KEYWORDS, Connection_pragma_DOC },
  { "read", (PyCFunction)Connection_read, METH_FASTCALL | METH_KEYWORDS, Connection_read_DOC },
  { "fts5_tokenizer", (PyCFunction)Connection_fts5_tokenizer, METH_FASTCALL | METH_KEYWORDS,
    Connection_fts5_tokenizer_DOC },
  { "register_fts5_tokenizer", (PyCFunction)Connection_register_fts5_tokenizer, METH_FASTCALL | METH_KEYWORDS,
    Connection_register_fts5_tokenizer_DOC },
  { "fts5_tokenizer_available", (PyCFunction)Connection_fts5_tokenizer_available, METH_FASTCALL | METH_KEYWORDS,
    Connection_fts5_tokenizer_available_DOC },
  { "register_fts5_function", (PyCFunction)Connection_register_fts5_function, METH_FASTCALL | METH_KEYWORDS,
    Connection_register_fts5_function_DOC },
  { "data_version", (PyCFunction)Connection_data_version, METH_FASTCALL | METH_KEYWORDS, Connection_data_version_DOC },
#ifndef APSW_OMIT_OLD_NAMES
  { Connection_set_busy_timeout_OLDNAME, (PyCFunction)Connection_set_busy_timeout, METH_FASTCALL | METH_KEYWORDS,
    Connection_set_busy_timeout_OLDDOC },
  { Connection_create_scalar_function_OLDNAME, (PyCFunction)Connection_create_scalar_function,
    METH_FASTCALL | METH_KEYWORDS, Connection_create_scalar_function_OLDDOC },
  { Connection_create_aggregate_function_OLDNAME, (PyCFunction)Connection_create_aggregate_function,
    METH_FASTCALL | METH_KEYWORDS, Connection_create_aggregate_function_OLDDOC },
  { Connection_set_busy_handler_OLDNAME, (PyCFunction)Connection_set_busy_handler, METH_FASTCALL | METH_KEYWORDS,
    Connection_set_busy_handler_OLDDOC },
  { Connection_total_changes_OLDNAME, (PyCFunction)Connection_total_changes, METH_NOARGS,
    Connection_total_changes_OLDDOC },
  { Connection_get_autocommit_OLDNAME, (PyCFunction)Connection_get_autocommit, METH_NOARGS,
    Connection_get_autocommit_OLDDOC },
  { Connection_create_collation_OLDNAME, (PyCFunction)Connection_create_collation, METH_FASTCALL | METH_KEYWORDS,
    Connection_create_collation_OLDDOC },
  { Connection_collation_needed_OLDNAME, (PyCFunction)Connection_collation_needed, METH_FASTCALL | METH_KEYWORDS,
    Connection_collation_needed_OLDDOC },
  { Connection_set_authorizer_OLDNAME, (PyCFunction)Connection_set_authorizer, METH_FASTCALL | METH_KEYWORDS,
    Connection_set_authorizer_OLDDOC },
  { Connection_set_update_hook_OLDNAME, (PyCFunction)Connection_set_update_hook, METH_FASTCALL | METH_KEYWORDS,
    Connection_set_update_hook_OLDDOC },
  { Connection_set_rollback_hook_OLDNAME, (PyCFunction)Connection_set_rollback_hook, METH_FASTCALL | METH_KEYWORDS,
    Connection_set_rollback_hook_OLDDOC },
  { Connection_blob_open_OLDNAME, (PyCFunction)Connection_blob_open, METH_FASTCALL | METH_KEYWORDS,
    Connection_blob_open_OLDDOC },
  { Connection_set_progress_handler_OLDNAME, (PyCFunction)Connection_set_progress_handler,
    METH_FASTCALL | METH_KEYWORDS, Connection_set_progress_handler_OLDDOC },
  { Connection_set_commit_hook_OLDNAME, (PyCFunction)Connection_set_commit_hook, METH_FASTCALL | METH_KEYWORDS,
    Connection_set_commit_hook_OLDDOC },
  { Connection_set_wal_hook_OLDNAME, (PyCFunction)Connection_set_wal_hook, METH_FASTCALL | METH_KEYWORDS,
    Connection_set_wal_hook_OLDDOC },
  { Connection_set_profile_OLDNAME, (PyCFunction)Connection_set_profile, METH_FASTCALL | METH_KEYWORDS,
    Connection_set_profile_OLDDOC },
  { Connection_create_module_OLDNAME, (PyCFunction)Connection_create_module, METH_FASTCALL | METH_KEYWORDS,
    Connection_create_module_OLDDOC },
  { Connection_overload_function_OLDNAME, (PyCFunction)Connection_overload_function, METH_FASTCALL | METH_KEYWORDS,
    Connection_overload_function_OLDDOC },
  { Connection_file_control_OLDNAME, (PyCFunction)Connection_file_control, METH_FASTCALL | METH_KEYWORDS,
    Connection_file_control_OLDDOC },
  { Connection_sqlite3_pointer_OLDNAME, (PyCFunction)Connection_sqlite3_pointer, METH_NOARGS,
    Connection_sqlite3_pointer_OLDDOC },
  { Connection_set_exec_trace_OLDNAME, (PyCFunction)Connection_set_exec_trace, METH_FASTCALL | METH_KEYWORDS,
    Connection_set_exec_trace_OLDDOC },
  { Connection_set_row_trace_OLDNAME, (PyCFunction)Connection_set_row_trace, METH_FASTCALL | METH_KEYWORDS,
    Connection_set_row_trace_OLDDOC },
  { Connection_get_exec_trace_OLDNAME, (PyCFunction)Connection_get_exec_trace, METH_NOARGS,
    Connection_get_exec_trace_OLDDOC },
  { Connection_get_row_trace_OLDNAME, (PyCFunction)Connection_get_row_trace, METH_NOARGS,
    Connection_get_row_trace_OLDDOC },
  { Connection_cache_flush_OLDNAME, (PyCFunction)Connection_cache_flush, METH_NOARGS, Connection_cache_flush_OLDDOC },
#ifndef SQLITE_OMIT_LOAD_EXTENSION
  { Connection_enable_load_extension_OLDNAME, (PyCFunction)Connection_enable_load_extension,
    METH_FASTCALL | METH_KEYWORDS, Connection_enable_load_extension_OLDDOC },
  { Connection_load_extension_OLDNAME, (PyCFunction)Connection_load_extension, METH_FASTCALL | METH_KEYWORDS,
    Connection_load_extension_OLDDOC },
#endif
#endif
  { 0, 0, 0, 0 } /* Sentinel */
};

static PyTypeObject ConnectionType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.Connection",
  .tp_basicsize = sizeof(Connection),
  .tp_dealloc = (destructor)Connection_dealloc,
  .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC,
  .tp_doc = Connection_class_DOC,
  .tp_traverse = (traverseproc)Connection_tp_traverse,
  .tp_weaklistoffset = offsetof(Connection, weakreflist),
  .tp_methods = Connection_methods,
  .tp_members = Connection_members,
  .tp_getset = Connection_getseters,
  .tp_init = (initproc)Connection_init,
  .tp_new = Connection_new,
  .tp_str = (reprfunc)Connection_tp_str,
};
