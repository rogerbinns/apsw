/*
  Another Python Sqlite Wrapper

  Wrap SQLite hot backup functionality

  See the accompanying LICENSE file.
*/

/**

.. _backup:

Backup
******

A backup object encapsulates copying one database to another.  You
call :meth:`Connection.backup` on the destination database to get the
Backup object.  Call :meth:`~Backup.step` to copy some pages
repeatedly dealing with errors as appropriate.  Finally
:meth:`~Backup.finish` cleans up committing or rolling back and
releasing locks.

See the :ref:`example <example_backup>`.

Important details
=================

The database is copied page by page.  This means that there is not a
round trip via SQL.  All pages are copied including free ones.

The destination database is locked during the copy.  You will get a
:exc:`ThreadingViolationError` if you attempt to use it.

The source database can change during the backup.  SQLite will
come back and copy those changes too until the backup is complete.
*/

/* we love us some macros */
#define CHECK_BACKUP_CLOSED(e)                                                                                         \
  do                                                                                                                   \
  {                                                                                                                    \
    if (!self->backup || (self->dest && !self->dest->db) || (self->source && !self->source->db))                       \
    {                                                                                                                  \
      if (!self->backup)                                                                                               \
        PyErr_SetString(ExcConnectionClosed, "The backup is finished");                                                \
      else if (self->dest && !self->dest->db)                                                                          \
        PyErr_SetString(ExcConnectionClosed, "The backup destination database is closed");                             \
      else                                                                                                             \
        PyErr_SetString(ExcConnectionClosed, "The backup source database is closed");                                  \
      return e;                                                                                                        \
    }                                                                                                                  \
  } while (0)

/** .. class:: Backup

  You create a backup instance by calling :meth:`Connection.backup`.
*/

typedef struct APSWBackup
{
  PyObject_HEAD
  Connection *dest;
  Connection *source;
  sqlite3_backup *backup;
  PyObject *done;
  PyObject *weakreflist;
} APSWBackup;

static void
APSWBackup_init(APSWBackup *self, Connection *dest, Connection *source, sqlite3_backup *backup)
{
  self->dest = dest;
  self->source = source;
  self->backup = backup;
  self->done = Py_NewRef(Py_False);
  self->weakreflist = NULL;
}

/* returns non-zero if it set an exception */
static int
APSWBackup_close_internal(APSWBackup *self, int force)
{
  int res, setexc = 0;

  /* should not have been called with active backup */
  assert(self->backup);

  res = sqlite3_backup_finish(self->backup);
  if (res)
  {
    switch (force)
    {
    case 0:
      SET_EXC(res, self->dest->db);
      setexc = 1;
      break;
    case 1:
      break;
    case 2: {
      PY_ERR_FETCH(exc_save);

      SET_EXC(res, self->dest->db);
      apsw_write_unraisable(NULL);

      PY_ERR_RESTORE(exc_save);
      break;
    }
    }
  }

  self->backup = 0;
  sqlite3_mutex_leave(self->source->dbmutex);
  sqlite3_mutex_leave(self->dest->dbmutex);

  Connection_remove_dependent(self->dest, (PyObject *)self);
  Connection_remove_dependent(self->source, (PyObject *)self);

  Py_CLEAR(self->dest);
  Py_CLEAR(self->source);

  return setexc;
}

static void
APSWBackup_dealloc(PyObject *self_)
{
  APSWBackup *self = (APSWBackup *)self_;
  APSW_CLEAR_WEAKREFS;

  if (self->backup)
  {
    DBMUTEX_FORCE(self->source->dbmutex);
    DBMUTEX_FORCE(self->dest->dbmutex);

    APSWBackup_close_internal(self, 2);
  }
  Py_CLEAR(self->done);

  Py_TpFree(self_);
}

/** .. method:: step(npages: int = -1) -> bool

  Copies *npages* pages from the source to destination database.  The source database is locked during the copy so
  using smaller values allows other access to the source database.  The destination database is always locked until the
  backup object is :meth:`finished <Backup.finish>`.

  :param npages: How many pages to copy. If the parameter is omitted
     or negative then all remaining pages are copied.

  This method may throw a :exc:`BusyError` or :exc:`LockedError` if
  unable to lock the source database.  You can catch those and try
  again.

  :returns: True if this copied the last remaining outstanding pages, else False.  This is the same value as :attr:`~Backup.done`

  -* sqlite3_backup_step
*/
static PyObject *
APSWBackup_step(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  APSWBackup *self = (APSWBackup *)self_;
  int npages = -1, res;

  CHECK_BACKUP_CLOSED(NULL);

  {
    Backup_step_CHECK;
    ARG_PROLOG(1, Backup_step_KWNAMES);
    ARG_OPTIONAL ARG_int(npages);
    ARG_EPILOG(NULL, Backup_step_USAGE, );
  }

  ASYNC_FASTCALL(self->dest, APSWBackup_step);

  DBMUTEXES_ENSURE(self->source->dbmutex, "Backup source Connection is busy in another thread", self->dest->dbmutex,
                   "Backup destination Connection is busy in another thread");

  res = sqlite3_backup_step(self->backup, npages);

  /* this would happen if there were errors deep in the vfs */
  MakeExistingException();

  if (res != SQLITE_OK && res != SQLITE_DONE)
    SET_EXC(res, self->dest->db);

  sqlite3_mutex_leave(self->source->dbmutex);
  sqlite3_mutex_leave(self->dest->dbmutex);

  if (PyErr_Occurred())
    return NULL;

  if (res == SQLITE_DONE)
  {
    if (!Py_IsTrue(self->done))
    {
      Py_CLEAR(self->done);
      self->done = Py_NewRef(Py_True);
    }
    res = SQLITE_OK;
  }

  return Py_NewRef(self->done);
}

/** .. method:: finish() -> None

  Completes the copy process.  If all pages have been copied then the
  transaction is committed on the destination database, otherwise it
  is rolled back.  This method must be called for your backup to take
  effect.  The backup object will always be finished even if there is
  an exception.  It is safe to call this method multiple times.

  -* sqlite3_backup_finish
*/
static PyObject *
APSWBackup_finish(PyObject *self_, PyObject *Py_UNUSED(unused))
{
  APSWBackup *self = (APSWBackup *)self_;
  int setexc;

  /* We handle CHECK_BACKUP_CLOSED internally */
  if (!self->backup)
    Py_RETURN_NONE;

  if (!IN_WORKER_THREAD(self->dest))
    return error_sync_in_async_context();

  DBMUTEXES_ENSURE(self->source->dbmutex, "Backup source Connection is busy in another thread", self->dest->dbmutex,
                   "Backup destination Connection is busy in another thread");

  setexc = APSWBackup_close_internal(self, 0);
  if (setexc)
    return NULL;

  Py_RETURN_NONE;
}

/** .. method:: afinish() -> None

  Async version of meth:`finish`
*/
static PyObject *
APSWBackup_afinish(PyObject *self_, PyObject *unused)
{
  APSWBackup *self = (APSWBackup *)self_;

  /* We handle CHECK_BACKUP_CLOSED internally */
  if (!self->backup)
    return async_return_value(Py_None);

  if (IN_WORKER_THREAD(self->dest))
    return error_async_in_sync_context();

  return do_async_binary((PyObject *)(self->dest), APSWBackup_finish, self_, unused);
}

/** .. method:: close(force: bool = False) -> None

  Does the same thing as :meth:`~Backup.finish`.  This extra api is
  provided to give the same api as other APSW objects and files.
  It is safe to call this method multiple  times.

  :param force: If true then any exceptions are ignored.
*/
static PyObject *
APSWBackup_close(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  APSWBackup *self = (APSWBackup *)self_;
  int force = 0, setexc;

  /* We handle CHECK_BACKUP_CLOSED internally */
  if (!self->backup)
    Py_RETURN_NONE; /* already closed */

  {
    Backup_close_CHECK;
    ARG_PROLOG(1, Backup_close_KWNAMES);
    ARG_OPTIONAL ARG_bool(force);
    ARG_EPILOG(NULL, Backup_close_USAGE, );
  }

  DBMUTEXES_ENSURE(self->source->dbmutex, "Backup source Connection is busy in another thread", self->dest->dbmutex,
                   "Backup destination Connection is busy in another thread");
  setexc = APSWBackup_close_internal(self, force);
  if (setexc)
    return NULL;
  Py_RETURN_NONE;
}

/** .. method:: aclose(force: bool = False) -> None

  Async version of :meth:`close`

*/
PyObject *
APSWBackup_aclose(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  APSWBackup *self = (APSWBackup *)self_;
  int force = 0;

  {
    Backup_aclose_CHECK;
    ARG_PROLOG(1, Backup_aclose_KWNAMES);
    ARG_OPTIONAL ARG_bool(force);
    ARG_EPILOG(NULL, Backup_aclose_USAGE, );
  }

  if (self->dest)
    ASYNC_FASTCALL(self->dest, APSWBackup_close);
  return async_return_value(Py_None);
}

/** .. attribute:: remaining
  :type: int

  Read only. How many pages were remaining to be copied after the last
  step.  If you haven't called :meth:`~Backup.step` or the backup
  object has been :meth:`finished <Backup.finish>` then zero is
  returned.

  -* sqlite3_backup_remaining
*/
static PyObject *
APSWBackup_get_remaining(PyObject *self_, void *Py_UNUSED(ignored))
{
  APSWBackup *self = (APSWBackup *)self_;
  return PyLong_FromLong(self->backup ? sqlite3_backup_remaining(self->backup) : 0);
}

/** .. attribute:: page_count
  :type: int

  Read only. How many pages were in the source database after the last
  step.  If you haven't called :meth:`~Backup.step` or the backup
  object has been :meth:`finished <Backup.finish>` then zero is
  returned.

  -* sqlite3_backup_pagecount
*/
static PyObject *
APSWBackup_get_page_count(PyObject *self_, void *Py_UNUSED(ignored))
{
  APSWBackup *self = (APSWBackup *)self_;
  return PyLong_FromLong(self->backup ? sqlite3_backup_pagecount(self->backup) : 0);
}

/** .. method:: __enter__() -> Backup

  You can use the backup object as a `context manager
  <https://docs.python.org/3/reference/datamodel.html#with-statement-context-managers>`_
  as defined in :pep:`0343`.  The :meth:`~Backup.__exit__` method ensures that backup
  is :meth:`finished <Backup.finish>`.
*/
static PyObject *
APSWBackup_enter(PyObject *self_, PyObject *Py_UNUSED(ignored))
{
  APSWBackup *self = (APSWBackup *)self_;
  CHECK_BACKUP_CLOSED(NULL);

  if (!IN_WORKER_THREAD(self->dest))
    return error_sync_in_async_context();

  return Py_NewRef(self_);
}

/** .. method:: __exit__(etype: type[BaseException] | None, evalue: type[BaseException] | None, etraceback: types.TracebackType | None) -> bool

  Implements context manager in conjunction with :meth:`~Backup.__enter__` ensuring
  that the copy is :meth:`finished <Backup.finish>`.
*/
static PyObject *
APSWBackup_exit(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  APSWBackup *self = (APSWBackup *)self_;
  PyObject *etype, *evalue, *etraceback;
  int setexc;

  {
    Backup_exit_CHECK;
    ARG_PROLOG(3, Backup_exit_KWNAMES);
    ARG_MANDATORY ARG_pyobject(etype);
    ARG_MANDATORY ARG_pyobject(evalue);
    ARG_MANDATORY ARG_pyobject(etraceback);
    ARG_EPILOG(NULL, Backup_exit_USAGE, );
  }

  if (self->dest && !IN_WORKER_THREAD(self->dest))
    return error_sync_in_async_context();

  /* If already closed then we are fine - CHECK_BACKUP_CLOSED not needed*/
  if (!self->backup)
    Py_RETURN_FALSE;

  /* we don't want to override any existing exception with the
     corresponding close exception, although there is a chance the
     close exception has more detail.  At the time of writing this
     code the step method only set an error code but not an error
     message */
  DBMUTEXES_ENSURE(self->source->dbmutex, "Backup source Connection is busy in another thread", self->dest->dbmutex,
                   "Backup destination Connection is busy in another thread");
  setexc = APSWBackup_close_internal(self, !Py_IsNone(etype) || !Py_IsNone(evalue) || !Py_IsNone(etraceback));

  if (setexc)
  {
    assert(PyErr_Occurred());
    return NULL;
  }

  Py_RETURN_FALSE;
}

/** .. method:: __aenter__() -> Self

  Async context manager enter
*/
static PyObject *
APSWBackup_aenter(PyObject *self_, PyObject *Py_UNUSED(unused))
{
  APSWBackup *self = (APSWBackup *)self_;

  CHECK_BACKUP_CLOSED(NULL);

  if (IN_WORKER_THREAD(self->dest))
    return error_async_in_sync_context();

  return async_return_value(self_);
}

/** .. method:: __aexit__(etype: type[BaseException] | None, evalue: BaseException | None, etraceback: types.TracebackType | None) -> None

  Async context manager exit
*/
static PyObject *
APSWBackup_aexit(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  APSWBackup *self = (APSWBackup *)self_;

  CHECK_BACKUP_CLOSED(NULL);

  PyObject *etype, *evalue, *etraceback;
  {
    Backup_aexit_CHECK;
    ARG_PROLOG(3, Backup_aexit_KWNAMES);
    ARG_MANDATORY ARG_pyobject(etype);
    ARG_MANDATORY ARG_pyobject(evalue);
    ARG_MANDATORY ARG_pyobject(etraceback);
    ARG_EPILOG(NULL, Backup_aexit_USAGE, );
  }

  (void)etype;
  (void)evalue;
  (void)etraceback;

  ASYNC_FASTCALL(self->dest, APSWBackup_exit);
  return error_async_in_sync_context();
}

static PyObject *
APSWBackup_tp_repr(PyObject *self_)
{
  APSWBackup *self = (APSWBackup *)self_;
  if (self->backup)
    return PyUnicode_FromFormat("<%s from %S to %S at %p>", Py_TypeName(self_), self->source, self->dest, self);
  return PyUnicode_FromFormat("<%s (closed) at %p>", Py_TypeName(self_), self_);
}

static int
APSWBackup_bool(PyObject *self_)
{
  APSWBackup *self = (APSWBackup *)self_;
  return self->backup ? 1 : 0;
}

/** .. attribute:: done
  :type: bool

  A boolean that is True if the copy completed in the last call to :meth:`~Backup.step`.
*/
static PyMemberDef backup_members[] = {
  /* name type offset flags doc */
  { "done", T_OBJECT, offsetof(APSWBackup, done), READONLY, Backup_done_DOC },
  { 0, 0, 0, 0, 0 }
};

static PyGetSetDef backup_getset[] = {
  /* name getter setter doc closure */
  { "remaining", APSWBackup_get_remaining, NULL, Backup_remaining_DOC, NULL },
  { "page_count", APSWBackup_get_page_count, NULL, Backup_page_count_DOC, NULL },
#ifndef APSW_OMIT_OLD_NAMES
  { Backup_page_count_OLDNAME, APSWBackup_get_page_count, NULL, Backup_page_count_OLDDOC, NULL },
#endif
  { 0, 0, 0, 0, 0 }
};

static PyMethodDef backup_methods[]
    = { { "__enter__", (PyCFunction)APSWBackup_enter, METH_NOARGS, Backup_enter_DOC },
        { "__exit__", (PyCFunction)APSWBackup_exit, METH_FASTCALL | METH_KEYWORDS, Backup_exit_DOC },
        { "__aenter__", (PyCFunction)APSWBackup_aenter, METH_NOARGS, Backup_aenter_DOC },
        { "__aexit__", (PyCFunction)APSWBackup_aexit, METH_FASTCALL | METH_KEYWORDS, Backup_aexit_DOC },
        { "step", (PyCFunction)APSWBackup_step, METH_FASTCALL | METH_KEYWORDS, Backup_step_DOC },
        { "finish", (PyCFunction)APSWBackup_finish, METH_NOARGS, Backup_finish_DOC },
        { "afinish", (PyCFunction)APSWBackup_afinish, METH_NOARGS, Backup_afinish_DOC },
        { "close", (PyCFunction)APSWBackup_close, METH_FASTCALL | METH_KEYWORDS, Backup_close_DOC },
        { "aclose", (PyCFunction)APSWBackup_aclose, METH_FASTCALL | METH_KEYWORDS, Backup_aclose_DOC },
        { 0, 0, 0, 0 } };

static PyNumberMethods backup_as_number = {
  .nb_bool = APSWBackup_bool,
};

static PyTypeObject APSWBackupType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.Backup",
  .tp_basicsize = sizeof(APSWBackup),
  .tp_dealloc = APSWBackup_dealloc,
  .tp_flags = Py_TPFLAGS_DEFAULT,
  .tp_doc = Backup_class_DOC,
  .tp_weaklistoffset = offsetof(APSWBackup, weakreflist),
  .tp_methods = backup_methods,
  .tp_members = backup_members,
  .tp_getset = backup_getset,
  .tp_as_number = &backup_as_number,
  .tp_repr = APSWBackup_tp_repr,
  .tp_str = NULL,
};
