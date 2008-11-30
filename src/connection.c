/*
  Connection handling code

  Copyright (C) 2004-2008 Roger Binns <rogerb@rogerbinns.com>

  This software is provided 'as-is', without any express or implied
  warranty.  In no event will the authors be held liable for any
  damages arising from the use of this software.
 
  Permission is granted to anyone to use this software for any
  purpose, including commercial applications, and to alter it and
  redistribute it freely, subject to the following restrictions:
 
  1. The origin of this software must not be misrepresented; you must
     not claim that you wrote the original software. If you use this
     software in a product, an acknowledgment in the product
     documentation would be appreciated but is not required.

  2. Altered source versions must be plainly marked as such, and must
     not be misrepresented as being the original software.

  3. This notice may not be removed or altered from any source
     distribution.
 
*/

/** 

Connections to a database
*************************

A :class:`Connection` encapsulates access to a database.  You then use
:class:`cursors <Cursor>` to issue queries against the database.

You can have multple :class:`Connections <Connection>` open against
the same database in the same process, across threads and in other
processes.

*/


/* CALLBACK INFO */

/* details of a registered function passed as user data to sqlite3_create_function */
typedef struct FunctionCBInfo {
  PyObject_HEAD
  char *name;                     /* ascii function name */
  PyObject *scalarfunc;           /* the function to call for stepping */
  PyObject *aggregatefactory;     /* factory for aggregate functions */
} FunctionCBInfo;

/* a particular aggregate function instance used as sqlite3_aggregate_context */
typedef struct _aggregatefunctioncontext 
{
  PyObject *aggvalue;             /* the aggregation value passed as first parameter */
  PyObject *stepfunc;             /* step function */
  PyObject *finalfunc;            /* final function */
} aggregatefunctioncontext;

/* CONNECTION TYPE */

struct Connection { 
  PyObject_HEAD
  sqlite3 *db;                    /* the actual database connection */
  unsigned inuse;                 /* track if we are in use preventing concurrent thread mangling */

  struct StatementCache *stmtcache;      /* prepared statement cache */

  PyObject *dependents;           /* tracking cursors & blobs belonging to this connection */
  PyObject *dependent_remove;     /* dependents.remove for weak ref processing */
  PyObject *functions;            /* list of registered functions */

  /* registered hooks/handlers (NULL or callable) */
  PyObject *busyhandler;     
  PyObject *rollbackhook;
  PyObject *profile;
  PyObject *updatehook;
  PyObject *commithook;           
  PyObject *progresshandler;      
  PyObject *authorizer;
  PyObject *collationneeded;
  PyObject *exectrace;
  PyObject *rowtrace;

  /* if we are using one of our VFS since sqlite doesn't reference count them */
  PyObject *vfs;

  /* informational attributes */
  PyObject *filename;
  PyObject *open_flags;
  PyObject *open_vfs;
};

typedef struct Connection Connection;

static PyTypeObject ConnectionType;

typedef struct _vtableinfo
{
  PyObject *datasource;           /* object with create/connect methods */
  Connection *connection;         /* the Connection this is registered against so we don't
				     have to have a global table mapping sqlite3_db* to
				     Connection* */
} vtableinfo;

/* forward declarations */
struct APSWBlob;
static void APSWBlob_init(struct APSWBlob *self, Connection *connection, sqlite3_blob *blob);
static PyTypeObject APSWBlobType;

struct APSWCursor;
static void APSWCursor_init(struct APSWCursor *, Connection *);
static PyTypeObject APSWCursorType;

struct ZeroBlobBind;
static PyTypeObject ZeroBlobBindType;



static void
FunctionCBInfo_dealloc(FunctionCBInfo *self)
{
  if(self->name)
    PyMem_Free(self->name);
  Py_CLEAR(self->scalarfunc);
  Py_CLEAR(self->aggregatefactory);
  Py_TYPE(self)->tp_free((PyObject*)self);
}


/** .. class:: Connection


  This object wraps a `sqlite3 pointer
  <http://www.sqlite.org/c3ref/sqlite3.html>`_.
*/

/* CONNECTION CODE */

static void
Connection_internal_cleanup(Connection *self)
{
  Py_CLEAR(self->functions);
  Py_CLEAR(self->busyhandler);
  Py_CLEAR(self->rollbackhook);
  Py_CLEAR(self->profile);
  Py_CLEAR(self->updatehook);
  Py_CLEAR(self->commithook);
  Py_CLEAR(self->progresshandler);
  Py_CLEAR(self->authorizer);
  Py_CLEAR(self->collationneeded);
  Py_CLEAR(self->exectrace);
  Py_CLEAR(self->rowtrace);
  Py_CLEAR(self->vfs);
  Py_CLEAR(self->filename);
  Py_CLEAR(self->open_flags);
  Py_CLEAR(self->open_vfs);
}

/** .. method:: close([force=False])

  Closes the database.  If there are any outstanding :class:`cursors
  <Cursor>` or :class:`blobs <blob>` then they are closed too.  It is
  normally not necessary to call this method as the database is
  automatically closed when there are no more references.  It is ok to
  call the method multiple times.

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
  database.

  If *force* is *True* then any exceptions are ignored.

   -* sqlite3_close
*/

/* Closes cursors and blobs belonging to this connection */
static PyObject *
Connection_close(Connection *self, PyObject *args)
{
  int res;
  int force=0;
  Py_ssize_t i;

  if(!self->db)
    goto finally;

  CHECK_USE(NULL);

  assert(!PyErr_Occurred());

  if(!PyArg_ParseTuple(args, "|i:close(force=False)", &force))
    return NULL;

  /* Traverse dependents calling close.  This won't work too well if
     calling close perturbs the list. */
  for(i=0; i<PyList_GET_SIZE(self->dependents); i++)
    {
      PyObject *item, *closeres;

      item=PyWeakref_GetObject(PyList_GET_ITEM(self->dependents, i));
      if(!item || item==Py_None)
        continue;
      
      closeres=Call_PythonMethodV(item, "close", 1, "(i)", force);
      Py_XDECREF(closeres);
      if(!closeres)
        return NULL;
    }
      
  statementcache_free(self->stmtcache);
  self->stmtcache=0;

  PYSQLITE_VOID_CALL(
    APSW_FAULT_INJECT(ConnectionCloseFail, res=sqlite3_close(self->db), res=SQLITE_IOERR)
    );

  if (res!=SQLITE_OK) 
    {
      SET_EXC(res, NULL);
    }

  if(PyErr_Occurred())
    {
      AddTraceBackHere(__FILE__, __LINE__, "Connection.close", NULL);
    }

  /* note: SQLite ignores error returns from vtabDisconnect, so the
     database still ends up closed and we return an exception! */

  if(res!=SQLITE_OK)
      return NULL;

  self->db=0;

  Connection_internal_cleanup(self);

 finally:
  if(PyErr_Occurred())
    return NULL;
  Py_RETURN_NONE;
}

static void
Connection_dealloc(Connection* self)
{
  if(self->db)
    {
      int res;

      if(self->stmtcache)
        {
          statementcache_free(self->stmtcache);
          self->stmtcache=0;
        }

      PYSQLITE_VOID_CALL(
        APSW_FAULT_INJECT(DestructorCloseFail, res=sqlite3_close(self->db), res=SQLITE_IOERR);
        );
      self->db=0;

      if(res!=SQLITE_OK)
        {
          /* not allowed to clobber existing exception */
          PyObject *etype=NULL, *evalue=NULL, *etraceback=NULL;
          PyErr_Fetch(&etype, &evalue, &etraceback);

          PyErr_Format(ExcConnectionNotClosed, 
                       "apsw.Connection at address %p. The destructor "
                       "has encountered an error %d closing the connection, but cannot raise an exception.",
                       self, res);
          
          apsw_write_unraiseable(NULL);
          PyErr_Restore(etype, evalue, etraceback);
        }
    }

  /* Our dependents all hold a refcount on us, so they must have all
     released before this destructor could be called */
  assert(PyList_GET_SIZE(self->dependents)==0);
  Py_CLEAR(self->dependents);
  Py_DECREF(self->dependent_remove);

  Connection_internal_cleanup(self);

  Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject*
Connection_new(PyTypeObject *type, APSW_ARGUNUSED PyObject *args, APSW_ARGUNUSED PyObject *kwds)
{
    Connection *self;

    self = (Connection *)type->tp_alloc(type, 0);
    if (self != NULL) {
      self->db=0;
      self->inuse=0;
      self->dependents=PyList_New(0);
      self->dependent_remove=PyObject_GetAttrString(self->dependents, "remove");
      self->stmtcache=0;
      self->functions=PyList_New(0);
      self->busyhandler=0;
      self->rollbackhook=0;
      self->profile=0;
      self->updatehook=0;
      self->commithook=0;
      self->progresshandler=0;
      self->authorizer=0;
      self->collationneeded=0;
      self->exectrace=0;
      self->rowtrace=0;
      self->vfs=0;
      self->filename=0;
      self->open_flags=0;
      self->open_vfs=0;
    }

    return (PyObject *)self;
}


/** .. method:: __init__(filename, flags=SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, vfs=None, statementcachesize=100)

  Opens the named database.  You can use ``:memory:`` to get a private temporary
  in-memory database that is not shared with any other connections.

  :param flags: One or more of the `open flags <http://www.sqlite.org/c3ref/c_open_create.html>`_ orred together
  :param vfs: The name of the `vfs <http://www.sqlite.org/c3ref/vfs.html>`_ to use.  If :const:`None` then the default
     vfs will be used.

  :param statementcachesize: Use zero to disable the statement cache,
    or a number larger than the total distinct SQL statements you
    execute frequently.

  -* sqlite3_open_v2

  .. seealso::

    * :ref:`statementcache`
    * :ref:`vfs`

*/
/* forward declaration so we can tell if it is one of ours */
static int apswvfs_xAccess(sqlite3_vfs *vfs, const char *zName, int flags, int *pResOut);

static int
Connection_init(Connection *self, PyObject *args, PyObject *kwds)
{
  static char *kwlist[]={"filename", "flags", "vfs", "statementcachesize", NULL};
  PyObject *hooks=NULL, *hook=NULL, *iterator=NULL, *hookargs=NULL, *hookresult=NULL;
  char *filename=NULL;
  int res=0;
  int flags=SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
  char *vfs=0;
  int statementcachesize=100;
  sqlite3_vfs *vfsused=0;

  if(!PyArg_ParseTupleAndKeywords(args, kwds, "es|izi:Connection(filename, flags=SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, vfs=None, statementcachesize=100)", kwlist, STRENCODING, &filename, &flags, &vfs, &statementcachesize))
    return -1;
  
  if(statementcachesize<0)
    statementcachesize=0;

  /* Technically there is a race condition as a vfs of the same name
     could be registered between our find and the open starting.
     Don't do that!  We also have to manage the error message thread
     safety manually as self->db is null on entry. */
  PYSQLITE_VOID_CALL(
    vfsused=sqlite3_vfs_find(vfs); res=sqlite3_open_v2(filename, &self->db, flags, vfs); if(res!=SQLITE_OK) apsw_set_errmsg(sqlite3_errmsg(self->db));
    );
  SET_EXC(res, self->db);  /* nb sqlite3_open always allocates the db even on error */
  
  if(res!=SQLITE_OK)
      goto pyexception;
    
  if(vfsused && vfsused->xAccess==apswvfs_xAccess)
    {
      PyObject *pyvfsused=(PyObject*)(vfsused->pAppData);
      Py_INCREF(pyvfsused);
      self->vfs=pyvfsused;
    }

  /* record information */
  self->filename=convertutf8string(filename);
  self->open_flags=PyInt_FromLong(flags);
  self->open_vfs=convertutf8string(vfsused->zName);

  /* get detailed error codes */
  PYSQLITE_VOID_CALL(sqlite3_extended_result_codes(self->db, 1));
  
  /* call connection hooks */
  hooks=PyObject_GetAttrString(apswmodule, "connection_hooks");
  if(!hooks)
    goto pyexception;

  hookargs=Py_BuildValue("(O)", self);
  if(!hookargs) goto pyexception;

  iterator=PyObject_GetIter(hooks);
  if(!iterator)
    {
      AddTraceBackHere(__FILE__, __LINE__, "Connection.__init__", "{s: O}", "connection_hooks", hooks);
      goto pyexception;
    }

  while( (hook=PyIter_Next(iterator)) )
    {
      hookresult=PyEval_CallObject(hook, hookargs);
      if(!hookresult) 
	goto pyexception;
      Py_DECREF(hook);
      Py_DECREF(hookresult);
    }

  if(!PyErr_Occurred())
    {
      res=0;
      self->stmtcache=statementcache_init(self->db, statementcachesize);
      if(self->stmtcache)
        goto finally;
    }

 pyexception:
  /* clean up db since it is useless - no need for user to call close */
  res= -1;
  sqlite3_close(self->db);  /* PYSQLITE_CALL not needed since noone else can have a reference to this connection */
  self->db=0;
  Connection_internal_cleanup(self);

finally:
  if(filename) PyMem_Free(filename);
  Py_XDECREF(hookargs);
  Py_XDECREF(iterator);
  Py_XDECREF(hooks);
  Py_XDECREF(hook);
  Py_XDECREF(hookresult);
  assert(PyErr_Occurred() || res==0);
  return res;
}

/** .. method:: blobopen(database, table, column, rowid, writeable)  -> blob

   Opens a blob for :ref:`incremental I/O <blobio>`.

   :param database: Name of the database.  This will be ``main`` for
     the main connection and the name you specified for `attached
     <http://www.sqlite.org/lang_attach.html>`_ databases.
   :param table: The name of the table
   :param column: The name of the column
   :param rowid: The id that uniquely identifies the row.
   :param writeable: If True then you can read and write the blob.  If False then you can only read it.

   :rtype: :class:`blob`

   .. seealso::

     * :ref:`Blob I/O example <example-blobio>`
     * `SQLite row ids <http://www.sqlite.org/autoinc.html>`_

   -* sqlite3_blob_open
*/
static PyObject *
Connection_blobopen(Connection *self, PyObject *args)
{
  struct APSWBlob *apswblob=0;
  sqlite3_blob *blob=0;
  const char *dbname, *tablename, *column;
  long long rowid;
  int writing;
  int res;
  PyObject *weakref;

  CHECK_USE(NULL);
  CHECK_CLOSED(self, NULL);
  
  if(!PyArg_ParseTuple(args, "esesesLi:blobopen(database, table, column, rowid, rd_wr)", 
                       STRENCODING, &dbname, STRENCODING, &tablename, STRENCODING, &column, &rowid, &writing))
    return NULL;

  PYSQLITE_CON_CALL(res=sqlite3_blob_open(self->db, dbname, tablename, column, rowid, writing, &blob));

  PyMem_Free((void*)dbname);
  PyMem_Free((void*)tablename);
  PyMem_Free((void*)column);
  SET_EXC(res, self->db);
  if(res!=SQLITE_OK)
    return NULL;
  
  APSW_FAULT_INJECT(BlobAllocFails,apswblob=PyObject_New(struct APSWBlob, &APSWBlobType), (PyErr_NoMemory(), apswblob=NULL));
  if(!apswblob)
    {
      PYSQLITE_CON_CALL(sqlite3_blob_close(blob));
      return NULL;
    }


  APSWBlob_init(apswblob, self, blob);
  weakref=PyWeakref_NewRef((PyObject*)apswblob, self->dependent_remove);
  PyList_Append(self->dependents, weakref);
  Py_DECREF(weakref);
  return (PyObject*)apswblob;
}

/** .. method:: cursor() -> Cursor

  Creates a new :class:`Cursor` object on this database.

  :rtype: :class:`Cursor`
*/
static PyObject *
Connection_cursor(Connection *self)
{
  struct APSWCursor* cursor = NULL;
  PyObject *weakref;

  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);

  APSW_FAULT_INJECT(CursorAllocFails,cursor = PyObject_New(struct APSWCursor, &APSWCursorType), (PyErr_NoMemory(), cursor=NULL));
  if(!cursor)
    return NULL;

  /* incref me since cursor holds a pointer */
  Py_INCREF((PyObject*)self);
  APSWCursor_init(cursor, self);
  weakref=PyWeakref_NewRef((PyObject*)cursor, self->dependent_remove);
  PyList_Append(self->dependents, weakref);
  Py_DECREF(weakref);
  
  return (PyObject*)cursor;
}

/** .. method:: setbusytimeout(millseconds)

  If the database is locked such as when another connection is making
  changes, SQLite will keep retrying.  This sets the maximum amount of
  time SQLite will keep retrying before giving up.  If the database is
  still busy then :class:`apsw.BusyError` will be returned.

  :param milliseconds: Maximum thousandths of a second to wait.

  If you previously called :meth:`~Connection.setbusyhandler` then
  calling this overrides that.

  .. seealso::

     * :meth:`Connection.setbusyhandler`

  -* sqlite3_busy_timeout
*/
static PyObject *
Connection_setbusytimeout(Connection *self, PyObject *args)
{
  int ms=0;
  int res;

  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);

  if(!PyArg_ParseTuple(args, "i:setbusytimeout(millseconds)", &ms))
    return NULL;

  PYSQLITE_CON_CALL(res=sqlite3_busy_timeout(self->db, ms));
  SET_EXC(res, self->db);
  if(res!=SQLITE_OK) return NULL;
  
  /* free any explicit busyhandler we may have had */
  Py_XDECREF(self->busyhandler);
  self->busyhandler=0;

  Py_RETURN_NONE;
}

/** .. method:: changes() -> int

  Returns the number of database rows that were changed (or inserted
  or deleted) by the most recently completed INSERT, UPDATE, or DELETE
  statement.

  -* sqlite3_changes
*/
static PyObject *
Connection_changes(Connection *self)
{
  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);
  return PyLong_FromLong(sqlite3_changes(self->db));
}

/** .. method:: totalchanges() -> int

  Returns the total number of database rows that have be modified,
  inserted, or deleted since the database connection was opened.

  -* sqlite3_total_changes
*/
static PyObject *
Connection_totalchanges(Connection *self)
{
  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);
  return PyLong_FromLong(sqlite3_total_changes(self->db));
}

/** .. method:: getautocommit() -> bool

  Returns if the Connection is in auto commit mode (ie not in a transaction).

  -* sqlite3_get_autocommit
*/
static PyObject *
Connection_getautocommit(Connection *self)
{
  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);
  if (sqlite3_get_autocommit(self->db))
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

/** .. method:: last_insert_rowid() -> int

  Returns the integer key of the most recent insert in the database.

  -* sqlite3_last_insert_rowid
*/
static PyObject *
Connection_last_insert_rowid(Connection *self)
{
  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);

  return PyLong_FromLongLong(sqlite3_last_insert_rowid(self->db));
}

/** .. method:: complete(statement) -> bool

  Returns True if the input string comprises one or more complete SQL
  statements by looking for an unquoted trailing semi-colon.

  An example use would be if you were prompting the user for SQL
  statements and needed to know if you had a whole statement, or
  needed to ask for another line::

    statement=raw_input("SQL> ")
    while not apsw.complete(statement):
       more=raw_input("  .. ")
       statement=statement+"\n"+more

  -* sqlite3_complete
*/
static PyObject *
Connection_complete(Connection *self, PyObject *args)
{
  char *statements=NULL;
  int res;

  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);
  
  if(!PyArg_ParseTuple(args, "es:complete(statement)", STRENCODING, &statements))
    return NULL;

  res=sqlite3_complete(statements);

  PyMem_Free(statements);

  if(res)
    {
      Py_INCREF(Py_True);
      return Py_True;
    }
  Py_INCREF(Py_False);
  return Py_False;
}

/** .. method:: interrupt()

  Causes any pending operations on the database to abort at the
  earliest opportunity. You can call this from any thread.  For
  example you may have a long running query when the user presses the
  stop button in your user interface.  :exc:`InterruptError`
  will be raised in the query that got interrupted.

  -* sqlite3_interrupt
*/
static PyObject *
Connection_interrupt(Connection *self)
{
  CHECK_CLOSED(self, NULL);

  sqlite3_interrupt(self->db);  /* no return value */
  Py_RETURN_NONE;
}

/** .. method:: limit(id[, newval]) -> int

  If called with one parameter then the current limit for that *id* is
  returned.  If called with two then the limit is set to *newval*.


  :param id: One of the `runtime limit ids <http://www.sqlite.org/c3ref/c_limit_attached.html>`_
  :param newval: The new limit.  This is a 32 bit signed integer even on 64 bit platforms.

  :returns: The limit in place on entry to the call.

  -* sqlite3_limit

  .. seealso::

    * :ref:`Example <example-limit>`

*/
#ifdef EXPERIMENTAL
static PyObject *
Connection_limit(Connection *self, PyObject *args)
{
  int val=-1, res, id;
  CHECK_USE(NULL);
  CHECK_CLOSED(self, NULL);
  if(!PyArg_ParseTuple(args, "i|i", &id, &val))
    return NULL;

  res=sqlite3_limit(self->db, id, val);

  return PyLong_FromLong(res);
}
#endif

static void
updatecb(void *context, int updatetype, char const *databasename, char const *tablename, sqlite3_int64 rowid)
{
  /* The hook returns void. That makes it impossible for us to
     abort immediately due to an error in the callback */
  
  PyGILState_STATE gilstate;
  PyObject *retval=NULL;
  Connection *self=(Connection *)context;

  assert(self);
  assert(self->updatehook);
  assert(self->updatehook!=Py_None);

  gilstate=PyGILState_Ensure();

  if(PyErr_Occurred())
    goto finally;  /* abort hook due to outstanding exception */

  retval=PyObject_CallFunction(self->updatehook, "(iO&O&L)", updatetype, convertutf8string, databasename, convertutf8string, tablename, rowid);

 finally:
  Py_XDECREF(retval);
  PyGILState_Release(gilstate);
}

/** .. method:: setupdatehook(callable)

  Calls *callable* whenever a row is updated, deleted or inserted.  If
  *callable* is :const:`None` then any existing update hook is
  removed.  The update hook cannot make changes to the database while
  the query is still executing, but can record them for later use or
  apply them in a different connection.

  The update hook is called with 4 paramaters: 

    type (int)
      :const:`SQLITE_INSERT`, :const:`SQLITE_DELETE` or :const:`SQLITE_UPDATE`
    database name (string)
      This is ``main`` for the database or the name specified in
      `ATTACH <http://sqlite.org/lang_attach.html>`_
    table name (string)
      The table on which the update happened
    rowid (64 bit integer)
      The affected row

  .. seealso::

      * :ref:`Example <example-updatehook>`

  -* sqlite3_update_hook
*/
static PyObject *
Connection_setupdatehook(Connection *self, PyObject *callable)
{
  /* sqlite3_update_hook doesn't return an error code */
  
  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);

  if(callable==Py_None)
    {
      PYSQLITE_VOID_CALL(sqlite3_update_hook(self->db, NULL, NULL));
      callable=NULL;
      goto finally;
    }

  if(!PyCallable_Check(callable))
    {
      PyErr_Format(PyExc_TypeError, "update hook must be callable");
      return NULL;
    }

  PYSQLITE_VOID_CALL(sqlite3_update_hook(self->db, updatecb, self));

  Py_INCREF(callable);

 finally:

  Py_XDECREF(self->updatehook);
  self->updatehook=callable;

  Py_RETURN_NONE;
}

static void
rollbackhookcb(void *context)
{
  /* The hook returns void. That makes it impossible for us to
     abort immediately due to an error in the callback */
  
  PyGILState_STATE gilstate;
  PyObject *retval=NULL;
  Connection *self=(Connection *)context;

  assert(self);
  assert(self->rollbackhook);
  assert(self->rollbackhook!=Py_None);

  gilstate=PyGILState_Ensure();

  APSW_FAULT_INJECT(RollbackHookExistingError,,PyErr_NoMemory());

  if(PyErr_Occurred())
    goto finally;  /* abort hook due to outstanding exception */

  retval=PyEval_CallObject(self->rollbackhook, NULL);

 finally:
  Py_XDECREF(retval);
  PyGILState_Release(gilstate);
}

/** .. method:: setrollbackhook(callable)

  Sets a callable which is invoked during a rollback.  If *callable*
  is :const:`None` then any existing rollback hook is removed.

  The *callable* is called with no parameters and the return value is ignored.

  -* sqlite3_rollback_hook
*/
static PyObject *
Connection_setrollbackhook(Connection *self, PyObject *callable)
{
  /* sqlite3_rollback_hook doesn't return an error code */
  
  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);

  if(callable==Py_None)
    {
      PYSQLITE_VOID_CALL(sqlite3_rollback_hook(self->db, NULL, NULL));
      callable=NULL;
      goto finally;
    }

  if(!PyCallable_Check(callable))
    {
      PyErr_Format(PyExc_TypeError, "rollback hook must be callable");
      return NULL;
    }

  PYSQLITE_VOID_CALL(sqlite3_rollback_hook(self->db, rollbackhookcb, self));

  Py_INCREF(callable);

 finally:

  Py_XDECREF(self->rollbackhook);
  self->rollbackhook=callable;

  Py_RETURN_NONE;
}

#ifdef EXPERIMENTAL /* sqlite3_profile */
static void
profilecb(void *context, const char *statement, sqlite_uint64 runtime)
{
  /* The hook returns void. That makes it impossible for us to
     abort immediately due to an error in the callback */
  
  PyGILState_STATE gilstate;
  PyObject *retval=NULL;
  Connection *self=(Connection *)context;

  assert(self);
  assert(self->profile);
  assert(self->profile!=Py_None);

  gilstate=PyGILState_Ensure();

  if(PyErr_Occurred())
    goto finally;  /* abort hook due to outstanding exception */

  retval=PyObject_CallFunction(self->profile, "(O&K)", convertutf8string, statement, runtime);

 finally:
  Py_XDECREF(retval);
  PyGILState_Release(gilstate);
}

/** .. method:: setprofile(callable)

  Sets a callable which is invoked at the end of execution of each
  statement and passed the statement string and how long it took to
  execute. (The execution time is in nanoseconds.) Note that it is
  called only on completion. If for example you do a ``SELECT`` and
  only read the first result, then you won't reach the end of the
  statement.

  -* sqlite3_profile
*/

static PyObject *
Connection_setprofile(Connection *self, PyObject *callable)
{
  /* sqlite3_profile doesn't return an error code */
  
  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);

  if(callable==Py_None)
    {
      PYSQLITE_VOID_CALL(sqlite3_profile(self->db, NULL, NULL));
      callable=NULL;
      goto finally;
    }

  if(!PyCallable_Check(callable))
    {
      PyErr_Format(PyExc_TypeError, "profile function must be callable");
      return NULL;
    }

  PYSQLITE_VOID_CALL(sqlite3_profile(self->db, profilecb, self));

  Py_INCREF(callable);

 finally:

  Py_XDECREF(self->profile);
  self->profile=callable;

  Py_RETURN_NONE;
}
#endif /* EXPERIMENTAL - sqlite3_profile */


#ifdef EXPERIMENTAL      /* commit hook */
static int 
commithookcb(void *context)
{
  /* The hook returns 0 for commit to go ahead and non-zero to abort
     commit (turn into a rollback). We return non-zero for errors */
  
  PyGILState_STATE gilstate;
  PyObject *retval=NULL;
  int ok=1; /* error state */
  Connection *self=(Connection *)context;

  assert(self);
  assert(self->commithook);
  assert(self->commithook!=Py_None);

  gilstate=PyGILState_Ensure();

  APSW_FAULT_INJECT(CommitHookExistingError,,PyErr_NoMemory());

  if(PyErr_Occurred())
    goto finally;  /* abort hook due to outstanding exception */

  retval=PyEval_CallObject(self->commithook, NULL);

  if(!retval)
    goto finally; /* abort hook due to exeception */

  ok=PyObject_IsTrue(retval);
  assert(ok==-1 || ok==0 || ok==1);
  if(ok==-1)
    {
      ok=1;
      goto finally;  /* abort due to exception in return value */
    }

 finally:
  Py_XDECREF(retval);
  PyGILState_Release(gilstate);
  return ok;
}

/** .. method:: setcommithook(callable)

  *callable* will be called just before a commit.  It should return
  zero for the commit to go ahead and non-zero for it to be turned
  into a rollback. In the case of an exception in your callable, a
  non-zero (ie rollback) value is returned. 

  .. seealso::

    * :ref:`Example <example-commithook>`

  -* sqlite3_commit_hook

*/
static PyObject *
Connection_setcommithook(Connection *self, PyObject *callable)
{
  /* sqlite3_commit_hook doesn't return an error code */
  
  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);

  if(callable==Py_None)
    {
      PYSQLITE_VOID_CALL(sqlite3_commit_hook(self->db, NULL, NULL));
      callable=NULL;
      goto finally;
    }

  if(!PyCallable_Check(callable))
    {
      PyErr_Format(PyExc_TypeError, "commit hook must be callable");
      return NULL;
    }

  PYSQLITE_VOID_CALL(sqlite3_commit_hook(self->db, commithookcb, self));

  Py_INCREF(callable);

 finally:

  Py_XDECREF(self->commithook);
  self->commithook=callable;

  Py_RETURN_NONE;
}
#endif  /* EXPERIMENTAL sqlite3_commit_hook */

#ifdef EXPERIMENTAL      /* sqlite3_progress_handler */
static int 
progresshandlercb(void *context)
{
  /* The hook returns 0 for continue and non-zero to abort (rollback).
     We return non-zero for errors */
  
  PyGILState_STATE gilstate;
  PyObject *retval=NULL;
  int ok=1; /* error state */
  Connection *self=(Connection *)context;

  assert(self);
  assert(self->progresshandler);

  gilstate=PyGILState_Ensure();

  retval=PyEval_CallObject(self->progresshandler, NULL);

  if(!retval)
    goto finally; /* abort due to exeception */

  ok=PyObject_IsTrue(retval);

  assert(ok==-1 || ok==0 || ok==1);
  if(ok==-1)
    {
      ok=1;
      goto finally;  /* abort due to exception in result */
    }

 finally:
  Py_XDECREF(retval);

  PyGILState_Release(gilstate);
  return ok;
}

/** .. method:: setprogresshandler(callable[, nsteps=20])

  Sets a callable which is invoked every *nsteps* SQLite
  inststructions. The callable should return a non-zero value to abort
  or zero to continue. (If there is an error in your Python *callable*
  then non-zero will be returned).

  .. seealso::

     * :ref:`Example <example-progress-handler>`

  -* sqlite3_progress_handler
*/

static PyObject *
Connection_setprogresshandler(Connection *self, PyObject *args)
{
  /* sqlite3_progress_handler doesn't return an error code */
  int nsteps=20;
  PyObject *callable=NULL;
  
  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);

  if(!PyArg_ParseTuple(args, "O|i:setprogresshandler(callable, nsteps=20)", &callable, &nsteps))
    return NULL;

  if(callable==Py_None)
    {
      PYSQLITE_VOID_CALL(sqlite3_progress_handler(self->db, 0, NULL, NULL));
      callable=NULL;
      goto finally;
    }

  if(!PyCallable_Check(callable))
    {
      PyErr_Format(PyExc_TypeError, "progress handler must be callable");
      return NULL;
    }

  PYSQLITE_VOID_CALL(sqlite3_progress_handler(self->db, nsteps, progresshandlercb, self));
  Py_INCREF(callable);

 finally:

  Py_XDECREF(self->progresshandler);
  self->progresshandler=callable;

  Py_RETURN_NONE;
}
#endif  /* EXPERIMENTAL sqlite3_progress_handler */

static int 
authorizercb(void *context, int operation, const char *paramone, const char *paramtwo, const char *databasename, const char *triggerview)
{
  /* should return one of SQLITE_OK, SQLITE_DENY, or
     SQLITE_IGNORE. (0, 1 or 2 respectively) */

  PyGILState_STATE gilstate;
  PyObject *retval=NULL;
  int result=SQLITE_DENY;  /* default to deny */
  Connection *self=(Connection *)context;

  assert(self);
  assert(self->authorizer);
  assert(self->authorizer!=Py_None);

  gilstate=PyGILState_Ensure();

  APSW_FAULT_INJECT(AuthorizerExistingError,,PyErr_NoMemory());

  if(PyErr_Occurred())
    goto finally;  /* abort due to earlier exception */

  retval=PyObject_CallFunction(self->authorizer, "(iO&O&O&O&)", operation, convertutf8string, paramone, 
                               convertutf8string, paramtwo, convertutf8string, databasename, 
                               convertutf8string, triggerview);

  if(!retval)
    goto finally; /* abort due to exeception */

  if (PyIntLong_Check(retval))
    {
      result=PyIntLong_AsLong(retval);
      goto haveval;
    }
  
  PyErr_Format(PyExc_TypeError, "Authorizer must return a number");
  AddTraceBackHere(__FILE__, __LINE__, "authorizer callback", "{s: i, s: s:, s: s, s: s}",
                   "operation", operation, "paramone", paramone, "paramtwo", paramtwo, 
                   "databasename", databasename, "triggerview", triggerview);

 haveval:
  if (PyErr_Occurred())
    result=SQLITE_DENY;

 finally:
  Py_XDECREF(retval);

  PyGILState_Release(gilstate);
  return result;
}

/** .. method:: setauthorizer(callable)

  While `preparing <http://www.sqlite.org/c3ref/prepare.html>`_
  statements, SQLite will call any defined authorizer to see if a
  particular action is ok to be part of the statement.

  Typical usage would be if you are running user supplied SQL and want
  to prevent harmful operations.  You should also
  set the :class:`statementcachesize <Connection>` to zero.

  The authorizer callback has 5 parameters:

    * An `operation code <http://www.sqlite.org/c3ref/c_alter_table.html>`_
    * A string (or None) dependent on the operation `(listed as 3rd) <http://www.sqlite.org/c3ref/c_alter_table.html>`_
    * A string (or None) dependent on the operation `(listed as 4th) <http://www.sqlite.org/c3ref/c_alter_table.html>`_
    * A string name of the database (or None)
    * Name of the innermost trigger or view doing the access (or None)

  The authorizer callback should return one of :const:`SQLITE_OK`,
  :const:`SQLITE_DENY` or :const:`SQLITE_IGNORE`.
  (:const:`SQLITE_DENY` is returned if there is an error in your
  Python code).

  .. seealso::
 
    * :ref:`Example <authorizer-example>`
    * :ref:`statementcache`

  -* sqlite3_set_authorizer
*/

static PyObject *
Connection_setauthorizer(Connection *self, PyObject *callable)
{
  int res;

  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);

  if(callable==Py_None)
    {
      APSW_FAULT_INJECT(SetAuthorizerNullFail,
                        PYSQLITE_CON_CALL(res=sqlite3_set_authorizer(self->db, NULL, NULL)),
                        res=SQLITE_IOERR);
      if(res!=SQLITE_OK)
        {
          SET_EXC(res, self->db);
          return NULL;
        }
      callable=NULL;
      goto finally;
    }

  if(!PyCallable_Check(callable))
    {
      PyErr_Format(PyExc_TypeError, "authorizer must be callable");
      return NULL;
    }

  APSW_FAULT_INJECT(SetAuthorizerFail,
                    PYSQLITE_CON_CALL(res=sqlite3_set_authorizer(self->db, authorizercb, self)),
                    res=SQLITE_IOERR);
  if(res!=SQLITE_OK)
    {
      SET_EXC(res, self->db);
      return NULL;
    }

  Py_INCREF(callable);

 finally:
  Py_XDECREF(self->authorizer);
  self->authorizer=callable;

  Py_RETURN_NONE;
}

static void
collationneeded_cb(void *pAux, APSW_ARGUNUSED sqlite3 *db, int eTextRep, const char *name)
{
  PyObject *res=NULL, *pyname=NULL;
  Connection *self=(Connection*)pAux;
  PyGILState_STATE gilstate=PyGILState_Ensure();

  assert(self->collationneeded);
  if(!self->collationneeded) goto finally;
  if(PyErr_Occurred()) goto finally;
  pyname=convertutf8string(name);
  if(pyname)  res=PyEval_CallFunction(self->collationneeded, "(OO)", self, pyname);
  if(!pyname || !res)
    AddTraceBackHere(__FILE__, __LINE__, "collationneeded callback", "{s: O, s: i, s: s}",
                     "Connection", self, "eTextRep", eTextRep, "name", name);
  Py_XDECREF(res);

 finally:
  Py_XDECREF(pyname);
  PyGILState_Release(gilstate);
}

/** .. method:: collationneeded(callable)

  *callable* will be called if a statement requires a `collation
  <http://en.wikipedia.org/wiki/Collation>`_ that hasn't been
  registered. Your callable will be passed two parameters. The first
  is the connection object. The second is the name of the
  collation. If you have the collation code available then call
  :meth:`Connection.createcollation`.

  This is useful for creating collations on demand.  For example you
  may include the `locale <http://en.wikipedia.org/wiki/Locale>`_ in
  the collation name, but since there are thousands of locales in
  popular use it would not be useful to :meth:`prereigster
  <Connection.createcollation>` them all.  Using
  :meth:`~Connection.collationneeded` tells you when you need to
  register them.

  .. seealso::

    * :meth:`~Connection.createcollation`

  -* sqlite3_collation_needed
*/
static PyObject *
Connection_collationneeded(Connection *self, PyObject *callable)
{
  int res;

  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);

  if(callable==Py_None)
    {
      APSW_FAULT_INJECT(CollationNeededNullFail,
                        PYSQLITE_CON_CALL(res=sqlite3_collation_needed(self->db, NULL, NULL)),
                        res=SQLITE_IOERR);
      if(res!=SQLITE_OK)
        {
          SET_EXC(res, self->db);
          return NULL;
        }
      callable=NULL;
      goto finally;
    }

  if(!PyCallable_Check(callable))
    {
      PyErr_Format(PyExc_TypeError, "collationneeded callback must be callable");
      return NULL;
    }

  APSW_FAULT_INJECT(CollationNeededFail,
                    PYSQLITE_CON_CALL(res=sqlite3_collation_needed(self->db, self, collationneeded_cb)), 
                    res=SQLITE_IOERR);
  if(res!=SQLITE_OK)
    {
      SET_EXC(res, self->db);
      return NULL;
    }

  Py_INCREF(callable);

 finally:
  Py_XDECREF(self->collationneeded);
  self->collationneeded=callable;

  Py_RETURN_NONE;
}

static int 
busyhandlercb(void *context, int ncall)
{
  /* Return zero for caller to get SQLITE_BUSY error. We default to
     zero in case of error. */

  PyGILState_STATE gilstate;
  PyObject *retval;
  int result=0;  /* default to fail with SQLITE_BUSY */
  Connection *self=(Connection *)context;

  assert(self);
  assert(self->busyhandler);

  gilstate=PyGILState_Ensure();

  retval=PyObject_CallFunction(self->busyhandler, "i", ncall);

  if(!retval)
    goto finally; /* abort due to exeception */

  result=PyObject_IsTrue(retval);
  assert(result==-1 || result==0 || result==1);
  Py_DECREF(retval);

  if(result==-1)
    {
      result=0;
      goto finally;  /* abort due to exception converting retval */
    }

 finally:
  PyGILState_Release(gilstate);
  return result;
}

/** .. method:: setbusyhandler(callable)

   Sets the busy handler to callable. callable will be called with one
   integer argument which is the number of prior calls to the busy
   callback for the same lock. If the busy callback returns something
   that evaluates to False, then SQLite returns :const:`SQLITE_BUSY` to the
   calling code.. If the callback returns something that evaluates to
   True, then SQLite tries to open the table again and the cycle
   repeats.

   If you previously called :meth:`~Connection.setbusytimeout` then
   calling this overrides that.

   .. seealso::

     * :meth:`Connection.setbusytimeout`

   -* sqlite3_busy_handler

*/
static PyObject *
Connection_setbusyhandler(Connection *self, PyObject *callable)
{
  int res=SQLITE_OK;

  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);

  if(callable==Py_None)
    {
      APSW_FAULT_INJECT(SetBusyHandlerNullFail,
                        PYSQLITE_CON_CALL(res=sqlite3_busy_handler(self->db, NULL, NULL)),
                        res=SQLITE_IOERR);
      if(res!=SQLITE_OK)
        {
          SET_EXC(res, self->db);
          return NULL;
        }
      callable=NULL;
      goto finally;
    }

  if(!PyCallable_Check(callable))
    {
      PyErr_Format(PyExc_TypeError, "busyhandler must be callable");
      return NULL;
    }

  APSW_FAULT_INJECT(SetBusyHandlerFail,
                    PYSQLITE_CON_CALL(res=sqlite3_busy_handler(self->db, busyhandlercb, self)),
                    res=SQLITE_IOERR);
  if(res!=SQLITE_OK)
    {
      SET_EXC(res, self->db);
      return NULL;
    }

  Py_INCREF(callable);

 finally:
  Py_XDECREF(self->busyhandler);
  self->busyhandler=callable;

  Py_RETURN_NONE;
}

#if defined(EXPERIMENTAL) && !defined(SQLITE_OMIT_LOAD_EXTENSION)  /* extension loading */

/** .. method:: enableloadextension(enable)

  Enables/disables `extension loading
  <http://www.sqlite.org/cvstrac/wiki/wiki?p=LoadableExtensions>`_
  which is disabled by default.

  :param enable: If True then extension loading is enabled, else it is disabled.

  -* sqlite3_enable_load_extension

  .. seealso::

    * :meth:`~Connection.loadextension`
*/

static PyObject *
Connection_enableloadextension(Connection *self, PyObject *enabled)
{
  int enabledp, res;

  CHECK_USE(NULL);
  CHECK_CLOSED(self, NULL);

  /* get the boolean value */
  enabledp=PyObject_IsTrue(enabled);
  if(enabledp==-1) return NULL;
  if (PyErr_Occurred()) return NULL;

  /* call function */
  APSW_FAULT_INJECT(EnableLoadExtensionFail, 
                    PYSQLITE_CON_CALL(res=sqlite3_enable_load_extension(self->db, enabledp)),
                    res=SQLITE_IOERR);
  SET_EXC(res, self->db);

  /* done */
  if (res==SQLITE_OK)
    Py_RETURN_NONE;
  return NULL;
}

/** .. method:: loadextension(filename[, entrypoint])

  Loads *filename* as an `extension <http://www.sqlite.org/cvstrac/wiki/wiki?p=LoadableExtensions>`_

  :param filename: The file to load.  This must be Unicode or Unicode compatible

  :param entrypoint: The initialization method to call.  If this
    parameter is not supplied then the SQLite default of
    ``sqlite3_extension_init`` is used.

  :raises ExtensionLoadingError: If the extension could not be
    loaded.  The exception string includes more details.

  .. seealso::

    * :meth:`~Connection.enableloadextension`
*/
static PyObject *
Connection_loadextension(Connection *self, PyObject *args)
{
  int res;
  char *zfile=NULL, *zproc=NULL, *errmsg=NULL;

  CHECK_USE(NULL);
  CHECK_CLOSED(self, NULL);
  
  if(!PyArg_ParseTuple(args, "es|z:loadextension(filename, entrypoint=None)", STRENCODING, &zfile, &zproc))
    return NULL;

  PYSQLITE_CON_CALL(res=sqlite3_load_extension(self->db, zfile, zproc, &errmsg));

  PyMem_Free(zfile);

  /* load_extension doesn't set the error message on the db so we have to make exception manually */
  if(res!=SQLITE_OK)
    {
      assert(errmsg);
      PyErr_Format(ExcExtensionLoading, "ExtensionLoadingError: %s", errmsg?errmsg:"unspecified");
      sqlite3_free(errmsg);
      return NULL;
    }
  Py_RETURN_NONE;
}

#endif /* EXPERIMENTAL extension loading */


/* USER DEFINED FUNCTION CODE.*/
static PyTypeObject FunctionCBInfoType =
  {
    APSW_PYTYPE_INIT
    "apsw.FunctionCBInfo",     /*tp_name*/
    sizeof(FunctionCBInfo),    /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)FunctionCBInfo_dealloc, /*tp_dealloc*/ 
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_VERSION_TAG, /*tp_flags*/
    "FunctionCBInfo object",   /* tp_doc */
    0,		               /* tp_traverse */
    0,		               /* tp_clear */
    0,		               /* tp_richcompare */
    0,		               /* tp_weaklistoffset */
    0,		               /* tp_iter */
    0,		               /* tp_iternext */
    0,                         /* tp_methods */
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    0,                         /* tp_init */
    0,                         /* tp_alloc */
    0,                         /* tp_new */
    0,                         /* tp_free */
    0,                         /* tp_is_gc */
    0,                         /* tp_bases */
    0,                         /* tp_mro */
    0,                         /* tp_cache */
    0,                         /* tp_subclasses */
    0,                         /* tp_weaklist */
    0                          /* tp_del */
    APSW_PYTYPE_VERSION
  };


static FunctionCBInfo *
allocfunccbinfo(void)
{
  FunctionCBInfo *res=PyObject_New(FunctionCBInfo, &FunctionCBInfoType);
  if(res)
    {
      res->name=0;
      res->scalarfunc=0;
      res->aggregatefactory=0;
    }
  return res;
}


/* converts a python object into a sqlite3_context result */
static void
set_context_result(sqlite3_context *context, PyObject *obj)
{
  if(!obj)
    {
      assert(PyErr_Occurred());
      sqlite3_result_error_code(context, MakeSqliteMsgFromPyException(NULL));
      sqlite3_result_error(context, "bad object given to set_context_result", -1);
      return;
    }

  /* DUPLICATE(ish) code: this is substantially similar to the code in
     APSWCursor_dobinding.  If you fix anything here then do it there as
     well. */

  if(obj==Py_None)
    {
      sqlite3_result_null(context);
      return;
    }
#if PY_MAJOR_VERSION < 3
  if(PyInt_Check(obj))
    {
      sqlite3_result_int64(context, PyInt_AS_LONG(obj));
      return;
    }
#endif
  if (PyLong_Check(obj))
    {
      sqlite3_result_int64(context, PyLong_AsLongLong(obj));
      return;
    }
  if (PyFloat_Check(obj))
    {
      sqlite3_result_double(context, PyFloat_AS_DOUBLE(obj));
      return;
    }
  if (PyUnicode_Check(obj))
    {
      UNIDATABEGIN(obj)
        APSW_FAULT_INJECT(SetContextResultUnicodeConversionFails,,strdata=(char*)PyErr_NoMemory());
        if(strdata)
          {
#ifdef APSW_TEST_LARGE_OBJECTS
            APSW_FAULT_INJECT(SetContextResultLargeUnicode,,strbytes=0x001234567890L);
#endif
	    if(strbytes>APSW_INT32_MAX)
	      {
                SET_EXC(SQLITE_TOOBIG, NULL);
                sqlite3_result_error_toobig(context);
	      }
	    else
              USE16(sqlite3_result_text)(context, strdata, strbytes, SQLITE_TRANSIENT);
          }
        else
          sqlite3_result_error(context, "Unicode conversions failed", -1);
      UNIDATAEND(obj);
      return;
    }
#if PY_MAJOR_VERSION < 3
  if (PyString_Check(obj))
    {
      const char *val=PyString_AS_STRING(obj);
      const Py_ssize_t lenval=PyString_GET_SIZE(obj);
      const char *chk=val;
      /* check if string is all ascii if less than 10kb in size */
      if(lenval<10000)
        for(;chk<val+lenval && !((*chk)&0x80); chk++);
      /* Non-ascii or long, so convert to unicode */
      if(chk<val+lenval)
        {
          PyObject *str2=PyUnicode_FromObject(obj);
          if(!str2)
            {
              sqlite3_result_error(context, "PyUnicode_FromObject failed", -1);
              return;
            }
          UNIDATABEGIN(str2)
            APSW_FAULT_INJECT(SetContextResultStringUnicodeConversionFails,,strdata=(char*)PyErr_NoMemory());
            if(strdata)
              {
#ifdef APSW_TEST_LARGE_OBJECTS
                APSW_FAULT_INJECT(SetContextResultLargeString,,strbytes=0x001234567890L);
#endif
		if(strbytes>APSW_INT32_MAX)
		  {
                    SET_EXC(SQLITE_TOOBIG, NULL);
                    sqlite3_result_error_toobig(context);
		  }
		else
                  USE16(sqlite3_result_text)(context, strdata, strbytes, SQLITE_TRANSIENT);
              }
            else
              sqlite3_result_error(context, "Unicode conversions failed", -1);
          UNIDATAEND(str2);
          Py_DECREF(str2);
        }
      else /* just ascii chars */
        sqlite3_result_text(context, val, lenval, SQLITE_TRANSIENT);

      return;
    }
#endif
  if (PyObject_CheckReadBuffer(obj))
    {
      const void *buffer;
      Py_ssize_t buflen;
      int asrb=PyObject_AsReadBuffer(obj, &buffer, &buflen);

      APSW_FAULT_INJECT(SetContextResultAsReadBufferFail,,(PyErr_NoMemory(),asrb=-1));

      if(asrb!=0)
        {
          sqlite3_result_error(context, "PyObject_AsReadBuffer failed", -1);
          return;
        }
      if (buflen>APSW_INT32_MAX)
	sqlite3_result_error_toobig(context);
      else
	sqlite3_result_blob(context, buffer, buflen, SQLITE_TRANSIENT);
      return;
    }

  PyErr_Format(PyExc_TypeError, "Bad return type from function callback");
  sqlite3_result_error(context, "Bad return type from function callback", -1);
}

/* Returns a new reference to a tuple formed from function parameters */
static PyObject *
getfunctionargs(sqlite3_context *context, PyObject *firstelement, int argc, sqlite3_value **argv)
{
  PyObject *pyargs=NULL;
  int i;
  int extra=0;

  /* extra first item */
  if(firstelement)
    extra=1;

  APSW_FAULT_INJECT(GFAPyTuple_NewFail,pyargs=PyTuple_New((long)argc+extra),pyargs=PyErr_NoMemory());
  if(!pyargs)
    {
      sqlite3_result_error(context, "PyTuple_New failed", -1);
      goto error;
    }

  if(extra)
    {
      Py_INCREF(firstelement);
      PyTuple_SET_ITEM(pyargs, 0, firstelement);
    }

  for(i=0;i<argc;i++)
    {
      PyObject *item=convert_value_to_pyobject(argv[i]);
      if(!item)
        {
          sqlite3_result_error(context, "convert_value_to_pyobject failed", -1);
          goto error;
        }
      PyTuple_SET_ITEM(pyargs, i+extra, item);
    }
  
  return pyargs;

 error:
  Py_XDECREF(pyargs);
  return NULL;
}


/* dispatches scalar function */
static void
cbdispatch_func(sqlite3_context *context, int argc, sqlite3_value **argv)
{
  PyGILState_STATE gilstate;
  PyObject *pyargs=NULL;
  PyObject *retval=NULL;
  FunctionCBInfo *cbinfo=(FunctionCBInfo*)sqlite3_user_data(context);
  assert(cbinfo);

  gilstate=PyGILState_Ensure();

  assert(cbinfo->scalarfunc);


  APSW_FAULT_INJECT(CBDispatchExistingError,,PyErr_NoMemory());

  if(PyErr_Occurred())
    {
      sqlite3_result_error_code(context, MakeSqliteMsgFromPyException(NULL));
      sqlite3_result_error(context, "Prior Python Error", -1);
      goto finalfinally;
    }

  pyargs=getfunctionargs(context, NULL, argc, argv);
  if(!pyargs)
      goto finally;

  assert(!PyErr_Occurred());
  retval=PyEval_CallObject(cbinfo->scalarfunc, pyargs);
  if(retval)
    set_context_result(context, retval);

 finally:
  if (PyErr_Occurred())
    {
      char *errmsg=NULL;
      char *funname=sqlite3_mprintf("user-defined-scalar-%s", cbinfo->name);
      sqlite3_result_error_code(context, MakeSqliteMsgFromPyException(&errmsg));
      sqlite3_result_error(context, errmsg, -1);
      AddTraceBackHere(__FILE__, __LINE__, funname, "{s: i, s: s}", "NumberOfArguments", argc, "message", errmsg);
      sqlite3_free(funname);
      sqlite3_free(errmsg);
    }
 finalfinally:
  Py_XDECREF(pyargs);
  Py_XDECREF(retval);
  
  PyGILState_Release(gilstate);
}

static aggregatefunctioncontext *
getaggregatefunctioncontext(sqlite3_context *context)
{
  aggregatefunctioncontext *aggfc=sqlite3_aggregate_context(context, sizeof(aggregatefunctioncontext));
  FunctionCBInfo *cbinfo;
  PyObject *retval;
  /* have we seen it before? */
  if(aggfc->aggvalue) 
    return aggfc;
  
  /* fill in with Py_None so we know it is valid */
  aggfc->aggvalue=Py_None;
  Py_INCREF(Py_None);

  cbinfo=(FunctionCBInfo*)sqlite3_user_data(context);
  assert(cbinfo);
  assert(cbinfo->aggregatefactory);

  /* call the aggregatefactory to get our working objects */
  retval=PyEval_CallObject(cbinfo->aggregatefactory, NULL);

  if(!retval)
    return aggfc;
  /* it should have returned a tuple of 3 items: object, stepfunction and finalfunction */
  if(!PyTuple_Check(retval))
    {
      PyErr_Format(PyExc_TypeError, "Aggregate factory should return tuple of (object, stepfunction, finalfunction)");
      goto finally;
    }
  if(PyTuple_GET_SIZE(retval)!=3)
    {
      PyErr_Format(PyExc_TypeError, "Aggregate factory should return 3 item tuple of (object, stepfunction, finalfunction)");
      goto finally;
    }
  /* we don't care about the type of the zeroth item (object) ... */

  /* stepfunc */
  if (!PyCallable_Check(PyTuple_GET_ITEM(retval,1)))
    {
      PyErr_Format(PyExc_TypeError, "stepfunction must be callable");
      goto finally;
    }
  
  /* finalfunc */
  if (!PyCallable_Check(PyTuple_GET_ITEM(retval,2)))
    {
      PyErr_Format(PyExc_TypeError, "final function must be callable");
      goto finally;
    }

  aggfc->aggvalue=PyTuple_GET_ITEM(retval,0);
  aggfc->stepfunc=PyTuple_GET_ITEM(retval,1);
  aggfc->finalfunc=PyTuple_GET_ITEM(retval,2);

  Py_INCREF(aggfc->aggvalue);
  Py_INCREF(aggfc->stepfunc);
  Py_INCREF(aggfc->finalfunc);
      
  Py_DECREF(Py_None);  /* we used this earlier as a sentinel */

 finally:
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
  PyObject *pyargs;
  PyObject *retval;
  aggregatefunctioncontext *aggfc=NULL;

  gilstate=PyGILState_Ensure();

  if (PyErr_Occurred())
    goto finalfinally;

  aggfc=getaggregatefunctioncontext(context);

  if (PyErr_Occurred())
    goto finally;

  assert(aggfc);
  
  pyargs=getfunctionargs(context, aggfc->aggvalue, argc, argv);
  if(!pyargs)
    goto finally;

  assert(!PyErr_Occurred());
  retval=PyEval_CallObject(aggfc->stepfunc, pyargs);
  Py_DECREF(pyargs);
  Py_XDECREF(retval);

  if(!retval)
    {
      assert(PyErr_Occurred());
    }

 finally:
  if(PyErr_Occurred())
    {
      char *funname=0;
      FunctionCBInfo *cbinfo=(FunctionCBInfo*)sqlite3_user_data(context);
      assert(cbinfo);
      funname=sqlite3_mprintf("user-defined-aggregate-step-%s", cbinfo->name);
      AddTraceBackHere(__FILE__, __LINE__, funname, "{s: i}", "NumberOfArguments", argc);
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
  PyObject *retval=NULL;
  aggregatefunctioncontext *aggfc=NULL;
  PyObject *err_type=NULL, *err_value=NULL, *err_traceback=NULL;

  gilstate=PyGILState_Ensure();

  PyErr_Fetch(&err_type, &err_value, &err_traceback);

  aggfc=getaggregatefunctioncontext(context);
  assert(aggfc);

  APSW_FAULT_INJECT(CBDispatchFinalError,,PyErr_NoMemory());
  
  if((err_type||err_value||err_traceback) || PyErr_Occurred() || !aggfc->finalfunc)
    {
      sqlite3_result_error(context, "Prior Python Error in step function", -1);
      goto finally;
    }

  retval=PyObject_CallFunctionObjArgs(aggfc->finalfunc, aggfc->aggvalue, NULL);
  set_context_result(context, retval);
  Py_XDECREF(retval);

 finally:
  /* we also free the aggregatefunctioncontext here */
  assert(aggfc->aggvalue);  /* should always be set, perhaps to Py_None */
  Py_XDECREF(aggfc->aggvalue);
  Py_XDECREF(aggfc->stepfunc);
  Py_XDECREF(aggfc->finalfunc);

  if(PyErr_Occurred() && (err_type||err_value||err_traceback))
    {
      PyErr_Format(PyExc_Exception, "An exception happened during cleanup of an aggregate function, but there was already error in the step function so only that can be returned");
      apsw_write_unraiseable(NULL);
    }

  if(err_type||err_value||err_traceback)
    PyErr_Restore(err_type, err_value, err_traceback);

  if(PyErr_Occurred())
    {
      char *funname=0;
      FunctionCBInfo *cbinfo=(FunctionCBInfo*)sqlite3_user_data(context);
      assert(cbinfo);
      funname=sqlite3_mprintf("user-defined-aggregate-final-%s", cbinfo->name);
      AddTraceBackHere(__FILE__, __LINE__, funname, NULL);
      sqlite3_free(funname);
    }

  /* sqlite3 frees the actual underlying memory we used (aggfc itself) */

  PyGILState_Release(gilstate);
}

/** .. method:: createscalarfunction(name, callable[, numargs=-1])

  Registers a scalar function.  Scalar functions operate on one set of paramaters once.  

  :param name: The string name of the function.  It should be less than 255 characters
  :param callable: The function that will be called
  :param numargs: How many arguments the function takes, with -1 meaning any number

  .. note:: 

    You can register the same named function but with different
    *callable* and *numargs*.  For example::

      connection.createscalarfunction("toip", ipv4convert, 4)
      connection.createscalarfunction("toip", ipv6convert, 16)
      connection.createscalarfunction("toip", strconvert, -1)

    The one with the correct *numargs* will be called and only if that
    doesn't exist then the one with negative *numargs* will be called.

  .. seealso::

     * :ref:`Example <scalar-example>`
     * :meth:`~Connection.createaggregatefunction`

  -* sqlite3_create_function
*/

static PyObject *
Connection_createscalarfunction(Connection *self, PyObject *args)
{
  int numargs=-1;
  PyObject *callable;
  char *name=0;
  char *chk;
  FunctionCBInfo *cbinfo;
  int res;
 
  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);

  if(!PyArg_ParseTuple(args, "esO|i:createscalarfunction(name,callback, numargs=-1)", STRENCODING, &name, &callable, &numargs))
    return NULL;

  assert(name);
  assert(callable);

  /* there isn't a C api to get a (potentially unicode) string and
     make it uppercase so we hack around  */

  /* validate the name */
  for(chk=name;*chk && !((*chk)&0x80);chk++);
  if(*chk)
    {
      PyMem_Free(name);
      PyErr_SetString(PyExc_TypeError, "function name must be ascii characters only");
      return NULL;
    }

  /* convert name to upper case */
  for(chk=name;*chk;chk++)
    if(*chk>='a' && *chk<='z')
      *chk-='a'-'A';

  if(callable!=Py_None && !PyCallable_Check(callable))
    {
      PyMem_Free(name);
      PyErr_SetString(PyExc_TypeError, "parameter must be callable");
      return NULL;
    }

  if(callable==Py_None)
    {
      cbinfo=0;
    }
  else
    {
      cbinfo=allocfunccbinfo();
      if(!cbinfo) goto finally;
      cbinfo->name=name;
      cbinfo->scalarfunc=callable;
      Py_INCREF(callable);
    }

  PYSQLITE_CON_CALL(
                res=sqlite3_create_function(self->db,
                                            name,
                                            numargs,
                                            SQLITE_UTF8,
                                            cbinfo,
                                            cbinfo?cbdispatch_func:NULL,
                                            NULL,
                                            NULL)
                );
  if(callable==Py_None)
    PyMem_Free(name);

  if(res)
    {
      SET_EXC(res, self->db);
      goto finally;
    }

  if(cbinfo)
    PyList_Append(self->functions, (PyObject*)cbinfo);
  
 finally:
  /* cbinfo will be copied into list on success else we need to dump
     it anyway */
  Py_XDECREF(cbinfo);
  if(PyErr_Occurred())
    return NULL;
  Py_RETURN_NONE;
}

/** .. method:: createaggregatefunction(name, factory[, numargs=-1])

  Registers an aggregate function.  Aggregate functions operate on all
  the relevant rows such as counting how many there are.

  :param name: The string name of the function.  It should be less than 255 characters
  :param callable: The function that will be called
  :param numargs: How many arguments the function takes, with -1 meaning any number

  When a query starts, the *factory* will be called and must return a tuple of 3 items:

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
    :meth:`~Connection.createscalarfunction` for an example.

  .. seealso::

     * :ref:`Example <aggregate-example>`
     * :meth:`~Connection.createscalarfunction`

  -* sqlite3_create_function
*/

static PyObject *
Connection_createaggregatefunction(Connection *self, PyObject *args)
{
  int numargs=-1;
  PyObject *callable;
  char *name=0;
  char *chk;
  FunctionCBInfo *cbinfo;
  int res;

  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);

  if(!PyArg_ParseTuple(args, "esO|i:createaggregatefunction(name, factorycallback, numargs=-1)", STRENCODING, &name, &callable, &numargs))
    return NULL;

  assert(name);
  assert(callable);

  /* there isn't a C api to get a (potentially unicode) string and make it uppercase so we hack around  */

  /* validate the name */
  for(chk=name;*chk && !((*chk)&0x80);chk++);
  if(*chk)
    {
      PyMem_Free(name);
      PyErr_SetString(PyExc_TypeError, "function name must be ascii characters only");
      return NULL;
    }

  /* convert name to upper case */
  for(chk=name;*chk;chk++)
    if(*chk>='a' && *chk<='z')
      *chk-='a'-'A';

  if(callable!=Py_None && !PyCallable_Check(callable))
    {
      PyMem_Free(name);
      PyErr_SetString(PyExc_TypeError, "parameter must be callable");
      return NULL;
    }

  if(callable==Py_None)
    cbinfo=0;
  else
    {
      cbinfo=allocfunccbinfo();
      if(!cbinfo) goto finally;

      cbinfo->name=name;
      cbinfo->aggregatefactory=callable;
      Py_INCREF(callable);
    }

  PYSQLITE_CON_CALL(
                res=sqlite3_create_function(self->db,
                                            name,
                                            numargs,
                                            SQLITE_UTF8,  /* it isn't very clear what this parameter does */
                                            cbinfo,
                                            NULL,
                                            cbinfo?cbdispatch_step:NULL,
                                            cbinfo?cbdispatch_final:NULL)
                );

  if(callable==Py_None)
    PyMem_Free(name);

  if(res)
    {
      SET_EXC(res, self->db);
      goto finally;
    }

  if(callable!=Py_None)
    /* put cbinfo into the list */
    PyList_Append(self->functions, (PyObject*)cbinfo);
    

 finally:
  Py_XDECREF(cbinfo);
  if(PyErr_Occurred())
    return NULL;
  Py_RETURN_NONE;
}

/* USER DEFINED COLLATION CODE.*/

static int 
collation_cb(void *context, 
	     int stringonelen, const void *stringonedata,
	     int stringtwolen, const void *stringtwodata)
{
  PyGILState_STATE gilstate;
  PyObject *cbinfo=(PyObject*)context;
  PyObject *pys1=NULL, *pys2=NULL, *retval=NULL;
  int result=0;

  assert(cbinfo);

  gilstate=PyGILState_Ensure();

  if(PyErr_Occurred()) goto finally;  /* outstanding error */

  pys1=convertutf8stringsize(stringonedata, stringonelen);
  pys2=convertutf8stringsize(stringtwodata, stringtwolen);

  if(!pys1 || !pys2)  
    goto finally;   /* failed to allocate strings */

  retval=PyObject_CallFunction(cbinfo, "(OO)", pys1, pys2);

  if(!retval) 
    {
      AddTraceBackHere(__FILE__, __LINE__, "Collation_callback", "{s: O, s: O, s: O}", "callback", cbinfo, "stringone", pys1, "stringtwo", pys2);
      goto finally;  /* execution failed */
    }

  if (PyIntLong_Check(retval))
    {
      result=PyIntLong_AsLong(retval);
      goto haveval;
    }
  
  PyErr_Format(PyExc_TypeError, "Collation callback must return a number");
  AddTraceBackHere(__FILE__, __LINE__, "collation callback", "{s: O, s: O}",
                   "stringone", pys1, "stringtwo", pys2);

 haveval:
  if(PyErr_Occurred())
      result=0;

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
  PyGILState_STATE gilstate=PyGILState_Ensure();
  Py_DECREF((PyObject*)context);
  PyGILState_Release(gilstate);
}

/** .. method:: createcollation(name, callback)

  You can control how SQLite sorts (termed `collation
  <http://en.wikipedia.org/wiki/Collation>`_) when giving the
  ``COLLATE`` term to a `SELECT
  <http://www.sqlite.org/lang_select.html>`_.  For example your
  collation could take into account locale or do numeric sorting.

  The *callback* will be called with two items.  It should return -1
  if the first is less then the second, 0 if they are equal, and 1 if
  first is greater::

     def mycollation(one, two):
         if one < two:
             return -1
         if one == two:
             return 0
         if one > two:
             return 1

  .. seealso::

    * :ref:`Example <collation-example>`

  -* sqlite3_create_collation_v2
*/

static PyObject *
Connection_createcollation(Connection *self, PyObject *args)
{
  PyObject *callable=NULL;
  char *name=0;
  int res;

  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);
  
  if(!PyArg_ParseTuple(args, "esO:createcollation(name,callback)", STRENCODING, &name, &callable))
      return NULL;

  assert(name);
  assert(callable);

  if(callable!=Py_None && !PyCallable_Check(callable))
    {
      PyMem_Free(name);
      PyErr_SetString(PyExc_TypeError, "parameter must be callable");
      return NULL;
    }

  PYSQLITE_CON_CALL(
                res=sqlite3_create_collation_v2(self->db,
                                                name,
                                                SQLITE_UTF8,
                                                (callable!=Py_None)?callable:NULL,
                                                (callable!=Py_None)?collation_cb:NULL,
                                                (callable!=Py_None)?collation_destroy:NULL)
                );
  PyMem_Free(name);
  if(res!=SQLITE_OK)
    {
      SET_EXC(res, self->db);
      return NULL;
    }

  if (callable!=Py_None)
    Py_INCREF(callable);
  
  Py_RETURN_NONE;
}

/** .. method:: filecontrol(op, pointer)

  Calls the :meth:`~VFSFile.xFileControl` method on the :ref:`VFS`
  implementing :class:`file access <VFSFile>` for the database.

  :param op: A `numeric code
    <http://sqlite.org/c3ref/c_fcntl_lockstate.html>`_ with values less
    than 100 reserved for SQLite internal use.
  :param pointer: A number which is treated as a ``void pointer`` at the C level.

  The method does not return anything.  If you want data returned back
  then the *pointer* needs to point to something mutable.  Here is an
  example using `ctypes
  <http://www.python.org/doc/2.5.2/lib/module-ctypes.html>`_ of
  passing a Python dictionary to :meth:`~VFSFile.xFileControl` which
  can then modify the dictionary to set return values::

    obj={"foo": 1, 2: 3}                 # object we want to pass
    objwrap=ctypes.py_object(obj)        # objwrap must live before and after the call else
                                         # it gets garbage collected
    connection.filecontrol(
             123,                        # our op code
             ctypes.addressof(objwrap))  # get pointer

  The :meth:`~VFSFile.xFileControl` method then looks like this::

    def xFileControl(self, op, pointer):
        if op==123:                      # our op code
            obj=ctypes.py_object.from_address(pointer).value
            # play with obj - you can use id() to verify it is the same
            print obj["foo"]
            obj["result"]="it worked"
        else:
            raise Exception("Unknown file control "+`op`)

  -* sqlite3_file_control
*/

static PyObject *
Connection_filecontrol(Connection *self, PyObject *args)
{
  PyObject *pyptr;
  void *ptr=NULL;
  int res, op;
  char *dbname=NULL;

  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);

  if(!PyArg_ParseTuple(args, "esiO", STRENCODING, &dbname, &op, &pyptr))
    return NULL;

  if(PyIntLong_Check(pyptr))
    ptr=PyLong_AsVoidPtr(pyptr);
  else
    PyErr_Format(PyExc_TypeError, "Argument is not a number (pointer)");

  if(PyErr_Occurred())
    {
      AddTraceBackHere(__FILE__, __LINE__, "Connection.filecontrol", "{s: O}", "args", args);
      goto finally;
    }

  PYSQLITE_CON_CALL(res=sqlite3_file_control(self->db, dbname, op, ptr));

  SET_EXC(res, self->db);

 finally:
  if(dbname) PyMem_Free(dbname);
  
  if(PyErr_Occurred())
    return NULL;

  Py_RETURN_NONE;
}

/** .. method:: sqlite3pointer() -> int

  Returns the underlying `sqlite3 *
  <http://sqlite.org/c3ref/sqlite3.html>`_ for the connection. This
  method is useful if there are other C level libraries in the same
  process and you want them to use the APSW connection handle. The
  value is returned as a number using :meth:`PyLong_FromVoidPtr` under the
  hood. You should also ensure that you increment the reference count on
  the :class:`Connection` for as long as the other libraries are using
  the pointer.  It is also a very good idea to call
  :meth:`sqlitelibversion` and ensure it is the same as the other
  libraries.

*/
static PyObject*
Connection_sqlite3pointer(Connection *self)
{
  CHECK_USE(NULL);
  CHECK_CLOSED(self, NULL);

  return PyLong_FromVoidPtr(self->db);
}

static struct sqlite3_module apsw_vtable_module;
static void vtabFree(void *context);

/** .. method:: createmodule(name, datasource)
    
    Registers a virtual table.  See :ref:`virtualtables` for details.

    .. seealso::

       * :ref:`Example <example-vtable>`

    -* sqlite3_create_module_v2
*/
static PyObject *
Connection_createmodule(Connection *self, PyObject *args)
{
  char *name=NULL;
  PyObject *datasource=NULL;
  vtableinfo *vti;
  int res;

  CHECK_USE(NULL);
  CHECK_CLOSED(self, NULL);

  if(!PyArg_ParseTuple(args, "esO:createmodule(name, datasource)", STRENCODING, &name, &datasource))
    return NULL;

  Py_INCREF(datasource);
  vti=PyMem_Malloc(sizeof(vtableinfo));
  vti->connection=self;
  vti->datasource=datasource;

  /* ::TODO:: - can we call this with NULL to unregister a module? */
  APSW_FAULT_INJECT(CreateModuleFail, 
                    PYSQLITE_CON_CALL(res=sqlite3_create_module_v2(self->db, name, &apsw_vtable_module, vti, vtabFree)), 
                    res=SQLITE_IOERR);
  PyMem_Free(name);
  SET_EXC(res, self->db);

  if(res!=SQLITE_OK)
    {
      Py_DECREF(datasource);
      PyMem_Free(vti);
      return NULL;
    }

  Py_RETURN_NONE;
}

/** .. method:: overloadfunction(name, nargs)

  Registers a placeholder function so that a virtual table can provide an implementation via
  :meth:`VTTable.FindFunction`.

  :param name: Function name
  :param nargs: How many arguments the function takes

  Due to `SQLite ticket 3507
  <http://www.sqlite.org/cvstrac/tktview?tn=3507>`_ underlying errors
  will not be returned.

  -* sqlite3_overload_function
*/
static PyObject*
Connection_overloadfunction(Connection *self, PyObject *args)
{
  char *name;
  int nargs, res;

  CHECK_USE(NULL);
  CHECK_CLOSED(self, NULL);

  if(!PyArg_ParseTuple(args, "esi:overloadfunction(name, nargs)", STRENCODING, &name, &nargs))
    return NULL;

  APSW_FAULT_INJECT(OverloadFails,
                    PYSQLITE_CON_CALL(res=sqlite3_overload_function(self->db, name, nargs)),
                    res=SQLITE_NOMEM);
  PyMem_Free(name);

  SET_EXC(res, self->db);

  if(res)
    return NULL;
  
  Py_RETURN_NONE;
}

/** .. method:: setexectrace(callable)

  *callable* is called with the cursor, statement and bindings for
  each :meth:`~Cursor.execute` or :meth:`~Cursor.executemany` on this
  Connection, unless the :class:`Cursor` installed its own
  tracer. Your execution tracer can also abort execution of a
  statement.

  If *callable* is :const:`None` then any existing execution tracer is
  removed.

  .. seealso::

    * :ref:`tracing`
    * :ref:`rowtracer`
    * :meth:`Cursor.setexectrace`
*/

static PyObject *
Connection_setexectrace(Connection *self, PyObject *func)
{
  CHECK_USE(NULL);
  CHECK_CLOSED(self, NULL);

  if(func!=Py_None && !PyCallable_Check(func))
    {
      PyErr_SetString(PyExc_TypeError, "parameter must be callable");
      return NULL;
    }

  if(func!=Py_None)
    Py_INCREF(func);
  Py_XDECREF(self->exectrace);
  self->exectrace=(func==Py_None)?0:func;

  Py_RETURN_NONE;
}


/** .. method:: setrowtrace(callable)

  *callable* is called with each row being returned for
  :class:`cursors <Cursor>` associated with this Connection, unless
  the Cursor installed its own tracer.  You can change the data that
  is returned or cause the row to be skipped altogether.

  If *callable* is :const:`None` then any existing row tracer is
  removed.

  .. seealso::

    * :ref:`tracing`
    * :ref:`rowtracer`
    * :meth:`Cursor.setexectrace`  
*/

static PyObject *
Connection_setrowtrace(Connection *self, PyObject *func)
{
  CHECK_USE(NULL);
  CHECK_CLOSED(self, NULL);

  if(func!=Py_None && !PyCallable_Check(func))
    {
      PyErr_SetString(PyExc_TypeError, "parameter must be callable");
      return NULL;
    }

  if(func!=Py_None)
    Py_INCREF(func);
  Py_XDECREF(self->rowtrace);
  self->rowtrace=(func==Py_None)?0:func;

  Py_RETURN_NONE;
}

/** .. method:: getexectrace() -> callable or None

  Returns the currently installed (via :meth:`~Connection.setexectrace`)
  execution tracer.

  .. seealso::

    * :ref:`tracing`
*/
static PyObject *
Connection_getexectrace(Connection *self)
{
  PyObject *ret;

  CHECK_USE(NULL);
  CHECK_CLOSED(self, NULL);

  ret=(self->exectrace)?(self->exectrace):Py_None;
  Py_INCREF(ret);
  return ret;
}

/** .. method:: getrowtrace() -> callable or None

  Returns the currently installed (via :meth:`~Connection.setrowtrace`)
  row tracer.

  .. seealso::

    * :ref:`tracing`
*/
static PyObject *
Connection_getrowtrace(Connection *self)
{
  PyObject *ret;

  CHECK_USE(NULL);
  CHECK_CLOSED(self, NULL);

  ret=(self->rowtrace)?(self->rowtrace):Py_None;
  Py_INCREF(ret);
  return ret;
}

/** .. attribute:: filename

  The filename used to open the database.
*/

/** .. attribute:: open_flags

  The integer flags used to open the database.
*/

/** .. attribute:: open_vfs

  The string name of the vfs used to open the database.
*/

static PyMemberDef Connection_members[] = {
  /* name type offset flags doc */
  {"filename", T_OBJECT, offsetof(Connection, filename), READONLY, "Filename connection was opened with"},
  {"open_flags", T_OBJECT, offsetof(Connection, open_flags), READONLY, "list of [flagsin, flagsout] used to open connection"},
  {"open_vfs", T_OBJECT, offsetof(Connection, open_vfs), READONLY, "VFS name used to open database"},
  {0, 0, 0, 0, 0}
};

static PyMethodDef Connection_methods[] = {
  {"cursor", (PyCFunction)Connection_cursor, METH_NOARGS,
   "Create a new cursor" },
  {"close",  (PyCFunction)Connection_close, METH_VARARGS,
   "Closes the connection" },
  {"setbusytimeout", (PyCFunction)Connection_setbusytimeout, METH_VARARGS,
   "Sets the sqlite busy timeout in milliseconds.  Use zero to disable the timeout"},
  {"interrupt", (PyCFunction)Connection_interrupt, METH_NOARGS,
   "Causes any pending database operations to abort at the earliest opportunity"},
  {"createscalarfunction", (PyCFunction)Connection_createscalarfunction, METH_VARARGS,
   "Creates a scalar function"},
  {"createaggregatefunction", (PyCFunction)Connection_createaggregatefunction, METH_VARARGS,
   "Creates an aggregate function"},
  {"setbusyhandler", (PyCFunction)Connection_setbusyhandler, METH_O,
   "Sets the busy handler"},
  {"changes", (PyCFunction)Connection_changes, METH_NOARGS, 
   "Returns the number of rows changed by last query"},
  {"totalchanges", (PyCFunction)Connection_totalchanges, METH_NOARGS, 
   "Returns the total number of changes to database since it was opened"},
  {"getautocommit", (PyCFunction)Connection_getautocommit, METH_NOARGS, 
   "Returns if the database is in auto-commit mode"},
  {"createcollation", (PyCFunction)Connection_createcollation, METH_VARARGS,
   "Creates a collation function"},
  {"last_insert_rowid", (PyCFunction)Connection_last_insert_rowid, METH_NOARGS,
   "Returns rowid for last insert"},
  {"complete", (PyCFunction)Connection_complete, METH_VARARGS,
   "Checks if a SQL statement is complete"},
  {"collationneeded", (PyCFunction)Connection_collationneeded, METH_O,
   "Sets collation needed callback"},
  {"setauthorizer", (PyCFunction)Connection_setauthorizer, METH_O,
   "Sets an authorizer function"},
  {"setupdatehook", (PyCFunction)Connection_setupdatehook, METH_O,
      "Sets an update hook"},
  {"setrollbackhook", (PyCFunction)Connection_setrollbackhook, METH_O,
   "Sets a callable invoked before each rollback"},
  {"blobopen", (PyCFunction)Connection_blobopen, METH_VARARGS,
   "Opens a blob for i/o"},
#ifdef EXPERIMENTAL
  {"limit", (PyCFunction)Connection_limit, METH_VARARGS,
   "Gets and sets limits"},
  {"setprofile", (PyCFunction)Connection_setprofile, METH_O,
   "Sets a callable invoked with profile information after each statement"},
  {"setcommithook", (PyCFunction)Connection_setcommithook, METH_O,
   "Sets a callable invoked before each commit"},
  {"setprogresshandler", (PyCFunction)Connection_setprogresshandler, METH_VARARGS,
   "Sets a callback invoked periodically during long running calls"},
#if !defined(SQLITE_OMIT_LOAD_EXTENSION)
  {"enableloadextension", (PyCFunction)Connection_enableloadextension, METH_O,
   "Enables loading of SQLite extensions from shared libraries"},
  {"loadextension", (PyCFunction)Connection_loadextension, METH_VARARGS,
   "loads SQLite extension"},
#endif
  {"createmodule", (PyCFunction)Connection_createmodule, METH_VARARGS,
   "registers a virtual table"},
#endif
  {"filecontrol", (PyCFunction)Connection_filecontrol, METH_VARARGS,
   "file control"},
  {"sqlite3pointer", (PyCFunction)Connection_sqlite3pointer, METH_NOARGS,
   "gets underlying pointer"},
  {"overloadfunction", (PyCFunction)Connection_overloadfunction, METH_VARARGS,
   "overloads function for virtual table"},
  {"setexectrace", (PyCFunction)Connection_setexectrace, METH_O,
   "Installs a function called for every statement executed"},
  {"setrowtrace", (PyCFunction)Connection_setrowtrace, METH_O,
   "Installs a function called for every row returned"},
  {"getexectrace", (PyCFunction)Connection_getexectrace, METH_NOARGS,
   "Returns the current exec tracer function"},
  {"getrowtrace", (PyCFunction)Connection_getrowtrace, METH_NOARGS,
   "Returns the current row tracer function"},
  {0, 0, 0, 0}  /* Sentinel */
};


static PyTypeObject ConnectionType = 
  {
    APSW_PYTYPE_INIT
    "apsw.Connection",         /*tp_name*/
    sizeof(Connection),        /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)Connection_dealloc, /*tp_dealloc*/ 
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_VERSION_TAG, /*tp_flags*/
    "Connection object",       /* tp_doc */
    0,		               /* tp_traverse */
    0,		               /* tp_clear */
    0,		               /* tp_richcompare */
    0,		               /* tp_weaklistoffset */
    0,		               /* tp_iter */
    0,		               /* tp_iternext */
    Connection_methods,        /* tp_methods */
    Connection_members,        /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)Connection_init, /* tp_init */
    0,                         /* tp_alloc */
    Connection_new,            /* tp_new */
    0,                         /* tp_free */
    0,                         /* tp_is_gc */
    0,                         /* tp_bases */
    0,                         /* tp_mro */
    0,                         /* tp_cache */
    0,                         /* tp_subclasses */
    0,                         /* tp_weaklist */
    0                          /* tp_del */
    APSW_PYTYPE_VERSION
};

