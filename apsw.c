/*
  Another Python Sqlite Wrapper

  This wrapper aims to be the minimum necessary layer over SQLite 3
  itself.

  It assumes we are running as 32 bit int with a 64 bit long long type
  available.

  Copyright (C) 2004-2006 Roger Binns <rogerb@rogerbinns.com>

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

/* Get the version number */
#include "apswversion.h"

/* Python headers */
#include <Python.h>
#include <pythread.h>
#include "structmember.h"

/* Python 2.5 compatibility when size_t types become 64 bit.
   SQLite3 is limited to 32 bit sizes even on a 64 bit machine. */
#if PY_VERSION_HEX < 0x02050000
typedef int Py_ssize_t;
#endif

/* A module to augment tracebacks */
#include "traceback.c"

/* SQLite 3 headers */
#include "sqlite3.h"

#if SQLITE_VERSION_NUMBER < 3003008
#error Your SQLite version is too old.  It must be at least 3.3.8
#endif

/* system headers */
#include <assert.h>

/* used to decide if we will use int or long long */
#define APSW_INT32_MIN (-2147483647 - 1)
#define APSW_INT32_MAX 2147483647

/* The encoding we use with SQLite.  SQLite supports either utf8 or 16
   bit unicode (host byte order).  If the latter is used then all
   functions have "16" appended to their name.  The encoding used also
   affects how strings are stored in the database.  We use utf8 since
   it is more space efficient, and Python can't make its mind up about
   Unicode (it uses 16 or 32 bit unichars and often likes to use Byte
   Order Markers). */
#define STRENCODING "utf_8"

/* Some macros used for frequent operations */

#define CHECK_THREAD(x,e)                                                \
  { if(x->thread_ident!=PyThread_get_thread_ident())                                                                                 \
      {    /* raise exception if we aren't already in one */                                                                         \
           if (!PyErr_Occurred())                                                                                                    \
             PyErr_Format(ExcThreadingViolation, "All SQLite objects created in a thread can only be used in that same thread.  "    \
                         "The object was created in thread id %d and this is %d",                                                    \
                         (int)(x->thread_ident), (int)(PyThread_get_thread_ident()));                                                \
           return e;                                                                                                                 \
      }                                                                                                                              \
  }



/* EXCEPTION TYPES */

static PyObject *APSWException;  /* root exception class */
static PyObject *ExcThreadingViolation; /* thread misuse */
static PyObject *ExcIncomplete;  /* didn't finish previous query */
static PyObject *ExcBindings;  /* wrong number of bindings */
static PyObject *ExcComplete;  /* query is finished */
static PyObject *ExcTraceAbort; /* aborted by exectrace */
static PyObject *ExcTooBig; /* object is too large for SQLite */
static PyObject *ExcExtensionLoading; /* error loading extension */

static struct { int code; const char *name; PyObject *cls;}
exc_descriptors[]=
  {
    /* Generic Errors */
    {SQLITE_ERROR,    "SQL"},       
    {SQLITE_MISMATCH, "Mismatch"},

    /* Internal Errors */
    {SQLITE_INTERNAL, "Internal"},  /* NOT USED */
    {SQLITE_PROTOCOL, "Protocol"},
    {SQLITE_MISUSE,   "Misuse"},
    {SQLITE_RANGE,    "Range"},

    /* permissions etc */
    {SQLITE_PERM,     "Permissions"},
    {SQLITE_READONLY, "ReadOnly"},
    {SQLITE_CANTOPEN, "CantOpen"},
    {SQLITE_AUTH,     "Auth"},

    /* abort/busy/etc */
    {SQLITE_ABORT,    "Abort"},
    {SQLITE_BUSY,     "Busy"},
    {SQLITE_LOCKED,   "Locked"},
    {SQLITE_INTERRUPT,"Interrupt"},
    {SQLITE_SCHEMA,   "SchemaChange"}, 
    {SQLITE_CONSTRAINT, "Constraint"},

    /* memory/disk/corrupt etc */
    {SQLITE_NOMEM,    "NoMem"},
    {SQLITE_IOERR,    "IO"},
    {SQLITE_CORRUPT,  "Corrupt"},
    {SQLITE_FULL,     "Full"},
    {SQLITE_TOOBIG,   "TooBig"},     /* NOT USED */
    {SQLITE_NOLFS,    "NoLFS"},
    {SQLITE_EMPTY,    "Empty"},
    {SQLITE_FORMAT,   "Format"},
    {SQLITE_NOTADB,   "NotADB"},

    {-1, 0, 0}
  };


/* EXCEPTION CODE */

static int init_exceptions(PyObject *m)
{
  char buffy[100]; /* more than enough for anyone :-) */
  int i;
  PyObject *obj;

  /* PyModule_AddObject uses borrowed reference so we incref whatever
     we give to it, so we still have a copy to use */

  /* custom ones first */

  APSWException=PyErr_NewException("apsw.Error", NULL, NULL);
  if(!APSWException) return -1;
  Py_INCREF(APSWException);
  if(PyModule_AddObject(m, "Error", (PyObject *)APSWException))
    return -1;

#define EXC(varname,name) \
  varname=PyErr_NewException("apsw." name, APSWException, NULL);  \
  if(!varname) return -1;                                          \
  Py_INCREF(varname);                                              \
  if(PyModule_AddObject(m, name, (PyObject *)varname))            \
    return -1;

  EXC(ExcThreadingViolation, "ThreadingViolationError");
  EXC(ExcIncomplete, "IncompleteExecutionError");
  EXC(ExcBindings, "BindingsError");
  EXC(ExcComplete, "ExecutionCompleteError");
  EXC(ExcTraceAbort, "ExecTraceAbort");
  EXC(ExcTooBig, "TooBigError");
  EXC(ExcExtensionLoading, "ExtensionLoadingError");

#undef EXC

  /* all the ones corresponding to SQLITE error codes */
  for(i=0;exc_descriptors[i].name;i++)
    {
      sprintf(buffy, "apsw.%sError", exc_descriptors[i].name);
      obj=PyErr_NewException(buffy, APSWException, NULL);
      if(!obj) return -1;
      Py_INCREF(obj);
      exc_descriptors[i].cls=obj;
      sprintf(buffy, "%sError", exc_descriptors[i].name);
      if(PyModule_AddObject(m, buffy, obj))
        return -1;
    }
  
  return 0;
}

static void make_exception(int res, sqlite3 *db)
{
  int i;
  
  for(i=0;exc_descriptors[i].name;i++)
    if (exc_descriptors[i].code==res)
      {
        assert(exc_descriptors[i].cls);
        PyErr_Format(exc_descriptors[i].cls, "%sError: %s", exc_descriptors[i].name, db?(sqlite3_errmsg(db)):"error");
        assert(PyErr_Occurred());
        return;
      }

  /* this line should only be reached if SQLite returns an error code not in the main list */
  PyErr_Format(APSWException, "Error %d: %s", res, db?(sqlite3_errmsg(db)):"error");  
}

/* If res indicates an SQLite error then do all the exception creation
 work.  We don't overwrite earlier exceptions hence the PyErr_Occurred
 check */
#define SET_EXC(db,res)  { if(res != SQLITE_OK && !PyErr_Occurred()) make_exception(res,db); }

/* CALLBACK INFO */

/* details of a registered function passed as user data to sqlite3_create_function */
typedef struct _funccbinfo 
{
  struct _funccbinfo *next;       /* we use a linked list */
  char *name;                     /* ascii function name which we uppercased */
  PyObject *scalarfunc;           /* the function to call for stepping */
  PyObject *aggregatefactory;     /* factory for aggregate functions */
} funccbinfo;

/* a particular aggregate function instance used as sqlite3_aggregate_context */
typedef struct _aggregatefunctioncontext 
{
  PyObject *aggvalue;             /* the aggregation value passed as first parameter */
  PyObject *stepfunc;             /* step function */
  PyObject *finalfunc;            /* final function */
} aggregatefunctioncontext;

static funccbinfo *freefunccbinfo(funccbinfo *);

typedef struct _collationcbinfo
{
  struct _collationcbinfo *next;  /* we use a linked list */
  char *name;                     /* ascii collation name which we uppercased */
  PyObject *func;                 /* the actual function to call */
} collationcbinfo;
  
static collationcbinfo *freecollationcbinfo(collationcbinfo *);

typedef struct Connection Connection; /* forward declaration */

typedef struct _vtableinfo
{
  struct _vtableinfo *next;       /* we use a linked list */
  char *name;                     /* module name */
  PyObject *datasource;           /* object with create/connect methods */
  Connection *connection;  /* the Connection this is registered against so we don't
				     have to have a global table mapping sqlite3_db* to
				     Connection* */
} vtableinfo;

static vtableinfo *freevtableinfo(vtableinfo *);

/* CONNECTION TYPE */

struct Connection { 
  PyObject_HEAD
  sqlite3 *db;                    /* the actual database connection */
  long thread_ident;              /* which thread we were made in */
  funccbinfo *functions;          /* linked list of registered functions */
  collationcbinfo *collations;    /* linked list of registered collations */
  vtableinfo *vtables;            /* linked list of registered vtables */

  /* registered hooks/handlers (NULL or callable) */
  PyObject *busyhandler;     
  PyObject *rollbackhook;
  PyObject *profile;
  PyObject *updatehook;
  PyObject *commithook;           
  PyObject *progresshandler;      
  PyObject *authorizer;
};

static PyTypeObject ConnectionType;

/* CURSOR TYPE */

typedef struct {
  PyObject_HEAD
  Connection *connection;          /* pointer to parent connection */
  sqlite3_stmt *statement;         /* current compiled statement */

  /* see sqlite3_prepare for the origin of these */
  const char *zsql;               /* current sqlstatement (which may include multiple statements) */
  const char *zsqlnextpos;        /* the next statement to execute (or NULL if no more) */

  /* what state we are in */
  enum { C_BEGIN, C_ROW, C_DONE } status;

  /* bindings for query */
  PyObject *bindings;             /* dict or sequence */
  Py_ssize_t bindingsoffset;             /* for sequence tracks how far along we are when dealing with multiple statements */

  /* iterator for executemany */
  PyObject *emiter;

  /* tracing functions */
  PyObject *exectrace;
  PyObject *rowtrace;
  
} Cursor;

static PyTypeObject CursorType;


/* CONVENIENCE FUNCTIONS */

/* Convert a NULL terminated UTF-8 string into a Python object.  None
   is returned if NULL is passed in. */
static PyObject *
convertutf8string(const char *str)
{
  const char *chk=str;

  if(!str)
    {
      Py_INCREF(Py_None);
      return Py_None;
    }
  
  for(chk=str;*chk && !((*chk)&0x80); chk++) ;
  if(*chk)
    return PyUnicode_DecodeUTF8(str, strlen(str), NULL);
  else
    return PyString_FromString(str);
}

/* Convert a pointer and size UTF-8 string into a Python object.
   Pointer must be non-NULL. */
static PyObject *
convertutf8stringsize(const char *str, Py_ssize_t size)
{
  const char *chk=str;
  Py_ssize_t i;
  
  assert(str);
  assert(size>=0);

  for(i=0;i<size && !(chk[i]&0x80);i++);

  if(i!=size)
    return PyUnicode_DecodeUTF8(str, size, NULL);
  else
    return PyString_FromStringAndSize(str, size);
}

/* 
   Python's handling of Unicode is horrible.  It can use 2 or 4 byte
   unicode chars and the conversion routines like to put out BOMs
   which makes life even harder.  These macros are used in pairs to do
   the right form of conversion and tell us whether to use the plain
   or -16 version of the SQLite function that is about to be called.
*/

#if Py_UNICODE_SIZE==2
#define UNIDATABEGIN(obj) \
{                                                        \
  const int use16=1;                                     \
  size_t strbytes=2*PyUnicode_GET_SIZE(obj);             \
  const void *strdata=PyUnicode_AS_DATA(obj);            

#define UNIDATAEND(obj)                                  \
}

#else  /* Py_UNICODE_SIZE!=2 */

#define UNIDATABEGIN(obj) \
{                                                        \
  const int use16=0;                                     \
  Py_ssize_t strbytes=0;				 \
  const char *strdata=NULL;                              \
  PyObject *_utf8=NULL;                                  \
                                                         \
  _utf8=PyUnicode_AsUTF8String(obj);                     \
  if(_utf8)                                              \
    {                                                    \
      strbytes=PyString_GET_SIZE(_utf8);                 \
      strdata=PyString_AsString(_utf8);                  \
    }                      

#define UNIDATAEND(obj)                                  \
  Py_XDECREF(_utf8);                                     \
}

#endif /* Py_UNICODE_SIZE */

/* CONNECTION CODE */

static void
Connection_dealloc(Connection* self)
{
  /* thread check - we can't use macro as that returns */

  if(self->thread_ident!=PyThread_get_thread_ident())
    {
          PyObject *err_type, *err_value, *err_traceback;
          int have_error=PyErr_Occurred()?1:0;
          if (have_error)
            PyErr_Fetch(&err_type, &err_value, &err_traceback);
          PyErr_Format(ExcThreadingViolation, "The destructor for Connection is called in a different thread than it"
                       "was created in.  All calls must be in the same thread.  It was created in thread %d" 
                       "and this is %d.  This SQLite database is not being closed as a result.",
                       (int)(self->thread_ident), (int)(PyThread_get_thread_ident()));            
          PyErr_WriteUnraisable((PyObject*)self);
          if (have_error)
            PyErr_Restore(err_type, err_value, err_traceback);
          
          return;
    }

  if (self->db)
    {
      int res;
      Py_BEGIN_ALLOW_THREADS
        res=sqlite3_close(self->db);
      Py_END_ALLOW_THREADS;

      if (res!=SQLITE_OK) 
        {
          PyObject *err_type, *err_value, *err_traceback;
          int have_error=PyErr_Occurred()?1:0;
          if (have_error)
            PyErr_Fetch(&err_type, &err_value, &err_traceback);
          make_exception(res,self->db);
          if (have_error)
            {
              PyErr_WriteUnraisable((PyObject*)self);
              PyErr_Restore(err_type, err_value, err_traceback);
            }
        }
      else
        self->db=0;
    }

  /* free functions */
  {
    funccbinfo *func=self->functions;
    while((func=freefunccbinfo(func)));
  }

  /* free collations */
  {
    collationcbinfo *coll=self->collations;
    while((coll=freecollationcbinfo(coll)));
  }

  /* free vtables */
  {
    vtableinfo *vtinfo=self->vtables;
    while((vtinfo=freevtableinfo(vtinfo)));
  }

  Py_XDECREF(self->busyhandler);
  self->busyhandler=0;

  Py_XDECREF(self->rollbackhook);
  self->rollbackhook=0;

  Py_XDECREF(self->profile);
  self->profile=0;

  Py_XDECREF(self->commithook);
  self->commithook=0;

  Py_XDECREF(self->progresshandler);
  self->progresshandler=0;
  
  Py_XDECREF(self->authorizer);
  self->authorizer=0;

  self->thread_ident=-1;
  self->ob_type->tp_free((PyObject*)self);
}

static PyObject*
Connection_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    Connection *self;

    self = (Connection *)type->tp_alloc(type, 0);
    if (self != NULL) {
      /* Strictly speaking the memory was already zeroed.  This is
         just defensive coding. */
      self->db=0;
      self->thread_ident=PyThread_get_thread_ident();
      self->functions=0;
      self->collations=0;
      self->vtables=0;
      self->busyhandler=0;
      self->rollbackhook=0;
      self->profile=0;
      self->updatehook=0;
      self->commithook=0;
      self->progresshandler=0;
      self->authorizer=0;
    }

    return (PyObject *)self;
}

static int
Connection_init(Connection *self, PyObject *args, PyObject *kwds)
{
  char *filename=NULL;
  int res=0;

  CHECK_THREAD(self,-1);

  if(kwds && PyDict_Size(kwds)!=0)
    {
      PyErr_Format(PyExc_TypeError, "Connection constructor does not take keyword arguments");
      return -1;
    }

  if(!PyArg_ParseTuple(args, "es:Connection(filename)", STRENCODING, &filename))
    return -1;
  
  Py_BEGIN_ALLOW_THREADS
    res=sqlite3_open(filename, &self->db);
  Py_END_ALLOW_THREADS;
  SET_EXC(self->db, res);  /* nb sqlite3_open always allocates the db even on error */

  PyMem_Free(filename);
  
  return (res==SQLITE_OK)?0:-1;
}

static void Cursor_init(Cursor *, Connection *);

static PyObject *
Connection_cursor(Connection *self)
{
  Cursor* cursor = NULL;

  CHECK_THREAD(self,NULL);

  cursor = PyObject_New(Cursor, &CursorType);
  if(!cursor)
    return NULL;

  /* incref me since cursor holds a pointer */
  Py_INCREF((PyObject*)self);

  Cursor_init(cursor, self);
  
  return (PyObject*)cursor;
}

static PyObject *
Connection_setbusytimeout(Connection *self, PyObject *args)
{
  int ms=0;
  int res;

  CHECK_THREAD(self,NULL);

  if(!PyArg_ParseTuple(args, "i:setbusytimeout(millseconds)", &ms))
    return NULL;

  res=sqlite3_busy_timeout(self->db, ms);
  SET_EXC(self->db, res);
  if(res!=SQLITE_OK)
    return NULL;
  
  /* free any explicit busyhandler we may have had */
  Py_XDECREF(self->busyhandler);
  self->busyhandler=0;

  return Py_BuildValue("");
}

static PyObject *
Connection_changes(Connection *self)
{
  CHECK_THREAD(self,NULL);
  return Py_BuildValue("i", sqlite3_changes(self->db));
}

static PyObject *
Connection_totalchanges(Connection *self)
{
  CHECK_THREAD(self,NULL);
  return Py_BuildValue("i", sqlite3_total_changes(self->db));
}

static PyObject *
Connection_getautocommit(Connection *self)
{
  PyObject *res;
  CHECK_THREAD(self,NULL);
  res=(sqlite3_get_autocommit(self->db))?(Py_True):(Py_False);
  Py_INCREF(res);
  return res;
}

static PyObject *
Connection_last_insert_rowid(Connection *self)
{
  long long int vint;

  CHECK_THREAD(self,NULL);

  vint=sqlite3_last_insert_rowid(self->db);
  
  if(vint<APSW_INT32_MIN || vint>APSW_INT32_MAX)
    return PyLong_FromLongLong(vint);
  else
    return PyInt_FromLong((long)vint);
}

static PyObject *
Connection_complete(Connection *self, PyObject *args)
{
  char *statements=NULL;
  int res;

  CHECK_THREAD(self,NULL);
  
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

static PyObject *
Connection_interrupt(Connection *self)
{
  CHECK_THREAD(self, NULL);

  sqlite3_interrupt(self->db);  /* no return value */
  return Py_BuildValue("");
}

static void
updatecb(void *context, int updatetype, char const *databasename, char const *tablename, sqlite_int64 rowid)
{
  /* The hook returns void. That makes it impossible for us to
     abort immediately due to an error in the callback */
  
  PyGILState_STATE gilstate;
  PyObject *retval=NULL, *args=NULL;
  Connection *self=(Connection *)context;
  PyObject *pupdatetype=NULL, *pdatabasename=NULL, *ptablename=NULL, *prowid=NULL;

  assert(self);
  assert(self->updatehook);
  assert(self->updatehook!=Py_None);

  /* defensive coding */
  if(!self->updatehook)
    return;

  gilstate=PyGILState_Ensure();

  if(PyErr_Occurred())
    goto finally;  /* abort hook due to outstanding exception */


  pupdatetype=Py_BuildValue("i", updatetype);
  pdatabasename=convertutf8string(databasename);
  ptablename=convertutf8string(tablename);
  prowid=PyLong_FromLongLong(rowid);

  if (!pupdatetype || !pdatabasename || !ptablename || !prowid)
    goto finally;

  args=PyTuple_New(4);
  if(!args)
    goto finally; /* abort hook on failure to allocate args */

  PyTuple_SET_ITEM(args, 0, pupdatetype);
  PyTuple_SET_ITEM(args, 1, pdatabasename);
  PyTuple_SET_ITEM(args, 2, ptablename);
  PyTuple_SET_ITEM(args, 3, prowid);

  pupdatetype=pdatabasename=ptablename=prowid=NULL; /* owned by args now */
  
  retval=PyEval_CallObject(self->updatehook, args);

 finally:
  Py_XDECREF(retval);
  Py_XDECREF(args);
  Py_XDECREF(pupdatetype);
  Py_XDECREF(pdatabasename);
  Py_XDECREF(ptablename);
  Py_XDECREF(prowid);
  PyGILState_Release(gilstate);
}

static PyObject *
Connection_setupdatehook(Connection *self, PyObject *callable)
{
  /* sqlite3_update_hook doesn't return an error code */
  
  CHECK_THREAD(self,NULL);

  if(callable==Py_None)
    {
      sqlite3_update_hook(self->db, NULL, NULL);
      callable=NULL;
      goto finally;
    }

  if(!PyCallable_Check(callable))
    {
      PyErr_Format(PyExc_TypeError, "update hook must be callable");
      return NULL;
    }

  sqlite3_update_hook(self->db, updatecb, self);

  Py_INCREF(callable);

 finally:

  Py_XDECREF(self->updatehook);
  self->updatehook=callable;

  return Py_BuildValue("");
}

static void
rollbackhookcb(void *context)
{
  /* The hook returns void. That makes it impossible for us to
     abort immediately due to an error in the callback */
  
  PyGILState_STATE gilstate;
  PyObject *retval=NULL, *args=NULL;
  Connection *self=(Connection *)context;

  assert(self);
  assert(self->rollbackhook);
  assert(self->rollbackhook!=Py_None);

  /* defensive coding */
  if(!self->rollbackhook)
    return;

  gilstate=PyGILState_Ensure();

  if(PyErr_Occurred())
    goto finally;  /* abort hook due to outstanding exception */

  args=PyTuple_New(0);
  if(!args)
    goto finally; /* abort hook on failure to allocate args */
  
  retval=PyEval_CallObject(self->rollbackhook, args);

 finally:
  Py_XDECREF(retval);
  Py_XDECREF(args);
  PyGILState_Release(gilstate);
}

static PyObject *
Connection_setrollbackhook(Connection *self, PyObject *callable)
{
  /* sqlite3_rollback_hook doesn't return an error code */
  
  CHECK_THREAD(self,NULL);

  if(callable==Py_None)
    {
      sqlite3_rollback_hook(self->db, NULL, NULL);
      callable=NULL;
      goto finally;
    }

  if(!PyCallable_Check(callable))
    {
      PyErr_Format(PyExc_TypeError, "rollback hook must be callable");
      return NULL;
    }

  sqlite3_rollback_hook(self->db, rollbackhookcb, self);

  Py_INCREF(callable);

 finally:

  Py_XDECREF(self->rollbackhook);
  self->rollbackhook=callable;

  return Py_BuildValue("");
}

#ifdef EXPERIMENTAL /* sqlite3_profile */
static void
profilecb(void *context, const char *statement, sqlite_uint64 runtime)
{
  /* The hook returns void. That makes it impossible for us to
     abort immediately due to an error in the callback */
  
  PyGILState_STATE gilstate;
  PyObject *retval=NULL, *args=NULL;
  Connection *self=(Connection *)context;
  PyObject *pstatement=NULL, *pruntime=NULL;

  assert(self);
  assert(self->profile);
  assert(self->profile!=Py_None);

  /* defensive coding */
  if(!self->profile)
    return;

  gilstate=PyGILState_Ensure();

  if(PyErr_Occurred())
    goto finally;  /* abort hook due to outstanding exception */

  pstatement=convertutf8string(statement);
  pruntime=PyLong_FromUnsignedLongLong(runtime);

  if (!pstatement || !pruntime)
    goto finally;

  args=PyTuple_New(2);
  if(!args)
    goto finally; /* abort hook on failure to allocate args */
  
  PyTuple_SET_ITEM(args, 0, pstatement);
  PyTuple_SET_ITEM(args, 1, pruntime);

  pstatement=pruntime=NULL; /* owned by args now */

  retval=PyEval_CallObject(self->profile, args);

 finally:
  Py_XDECREF(retval);
  Py_XDECREF(args);
  Py_XDECREF(pstatement);
  Py_XDECREF(pruntime);
  PyGILState_Release(gilstate);
}

static PyObject *
Connection_setprofile(Connection *self, PyObject *callable)
{
  /* sqlite3_profile doesn't return an error code */
  
  CHECK_THREAD(self,NULL);

  if(callable==Py_None)
    {
      sqlite3_profile(self->db, NULL, NULL);
      callable=NULL;
      goto finally;
    }

  if(!PyCallable_Check(callable))
    {
      PyErr_Format(PyExc_TypeError, "profile function must be callable");
      return NULL;
    }

  sqlite3_profile(self->db, profilecb, self);

  Py_INCREF(callable);

 finally:

  Py_XDECREF(self->profile);
  self->profile=callable;

  return Py_BuildValue("");
}
#endif /* EXPERIMENTAL - sqlite3_profile */


#ifdef EXPERIMENTAL      /* commit hook */
static int 
commithookcb(void *context)
{
  /* The hook returns 0 for commit to go ahead and non-zero to abort
     commit (turn into a rollback). We return non-zero for errors */
  
  PyGILState_STATE gilstate;
  PyObject *retval=NULL, *args=NULL;
  int ok=1; /* error state */
  Connection *self=(Connection *)context;

  assert(self);
  assert(self->commithook);
  assert(self->commithook!=Py_None);

  /* defensive coding */
  if(!self->commithook)
    return 0;

  gilstate=PyGILState_Ensure();

  if(PyErr_Occurred())
    goto finally;  /* abort hook due to outstanding exception */

  args=PyTuple_New(0);
  if(!args)
    goto finally; /* abort hook on failure to allocate args */
  
  retval=PyEval_CallObject(self->commithook, args);

  if(!retval)
    goto finally; /* abort hook due to exeception */

  ok=PyObject_IsTrue(retval);
  assert(ok==-1 || ok==0 || ok==1);
  /* the docs say -1 can be returned, but the code for PyObject_IsTrue always returns 1 or 0.  
     this is a defensive check */
  if(ok==-1)
    {
      ok=1;
      goto finally;  /* abort due to exception in return value */
    }

 finally:
  Py_XDECREF(retval);
  Py_XDECREF(args);
  PyGILState_Release(gilstate);
  return ok;
}

static PyObject *
Connection_setcommithook(Connection *self, PyObject *callable)
{
  /* sqlite3_commit_hook doesn't return an error code */
  
  CHECK_THREAD(self,NULL);

  if(callable==Py_None)
    {
      sqlite3_commit_hook(self->db, NULL, NULL);
      callable=NULL;
      goto finally;
    }

  if(!PyCallable_Check(callable))
    {
      PyErr_Format(PyExc_TypeError, "commit hook must be callable");
      return NULL;
    }

  sqlite3_commit_hook(self->db, commithookcb, self);

  Py_INCREF(callable);

 finally:

  Py_XDECREF(self->commithook);
  self->commithook=callable;

  return Py_BuildValue("");
}
#endif  /* EXPERIMENTAL sqlite3_commit_hook */

#ifdef EXPERIMENTAL      /* sqlite3_progress_handler */
static int 
progresshandlercb(void *context)
{
  /* The hook returns 0 for continue and non-zero to abort (rollback).
     We return non-zero for errors */
  
  PyGILState_STATE gilstate;
  PyObject *retval=NULL, *args=NULL;
  int ok=1; /* error state */
  Connection *self=(Connection *)context;

  assert(self);
  assert(self->progresshandler);

  /* defensive coding */
  if(!self->progresshandler)
    return 0;

  gilstate=PyGILState_Ensure();

  args=PyTuple_New(0);
  if(!args)
    goto finally; /* abort handler due to failure to allocate args */
  
  retval=PyEval_CallObject(self->progresshandler, args);

  if(!retval)
    goto finally; /* abort due to exeception */

  ok=PyObject_IsTrue(retval);

  assert(ok==-1 || ok==0 || ok==1);
  /* see earlier comment about PyObject_IsTrue */
  if(ok==-1)
    {
      ok=1;
      goto finally;  /* abort due to exception in result */
    }

 finally:
  Py_XDECREF(retval);
  Py_XDECREF(args);

  PyGILState_Release(gilstate);
  return ok;
}

static PyObject *
Connection_setprogresshandler(Connection *self, PyObject *args)
{
  /* sqlite3_progress_handler doesn't return an error code */
  int nsteps=20;
  PyObject *callable=NULL;
  
  CHECK_THREAD(self,NULL);

  if(!PyArg_ParseTuple(args, "O|i:setprogresshandler(callable, nsteps=20)", &callable, &nsteps))
    return NULL;

  if(callable==Py_None)
    {
      sqlite3_progress_handler(self->db, 0, NULL, NULL);
      callable=NULL;
      goto finally;
    }

  if(!PyCallable_Check(callable))
    {
      PyErr_Format(PyExc_TypeError, "progress handler must be callable");
      return NULL;
    }

  sqlite3_progress_handler(self->db, nsteps, progresshandlercb, self);
  Py_INCREF(callable);

 finally:

  Py_XDECREF(self->progresshandler);
  self->progresshandler=callable;

  return Py_BuildValue("");
}
#endif  /* EXPERIMENTAL sqlite3_progress_handler */

static int 
authorizercb(void *context, int operation, const char *paramone, const char *paramtwo, const char *databasename, const char *triggerview)
{
  /* should return one of SQLITE_OK, SQLITE_DENY, or
     SQLITE_IGNORE. (0, 1 or 2 respectively) */

  PyGILState_STATE gilstate;
  PyObject *args=NULL, *retval=NULL;
  int result=SQLITE_DENY;  /* default to deny */
  Connection *self=(Connection *)context;

  PyObject *poperation=NULL, *pone=NULL, *ptwo=NULL, *pdatabasename=NULL, *ptriggerview=NULL;

  assert(self);
  assert(self->authorizer);
  assert(self->authorizer!=Py_None);

  /* defensive coding */
  if(!self->authorizer)
    return SQLITE_OK;

  gilstate=PyGILState_Ensure();

  if(PyErr_Occurred())
    goto finally;  /* abort due to earlier exception */

  poperation=Py_BuildValue("i", operation);
  pone=convertutf8string(paramone);
  ptwo=convertutf8string(paramtwo);
  pdatabasename=convertutf8string(databasename);
  ptriggerview=convertutf8string(triggerview);
  args=PyTuple_New(5);

  if(!poperation || !pone || !ptwo || !pdatabasename || !ptriggerview || !args)
    goto finally;

  PyTuple_SET_ITEM(args, 0, poperation);
  PyTuple_SET_ITEM(args, 1, pone);
  PyTuple_SET_ITEM(args, 2, ptwo);
  PyTuple_SET_ITEM(args, 3, pdatabasename);
  PyTuple_SET_ITEM(args, 4, ptriggerview);

  poperation=pone=ptwo=pdatabasename=ptriggerview=NULL;  /* owned by args now */

  retval=PyEval_CallObject(self->authorizer, args);

  if(!retval)
    goto finally; /* abort due to exeception */

  result=PyInt_AsLong(retval);
  if (PyErr_Occurred())
    result=SQLITE_DENY;

 finally:
  Py_XDECREF(poperation);
  Py_XDECREF(pone);
  Py_XDECREF(ptwo);
  Py_XDECREF(pdatabasename);
  Py_XDECREF(ptriggerview);
  Py_XDECREF(args);
  Py_XDECREF(retval);

  PyGILState_Release(gilstate);
  return result;
}

static PyObject *
Connection_setauthorizer(Connection *self, PyObject *callable)
{
  int res;

  CHECK_THREAD(self,NULL);

  if(callable==Py_None)
    {
      res=sqlite3_set_authorizer(self->db, NULL, NULL);
      callable=NULL;
      goto finally;
    }

  if(!PyCallable_Check(callable))
    {
      PyErr_Format(PyExc_TypeError, "authorizer must be callable");
      return NULL;
    }

  res=sqlite3_set_authorizer(self->db, authorizercb, self);
  SET_EXC(self->db, res);

  Py_INCREF(callable);

 finally:
  Py_XDECREF(self->authorizer);
  self->authorizer=callable;

  return (res==SQLITE_OK)?Py_BuildValue(""):NULL;
}

static int 
busyhandlercb(void *context, int ncall)
{
  /* Return zero for caller to get SQLITE_BUSY error. We default to
     zero in case of error. */

  PyGILState_STATE gilstate;
  PyObject *args, *retval;
  int result=0;  /* default to fail with SQLITE_BUSY */
  Connection *self=(Connection *)context;

  assert(self);
  assert(self->busyhandler);

  /* defensive coding */
  if(!self->busyhandler)
    return result;

  gilstate=PyGILState_Ensure();

  args=Py_BuildValue("(i)", ncall);
  if(!args)
    goto finally; /* abort busy due to memory allocation failure */
  
  retval=PyEval_CallObject(self->busyhandler, args);
  Py_DECREF(args);

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

#ifdef EXPERIMENTAL  /* extension loading */
static PyObject *
Connection_enableloadextension(Connection *self, PyObject *enabled)
{
  int enabledp, res;

  CHECK_THREAD(self, NULL);

  /* get the boolean value */
  enabledp=PyObject_IsTrue(enabled);
  if(enabledp==-1) return NULL;
  if (PyErr_Occurred()) return NULL;

  /* call function */
  res=sqlite3_enable_load_extension(self->db, enabledp);
  SET_EXC(self->db, res);  /* the function will currently always succeed */

  /* done */
  return (res==SQLITE_OK)?Py_BuildValue(""):NULL;
}

static PyObject *
Connection_loadextension(Connection *self, PyObject *args)
{
  int res;
  char *zfile=NULL, *zproc=NULL, *errmsg=NULL;

  CHECK_THREAD(self, NULL);
  
  if(!PyArg_ParseTuple(args, "s|z:loadextension(filename, entrypoint=None)", &zfile, &zproc))
    return NULL;

  res=sqlite3_load_extension(self->db, zfile, zproc, &errmsg);
  /* load_extension doesn't set the error message on the db so we have to make exception manually */
  if(res!=SQLITE_OK)
    {
      assert(errmsg);
      PyErr_Format(ExcExtensionLoading, "ExtensionLoadingError: %s", errmsg?errmsg:"unspecified");
      return NULL;
    }
  return Py_BuildValue("");
}

#endif /* EXPERIMENTAL extension loading */

static PyObject *
Connection_setbusyhandler(Connection *self, PyObject *callable)
{
  int res=SQLITE_OK;

  CHECK_THREAD(self,NULL);

  if(callable==Py_None)
    {
      res=sqlite3_busy_handler(self->db, NULL, NULL);
      callable=NULL;
      goto finally;
    }

  if(!PyCallable_Check(callable))
    {
      PyErr_Format(PyExc_TypeError, "busyhandler must be callable");
      return NULL;
    }

  res=sqlite3_busy_handler(self->db, busyhandlercb, self);
  SET_EXC(self->db, res);

  Py_INCREF(callable);

 finally:
  Py_XDECREF(self->busyhandler);
  self->busyhandler=callable;

  return (res==SQLITE_OK)?Py_BuildValue(""):NULL;
}


/* USER DEFINED FUNCTION CODE.*/

/* We store the registered functions in a linked list hooked into the
   connection object so we can free them.  There is probably a better
   data structure to use but this was most convenient. */

static funccbinfo *
freefunccbinfo(funccbinfo *func)
{
  funccbinfo *fnext;
  if(!func) 
    return NULL;

  if(func->name)
    PyMem_Free(func->name);
  Py_XDECREF(func->scalarfunc);
  Py_XDECREF(func->aggregatefactory);
  fnext=func->next;
  PyMem_Free(func);
  return fnext;
}

static funccbinfo *
allocfunccbinfo(void)
{
  funccbinfo *res=PyMem_Malloc(sizeof(funccbinfo));
  if(res)
    memset(res, 0, sizeof(funccbinfo));
  return res;
}

/* Converts sqlite3_value to PyObject.  Returns a new reference. */
static PyObject *
convert_value_to_pyobject(sqlite3_value *value)
{
  const int coltype=sqlite3_value_type(value);

  switch(coltype)
    {
    case SQLITE_INTEGER:
      {
        long long vint=sqlite3_value_int64(value);
        if(vint<APSW_INT32_MIN || vint>APSW_INT32_MAX)
          return PyLong_FromLongLong(vint);
        else
          return PyInt_FromLong((long)vint);
      }

    case SQLITE_FLOAT:
      return PyFloat_FromDouble(sqlite3_value_double(value));
      
    case SQLITE_TEXT:
      return convertutf8stringsize((const char*)sqlite3_value_text(value), sqlite3_value_bytes(value));

    case SQLITE_NULL:
      Py_INCREF(Py_None);
      return Py_None;

    case SQLITE_BLOB:
      {
        PyObject *item;
        Py_ssize_t sz=sqlite3_value_bytes(value);
        item=PyBuffer_New(sz);
        if(item)
          {
            void *buffy=0;
            Py_ssize_t sz2=sz;
            if(!PyObject_AsWriteBuffer(item, &buffy, &sz2))
              memcpy(buffy, sqlite3_value_blob(value), sz);
            else
              {
                Py_DECREF(item);
                return NULL;
              }
	    return item;
          }
        return NULL;
      }

    default:
      PyErr_Format(APSWException, "Unknown sqlite column type %d!", coltype);
      return NULL;
    }
  /* can't get here */
  assert(0);
  return NULL;
}

static void
set_context_result(sqlite3_context *context, PyObject *obj)
{
  if(!obj)
    {
      assert(PyErr_Occurred());
      /* TODO: possibly examine exception and return appropriate error
         code eg for BusyError set error to SQLITE_BUSY */
      sqlite3_result_error(context, "executing scalarcallback failed", -1);
      return;
    }

  /* DUPLICATE(ish) code: this is substantially similar to the code in
     Cursor_dobinding.  If you fix anything here then do it there as
     well. */

  if(obj==Py_None)
    {
      sqlite3_result_null(context);
      return;
    }
  if(PyInt_Check(obj))
    {
      sqlite3_result_int64(context, PyInt_AS_LONG(obj));
      return;
    }
  if (PyLong_Check(obj))
    {
      sqlite3_result_int64(context, PyLong_AsLongLong(obj));
      return;
    }
  if (PyFloat_CheckExact(obj))
    {
      sqlite3_result_double(context, PyFloat_AS_DOUBLE(obj));
      return;
    }
  if (PyUnicode_Check(obj))
    {
      UNIDATABEGIN(obj)
        if(strdata)
          {
	    if(strbytes>APSW_INT32_MAX)
	      {
		PyErr_Format(ExcTooBig, "Unicode object is too large - SQLite only supports up to 2GB");
	      }
	    else
	      {
		if(use16)
		  sqlite3_result_text16(context, strdata, (int)strbytes, SQLITE_TRANSIENT);
		else
		  sqlite3_result_text(context, strdata, (int)strbytes, SQLITE_TRANSIENT);
	      }
          }
        else
          sqlite3_result_error(context, "Unicode conversions failed", -1);
      UNIDATAEND(obj);
      return;
    }
  if (PyString_Check(obj))
    {
      const char *val=PyString_AS_STRING(obj);
      const Py_ssize_t lenval=PyString_GET_SIZE(obj);
      const char *chk=val;
      for(;chk<val+lenval && !((*chk)&0x80); chk++);
      if(chk<val+lenval)
        {
          PyObject *str2=PyUnicode_FromObject(obj);
          if(!str2)
            {
              sqlite3_result_error(context, "PyUnicode_FromObject failed", -1);
              return;
            }
          UNIDATABEGIN(str2)
            if(strdata)
              {
		if(strbytes>APSW_INT32_MAX)
		  {
		    PyErr_Format(ExcTooBig, "Unicode object is too large - SQLite only supports up to 2GB");
		  }
		else
		  {
		    if(use16)
		      sqlite3_result_text16(context, strdata, (int)strbytes, SQLITE_TRANSIENT);
		    else
		      sqlite3_result_text(context, strdata, (int)strbytes, SQLITE_TRANSIENT);
		  }
              }
            else
              sqlite3_result_error(context, "Unicode conversions failed", -1);
          UNIDATAEND(str2);
          Py_DECREF(str2);
        }
      else
	{
	  if(lenval>APSW_INT32_MAX)
	      {
		PyErr_Format(ExcTooBig, "String object is too large - SQLite only supports up to 2GB");
		}
	  else
	    sqlite3_result_text(context, val, (int)lenval, SQLITE_TRANSIENT);
	}
      return;
    }
  if (PyBuffer_Check(obj))
    {
      const char *buffer;
      Py_ssize_t buflen;
      if(PyObject_AsCharBuffer(obj, &buffer, &buflen))
        {
          sqlite3_result_error(context, "PyObject_AsCharBuffer failed", -1);
          return;
        }
      if (buflen>APSW_INT32_MAX)
	sqlite3_result_error(context, "Buffer object is too large for SQLite - only up to 2GB is supported", -1);
      else
	sqlite3_result_blob(context, buffer, (int)buflen, SQLITE_TRANSIENT);
      return;
    }

  PyErr_Format(PyExc_TypeError, "Bad return type from function callback");
  sqlite3_result_error(context, "Bad return type from function callback", -1);

}

/* Returns a new reference to a tuple formed from function parameters */
PyObject *
getfunctionargs(sqlite3_context *context, PyObject *firstelement, int argc, sqlite3_value **argv)
{
  PyObject *pyargs=NULL;
  int i;
  int extra=0;

  /* extra first item */
  if(firstelement)
    extra=1;

  pyargs=PyTuple_New((long)argc+extra);
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
          Py_DECREF(pyargs);
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
  PyObject *pyargs;
  PyObject *retval;
  funccbinfo *cbinfo=(funccbinfo*)sqlite3_user_data(context);
  assert(cbinfo);

  gilstate=PyGILState_Ensure();

  assert(cbinfo->scalarfunc);

  if(PyErr_Occurred())
    {
      sqlite3_result_error(context, "Prior Python Error", -1);
      goto finalfinally;
    }

  pyargs=getfunctionargs(context, NULL, argc, argv);
  if(!pyargs)
      goto finally;

  assert(!PyErr_Occurred());
  retval=PyEval_CallObject(cbinfo->scalarfunc, pyargs);

  Py_DECREF(pyargs);
  set_context_result(context, retval);
  Py_XDECREF(retval);

 finally:
  if (PyErr_Occurred())
    {
      char *funname=sqlite3_mprintf("user-defined-scalar-%s", cbinfo->name);
      AddTraceBackHere(__FILE__, __LINE__, funname, "{s: i}", "NumberOfArguments", argc);
      sqlite3_free(funname);
    }
 finalfinally:
   PyGILState_Release(gilstate);
}

static aggregatefunctioncontext *
getaggregatefunctioncontext(sqlite3_context *context)
{
  aggregatefunctioncontext *aggfc=sqlite3_aggregate_context(context, sizeof(aggregatefunctioncontext));
  funccbinfo *cbinfo;
  PyObject *retval;
  PyObject *args;
  /* have we seen it before? */
  if(aggfc->aggvalue) 
    return aggfc;
  
  /* fill in with Py_None so we know it is valid */
  aggfc->aggvalue=Py_None;
  Py_INCREF(Py_None);

  cbinfo=(funccbinfo*)sqlite3_user_data(context);
  assert(cbinfo);
  assert(cbinfo->aggregatefactory);

  /* call the aggregatefactory to get our working objects */
  args=PyTuple_New(0);
  if(!args)
    return aggfc;
  retval=PyEval_CallObject(cbinfo->aggregatefactory, args);
  Py_DECREF(args);
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
      funccbinfo *cbinfo=(funccbinfo*)sqlite3_user_data(context);
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
  PyObject *pyargs=NULL;
  PyObject *retval=NULL;
  aggregatefunctioncontext *aggfc=NULL;
  PyObject *err_type=NULL, *err_value=NULL, *err_traceback=NULL;

  gilstate=PyGILState_Ensure();

  PyErr_Fetch(&err_type, &err_value, &err_traceback);
  PyErr_Clear();

  aggfc=getaggregatefunctioncontext(context);

  assert(aggfc);
  
  if((err_type||err_value||err_traceback) || PyErr_Occurred() || !aggfc->finalfunc)
    {
      sqlite3_result_error(context, "Prior Python Error in step function", -1);
      goto finally;
    }

  pyargs=PyTuple_New(1);
  if(!pyargs)
    goto finally;

  Py_INCREF(aggfc->aggvalue);
  PyTuple_SET_ITEM(pyargs, 0, aggfc->aggvalue);

  retval=PyEval_CallObject(aggfc->finalfunc, pyargs);
  Py_DECREF(pyargs);
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
      PyErr_Format(PyExc_StandardError, "An exception happened during cleanup of an aggregate function, but there was already error in the step function so only that can be returned");
      PyErr_WriteUnraisable(Py_None); /* there is no object to give, and NULL causes some versions to core dump */
    }

  if(err_type||err_value||err_traceback)
    PyErr_Restore(err_type, err_value, err_traceback);

  if(PyErr_Occurred())
    {
      char *funname=0;
      funccbinfo *cbinfo=(funccbinfo*)sqlite3_user_data(context);
      assert(cbinfo);
      funname=sqlite3_mprintf("user-defined-aggregate-final-%s", cbinfo->name);
      AddTraceBackHere(__FILE__, __LINE__, funname, NULL);
      sqlite3_free(funname);
    }

  /* sqlite3 frees the actual underlying memory we used (aggfc itself) */

  PyGILState_Release(gilstate);
}


static PyObject *
Connection_createscalarfunction(Connection *self, PyObject *args)
{
  int numargs=-1;
  PyObject *callable;
  char *name=0;
  char *chk;
  funccbinfo *cbinfo;
  int res;
 
  CHECK_THREAD(self,NULL);

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

  /* ::TODO:: check if name points to already defined function and free relevant funccbinfo */

  if(callable!=Py_None && !PyCallable_Check(callable))
    {
      PyMem_Free(name);
      PyErr_SetString(PyExc_TypeError, "parameter must be callable");
      return NULL;
    }

  Py_INCREF(callable);

  cbinfo=allocfunccbinfo();
  cbinfo->name=name;
  cbinfo->scalarfunc=callable;

  res=sqlite3_create_function(self->db,
                              name,
                              numargs,
                              SQLITE_UTF8,  /* it isn't very clear what this parameter does */
                              (callable!=Py_None)?cbinfo:NULL,
                              (callable!=Py_None)?cbdispatch_func:NULL,
                              NULL,
                              NULL);

  if(res)
    {
      freefunccbinfo(cbinfo);
      SET_EXC(self->db, res);
      return NULL;
    }

  if(callable!=Py_None)
    {
      /* put cbinfo into the linked list */
      cbinfo->next=self->functions;
      self->functions=cbinfo;
    }
  else
    {
      /* free it since we cancelled the function */
      freefunccbinfo(cbinfo);
    }
  
  return Py_BuildValue("");
}

static PyObject *
Connection_createaggregatefunction(Connection *self, PyObject *args)
{
  int numargs=-1;
  PyObject *callable;
  char *name=0;
  char *chk;
  funccbinfo *cbinfo;
  int res;

  CHECK_THREAD(self,NULL);

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

  /* ::TODO:: check if name points to already defined function and free relevant funccbinfo */

  if(callable!=Py_None && !PyCallable_Check(callable))
    {
      PyMem_Free(name);
      PyErr_SetString(PyExc_TypeError, "parameter must be callable");
      return NULL;
    }

  Py_INCREF(callable);

  cbinfo=allocfunccbinfo();
  cbinfo->name=name;
  cbinfo->aggregatefactory=callable;

  res=sqlite3_create_function(self->db,
                              name,
                              numargs,
                              SQLITE_UTF8,  /* it isn't very clear what this parameter does */
                              (callable!=Py_None)?cbinfo:NULL,
                              NULL,
                              (callable!=Py_None)?cbdispatch_step:NULL,
                              (callable!=Py_None)?cbdispatch_final:NULL);

  if(res)
    {
      freefunccbinfo(cbinfo);
      SET_EXC(self->db, res);
      return NULL;
    }

  if(callable!=Py_None)
    {
      /* put cbinfo into the linked list */
      cbinfo->next=self->functions;
      self->functions=cbinfo;
    }
  else
    {
      /* free things up */
      freefunccbinfo(cbinfo);
    }
  
  return Py_BuildValue("");
}

/* USER DEFINED COLLATION CODE.*/

/*  We store the registered collations in a linked list hooked into
   the connection object so we can free them.  There is probably a
   better data structure to use but this was most convenient. */

static collationcbinfo *
freecollationcbinfo(collationcbinfo *collation)
{
  collationcbinfo *cnext;
  if(!collation) 
    return NULL;

  if(collation->name)
    PyMem_Free(collation->name);
  Py_XDECREF(collation->func);
  cnext=collation->next;
  PyMem_Free(collation);
  return cnext;
}

static collationcbinfo *
alloccollationcbinfo(void)
{
  collationcbinfo *res=PyMem_Malloc(sizeof(collationcbinfo));
  memset(res, 0, sizeof(collationcbinfo));
  return res;
}

static int 
collation_cb(void *context, 
	     int stringonelen, const void *stringonedata,
	     int stringtwolen, const void *stringtwodata)
{
  PyGILState_STATE gilstate;
  collationcbinfo *cbinfo=(collationcbinfo*)context;
  PyObject *pys1=NULL, *pys2=NULL, *retval=NULL, *pyargs=NULL;
  int result=0;

  assert(cbinfo);

  gilstate=PyGILState_Ensure();

  if(PyErr_Occurred()) goto finally;  /* outstanding error */

  pys1=convertutf8stringsize(stringonedata, stringonelen);
  pys2=convertutf8stringsize(stringtwodata, stringtwolen);

  if(!pys1 || !pys2)  
    goto finally;   /* failed to allocate strings */

  pyargs=PyTuple_New(2);
  if(!pyargs) 
    goto finally; /* failed to allocate arg tuple */

  PyTuple_SET_ITEM(pyargs, 0, pys1);
  PyTuple_SET_ITEM(pyargs, 1, pys2);

  pys1=pys2=NULL;  /* pyargs owns them now */

  assert(!PyErr_Occurred());

  retval=PyEval_CallObject(cbinfo->func, pyargs);

  if(!retval) goto finally;  /* execution failed */

  result=PyInt_AsLong(retval);
  if(PyErr_Occurred())
      result=0;


 finally:
  Py_XDECREF(pys1);
  Py_XDECREF(pys2);
  Py_XDECREF(retval);
  Py_XDECREF(pyargs);
  PyGILState_Release(gilstate);
  return result;

}

static PyObject *
Connection_createcollation(Connection *self, PyObject *args)
{
  PyObject *callable;
  char *name=0;
  char *chk;
  collationcbinfo *cbinfo;
  int res;

  CHECK_THREAD(self,NULL);
  
  if(!PyArg_ParseTuple(args, "esO:createcollation(name,callback)", STRENCODING, &name, &callable))
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

  /* ::TODO:: check if name points to already defined collation and free relevant collationcbinfo */

  if(callable!=Py_None && !PyCallable_Check(callable))
    {
      PyMem_Free(name);
      PyErr_SetString(PyExc_TypeError, "parameter must be callable");
      return NULL;
    }

  Py_INCREF(callable);

  cbinfo=alloccollationcbinfo();
  cbinfo->name=name;
  cbinfo->func=callable;

  res=sqlite3_create_collation(self->db,
                               name,
                               SQLITE_UTF8,
                               (callable!=Py_None)?cbinfo:NULL,
                               (callable!=Py_None)?collation_cb:NULL);
  if(res)
    {
      freecollationcbinfo(cbinfo);
      SET_EXC(self->db, res);
      return NULL;
    }

  if (callable!=Py_None)
    {
      /* put cbinfo into the linked list */
      cbinfo->next=self->collations;
      self->collations=cbinfo;
    }
  else
    {
      /* destroy info */
      freecollationcbinfo(cbinfo);
    }
  
  return Py_BuildValue("");
}

/* Virtual table code */

/* this function is outside of experimental since it is always called by the destructor */
static vtableinfo *
freevtableinfo(vtableinfo *vtinfo)
{
  vtableinfo *next;
  if(!vtinfo)
    return NULL;

  if(vtinfo->name)
    PyMem_Free(vtinfo->name);
  Py_XDECREF(vtinfo->datasource);
  /* connection was a borrowed reference so no decref needed */

  next=vtinfo->next;
  PyMem_Free(vtinfo);
  return next;
}


#ifdef EXPERIMENTAL

/* Calls the named method of object with the provided args */
static PyObject*
Py_CallMethod(PyObject *obj, const char *methodname, PyObject *args)
{
  PyObject *method=NULL;
  PyObject *res=NULL;

  /* we should only be called with ascii methodnames so no need to do
     character set conversions etc */
  method=PyObject_GetAttrString(obj, methodname);
  if (!method) 
    goto finally;

  res=PyEval_CallObject(method, args);

 finally:
  Py_XDECREF(method);
  return res;
}

/* Turns the current Python exception into an SQLite error code and
   stores the string in the errmsg field (if not NULL).  The errmsg
   field is expected to belong to sqlite and hence uses sqlite
   semantics/ownership - for example see the pzErr parameter to
   xCreate */

static int
MakeSqliteMsgFromPyException(char **errmsg)
{
  int res=SQLITE_ERROR;
  PyObject *str=NULL;
  PyObject *etype=NULL, *evalue=NULL, *etraceback=NULL;

  assert(PyErr_Occurred());
  if(PyErr_Occurred())
    {
      /* find out if the exception corresponds to an apsw exception descriptor */
      int i;
      for(i=0;exc_descriptors[i].code!=-1;i++)
	if(PyErr_ExceptionMatches(exc_descriptors[i].cls))
	{
	  res=exc_descriptors[i].code;
	  break;
	}
    }


  /* I just want a string of the error! */
  
  PyErr_Fetch(&etype, &evalue, &etraceback);
  if(!str && evalue)
    str=PyObject_Str(evalue);
  if(!str && etype)
    str=PyObject_Str(etype);
  if(!str)
    str=PyString_FromString("python exception with no information");
  if(evalue)
    PyErr_Restore(etype, evalue, etraceback);
  
  if(*errmsg)
    sqlite3_free(*errmsg);
  *errmsg=sqlite3_mprintf("%s",PyString_AsString(str));

  Py_XDECREF(str);
  return res;
}

typedef struct {
  sqlite3_vtab used_by_sqlite; /* I don't touch this */
  PyObject *vtable;            /* object implementing vtable */
} apsw_vtable;

static int vtabCreate(sqlite3 *db, 
		      void *pAux, 
		      int argc, 
		      const char *const *argv,
		      sqlite3_vtab **pVTab,
		      char **errmsg)
{
  PyGILState_STATE gilstate;
  vtableinfo *vti;
  PyObject *args=NULL, *res=NULL, *schema=NULL, *unischema=NULL, *vtable=NULL;
  apsw_vtable *avi=NULL;
  int sqliteres=SQLITE_OK;
  int i;
  
  gilstate=PyGILState_Ensure();

  vti=(vtableinfo*) pAux;
  assert(db==vti->connection->db);

  args=PyTuple_New(1+argc);
  if(!args) goto pyexception;

  Py_INCREF((PyObject*)(vti->connection));
  PyTuple_SET_ITEM(args, 0, (PyObject*)(vti->connection));
  for(i=0;i<argc;i++)
    {
      PyObject *str=convertutf8string(argv[i]);
      if(!str) 
	goto pyexception;
      PyTuple_SET_ITEM(args, 1+i, str);
    }

  res=Py_CallMethod(vti->datasource, "Create", args);
  if(!res)
    goto pyexception;

  /* res should be a tuple of two values - a string of sql describing
     the table and an object implementing it */
  if(!PySequence_Check(res) || PySequence_Size(res)!=2)
    {
      PyErr_Format(PyExc_TypeError, "Expected two values - a string with the table schema and a vtable object implementing it");
      goto pyexception;
    }
  
  vtable=PySequence_GetItem(res, 1);
  if(!vtable)
    goto pyexception;

  avi=PyMem_Malloc(sizeof(apsw_vtable));
  if(!avi) goto pyexception;
  assert((void*)avi==(void*)&(avi->used_by_sqlite)); /* detect if wierd padding happens */
  memset(avi, 0, sizeof(apsw_vtable));

  schema=PySequence_GetItem(res, 0);
  if(!schema) goto pyexception;

  {
    /* We need the schema as a UTF-8 string, but it could be a String
       or Unicode so we have to got to a lot of work to get the
       necessary conversions done and the utf8 bytes out */
    PyObject *utf8string=NULL;
    if(PyUnicode_Check(schema))
      {
	unischema=schema;
	Py_INCREF(unischema);
      }
    else
      {
	unischema=PyUnicode_FromObject(schema);
	if(!unischema)
	  goto pyexception;
      }
      
    assert(!PyErr_Occurred());
    utf8string=PyUnicode_AsUTF8String(unischema);
    if(!utf8string)
      goto pyexception;

    sqliteres=sqlite3_declare_vtab(db, PyString_AsString(utf8string));
    Py_DECREF(utf8string);
    if(sqliteres!=SQLITE_OK)
      {
	AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xCreate.sqlite3_declare_vtab", "{s: O}", "schema", schema);
	goto finally;
      }
  }
  
  assert(sqliteres==SQLITE_OK);
  *pVTab=(sqlite3_vtab*)avi;
  avi->vtable=vtable;
  Py_INCREF(avi->vtable);
  avi=NULL;
  goto finally;

 pyexception: /* we had an exception in python code */
  sqliteres=MakeSqliteMsgFromPyException(errmsg);
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xCreate", "{s: s, s: s, s: s}", "modulename", argv[0], "database", argv[1], "tablename", argv[2]);

 finally: /* cleanup */
  Py_XDECREF(args);  
  Py_XDECREF(res);
  Py_XDECREF(schema);
  Py_XDECREF(unischema);
  Py_XDECREF(vtable);
  if(avi)
    PyMem_Free(avi);

  PyGILState_Release(gilstate);
  return sqliteres;
}

/* it would be nice to use C99 style initializers here ... */
static struct sqlite3_module apsw_vtable_module=
  {
    1,                    /* version */
    vtabCreate,           /* xCreate */
    /* xConnect */
    /* xBestIndex */
    /* xDisconnect */
    /* xDestroy */
    /* xOpen */
    /* xClose */
    /* xFilter */
    /* xNext */
    /* xEof */
    /* xColumn */
    /* xRowid */
    /* xUpdate */
    /* xBegin */
    /* xSync */
    /* xCommit */
    /* xRollback */
    /* xFindFunction */
  };

static vtableinfo *
allocvtableinfo(void)
{
  vtableinfo *res=PyMem_Malloc(sizeof(vtableinfo));
  if(res)
    memset(res, 0, sizeof(vtableinfo));
  return res;
}

static PyObject *
Connection_createmodule(Connection *self, PyObject *args)
{
  char *name=NULL;
  PyObject *datasource=NULL;
  vtableinfo *vti;
  int res;

  CHECK_THREAD(self, NULL);

  if(!PyArg_ParseTuple(args, "esO:createmodule(name, datasource)", "utf_8", &name, &datasource))
    return NULL;

  Py_INCREF(datasource);
  vti=allocvtableinfo();
  vti->connection=self;
  vti->name=name;
  vti->datasource=datasource;

  /* ::TODO:: - can we call this with NULL to unregister a module? */
  res=sqlite3_create_module(self->db, name, &apsw_vtable_module, vti);
  SET_EXC(self->db, res);

  if(res!=SQLITE_OK)
    {
      freevtableinfo(vti);
      return NULL;
    }

  /* add vti to linked list */
  vti->next=self->vtables;
  self->vtables=vti;
  
  return Py_BuildValue("");
}

#endif /* EXPERIMENTAL */
/* end of Virtual table code */


static PyMethodDef Connection_methods[] = {
  {"cursor", (PyCFunction)Connection_cursor, METH_NOARGS,
   "Create a new cursor" },
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
  {"setauthorizer", (PyCFunction)Connection_setauthorizer, METH_O,
   "Sets an authorizer function"},
  {"setupdatehook", (PyCFunction)Connection_setupdatehook, METH_O,
      "Sets an update hook"},
  {"setrollbackhook", (PyCFunction)Connection_setrollbackhook, METH_O,
   "Sets a callable invoked before each rollback"},
#ifdef EXPERIMENTAL
  {"setprofile", (PyCFunction)Connection_setprofile, METH_O,
   "Sets a callable invoked with profile information after each statement"},
  {"setcommithook", (PyCFunction)Connection_setcommithook, METH_O,
   "Sets a callable invoked before each commit"},
  {"setprogresshandler", (PyCFunction)Connection_setprogresshandler, METH_VARARGS,
   "Sets a callback invoked periodically during long running calls"},
  {"enableloadextension", (PyCFunction)Connection_enableloadextension, METH_O,
   "Enables loading of SQLite extensions from shared libraries"},
  {"loadextension", (PyCFunction)Connection_loadextension, METH_VARARGS,
   "loads SQLite extension"},
  {"createmodule", (PyCFunction)Connection_createmodule, METH_VARARGS,
   "registers a virtual table"},
#endif
  {NULL}  /* Sentinel */
};


static PyTypeObject ConnectionType = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
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
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    "Connection object",       /* tp_doc */
    0,		               /* tp_traverse */
    0,		               /* tp_clear */
    0,		               /* tp_richcompare */
    0,		               /* tp_weaklistoffset */
    0,		               /* tp_iter */
    0,		               /* tp_iternext */
    Connection_methods,        /* tp_methods */
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)Connection_init, /* tp_init */
    0,                         /* tp_alloc */
    Connection_new,            /* tp_new */
};


/* CURSOR CODE */

/* Do finalization and free resources.  Returns the SQLITE error code */
static int
resetcursor(Cursor *self)
{
  int res=SQLITE_OK;

  Py_XDECREF(self->bindings);
  self->bindings=NULL;
  self->bindingsoffset=-1;

  if(self->statement)
    {
      res=sqlite3_finalize(self->statement);
      SET_EXC(self->connection->db, res);
      self->statement=0;
    }

  if(self->status!=C_DONE && self->zsqlnextpos)
    {
      if (*self->zsqlnextpos && res==SQLITE_OK)
        {
          /* We still have more, so this is actually an abort. */
          res=SQLITE_ERROR;
          if(!PyErr_Occurred())
            PyErr_Format(ExcIncomplete, "Error: there are still remaining sql statements to execute");
        }
    }
  self->zsqlnextpos=NULL;
  
  if(self->status!=C_DONE && self->emiter)
    {
      PyObject *next=PyIter_Next(self->emiter);
      if(next)
        {
          Py_DECREF(next);
          res=SQLITE_ERROR;
          if (!PyErr_Occurred())
	    /* Technically this line won't get executed since the
	       block above will already have set ExcIncomplete.
	       Leaving it in as defensive coding. */
            PyErr_Format(ExcIncomplete, "Error: there are still many remaining sql statements to execute");
        }
    }
     
  Py_XDECREF(self->emiter);
  self->emiter=NULL;

  if(self->zsql)
    {
      PyMem_Free((void*)self->zsql);
      self->zsql=0;
    }

  self->status=C_DONE;

  if (PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "resetcursor", NULL);

  return res;
}

static void
Cursor_dealloc(Cursor * self)
{
  /* thread check - we can't use macro as it returns*/
  PyObject *err_type, *err_value, *err_traceback;
  int have_error=PyErr_Occurred()?1:0;

  if(self->connection->thread_ident!=PyThread_get_thread_ident())
    {
      if (have_error)
        PyErr_Fetch(&err_type, &err_value, &err_traceback);
      PyErr_Format(PyExc_RuntimeError, "The destructor for Cursor is called in a different thread than it"
                   "was created in.  All calls must be in the same thread.  It was created in thread %d " 
                   "and this is %d.  SQLite is not being closed as a result.",
                   (int)(self->connection->thread_ident), (int)(PyThread_get_thread_ident()));            
      PyErr_WriteUnraisable((PyObject*)self);
      if (have_error)
        PyErr_Restore(err_type, err_value, err_traceback);
      
      return;
    }

  /* do our finalisation ... */

  if (have_error)
    {
      /* remember the existing error so that resetcursor won't immediately return */
      PyErr_Fetch(&err_type, &err_value, &err_traceback);
      PyErr_Clear();
    }

  resetcursor(self);
  if(PyErr_Occurred())
    PyErr_Clear(); /* clear out any exceptions from resetcursor since we don't care */

  if (have_error)
    /* restore earlier error if there was one */
    PyErr_Restore(err_type, err_value, err_traceback);

  /* we no longer need connection */
  if(self->connection)
    {
      Py_DECREF(self->connection);
      self->connection=0;
    }

  /* executemany iterator */
  Py_XDECREF(self->emiter);
  self->emiter=NULL;

  /* no need for tracing */
  Py_XDECREF(self->exectrace);
  Py_XDECREF(self->rowtrace);
  self->exectrace=self->rowtrace=0;
  
  self->ob_type->tp_free((PyObject*)self);
}

static void
Cursor_init(Cursor *self, Connection *connection)
{
  self->connection=connection;
  self->statement=0;
  self->zsql=0;
  self->zsqlnextpos=0;
  self->status=C_DONE;
  self->bindings=0;
  self->bindingsoffset=0;
  self->emiter=0;
  self->exectrace=0;
  self->rowtrace=0;
}

static PyObject *
Cursor_getdescription(Cursor *self)
{
  int ncols,i;
  PyObject *result=NULL;
  PyObject *pair=NULL;
  PyObject *first=NULL;
  PyObject *second=NULL;
  const char *str;

  CHECK_THREAD(self->connection,NULL);

  if(!self->statement)
    {
      PyErr_Format(ExcComplete, "Can't get description for statements that have completed execution");
      return NULL;
    }
  
  ncols=sqlite3_column_count(self->statement);
  result=PyTuple_New(ncols);
  if(!result) goto error;

  for(i=0;i<ncols;i++)
    {
      pair=PyTuple_New(2);
      if(!pair) goto error;

      str=sqlite3_column_name(self->statement, i);
      first=convertutf8string(str);

      str=sqlite3_column_decltype(self->statement, i);
      second=convertutf8string(str);

      if(!first || !second) goto error;

      PyTuple_SET_ITEM(pair, 0, first);
      PyTuple_SET_ITEM(pair, 1, second);

      /* owned by pair now */
      first=second=0;

      PyTuple_SET_ITEM(result, i, pair);
      /* owned by result now */
      pair=0;
    }
  
  return result;

 error:
  Py_XDECREF(result);
  Py_XDECREF(pair);
  Py_XDECREF(first);
  Py_XDECREF(second);
  return NULL;
}

/* internal function - returns SQLite error code (ie SQLITE_OK if all is well) */
static int
Cursor_dobinding(Cursor *self, int arg, PyObject *obj)
{

  /* DUPLICATE(ish) code: this is substantially similar to the code in
     set_context_result.  If you fix anything here then do it there as
     well. */

  int res=SQLITE_OK;

  if(PyErr_Occurred()) 
    return -1;

  if(obj==Py_None)
    res=sqlite3_bind_null(self->statement, arg);
  /* Python uses a 'long' for storage of PyInt.  This could
     be a 32bit or 64bit quantity depending on the platform. */
  else if(PyInt_Check(obj))
    res=sqlite3_bind_int64(self->statement, arg, PyInt_AS_LONG(obj));
  else if (PyLong_Check(obj))
    /* nb: PyLong_AsLongLong can cause Python level error */
    res=sqlite3_bind_int64(self->statement, arg, PyLong_AsLongLong(obj));
  else if (PyFloat_Check(obj))
    res=sqlite3_bind_double(self->statement, arg, PyFloat_AS_DOUBLE(obj));
  else if (PyUnicode_Check(obj))
    {
      const void *badptr=NULL;
      UNIDATABEGIN(obj)
        badptr=strdata;
        if(strdata)
          {
	    if(strbytes>APSW_INT32_MAX)
	      {
		PyErr_Format(ExcTooBig, "Unicode object is too large - SQLite only supports up to 2GB");
	      }
	    else
	      {
		if(use16)
		  res=sqlite3_bind_text16(self->statement, arg, strdata, (int)strbytes, SQLITE_TRANSIENT);
		else
		  res=sqlite3_bind_text(self->statement, arg, strdata, (int)strbytes, SQLITE_TRANSIENT);
	      }
          }
      UNIDATAEND(obj);
      if(!badptr) 
        {
          assert(PyErr_Occurred());
          return -1;
        }
    }
  else if (PyString_Check(obj))
    {
      const char *val=PyString_AS_STRING(obj);
      const size_t lenval=PyString_GET_SIZE(obj);
      const char *chk=val;
      for(;chk<val+lenval && !((*chk)&0x80); chk++);
      if(chk<val+lenval)
        {
          const void *badptr=NULL;
          PyObject *str2=PyUnicode_FromObject(obj);
          if(!str2)
            return -1;
          UNIDATABEGIN(str2)
            badptr=strdata;
            if(strdata)
              {
		if(strbytes>APSW_INT32_MAX)
		  {
		    PyErr_Format(ExcTooBig, "Unicode object is too large - SQLite only supports up to 2GB");
		  }
		else
		  {
		    if(use16)
		      res=sqlite3_bind_text16(self->statement, arg, strdata, (int)strbytes, SQLITE_TRANSIENT);
		    else
		      res=sqlite3_bind_text(self->statement, arg, strdata, (int)strbytes, SQLITE_TRANSIENT);
		  }
              }
          UNIDATAEND(str2);
          Py_DECREF(str2);
          if(!badptr) 
            {
              assert(PyErr_Occurred());
              return -1;
            }
        }
      else
	{
	  if(lenval>APSW_INT32_MAX)
	      {
		PyErr_Format(ExcTooBig, "String object is too large - SQLite only supports up to 2GB");
		return -1;
	      }
	  res=sqlite3_bind_text(self->statement, arg, val, (int)lenval, SQLITE_TRANSIENT);
	}
    }
  else if (PyBuffer_Check(obj))
    {
      const char *buffer;
      Py_ssize_t buflen;
      if(PyObject_AsCharBuffer(obj, &buffer, &buflen))
        return -1;
      if (buflen>APSW_INT32_MAX)
	{
	  PyErr_Format(ExcTooBig, "Binding object is too large - SQLite only supports up to 2GB");
	  return -1;
	}
      res=sqlite3_bind_blob(self->statement, arg, buffer, (int)buflen, SQLITE_TRANSIENT);
    }
  else 
    {
      PyObject *strrep=PyObject_Str(obj);
      PyErr_Format(PyExc_TypeError, "Bad binding argument type supplied - argument #%d: %s", (int)(arg+self->bindingsoffset), strrep?PyString_AsString(strrep):"<str failed>");
      Py_XDECREF(strrep);
      return -1;
    }
  if(res!=SQLITE_OK)
    {
      SET_EXC(self->connection->db, res);
      return -1;
    }
  if(PyErr_Occurred())
    return -1;
  return 0;
}

/* internal function */
static int
Cursor_dobindings(Cursor *self)
{
  int nargs, arg, res, sz=0;
  PyObject *obj;

  if(PyErr_Occurred()) 
    return -1;

  assert(self->bindingsoffset>=0);

  nargs=sqlite3_bind_parameter_count(self->statement);

  if (nargs>0 && !self->bindings)
    {
      PyErr_Format(ExcBindings, "Statement has %d bindings but you didn't supply any!", nargs);
      return -1;
    }

  /* a dictionary? */
  if (self->bindings && PyDict_Check(self->bindings))
    {
      for(arg=1;arg<=nargs;arg++)
        {
	  PyObject *keyo=NULL;
          const char *key=sqlite3_bind_parameter_name(self->statement, arg);

          if(!key)
            {
              PyErr_Format(ExcBindings, "Binding %d has no name, but you supplied a dict (which only has names).", arg-1);
              return -1;
            }

	  assert(*key==':' || *key=='$');
          key++; /* first char is a colon or dollar which we skip */

	  keyo=PyUnicode_DecodeUTF8(key, strlen(key), NULL);
	  if(!keyo) 
	    return -1;

	  obj=PyDict_GetItem(self->bindings, keyo);
	  Py_DECREF(keyo);

          if(!obj)
            /* this is where we could error on missing keys */
            continue;
          if(Cursor_dobinding(self,arg,obj))
            {
              assert(PyErr_Occurred());
              return -1;
            }
        }

      return 0;
    }

  /* it must be a fast sequence */
  /* verify the number of args supplied */
  if (self->bindings)
    sz=PySequence_Fast_GET_SIZE(self->bindings);
  /* there is another statement after this one ... */
  if(*self->zsqlnextpos && sz-self->bindingsoffset<nargs)
    {
      PyErr_Format(ExcBindings, "Incorrect number of bindings supplied.  The current statement uses %d and there are only %d left.  Current offset is %d",
                   nargs, (self->bindings)?sz:0, (int)(self->bindingsoffset));
      return -1;
    }
  /* no more statements */
  if(!*self->zsqlnextpos && sz-self->bindingsoffset!=nargs)
    {
      PyErr_Format(ExcBindings, "Incorrect number of bindings supplied.  The current statement uses %d and there are %d supplied.  Current offset is %d",
                   nargs, (self->bindings)?sz:0, (int)(self->bindingsoffset));
      return -1;
    }
  
  res=SQLITE_OK;

  /* nb sqlite starts bind args at one not zero */
  for(arg=1;arg<=nargs;arg++)
    {
      obj=PySequence_Fast_GET_ITEM(self->bindings, arg-1+self->bindingsoffset);
      if(Cursor_dobinding(self, arg, obj))
        {
          assert(PyErr_Occurred());
          return -1;
        }
    }

  self->bindingsoffset+=nargs;
  assert(res==0);
  return 0;
}

typedef struct { 
  const char *previouszsqlpos;  /* where the begining of the statement was */
  Py_ssize_t savedbindingsoffset;      /* where the bindings began */
} exectrace_oldstate;
  
static int
Cursor_doexectrace(Cursor *self, exectrace_oldstate *etos)
{
  PyObject *retval=NULL;
  PyObject *args=NULL;
  PyObject *sqlcmd=NULL;
  PyObject *bindings=NULL;
  int result;

  assert(self->exectrace);

  /* make a string of the command */
  sqlcmd=convertutf8stringsize(etos->previouszsqlpos, self->zsqlnextpos-etos->previouszsqlpos);

  if(!sqlcmd) 
    return -1;
  /* now deal with the bindings */
  if(self->bindings)
    {
      if(PyDict_Check(self->bindings))
        {
          bindings=self->bindings;
          Py_INCREF(self->bindings);
        }
      else
        {
          bindings=PySequence_GetSlice(self->bindings, etos->savedbindingsoffset, self->bindingsoffset);
          if(!bindings)
            {
              Py_DECREF(sqlcmd);
              return -1;
            }
        }
    }
  else
    {
      bindings=Py_None;
      Py_INCREF(bindings);
    }
  args=PyTuple_New(2);
  if(!args)
    {
      Py_DECREF(sqlcmd);
      Py_DECREF(bindings);
      return -1;
    }
  PyTuple_SET_ITEM(args, 0, sqlcmd);
  PyTuple_SET_ITEM(args, 1, bindings);
  
  retval=PyEval_CallObject(self->exectrace, args);
  Py_DECREF(args);
  if(!retval) 
    {
      assert(PyErr_Occurred());
      return -1;
    }
  result=PyObject_IsTrue(retval);
  Py_DECREF(retval);
  assert (result==-1 || result==0 || result ==1);
  if(result==-1)
    {
      assert(PyErr_Occurred());
      return -1;
    }
  if(result)
    return 0;

  /* callback didn't want us to continue */
  PyErr_Format(ExcTraceAbort, "Aborted by false/null return value of exec tracer");
  return -1;
}

static PyObject*
Cursor_dorowtrace(Cursor *self, PyObject *retval)
{
  assert(self->rowtrace);

  retval=PyEval_CallObject(self->rowtrace, retval);
  if(!retval) 
    return NULL;
  
  return retval;
}

/* Returns a borrowed reference to self if all is ok, else NULL on error */
static PyObject *
Cursor_step(Cursor *self)
{
  int res;
  exectrace_oldstate etos;

  if(self->status==C_DONE)
    {
      PyErr_Format(ExcComplete, "The statement(s) have finished or errored, so you can't keep running them");
      return NULL;
    }

  for(;;)
    {
      assert(!PyErr_Occurred());
      Py_BEGIN_ALLOW_THREADS
        res=sqlite3_step(self->statement);
      Py_END_ALLOW_THREADS;

      switch(res)
        {
        case SQLITE_ROW:
          self->status=C_ROW;
          return (PyErr_Occurred())?(NULL):((PyObject*)self);
        case SQLITE_BUSY:
          self->status=C_BEGIN;
          SET_EXC(self->connection->db,res);
          return NULL;

        default:    /* no other value should happen, but we'll
                       defensively code and treat them the same as
                        SQLITE_ERROR */
          /* FALLTHRU */
        case SQLITE_ERROR:
          /* there was an error - we need to get actual error code from sqlite3_finalize */
          self->status=C_DONE;
          res=resetcursor(self);  /* this will get the error code for us */
          assert(res!=SQLITE_OK);
          return NULL;

        case SQLITE_MISUSE:
          /* this would be an error in apsw itself */
          self->status=C_DONE;
          SET_EXC(self->connection->db,res);
          resetcursor(self);
          return NULL;

        case SQLITE_DONE:
	  if (PyErr_Occurred())
	    {
	      self->status=C_DONE;
	      return NULL;
	    }
          break;
          
        }
      assert(res==SQLITE_DONE);

      /* done with that statement, are there any more? */
      self->status=C_DONE;
      if(!self->zsqlnextpos || !*self->zsqlnextpos)
        {
          PyObject *next;
          if(!self->emiter)
            {
              /* no more so we finalize */
              if(resetcursor(self)!=SQLITE_OK)
                {
                  assert(PyErr_Occurred());
                  return NULL; /* exception */
                }
              return (PyObject*)self;
            }
          next=PyIter_Next(self->emiter);
          if(PyErr_Occurred())
            return NULL;
          if(!next)
            {
              /* no more from executemanyiter so we finalize */
              if(resetcursor(self)!=SQLITE_OK)
                {
                  assert(PyErr_Occurred());
                  return NULL;
                }
              return (PyObject*)self;
            }
          self->zsqlnextpos=self->zsql; /* start at begining of string again */
          /* don't need bindings from last round if emiter.next() */
          Py_XDECREF(self->bindings);
          self->bindings=0;
          self->bindingsoffset=0;
          /* verify type of next before putting in bindings */
          if(PyDict_Check(next))
            self->bindings=next;
          else
            {
              self->bindings=PySequence_Fast(next, "You must supply a dict or a sequence");
              /* we no longer need next irrespective of what happens in line above */
              Py_DECREF(next);
              if(!self->bindings)
                return NULL;
            }
          assert(self->bindings);
        }

      /* finalise and go again */
      res=sqlite3_finalize(self->statement);
      self->statement=0;
      SET_EXC(self->connection->db,res);
      if (res!=SQLITE_OK)
        {
          assert(res!=SQLITE_BUSY); /* finalize shouldn't be returning busy, only step */
          return NULL;
        }

      assert(!self->statement);
      if(self->exectrace)
        {
          etos.previouszsqlpos=self->zsqlnextpos;
          etos.savedbindingsoffset=self->bindingsoffset;
        }
      res=sqlite3_prepare(self->connection->db, self->zsqlnextpos, -1, &self->statement, &self->zsqlnextpos);
      SET_EXC(self->connection->db,res);
      if (res!=SQLITE_OK)
        {
          assert(res!=SQLITE_BUSY); /* prepare definitely shouldn't be returning busy */
          return NULL;
        }

      if(Cursor_dobindings(self))
        {
          assert(PyErr_Occurred());
          return NULL;
        }

      if(self->exectrace)
        {
          if(Cursor_doexectrace(self, &etos))
            {
              assert(self->status==C_DONE);
              assert(PyErr_Occurred());
              return NULL;
            }
        }
      assert(self->status==C_DONE);
      self->status=C_BEGIN;
    }

  /* you can't actually get here */
  assert(0);
  return NULL;
}

static PyObject *
Cursor_execute(Cursor *self, PyObject *args)
{
  int res;
  PyObject *retval=NULL;
  exectrace_oldstate etos;

  CHECK_THREAD(self->connection, NULL);

  res=resetcursor(self);
  if(res!=SQLITE_OK)
    return NULL;
  
  assert(!self->bindings);

  if(!PyArg_ParseTuple(args, "es|O:execute(statements,bindings=())", STRENCODING, &self->zsql, &self->bindings))
    return NULL;

  if(self->bindings)
    {
      if(PyDict_Check(self->bindings))
        Py_INCREF(self->bindings);
      else
        {
          self->bindings=PySequence_Fast(self->bindings, "You must supply a dict or a sequence");
          if(!self->bindings)
            return NULL;
        }
    }

  assert(!self->statement);
  if(self->exectrace)
    {
      etos.previouszsqlpos=self->zsql;
      etos.savedbindingsoffset=0;
    }
  res=sqlite3_prepare(self->connection->db, self->zsql, -1, &self->statement, &self->zsqlnextpos);
  SET_EXC(self->connection->db,res);
  if (res!=SQLITE_OK)
      return NULL;
  
  self->bindingsoffset=0;
  if(Cursor_dobindings(self))
    {
      assert(PyErr_Occurred());
      return NULL;
    }

  if(self->exectrace)
    {
      if(Cursor_doexectrace(self, &etos))
        {
          assert(PyErr_Occurred());
          return NULL;  
        }
    }

  self->status=C_BEGIN;

  retval=Cursor_step(self);
  if (!retval) 
    {
      assert(PyErr_Occurred());
      return NULL;
    }
  Py_INCREF(retval);
  return retval;
}

static PyObject *
Cursor_executemany(Cursor *self, PyObject *args)
{
  int res;
  PyObject *retval=NULL;
  PyObject *theiterable=NULL;
  PyObject *next=NULL;
  exectrace_oldstate etos;

  CHECK_THREAD(self->connection, NULL);

  res=resetcursor(self);
  if(res!=SQLITE_OK)
    return NULL;
  
  assert(!self->bindings);
  assert(!self->emiter);
  assert(!self->zsql);
  assert(self->status=C_DONE);

  if(!PyArg_ParseTuple(args, "esO:executemany(statements, sequenceofbindings)", STRENCODING, &self->zsql, &theiterable))
    return NULL;

  self->emiter=PyObject_GetIter(theiterable);
  if (!self->emiter)
    {
      PyErr_Format(PyExc_TypeError, "2nd parameter must be iterable");
      return NULL;
    }

  next=PyIter_Next(self->emiter);
  if(!next && PyErr_Occurred())
    return NULL;
  if(!next)
    {
      /* empty list */
      Py_INCREF(self);
      return (PyObject*)self;
    }

  if(PyDict_Check(next))
    self->bindings=next;
  else
    {
      self->bindings=PySequence_Fast(next, "You must supply a dict or a sequence");
      Py_DECREF(next); /* _Fast makes new reference */
      if(!self->bindings)
          return NULL;
    }

  assert(!self->statement);
  if(self->exectrace)
    {
      etos.previouszsqlpos=self->zsql;
      etos.savedbindingsoffset=0;
    }
  res=sqlite3_prepare(self->connection->db, self->zsql, -1, &self->statement, &self->zsqlnextpos);
  SET_EXC(self->connection->db,res);
  if (res!=SQLITE_OK)
    return NULL;
  
  self->bindingsoffset=0;
  if(Cursor_dobindings(self))
    {
      assert(PyErr_Occurred());
      return NULL;
    }

  if(self->exectrace)
    {
      if(Cursor_doexectrace(self, &etos))
        {
          assert(PyErr_Occurred());
          return NULL;  
        }
    }

  self->status=C_BEGIN;

  retval=Cursor_step(self);
  if (!retval) 
    {
      assert(PyErr_Occurred());
      return NULL;
    }
  Py_INCREF(retval);
  return retval;
}

static PyObject *
Cursor_next(Cursor *self)
{
  PyObject *retval;
  PyObject *item;
  int numcols=-1;
  int i;

  CHECK_THREAD(self->connection, NULL);

 again:
  if(self->status==C_BEGIN)
    if(!Cursor_step(self))
      {
        assert(PyErr_Occurred());
        return NULL;
      }
  if(self->status==C_DONE)
    return NULL;

  assert(self->status==C_ROW);

  self->status=C_BEGIN;
  
  /* DUPLICATE(ish) code: this is substantially similar to the code in
     convert_value_to_pyobject.  If you fix anything here then do it
     there as well. */

  /* return the row of data */
  numcols=sqlite3_data_count(self->statement);
  retval=PyTuple_New(numcols);
  if(!retval) 
    return NULL;

  for(i=0;i<numcols;i++)
    {
      item=convert_value_to_pyobject(sqlite3_column_value(self->statement, i));
      if(!item) 
	return NULL;
      PyTuple_SET_ITEM(retval, i, item);
    }
  if(self->rowtrace)
    {
      PyObject *r2=Cursor_dorowtrace(self, retval);
      Py_DECREF(retval);
      if(!r2) 
	return NULL;
      if (r2==Py_None)
        {
          Py_DECREF(r2);
          goto again;
        }
      return r2;
    }
  return retval;
}

static PyObject *
Cursor_iter(Cursor *self)
{
  CHECK_THREAD(self->connection, NULL);

  Py_INCREF(self);
  return (PyObject*)self;
}

static PyObject *
Cursor_setexectrace(Cursor *self, PyObject *func)
{
  CHECK_THREAD(self->connection, NULL);

  if(func!=Py_None && !PyCallable_Check(func))
    {
      PyErr_SetString(PyExc_TypeError, "parameter must be callable");
      return NULL;
    }

  if(func!=Py_None)
    Py_INCREF(func);

  Py_XDECREF(self->exectrace);
  self->exectrace=(func!=Py_None)?func:NULL;

  return Py_BuildValue("");
}

static PyObject *
Cursor_setrowtrace(Cursor *self, PyObject *func)
{
  CHECK_THREAD(self->connection, NULL);

  if(func!=Py_None && !PyCallable_Check(func))
    {
      PyErr_SetString(PyExc_TypeError, "parameter must be callable");
      return NULL;
    }

  if(func!=Py_None)
    Py_INCREF(func);

  Py_XDECREF(self->rowtrace);
  self->rowtrace=(func!=Py_None)?func:NULL;

  return Py_BuildValue("");
}

static PyObject *
Cursor_getexectrace(Cursor *self)
{
  PyObject *ret;

  CHECK_THREAD(self->connection, NULL);

  ret=(self->exectrace)?(self->exectrace):Py_None;
  Py_INCREF(ret);
  return ret;
}

static PyObject *
Cursor_getrowtrace(Cursor *self)
{
  PyObject *ret;
  CHECK_THREAD(self->connection, NULL);
  ret =(self->rowtrace)?(self->rowtrace):Py_None;
  Py_INCREF(ret);
  return ret;
}

static PyObject *
Cursor_getconnection(Cursor *self)
{
  CHECK_THREAD(self->connection, NULL);

  Py_INCREF(self->connection);
  return (PyObject*)self->connection;
}

static PyMethodDef Cursor_methods[] = {
  {"execute", (PyCFunction)Cursor_execute, METH_VARARGS,
   "Executes one or more statements" },
  {"executemany", (PyCFunction)Cursor_executemany, METH_VARARGS,
   "Repeatedly executes statements on sequence" },
  {"next", (PyCFunction)Cursor_next, METH_NOARGS,
   "Returns next row returned from query"},
  {"setexectrace", (PyCFunction)Cursor_setexectrace, METH_O,
   "Installs a function called for every statement executed"},
  {"setrowtrace", (PyCFunction)Cursor_setrowtrace, METH_O,
   "Installs a function called for every row returned"},
  {"getexectrace", (PyCFunction)Cursor_getexectrace, METH_NOARGS,
   "Returns the current exec tracer function"},
  {"getrowtrace", (PyCFunction)Cursor_getrowtrace, METH_NOARGS,
   "Returns the current row tracer function"},
  {"getrowtrace", (PyCFunction)Cursor_getrowtrace, METH_NOARGS,
   "Returns the current row tracer function"},
  {"getconnection", (PyCFunction)Cursor_getconnection, METH_NOARGS,
   "Returns the connection object for this cursor"},
  {"getdescription", (PyCFunction)Cursor_getdescription, METH_NOARGS,
   "Returns the description for the current row"},
  {NULL}  /* Sentinel */
};


static PyTypeObject CursorType = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "apsw.Cursor",             /*tp_name*/
    sizeof(Cursor),            /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)Cursor_dealloc, /*tp_dealloc*/
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
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_ITER , /*tp_flags*/
    "Cursor object",           /* tp_doc */
    0,		               /* tp_traverse */
    0,		               /* tp_clear */
    0,		               /* tp_richcompare */
    0,		               /* tp_weaklistoffset */
    (getiterfunc)Cursor_iter,  /* tp_iter */
    (iternextfunc)Cursor_next, /* tp_iternext */
    Cursor_methods,            /* tp_methods */
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
};

/* MODULE METHODS */
static PyObject *
getsqliteversion(void)
{
  return PyString_FromString(sqlite3_libversion());
}

static PyObject *
getapswversion(void)
{
  return PyString_FromString(APSW_VERSION);
}

static PyObject *
enablesharedcache(PyObject *self, PyObject *args)
{
  int setting,res;
  if(!PyArg_ParseTuple(args, "i:enablesharedcache(boolean)", &setting))
    return NULL;

  res=sqlite3_enable_shared_cache(setting);
  SET_EXC(NULL, res);

  if(res!=SQLITE_OK)
    return NULL;

  return Py_BuildValue("");
}

static PyMethodDef module_methods[] = {
  {"sqlitelibversion", (PyCFunction)getsqliteversion, METH_NOARGS,
   "Return the version of the SQLite library"},
  {"apswversion", (PyCFunction)getapswversion, METH_NOARGS,
   "Return the version of the APSW wrapper"},
  {"enablesharedcache", (PyCFunction)enablesharedcache, METH_VARARGS,
   "Sets shared cache semantics for this thread"},

    {NULL}  /* Sentinel */
};



#ifndef PyMODINIT_FUNC	/* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif
PyMODINIT_FUNC
initapsw(void) 
{
    PyObject* m;

    assert(sizeof(int)==4);             /* we expect 32 bit ints */
    assert(sizeof(long long)==8);             /* we expect 64 bit long long */

    if (PyType_Ready(&ConnectionType) < 0)
        return;

    if (PyType_Ready(&CursorType) < 0)
        return;

    /* ensure threads are available */
    PyEval_InitThreads();


    m = Py_InitModule3("apsw", module_methods,
                       "Another Python SQLite Wrapper.");

    if (m == NULL)
      return;

    if(init_exceptions(m))
      {
        fprintf(stderr, "init_exceptions failed\n");
        return;
      }

    Py_INCREF(&ConnectionType);
    PyModule_AddObject(m, "Connection", (PyObject *)&ConnectionType);

    /* we don't add cursor to the module since users shouldn't be able to instantiate them directly */

    /* add in some constants */

#define ADDINT(v) PyModule_AddObject(m, #v, Py_BuildValue("i", v));

    ADDINT(SQLITE_DENY);
    ADDINT(SQLITE_IGNORE);
    ADDINT(SQLITE_OK);

    /* authorizer functions */
    ADDINT(SQLITE_CREATE_INDEX);
    ADDINT(SQLITE_CREATE_TABLE);
    ADDINT(SQLITE_CREATE_TEMP_INDEX);
    ADDINT(SQLITE_CREATE_TEMP_TABLE);
    ADDINT(SQLITE_CREATE_TEMP_TRIGGER);
    ADDINT(SQLITE_CREATE_TEMP_VIEW);
    ADDINT(SQLITE_CREATE_TRIGGER);
    ADDINT(SQLITE_CREATE_VIEW);
    ADDINT(SQLITE_DELETE);
    ADDINT(SQLITE_DROP_INDEX);
    ADDINT(SQLITE_DROP_TABLE);
    ADDINT(SQLITE_DROP_TEMP_INDEX);
    ADDINT(SQLITE_DROP_TEMP_TABLE);
    ADDINT(SQLITE_DROP_TEMP_TRIGGER);
    ADDINT(SQLITE_DROP_TEMP_VIEW);
    ADDINT(SQLITE_DROP_TRIGGER);
    ADDINT(SQLITE_DROP_VIEW);
    ADDINT(SQLITE_INSERT);
    ADDINT(SQLITE_PRAGMA);
    ADDINT(SQLITE_READ);
    ADDINT(SQLITE_SELECT);
    ADDINT(SQLITE_TRANSACTION);
    ADDINT(SQLITE_UPDATE);
    ADDINT(SQLITE_ATTACH);
    ADDINT(SQLITE_DETACH);
    ADDINT(SQLITE_ALTER_TABLE);
    ADDINT(SQLITE_REINDEX);
    ADDINT(SQLITE_COPY);
    ADDINT(SQLITE_ANALYZE);
    ADDINT(SQLITE_CREATE_VTABLE);
    ADDINT(SQLITE_DROP_VTABLE);

    /* Version number */
    ADDINT(SQLITE_VERSION_NUMBER);
}

