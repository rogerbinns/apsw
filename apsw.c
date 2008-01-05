/*
  Another Python Sqlite Wrapper

  This wrapper aims to be the minimum necessary layer over SQLite 3
  itself.

  It assumes we are running as 32 bit int with a 64 bit long long type
  available.

  Copyright (C) 2004-2007 Roger Binns <rogerb@rogerbinns.com>

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


/* SQLite amalgamation */
#ifdef APSW_USE_SQLITE_AMALGAMATION
/* See SQLite ticket 2554 */
#define SQLITE_API static
#define SQLITE_EXTERN static
#include APSW_USE_SQLITE_AMALGAMATION
#else
/* SQLite 3 headers */
#include "sqlite3.h"
#endif

#if SQLITE_VERSION_NUMBER < 3005000
#error Your SQLite version is too old.  It must be at least 3.5.0
#endif

/* system headers */
#include <assert.h>
#include <stdarg.h>

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

/* Python 2.3 doesn't have this */
#ifndef Py_RETURN_NONE
#define Py_RETURN_NONE return Py_INCREF(Py_None), Py_None
#endif

/* A module to augment tracebacks */
#include "traceback.c"

/* A list of pointers (used by Connection to keep track of Cursors) */
#include "pointerlist.c"

/* Prepared statement caching */
/* #define SCSTATS */
#define STATEMENTCACHE_LINKAGE static
#include "statementcache.c"

/* used to decide if we will use int or long long */
#define APSW_INT32_MIN (-2147483647-1)
#define APSW_INT32_MAX 2147483647

/* The module object */
static PyObject *apswmodule;

/* The encoding we use with SQLite.  SQLite supports either utf8 or 16
   bit unicode (host byte order).  If the latter is used then all
   functions have "16" appended to their name.  The encoding used also
   affects how strings are stored in the database.  We use utf8 since
   it is more space efficient, and Python can't make its mind up about
   Unicode (it uses 16 or 32 bit unichars and often likes to use Byte
   Order Markers as well). */
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

#define CHECK_CLOSED(connection,e) \
{ if(!connection->db) { PyErr_Format(ExcConnectionClosed, "The connection has been closed"); return e; } }

/* EXCEPTION TYPES */

static PyObject *APSWException;  /* root exception class */
static PyObject *ExcThreadingViolation; /* thread misuse */
static PyObject *ExcIncomplete;  /* didn't finish previous query */
static PyObject *ExcBindings;  /* wrong number of bindings */
static PyObject *ExcComplete;  /* query is finished */
static PyObject *ExcTraceAbort; /* aborted by exectrace */
static PyObject *ExcExtensionLoading; /* error loading extension */
static PyObject *ExcConnectionNotClosed; /* connection wasn't closed when destructor called */
static PyObject *ExcConnectionClosed; /* connection was closed when function called */

static struct { int code; const char *name; PyObject *cls;}
exc_descriptors[]=
  {
    /* Generic Errors */
    {SQLITE_ERROR,    "SQL", NULL},       
    {SQLITE_MISMATCH, "Mismatch", NULL},

    /* Internal Errors */
    {SQLITE_INTERNAL, "Internal", NULL},  /* NOT USED */
    {SQLITE_PROTOCOL, "Protocol", NULL},
    {SQLITE_MISUSE,   "Misuse", NULL},
    {SQLITE_RANGE,    "Range", NULL},

    /* permissions etc */
    {SQLITE_PERM,     "Permissions", NULL},
    {SQLITE_READONLY, "ReadOnly", NULL},
    {SQLITE_CANTOPEN, "CantOpen", NULL},
    {SQLITE_AUTH,     "Auth", NULL},

    /* abort/busy/etc */
    {SQLITE_ABORT,    "Abort", NULL},
    {SQLITE_BUSY,     "Busy", NULL},
    {SQLITE_LOCKED,   "Locked", NULL},
    {SQLITE_INTERRUPT,"Interrupt", NULL},
    {SQLITE_SCHEMA,   "SchemaChange", NULL}, 
    {SQLITE_CONSTRAINT, "Constraint", NULL},

    /* memory/disk/corrupt etc */
    {SQLITE_NOMEM,    "NoMem", NULL},
    {SQLITE_IOERR,    "IO", NULL},
    {SQLITE_CORRUPT,  "Corrupt", NULL},
    {SQLITE_FULL,     "Full", NULL},
    {SQLITE_TOOBIG,   "TooBig", NULL},
    {SQLITE_NOLFS,    "NoLFS", NULL},
    {SQLITE_EMPTY,    "Empty", NULL},
    {SQLITE_FORMAT,   "Format", NULL},
    {SQLITE_NOTADB,   "NotADB", NULL},

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
  EXC(ExcExtensionLoading, "ExtensionLoadingError");
  EXC(ExcConnectionNotClosed, "ConnectionNotClosedError");
  EXC(ExcConnectionClosed, "ConnectionClosedError");

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
    if (exc_descriptors[i].code==(res&0xff))
      {
	PyObject *etype, *eval, *etb;
        assert(exc_descriptors[i].cls);
        PyErr_Format(exc_descriptors[i].cls, "%sError: %s", exc_descriptors[i].name, db?(sqlite3_errmsg(db)):"error");
	PyErr_Fetch(&etype, &eval, &etb);
	PyErr_NormalizeException(&etype, &eval, &etb);
	PyObject_SetAttrString(eval, "result", Py_BuildValue("i", res&0xff));
	PyObject_SetAttrString(eval, "extendedresult", Py_BuildValue("i", res));
	PyErr_Restore(etype, eval, etb);
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


/* The default Python PyErr_WriteUnraiseable is almost useless.  It
   only prints the str() of the exception and the str() of the object
   passed in.  This gives the developer no clue whatsoever where in
   the code it is happening.  It also does funky things to the passed
   in object which can cause the destructor to fire twice.
   Consequently we use our version here.  It makes a traceback if
   necessary, invokes sys.excepthook and if that fails then
   PyErr_Display. When we return, any error will be cleared. */
static void 
apsw_write_unraiseable(void)
{
  PyObject *err_type=NULL, *err_value=NULL, *err_traceback=NULL;
  PyObject *excepthook=NULL;
  PyObject *result=NULL;
  
  PyErr_Fetch(&err_type, &err_value, &err_traceback);
  PyErr_NormalizeException(&err_type, &err_value, &err_traceback);

  /* err_traceback is normally NULL, so lets fake one */
  if(!err_traceback)
    {
      PyObject *e2t=NULL, *e2v=NULL, *e2tb=NULL;
      PyFrameObject *frame = PyThreadState_GET()->frame;
      while(frame)
	{
	  PyTraceBack_Here(frame);
	  frame=frame->f_back;
	}
      PyErr_Fetch(&e2t, &e2v, &e2tb);
      Py_XDECREF(e2t);
      Py_XDECREF(e2v);
      err_traceback=e2tb;
    }

  excepthook=PySys_GetObject("excepthook");
  if(excepthook)
    result=PyEval_CallFunction(excepthook, "(OOO)", err_type?err_type:Py_None, err_value?err_value:Py_None, err_traceback?err_traceback:Py_None);
  if(!excepthook || !result)
    PyErr_Display(err_type, err_value, err_traceback);

  /* excepthook is a borrowed reference */
  Py_XDECREF(result);
  Py_XDECREF(err_traceback);
  Py_XDECREF(err_value);
  Py_XDECREF(err_type);
  PyErr_Clear();
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

  if(errmsg)
    {
      /* I just want a string of the error! */
      
      PyErr_Fetch(&etype, &evalue, &etraceback);
      if(!str && evalue)
	str=PyObject_Str(evalue);
      if(!str && etype)
	str=PyObject_Str(etype);
      if(!str)
	str=PyString_FromString("python exception with no information");
      if(etype)
	PyErr_Restore(etype, evalue, etraceback);
  
      if(*errmsg)
	sqlite3_free(*errmsg);
      *errmsg=sqlite3_mprintf("%s",PyString_AsString(str));

      Py_XDECREF(str);
    }

  return res;
}

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
  Connection *connection;         /* the Connection this is registered against so we don't
				     have to have a global table mapping sqlite3_db* to
				     Connection* */
} vtableinfo;

/* forward declarations */
static vtableinfo *freevtableinfo(vtableinfo *);

/* CONNECTION TYPE */

struct Connection { 
  PyObject_HEAD
  sqlite3 *db;                    /* the actual database connection */
  const char *filename;           /* utf8 filename of the database */
  int co_linenumber;              /* line number of allocation */
  PyObject *co_filename;          /* filename of allocation */
  long thread_ident;              /* which thread we were made in */

  pointerlist cursors;            /* tracking cursors */
  StatementCache *stmtcache;      /* prepared statement cache */

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

  /* see sqlite3_prepare_v2 for the origin of these */
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
  
} APSWCursor;

static PyTypeObject APSWCursorType;

/* forward declarations */
static PyObject *APSWCursor_close(APSWCursor *self, PyObject *args);

/* BLOB TYPE */
typedef struct {
  PyObject_HEAD
  Connection *connection;
  sqlite3_blob *pBlob;
  int curoffset;
} APSWBlob;

static PyTypeObject APSWBlobType;



/* CONVENIENCE FUNCTIONS */

/* Convert a NULL terminated UTF-8 string into a Python object.  None
   is returned if NULL is passed in. */
static PyObject *
convertutf8string(const char *str)
{
  if(!str)
    Py_RETURN_NONE;

  /* new behaviour in 3.3.8 - always return unicode strings */
  return PyUnicode_DecodeUTF8(str, strlen(str), NULL);
}

/* Convert a pointer and size UTF-8 string into a Python object.
   Pointer must be non-NULL. */
static PyObject *
convertutf8stringsize(const char *str, Py_ssize_t size)
{
  assert(str);
  assert(size>=0);
  
  /* new behaviour in 3.3.8 - always return Unicode strings */
  return PyUnicode_DecodeUTF8(str, size, NULL);
}

/* Returns a PyString encoded in UTF8 - new reference.
   Use PyString_AsString on the return value to get a
   const char * to utf8 bytes */
static PyObject *
getutf8string(PyObject *string)
{
  PyObject *inunicode=NULL;
  PyObject *utf8string=NULL;

  if(PyUnicode_Check(string))
    {
      inunicode=string;
      Py_INCREF(string);
    }
  else
    {
      inunicode=PyUnicode_FromObject(string);
      if(!inunicode) 
	return NULL;
    }
  assert(!PyErr_Occurred());

  utf8string=PyUnicode_AsUTF8String(inunicode);
  Py_DECREF(inunicode);
  return utf8string;
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
Connection_internal_cleanup(Connection *self)
{
  if(self->filename)
    {
      PyMem_Free((void*)self->filename);
      self->filename=0;
    }

  Py_XDECREF(self->co_filename);
  self->co_filename=0;

  /* free functions */
  {
    funccbinfo *func=self->functions;
    while((func=freefunccbinfo(func)));
    self->functions=0;
  }

  /* free collations */
  {
    collationcbinfo *coll=self->collations;
    while((coll=freecollationcbinfo(coll)));
    self->collations=0;
  }

  /* free vtables */
  {
    vtableinfo *vtinfo=self->vtables;
    while((vtinfo=freevtableinfo(vtinfo)));
    self->vtables=0;
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

}

static PyObject *
Connection_close(Connection *self, PyObject *args)
{
  PyObject *cursorcloseargs=NULL;
  int res;
  pointerlist_visit plv;
  int force=0;

  if(!self->db)
    goto finally;

  CHECK_THREAD(self,NULL);

  assert(!PyErr_Occurred());

  if(!PyArg_ParseTuple(args, "|i:close(force=False)", &force))
    return NULL;

  cursorcloseargs=Py_BuildValue("(i)", force);
  if(!cursorcloseargs) return NULL;

  for(pointerlist_visit_begin(&self->cursors, &plv);
      pointerlist_visit_finished(&plv);
      pointerlist_visit_next(&plv))
    {
      PyObject *closeres=NULL;
      APSWCursor *cur=(APSWCursor*)pointerlist_visit_get(&plv);

      closeres=APSWCursor_close(cur, args);
      Py_XDECREF(closeres);
      if(!closeres)
	{
	  Py_DECREF(cursorcloseargs);
	  return NULL;
	}
    }

  Py_DECREF(cursorcloseargs);

  res=statementcache_free(self->stmtcache);
  assert(res==0);
  self->stmtcache=0;

  Py_BEGIN_ALLOW_THREADS
    res=sqlite3_close(self->db);
  Py_END_ALLOW_THREADS;

  if (res!=SQLITE_OK) 
    {
      SET_EXC(self->db, res);
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
  else Py_RETURN_NONE;
  
}

static void
Connection_dealloc(Connection* self)
{
  if(self->db)
    {
      /* not allowed to clobber existing exception */
      PyObject *etype=NULL, *evalue=NULL, *etraceback=NULL;
      PyErr_Fetch(&etype, &evalue, &etraceback);

      PyErr_Format(ExcConnectionNotClosed, 
		   "apsw.Connection on \"%s\" at address %p, allocated at %s:%d. The destructor "
		   "has been called, but you haven't closed the connection.  All connections must "
		   "be explicitly closed.  The SQLite database object is being leaked.", 
		   self->filename?self->filename:"NULL", self,
		   PyString_AsString(self->co_filename), self->co_linenumber);

      apsw_write_unraiseable();
      PyErr_Fetch(&etype, &evalue, &etraceback);
    }

  assert(self->cursors.numentries==0);
  pointerlist_free(&self->cursors);

  Connection_internal_cleanup(self);

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
      self->filename=0;
      self->co_linenumber=0;
      self->co_filename=0;
      self->thread_ident=PyThread_get_thread_ident();
      memset(&self->cursors, 0, sizeof(self->cursors));
      pointerlist_init(&self->cursors);
      self->stmtcache=0;
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
  PyObject *hooks=NULL, *hook=NULL, *iterator=NULL, *hookargs=NULL, *hookresult=NULL;
  PyFrameObject *frame;
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
  
  if(res!=SQLITE_OK)
      goto pyexception;
    
  /* record where it was allocated */
  frame = PyThreadState_GET()->frame;
  self->co_linenumber=PyCode_Addr2Line(frame->f_code, frame->f_lasti);
  self->co_filename=frame->f_code->co_filename;
  Py_INCREF(self->co_filename);
  self->filename=filename;
  filename=NULL; /* connection has ownership now */

  /* get detailed error codes */
  sqlite3_extended_result_codes(self->db, 1);
  
  /* call connection hooks */
  hooks=PyObject_GetAttrString(apswmodule, "connection_hooks");
  if(!hooks)
    goto pyexception;

  hookargs=Py_BuildValue("(O)", self);
  if(!hookargs) goto pyexception;

  iterator=PyObject_GetIter(hooks);
  if(!iterator)
    {
      AddTraceBackHere(__FILE__, __LINE__, "Connection.__init__", "{s: i}", "connection_hooks", hooks);
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
      self->stmtcache=statementcache_init(self->db, 100);
      goto finally;
    }

 pyexception:
  /* clean up db since it is useless - no need for user to call close */
  res=-1;
  sqlite3_close(self->db);
  self->db=0;
  Connection_internal_cleanup(self);

finally:
  if(filename) PyMem_Free(filename);
  Py_XDECREF(hookargs);
  Py_XDECREF(iterator);
  Py_XDECREF(hooks);
  Py_XDECREF(hook);
  Py_XDECREF(hookresult);
  return res;
}


static void APSWBlob_init(APSWBlob *, Connection *, sqlite3_blob *);

static PyObject *
Connection_blobopen(Connection *self, PyObject *args)
{
  APSWBlob *apswblob=0;
  sqlite3_blob *blob=0;
  const char *dbname, *tablename, *column;
  long long rowid;
  int writing;
  int res;

  CHECK_THREAD(self, NULL);
  CHECK_CLOSED(self, NULL);
  
  if(!PyArg_ParseTuple(args, "esesesLi:blobopen(database, table, column, rowid, rd_wr)", 
                       STRENCODING, &dbname, STRENCODING, &tablename, STRENCODING, &column, &rowid, &writing))
    return NULL;

  res=sqlite3_blob_open(self->db, dbname, tablename, column, rowid, writing, &blob);
  PyMem_Free((void*)dbname);
  PyMem_Free((void*)tablename);
  PyMem_Free((void*)column);
  SET_EXC(self->db, res);
  if(res!=SQLITE_OK)
    return NULL;
  
  apswblob=PyObject_New(APSWBlob, &APSWBlobType);
  if(!apswblob)
    {
      sqlite3_blob_close(blob);
      return NULL;
    }

  APSWBlob_init(apswblob, self, blob);
  return (PyObject*)apswblob;
}

static void APSWCursor_init(APSWCursor *, Connection *);

static PyObject *
Connection_cursor(Connection *self)
{
  APSWCursor* cursor = NULL;

  CHECK_THREAD(self,NULL);
  CHECK_CLOSED(self,NULL);

  cursor = PyObject_New(APSWCursor, &APSWCursorType);
  if(!cursor)
    return NULL;

  /* incref me since cursor holds a pointer */
  Py_INCREF((PyObject*)self);
  pointerlist_add(&self->cursors, cursor);
  APSWCursor_init(cursor, self);
  
  return (PyObject*)cursor;
}

static PyObject *
Connection_setbusytimeout(Connection *self, PyObject *args)
{
  int ms=0;
  int res;

  CHECK_THREAD(self,NULL);
  CHECK_CLOSED(self,NULL);

  if(!PyArg_ParseTuple(args, "i:setbusytimeout(millseconds)", &ms))
    return NULL;

  res=sqlite3_busy_timeout(self->db, ms);
  SET_EXC(self->db, res);
  if(res!=SQLITE_OK) return NULL;
  
  /* free any explicit busyhandler we may have had */
  Py_XDECREF(self->busyhandler);
  self->busyhandler=0;

  Py_RETURN_NONE;
}

static PyObject *
Connection_changes(Connection *self)
{
  CHECK_THREAD(self,NULL);
  CHECK_CLOSED(self,NULL);
  return PyInt_FromLong(sqlite3_changes(self->db));
}

static PyObject *
Connection_totalchanges(Connection *self)
{
  CHECK_THREAD(self,NULL);
  CHECK_CLOSED(self,NULL);
  return PyInt_FromLong(sqlite3_total_changes(self->db));
}

static PyObject *
Connection_getautocommit(Connection *self)
{
  CHECK_THREAD(self,NULL);
  CHECK_CLOSED(self,NULL);
  if (sqlite3_get_autocommit(self->db))
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

static PyObject *
Connection_last_insert_rowid(Connection *self)
{
  long long int vint;

  CHECK_THREAD(self,NULL);
  CHECK_CLOSED(self,NULL);

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

static PyObject *
Connection_interrupt(Connection *self)
{
  CHECK_CLOSED(self, NULL);

  sqlite3_interrupt(self->db);  /* no return value */
  Py_RETURN_NONE;
}

static void
updatecb(void *context, int updatetype, char const *databasename, char const *tablename, sqlite_int64 rowid)
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

static PyObject *
Connection_setupdatehook(Connection *self, PyObject *callable)
{
  /* sqlite3_update_hook doesn't return an error code */
  
  CHECK_THREAD(self,NULL);
  CHECK_CLOSED(self,NULL);

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

  if(PyErr_Occurred())
    goto finally;  /* abort hook due to outstanding exception */

  retval=PyEval_CallObject(self->rollbackhook, NULL);

 finally:
  Py_XDECREF(retval);
  PyGILState_Release(gilstate);
}

static PyObject *
Connection_setrollbackhook(Connection *self, PyObject *callable)
{
  /* sqlite3_rollback_hook doesn't return an error code */
  
  CHECK_THREAD(self,NULL);
  CHECK_CLOSED(self,NULL);

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

static PyObject *
Connection_setprofile(Connection *self, PyObject *callable)
{
  /* sqlite3_profile doesn't return an error code */
  
  CHECK_THREAD(self,NULL);
  CHECK_CLOSED(self,NULL);

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

  if(PyErr_Occurred())
    goto finally;  /* abort hook due to outstanding exception */

  retval=PyEval_CallObject(self->commithook, NULL);

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
  PyGILState_Release(gilstate);
  return ok;
}

static PyObject *
Connection_setcommithook(Connection *self, PyObject *callable)
{
  /* sqlite3_commit_hook doesn't return an error code */
  
  CHECK_THREAD(self,NULL);
  CHECK_CLOSED(self,NULL);

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
  /* see earlier comment about PyObject_IsTrue */
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

static PyObject *
Connection_setprogresshandler(Connection *self, PyObject *args)
{
  /* sqlite3_progress_handler doesn't return an error code */
  int nsteps=20;
  PyObject *callable=NULL;
  
  CHECK_THREAD(self,NULL);
  CHECK_CLOSED(self,NULL);

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

  if(PyErr_Occurred())
    goto finally;  /* abort due to earlier exception */

  retval=PyObject_CallFunction(self->authorizer, "(iO&O&O&O&)", operation, convertutf8string, paramone, 
                               convertutf8string, paramtwo, convertutf8string, databasename, 
                               convertutf8string, triggerview);

  if(!retval)
    goto finally; /* abort due to exeception */

  result=PyInt_AsLong(retval);
  if (PyErr_Occurred())
    result=SQLITE_DENY;

 finally:
  Py_XDECREF(retval);

  PyGILState_Release(gilstate);
  return result;
}

static PyObject *
Connection_setauthorizer(Connection *self, PyObject *callable)
{
  int res;

  CHECK_THREAD(self,NULL);
  CHECK_CLOSED(self,NULL);

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

  if(res==SQLITE_OK)
    Py_RETURN_NONE;
  return NULL;
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

#if defined(EXPERIMENTAL) && !defined(SQLITE_OMIT_LOAD_EXTENSION)  /* extension loading */
static PyObject *
Connection_enableloadextension(Connection *self, PyObject *enabled)
{
  int enabledp, res;

  CHECK_THREAD(self, NULL);
  CHECK_CLOSED(self, NULL);

  /* get the boolean value */
  enabledp=PyObject_IsTrue(enabled);
  if(enabledp==-1) return NULL;
  if (PyErr_Occurred()) return NULL;

  /* call function */
  res=sqlite3_enable_load_extension(self->db, enabledp);
  SET_EXC(self->db, res);  /* the function will currently always succeed */

  /* done */
  if (res==SQLITE_OK)
    Py_RETURN_NONE;
  return NULL;
}

static PyObject *
Connection_loadextension(Connection *self, PyObject *args)
{
  int res;
  char *zfile=NULL, *zproc=NULL, *errmsg=NULL;

  CHECK_THREAD(self, NULL);
  CHECK_CLOSED(self, NULL);
  
  if(!PyArg_ParseTuple(args, "s|z:loadextension(filename, entrypoint=None)", &zfile, &zproc))
    return NULL;

  Py_BEGIN_ALLOW_THREADS
    res=sqlite3_load_extension(self->db, zfile, zproc, &errmsg);
  Py_END_ALLOW_THREADS;

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

static PyObject *
Connection_setbusyhandler(Connection *self, PyObject *callable)
{
  int res=SQLITE_OK;

  CHECK_THREAD(self,NULL);
  CHECK_CLOSED(self,NULL);

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

  if (res==SQLITE_OK)
    Py_RETURN_NONE;
  return NULL;
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
      Py_RETURN_NONE;

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

/* converts a python object into a sqlite3_context result */
static void
set_context_result(sqlite3_context *context, PyObject *obj)
{
  if(!obj)
    {
      assert(PyErr_Occurred());
      /* TODO: possibly examine exception and return appropriate error
         code eg for BusyError set error to SQLITE_BUSY */
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
  if (PyFloat_Check(obj))
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
                SET_EXC(NULL, SQLITE_TOOBIG);
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
                    SET_EXC(NULL, SQLITE_TOOBIG);
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
              SET_EXC(NULL, SQLITE_TOOBIG);
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
  if(retval)
    set_context_result(context, retval);

 finally:
  if (PyErr_Occurred())
    {
      char *funname=sqlite3_mprintf("user-defined-scalar-%s", cbinfo->name);
      AddTraceBackHere(__FILE__, __LINE__, funname, "{s: i}", "NumberOfArguments", argc);
      sqlite3_free(funname);
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
  funccbinfo *cbinfo;
  PyObject *retval;
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
      PyErr_Format(PyExc_StandardError, "An exception happened during cleanup of an aggregate function, but there was already error in the step function so only that can be returned");
      apsw_write_unraiseable();
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
  
  Py_RETURN_NONE;
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
  
  Py_RETURN_NONE;
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
  PyObject *pys1=NULL, *pys2=NULL, *retval=NULL;
  int result=0;

  assert(cbinfo);

  gilstate=PyGILState_Ensure();

  if(PyErr_Occurred()) goto finally;  /* outstanding error */

  pys1=convertutf8stringsize(stringonedata, stringonelen);
  pys2=convertutf8stringsize(stringtwodata, stringtwolen);

  if(!pys1 || !pys2)  
    goto finally;   /* failed to allocate strings */

  retval=PyObject_CallFunction(cbinfo->func, "(OO)", pys1, pys2);

  if(!retval) goto finally;  /* execution failed */

  result=PyInt_AsLong(retval);
  if(PyErr_Occurred())
      result=0;

 finally:
  Py_XDECREF(pys1);
  Py_XDECREF(pys2);
  Py_XDECREF(retval);
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
  CHECK_CLOSED(self,NULL);
  
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
Call_PythonMethod(PyObject *obj, const char *methodname, int mandatory, PyObject *args)
{
  PyObject *method=NULL;
  PyObject *res=NULL;

  /* we may be called when there is already an error.  eg if you return an error in
     a cursor method, then SQLite calls vtabClose which calls us.  We don't want to 
     clear pre-existing errors, but we do want to clear ones when the function doesn't
     exist but is optional */
  PyObject *etype=NULL, *evalue=NULL, *etraceback=NULL;
  void *pyerralreadyoccurred=PyErr_Occurred();
  if(pyerralreadyoccurred)
    PyErr_Fetch(&etype, &evalue, &etraceback);


  /* we should only be called with ascii methodnames so no need to do
     character set conversions etc */
#if PY_VERSION_HEX < 0x02050000
  method=PyObject_GetAttrString(obj, (char*)methodname);
#else
  method=PyObject_GetAttrString(obj, methodname);
#endif
  if (!method)
    {
      if(!mandatory)
	{
	  /* pretend method existed and returned None */
	  PyErr_Clear();
	  res=Py_None;
	  Py_INCREF(res);
	}
      goto finally;
    }

  res=PyEval_CallObject(method, args);

 finally:
  if(pyerralreadyoccurred)
    PyErr_Restore(etype, evalue, etraceback);
  Py_XDECREF(method);
  return res;
}

static PyObject *
Call_PythonMethodV(PyObject *obj, const char *methodname, int mandatory, const char *format, ...)
{
  PyObject *args=NULL, *result=NULL;
  va_list list;
  va_start (list, format);
  args=Py_VaBuildValue(format, list);
  va_end(list);

  if (args)
    result=Call_PythonMethod(obj, methodname, mandatory, args);

  Py_XDECREF(args);
  return result;
}


typedef struct {
  sqlite3_vtab used_by_sqlite; /* I don't touch this */
  PyObject *vtable;            /* object implementing vtable */
} apsw_vtable;

static struct {
  const char *methodname;
  const char *declarevtabtracebackname;
  const char *pyexceptionname;
} create_or_connect_strings[]=
  {
    {
      "Create",
      "VirtualTable.xCreate.sqlite3_declare_vtab",
      "VirtualTable.xCreate"
    },
    {
      "Connect",
      "VirtualTable.xConnect.sqlite3_declare_vtab",
      "VirtualTable.xConnect"
    }
  };

static int 
vtabCreateOrConnect(sqlite3 *db, 
		    void *pAux, 
		    int argc, 
		    const char *const *argv,
		    sqlite3_vtab **pVTab,
		    char **errmsg,
		    /* args above are to Create/Connect method */
		    int stringindex)
{
  PyGILState_STATE gilstate;
  vtableinfo *vti;
  PyObject *args=NULL, *res=NULL, *schema=NULL, *vtable=NULL;
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

  res=Call_PythonMethod(vti->datasource, create_or_connect_strings[stringindex].methodname, 1, args);
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
    PyObject *utf8schema=getutf8string(schema);
    if(!utf8schema) 
      goto pyexception;
    sqliteres=sqlite3_declare_vtab(db, PyString_AsString(utf8schema));
    Py_DECREF(utf8schema);
    if(sqliteres!=SQLITE_OK)
      {
	SET_EXC(db, sqliteres);
	AddTraceBackHere(__FILE__, __LINE__,  create_or_connect_strings[stringindex].declarevtabtracebackname, "{s: O}", "schema", schema);
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
  AddTraceBackHere(__FILE__, __LINE__, create_or_connect_strings[stringindex].pyexceptionname, 
		   "{s: s, s: s, s: s, s: O}", "modulename", argv[0], "database", argv[1], "tablename", argv[2], "schema", schema?schema:Py_None);

 finally: /* cleanup */
  Py_XDECREF(args);  
  Py_XDECREF(res);
  Py_XDECREF(schema);
  Py_XDECREF(vtable);
  if(avi)
    PyMem_Free(avi);

  PyGILState_Release(gilstate);
  return sqliteres;
}

static int 
vtabCreate(sqlite3 *db, 
	   void *pAux, 
	   int argc, 
	   const char *const *argv,
	   sqlite3_vtab **pVTab,
	   char **errmsg)
{
  return vtabCreateOrConnect(db, pAux, argc, argv, pVTab, errmsg, 0);
}

static int 
vtabConnect(sqlite3 *db, 
	   void *pAux, 
	   int argc, 
	   const char *const *argv,
	   sqlite3_vtab **pVTab,
	   char **errmsg)
{
  return vtabCreateOrConnect(db, pAux, argc, argv, pVTab, errmsg, 1);
}


static struct
{
  const char *methodname;
  const char *pyexceptionname;
} destroy_disconnect_strings[]=
  {
    {
      "Destroy",
      "VirtualTable.xDestroy"
    },
    {
      "Disconnect",
      "VirtualTable.xDisconnect"
    }
  };

/* See SQLite ticket 2099 */
static int
vtabDestroyOrDisconnect(sqlite3_vtab *pVtab, int stringindex)
{ 
  PyObject *vtable, *res=NULL;
  PyGILState_STATE gilstate;
  int sqliteres=SQLITE_OK;

  gilstate=PyGILState_Ensure();
  vtable=((apsw_vtable*)pVtab)->vtable;

  /* mandatory for Destroy, optional for Disconnect */
  res=Call_PythonMethod(vtable, destroy_disconnect_strings[stringindex].methodname, (stringindex==0), NULL);
  /* sqlite 3.3.8 ignore return code for disconnect so we always free */
  if (res || stringindex==1)
    {
      /* see SQLite ticket 2127 */
      if(pVtab->zErrMsg)
	sqlite3_free(pVtab->zErrMsg);
      
      Py_DECREF(vtable);
      PyMem_Free(pVtab);
      goto finally;
    }

  if(stringindex==0)
    {
      /* ::TODO:: waiting on ticket 2099 to know if the pVtab should also be freed in case of error return with Destroy. */
#if 0
      /* see SQLite ticket 2127 */
      if(pVtab->zErrMsg)
	sqlite3_free(pVtab->zErrMsg);
      
      Py_DECREF(vtable);
      PyMem_Free(pVtab);    
#endif
    }

  /* pyexception:  we had an exception in python code */
  sqliteres=MakeSqliteMsgFromPyException(&(pVtab->zErrMsg));
  AddTraceBackHere(__FILE__, __LINE__,  destroy_disconnect_strings[stringindex].pyexceptionname, "{s: O}", "self", vtable);

 finally:
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres;
}

static int
vtabDestroy(sqlite3_vtab *pVTab)
{
  return vtabDestroyOrDisconnect(pVTab, 0);
}

static int
vtabDisconnect(sqlite3_vtab *pVTab)
{
  return vtabDestroyOrDisconnect(pVTab, 1);
}


static int
vtabBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *indexinfo)
{
  PyGILState_STATE gilstate;
  PyObject *vtable;
  PyObject *constraints=NULL, *orderbys=NULL;
  PyObject *res=NULL, *indices=NULL;
  int i,j;
  int nconstraints=0;
  int sqliteres=SQLITE_OK;

  gilstate=PyGILState_Ensure();

  vtable=((apsw_vtable*)pVtab)->vtable;
  
  /* count how many usable constraints there are */
  for(i=0;i<indexinfo->nConstraint;i++)
    if (indexinfo->aConstraint[i].usable)
      nconstraints++;

  constraints=PyTuple_New(nconstraints);
  if(!constraints) goto pyexception;
  
  /* fill them in */
  for(i=0, j=0;i<indexinfo->nConstraint;i++)
    {
      PyObject *constraint=NULL;
      if(!indexinfo->aConstraint[i].usable) continue;
      
      constraint=Py_BuildValue("(iB)", indexinfo->aConstraint[i].iColumn, indexinfo->aConstraint[i].op);
      if(!constraint) goto pyexception;

      PyTuple_SET_ITEM(constraints, j, constraint);
      j++;
    }

  /* group bys */
  orderbys=PyTuple_New(indexinfo->nOrderBy);
  if(!orderbys) goto pyexception;

  /* fill them in */
  for(i=0;i<indexinfo->nOrderBy;i++)
    {
      PyObject *order=NULL;

      order=Py_BuildValue("(iN)", indexinfo->aOrderBy[i].iColumn, PyBool_FromLong(indexinfo->aOrderBy[i].desc));
      if(!order) goto pyexception;

      PyTuple_SET_ITEM(orderbys, i, order);
    }

  /* actually call the function */
  res=Call_PythonMethodV(vtable, "BestIndex", 1, "(OO)", constraints, orderbys);
  if(!res)
    goto pyexception;

  /* do we have useful index information? */
  if(res==Py_None)
    goto finally;

  /* check we have a sequence */
  if(!PySequence_Check(res) || PySequence_Size(res)>5)
    {
      PyErr_Format(PyExc_TypeError, "Bad result from BestIndex.  It should be a sequence of up to 5 items");
      AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xBestIndex.result_check", "{s: O, s: O}", "self", vtable, "result", res);
      goto pyexception;
    }

  /* dig the argv indices out */
  if(PySequence_Size(res)==0)
    goto finally;

  indices=PySequence_GetItem(res, 0);
  if(indices!=Py_None)
    {
      if(!PySequence_Check(indices) || PySequence_Size(indices)!=nconstraints)
	{
	  PyErr_Format(PyExc_TypeError, "Bad constraints (item 0 in BestIndex return).  It should be a sequence the same length as the constraints passed in (%d) items", nconstraints);
	  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xBestIndex.result_indices", "{s: O, s: O, s: O}", 
			   "self", vtable, "result", res, "indices", indices);
	  goto pyexception;
	}
      /* iterate through the items - i is the SQLite sequence number and j is the apsw one (usable entries) */
      for(i=0,j=0;i<indexinfo->nConstraint;i++)
	{
	  PyObject *constraint=NULL, *argvindex=NULL, *omit=NULL;
	  int omitv;
	  if(!indexinfo->aConstraint[i].usable) continue;
	  constraint=PySequence_GetItem(indices, j);
	  if(!constraint) goto pyexception;
	  j++;
	  /* it can be None */
	  if(constraint==Py_None)
	    {
	      Py_DECREF(constraint);
	      continue;
	    }
	  /* or an integer */
	  if(PyInt_Check(constraint))
	    {
	      indexinfo->aConstraintUsage[i].argvIndex=PyInt_AsLong(constraint);
	      Py_DECREF(constraint);
	      continue;
	    }
	  /* or a sequence two items long */
	  if(!PySequence_Check(constraint) || PySequence_Size(constraint)!=2)
	    {
	      PyErr_Format(PyExc_TypeError, "Bad constraint (#%d) - it should be one of None, an integer or a tuple of an integer and a boolean", j);
	      AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xBestIndex.result_constraint", "{s: O, s: O, s: O, s: O}", 
			       "self", vtable, "result", res, "indices", indices, "constraint", constraint);
	      Py_DECREF(constraint);
	      goto pyexception;
	    }
	  argvindex=PySequence_GetItem(constraint, 0);
	  omit=PySequence_GetItem(constraint, 1);
	  if(!argvindex || !omit) goto constraintfail;
	  if(!PyInt_Check(argvindex))
	    {
	      PyErr_Format(PyExc_TypeError, "argvindex for constraint #%d should be an integer", j);
	      AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xBestIndex.result_constraint_argvindex", "{s: O, s: O, s: O, s: O, s: O}", 
			       "self", vtable, "result", res, "indices", indices, "constraint", constraint, "argvindex", argvindex);
	      goto constraintfail;
	    }
	  omitv=PyObject_IsTrue(omit);
	  if(omitv==-1) goto constraintfail;
	  indexinfo->aConstraintUsage[i].argvIndex=PyInt_AsLong(argvindex);
	  indexinfo->aConstraintUsage[i].omit=omitv;
	  Py_DECREF(constraint);
	  Py_DECREF(argvindex);
	  Py_DECREF(omit);
	  continue;

	constraintfail:
	  Py_DECREF(constraint);
	  Py_XDECREF(argvindex);
	  Py_XDECREF(omit);
	  goto pyexception;
	}
    }

  /* item #1 is idxnum */
  if(PySequence_Size(res)<2)
    goto finally;
  {
    PyObject *idxnum=PySequence_GetItem(res, 1);
    if(!idxnum) goto pyexception;
    if(idxnum!=Py_None)
      {
	if(!PyInt_Check(idxnum))
	  {
	    PyErr_Format(PyExc_TypeError, "idxnum must be an integer");
	      AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xBestIndex.result_indexnum", "{s: O, s: O, s: O}", "self", vtable, "result", res, "indexnum", idxnum);
	    Py_DECREF(idxnum);
	    goto pyexception;
	  }
	indexinfo->idxNum=PyInt_AsLong(idxnum);
      }
    Py_DECREF(idxnum);
  }

  /* item #2 is idxStr */
  if(PySequence_Size(res)<3)
    goto finally;
  {
    PyObject *utf8str=NULL, *idxstr=NULL;
    idxstr=PySequence_GetItem(res, 2);
    if(!idxstr) goto pyexception;
    if(idxstr!=Py_None)
      {
	utf8str=getutf8string(idxstr);
	if(!utf8str)
	  {
	    Py_DECREF(idxstr);
	    goto pyexception;
	  }
	indexinfo->idxStr=sqlite3_mprintf("%s", PyString_AsString(utf8str));
	indexinfo->needToFreeIdxStr=1;
      }
    Py_XDECREF(utf8str);
    Py_DECREF(idxstr);
  }

  /* item 3 is orderByConsumed */
  if(PySequence_Size(res)<4)
    goto finally;
  {
    PyObject *orderbyconsumed=NULL;
    int iorderbyconsumed;
    orderbyconsumed=PySequence_GetItem(res, 3);
    if(!orderbyconsumed) goto pyexception;
    if(orderbyconsumed!=Py_None)
      {
	iorderbyconsumed=PyObject_IsTrue(orderbyconsumed);
	if(iorderbyconsumed==-1)
	  {
	    Py_DECREF(orderbyconsumed);
	    goto pyexception;
	  }
	indexinfo->orderByConsumed=iorderbyconsumed;
      }
    Py_DECREF(orderbyconsumed);
  }

  /* item 4 (final) is estimated cost */
  if(PySequence_Size(res)<5)
    goto finally;
  assert(PySequence_Size(res)==5);
  {
    PyObject *estimatedcost=NULL, *festimatedcost=NULL;
    estimatedcost=PySequence_GetItem(res,4);
    if(!estimatedcost) goto pyexception;
    if(estimatedcost!=Py_None)
      {
	festimatedcost=PyNumber_Float(estimatedcost);
	if(!festimatedcost)
	  {
	    Py_DECREF(estimatedcost);
	    goto pyexception;
	  }
	indexinfo->estimatedCost=PyFloat_AsDouble(festimatedcost);
      }
    Py_XDECREF(festimatedcost);
    Py_DECREF(estimatedcost);
  }

  goto finally;

 pyexception: /* we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres=MakeSqliteMsgFromPyException(&(pVtab->zErrMsg));
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xBestIndex", "{s: O, s: O, s: (OO)}", "self", vtable, "result", res?res:Py_None, "args", constraints?constraints:Py_None, orderbys?orderbys:Py_None);

 finally:
  Py_XDECREF(indices);
  Py_XDECREF(res);
  Py_XDECREF(constraints);
  Py_XDECREF(orderbys);
  PyGILState_Release(gilstate);
  return sqliteres;
}

struct {
  const char *methodname;
  const char *pyexceptionname;
} transaction_strings[]=
  {
    {
      "Begin",
      "VirtualTable.Begin"
    },
    {
      "Sync",
      "VirtualTable.Sync"
    },
    {
      "Commit",
      "VirtualTable.Commit"
    },
    {
      "Rollback",
      "VirtualTable.Rollback"
    },

  };

static int
vtabTransactionMethod(sqlite3_vtab *pVtab, int stringindex)
{
  PyObject *vtable, *res=NULL;
  PyGILState_STATE gilstate;
  int sqliteres=SQLITE_OK;

  gilstate=PyGILState_Ensure();
  vtable=((apsw_vtable*)pVtab)->vtable;

  res=Call_PythonMethod(vtable, transaction_strings[stringindex].methodname, 0, NULL);
  if(res) goto finally;

  /*  pyexception: we had an exception in python code */
  sqliteres=MakeSqliteMsgFromPyException(&(pVtab->zErrMsg));
  AddTraceBackHere(__FILE__, __LINE__,  transaction_strings[stringindex].pyexceptionname, "{s: O}", "self", vtable);

 finally:
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres;
}

static int 
vtabBegin(sqlite3_vtab *pVtab) 
{ 
  return vtabTransactionMethod(pVtab, 0);
}

static int 
vtabSync(sqlite3_vtab *pVtab) 
{ 
  return vtabTransactionMethod(pVtab, 1);
}

static int 
vtabCommit(sqlite3_vtab *pVtab) 
{ 
  return vtabTransactionMethod(pVtab, 2);
}

static int 
vtabRollback(sqlite3_vtab *pVtab) 
{ 
  return vtabTransactionMethod(pVtab, 3);
}

typedef struct {
  sqlite3_vtab_cursor used_by_sqlite;   /* I don't touch this */
  PyObject *cursor;                     /* Object implementing cursor */
} apsw_vtable_cursor;


static int
vtabOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor)
{ 
  PyObject *vtable=NULL, *res=NULL;
  PyGILState_STATE gilstate;
  apsw_vtable_cursor *avc=NULL;
  int sqliteres=SQLITE_OK;

  gilstate=PyGILState_Ensure();

  vtable=((apsw_vtable*)pVtab)->vtable;

  res=Call_PythonMethod(vtable, "Open", 1, NULL);
  if(!res)
    goto pyexception;
  avc=PyMem_Malloc(sizeof(apsw_vtable_cursor));
  assert((void*)avc==(void*)&(avc->used_by_sqlite)); /* detect if wierd padding happens */
  memset(avc, 0, sizeof(apsw_vtable_cursor));

  avc->cursor=res;
  res=NULL;
  *ppCursor=(sqlite3_vtab_cursor*)avc;
  goto finally;

 pyexception: /* we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres=MakeSqliteMsgFromPyException(&(pVtab->zErrMsg));
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xOpen", "{s: O}", "self", vtable);

 finally:
  Py_XDECREF(res);
  PyGILState_Release(gilstate);
  return sqliteres;
}

static int
vtabFilter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr,
                  int argc, sqlite3_value **sqliteargv)
{ 
  PyObject *cursor, *argv=NULL, *res=NULL;
  PyGILState_STATE gilstate;
  int sqliteres=SQLITE_OK;
  int i;

  gilstate=PyGILState_Ensure();

  cursor=((apsw_vtable_cursor*)pCursor)->cursor;


  argv=PyTuple_New(argc);
  if(!argv) goto pyexception;
  for(i=0;i<argc;i++)
    {
      PyObject *value=convert_value_to_pyobject(sqliteargv[i]);
      if(!value) goto pyexception;
      PyTuple_SET_ITEM(argv, i, value);
    }

  res=Call_PythonMethodV(cursor, "Filter", 1, "(iO&O)", idxNum, convertutf8string, idxStr, argv);
  if(res) goto finally; /* result is ignored */

 pyexception: /* we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres=MakeSqliteMsgFromPyException(&(pCursor->pVtab->zErrMsg)); /* SQLite flaw: errMsg should be on the cursor not the table! */
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xFilter", "{s: O}", "self", cursor);

 finally:
  Py_XDECREF(argv);
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres;
}

/* note that we can only return true/false and cannot indicate there was an error */
static int
vtabEof(sqlite3_vtab_cursor *pCursor)
{ 
  PyObject *cursor, *res=NULL;
  PyGILState_STATE gilstate;
  int sqliteres=0; /* nb a true/false value not error code */

  gilstate=PyGILState_Ensure();

  /* is there already an error? */
  if(PyErr_Occurred()) goto finally;

  cursor=((apsw_vtable_cursor*)pCursor)->cursor;

  res=Call_PythonMethod(cursor, "Eof", 1, NULL);
  if(!res) goto pyexception;

  sqliteres=PyObject_IsTrue(res);
  if(sqliteres==0 || sqliteres==1)
    goto finally;

  sqliteres=0; /* we say there are no more records on error */
  
 pyexception: /* we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres=MakeSqliteMsgFromPyException(&(pCursor->pVtab->zErrMsg)); /* SQLite flaw: errMsg should be on the cursor not the table! */
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xEof", "{s: O}", "self", cursor);

 finally:
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres;
}

static int
vtabColumn(sqlite3_vtab_cursor *pCursor, sqlite3_context *result, int ncolumn)
{ 
  PyObject *cursor, *res=NULL;
  PyGILState_STATE gilstate;
  int sqliteres=SQLITE_OK; 

  gilstate=PyGILState_Ensure();

  cursor=((apsw_vtable_cursor*)pCursor)->cursor;

  res=Call_PythonMethodV(cursor, "Column", 1, "(i)", ncolumn);
  if(!res) goto pyexception;

  set_context_result(result, res);
  if(!PyErr_Occurred()) goto finally;
  
 pyexception: /* we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres=MakeSqliteMsgFromPyException(&(pCursor->pVtab->zErrMsg)); /* SQLite flaw: errMsg should be on the cursor not the table! */
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xColumn", "{s: O}", "self", cursor);

 finally:
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres;
} 

static int
vtabNext(sqlite3_vtab_cursor *pCursor)
{ 
  PyObject *cursor, *res=NULL;
  PyGILState_STATE gilstate;
  int sqliteres=SQLITE_OK;

  gilstate=PyGILState_Ensure();

  cursor=((apsw_vtable_cursor*)pCursor)->cursor;

  res=Call_PythonMethod(cursor, "Next", 1, NULL);
  if(res) goto finally;

  /* pyexception:  we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres=MakeSqliteMsgFromPyException(&(pCursor->pVtab->zErrMsg)); /* SQLite flaw: errMsg should be on the cursor not the table! */
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xNext", "{s: O}", "self", cursor);

 finally:
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres; 
}

static int
vtabClose(sqlite3_vtab_cursor *pCursor)
{
  PyObject *cursor, *res=NULL;
  PyGILState_STATE gilstate;
  char **zErrMsgLocation=&(pCursor->pVtab->zErrMsg); /* we free pCursor but still need this field */
  int sqliteres=SQLITE_OK;

  gilstate=PyGILState_Ensure();

  cursor=((apsw_vtable_cursor*)pCursor)->cursor;

  res=Call_PythonMethod(cursor, "Close", 1, NULL);
  PyMem_Free(pCursor); /* always free */
  if(res) goto finally;

  /* pyexception: we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres=MakeSqliteMsgFromPyException(zErrMsgLocation); /* SQLite flaw: errMsg should be on the cursor not the table! */
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xClose", "{s: O}", "self", cursor);

 finally:
  Py_DECREF(cursor);  /* this is where cursor gets freed */
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres; 
}

static int
vtabRowid(sqlite3_vtab_cursor *pCursor, sqlite_int64 *pRowid)
{ 
  PyObject *cursor, *res=NULL, *pyrowid=NULL;
  PyGILState_STATE gilstate;
  int sqliteres=SQLITE_OK; 

  gilstate=PyGILState_Ensure();

  cursor=((apsw_vtable_cursor*)pCursor)->cursor;

  res=Call_PythonMethod(cursor, "Rowid", 1, NULL);
  if(!res) goto pyexception;
  
  /* extract result */
  pyrowid=PyNumber_Long(res);
  if(!pyrowid) 
    goto pyexception;
  *pRowid=PyLong_AsLongLong(pyrowid);
  if(!PyErr_Occurred()) /* could be bigger than 64 bits */
    goto finally;
  
 pyexception: /* we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres=MakeSqliteMsgFromPyException(&(pCursor->pVtab->zErrMsg)); /* SQLite flaw: errMsg should be on the cursor not the table! */
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xRowid", "{s: O}", "self", cursor);

 finally:
  Py_XDECREF(pyrowid);
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres;
}

static int
vtabUpdate(sqlite3_vtab *pVtab, int argc, sqlite3_value **argv, sqlite_int64 *pRowid)
{
  PyObject *vtable, *args=NULL, *res=NULL;
  PyGILState_STATE gilstate;
  int sqliteres=SQLITE_OK; 
  int i;
  const char *methodname="unknown";
  
  assert(argc); /* should always be >0 */
  
  gilstate=PyGILState_Ensure();

  vtable=((apsw_vtable*)pVtab)->vtable;

  /* case 1 - argc=1 means delete row */
  if(argc==1)
    {
      methodname="UpdateDeleteRow";
      args=Py_BuildValue("(O&)", convert_value_to_pyobject, argv[0]);
      if(!args) goto pyexception;
    }
  /* case 2 - insert a row */
  else if(sqlite3_value_type(argv[0])==SQLITE_NULL)
    {
      PyObject *newrowid;
      methodname="UpdateInsertRow";
      args=PyTuple_New(2);
      if(!args) goto pyexception;
      if(sqlite3_value_type(argv[1])==SQLITE_NULL)
	{
	  newrowid=Py_None;
	  Py_INCREF(newrowid);
	}
      else
	{
	  newrowid=convert_value_to_pyobject(argv[1]);
	  if(!newrowid) goto pyexception;
	}
      PyTuple_SET_ITEM(args, 0, newrowid);
    }
  /* otherwise changing a row */
  else
    {
      PyObject *oldrowid=NULL, *newrowid=NULL;
      methodname="UpdateChangeRow";
      args=PyTuple_New(3);
      oldrowid=convert_value_to_pyobject(argv[0]);
      if(sqlite3_value_type(argv[1])==SQLITE_NULL)
	{
	  newrowid=Py_None;
	  Py_INCREF(newrowid);
	}
      else
	{
	  newrowid=convert_value_to_pyobject(argv[1]);
	}
      if(!oldrowid || !newrowid)
	{
	  Py_XDECREF(oldrowid);
	  Py_XDECREF(newrowid);
	  goto pyexception;
	}
      PyTuple_SET_ITEM(args,0,oldrowid);
      PyTuple_SET_ITEM(args,1,newrowid);
    }

  /* new row values */
  if(argc!=1)
    {
      PyObject *fields=NULL;
      fields=PyTuple_New(argc-2);
      if(!fields) goto pyexception;
      for(i=0;i+2<argc;i++)
	{
	  PyObject *field=convert_value_to_pyobject(argv[i+2]);
	  if(!field)
	    {
	      Py_DECREF(fields);
	      goto pyexception;
	    }
	  PyTuple_SET_ITEM(fields, i, field);
	}
      PyTuple_SET_ITEM(args, PyTuple_GET_SIZE(args)-1, fields);
    }

  res=Call_PythonMethod(vtable, methodname, 1, args);
  if(!res) 
    goto pyexception;

  /* if row deleted then we don't care about return */
  if(argc==1) 
    goto finally;

  if(sqlite3_value_type(argv[0])==SQLITE_NULL && sqlite3_value_type(argv[1])==SQLITE_NULL)
    {
      /* did an insert and must provide a row id */
      PyObject *rowid=PyNumber_Long(res);
      if(!rowid) goto pyexception;

      *pRowid=PyLong_AsLongLong(rowid);
      Py_DECREF(rowid);
      if(PyErr_Occurred()) 
	{
	  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xUpdateInsertRow.ReturnedValue", "{s: O}", "result", rowid);
	  goto pyexception;
	}
    }
  
  goto finally;

 pyexception: /* we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres=MakeSqliteMsgFromPyException(&pVtab->zErrMsg);
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xUpdate", "{s: O, s: i, s: s, s: O}", "self", vtable, "argc", argc, "methodname", methodname, "args", args?args:Py_None);

 finally:
  Py_XDECREF(args);
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres;
}


#if 0
/* I can't implement this yet since I need to know when
   ppArg will get freed.  See SQLite ticket 2095. */

static int 
vtabFindFunction(sqlite3_vtab *pVtab, int nArg, const char *zName,
		 void (**pxFunc)(sqlite3_context*,int,sqlite3_value**),
		 void **ppArg)
{ 
  return 0;
}
#endif


/* it would be nice to use C99 style initializers here ... */
static struct sqlite3_module apsw_vtable_module=
  {
    1,                    /* version */
    vtabCreate,           /* methods */
    vtabConnect,          
    vtabBestIndex,        
    vtabDisconnect,
    vtabDestroy,
    vtabOpen,
    vtabClose, 
    vtabFilter, 
    vtabNext, 
    vtabEof, 
    vtabColumn,
    vtabRowid, 
    vtabUpdate, 
    vtabBegin, 
    vtabSync, 
    vtabCommit, 
    vtabRollback,
    0,                /* vtabFindFunction */
    0                 /* vtabRename */
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
  CHECK_CLOSED(self, NULL);

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
  
  Py_RETURN_NONE;
}

#endif /* EXPERIMENTAL */
/* end of Virtual table code */


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
  {"setauthorizer", (PyCFunction)Connection_setauthorizer, METH_O,
   "Sets an authorizer function"},
  {"setupdatehook", (PyCFunction)Connection_setupdatehook, METH_O,
      "Sets an update hook"},
  {"setrollbackhook", (PyCFunction)Connection_setrollbackhook, METH_O,
   "Sets a callable invoked before each rollback"},
  {"blobopen", (PyCFunction)Connection_blobopen, METH_VARARGS,
   "Opens a blob for i/o"},
#ifdef EXPERIMENTAL
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
  {0, 0, 0, 0}  /* Sentinel */
};


static PyTypeObject ConnectionType = 
  {
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
    0,                         /* tp_free */
    0,                         /* tp_is_gc */
    0,                         /* tp_bases */
    0,                         /* tp_mro */
    0,                         /* tp_cache */
    0,                         /* tp_subclasses */
    0,                         /* tp_weaklist */
    0,                         /* tp_del */
};


/* ZEROBLOB CODE */

/*  Zeroblob is used for binding and results - takes a single integer
   in constructor and has no other methods */

typedef struct {
  PyObject_HEAD
  int blobsize;
} ZeroBlobBind;

static PyObject*
ZeroBlobBind_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
  ZeroBlobBind *self;
  int n;
  
  if(kwargs && PyDict_Size(kwargs)!=0)
    {
      PyErr_Format(PyExc_TypeError, "Zeroblob constructor does not take keyword arguments");
      return NULL;
    }

  if(!PyArg_ParseTuple(args, "i", &n))
    return NULL;

  if(n<0)
    {
      PyErr_Format(PyExc_TypeError, "zeroblob size must be >= 0");
      return NULL;
    }

  self=(ZeroBlobBind*)type->tp_alloc(type, 0);
  if(self) self->blobsize=n;
  return (PyObject*)self;
}

static PyTypeObject ZeroBlobBindType = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "apsw.zeroblob",           /*tp_name*/
    sizeof(ZeroBlobBind),      /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    0,                         /*tp_dealloc*/ 
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
    "ZeroBlobBind object",     /* tp_doc */
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
    ZeroBlobBind_new,          /* tp_new */
    0,                         /* tp_free */
    0,                         /* tp_is_gc */
    0,                         /* tp_bases */
    0,                         /* tp_mro */
    0,                         /* tp_cache */
    0,                         /* tp_subclasses */
    0,                         /* tp_weaklist */
    0,                         /* tp_del */
};


/* BLOB CODE */

static PyObject*
APSWBlob_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
  if(kwargs && PyDict_Size(kwargs)!=0)
    {
      PyErr_Format(PyExc_TypeError, "blob constructor does not take keyword arguments");
      return NULL;
    }

  if(!PyArg_ParseTuple(args, ""))
    return NULL;

  return type->tp_alloc(type, 0);
}

static void
APSWBlob_init(APSWBlob *self, Connection *connection, sqlite3_blob *blob)
{
  Py_INCREF(connection);
  self->connection=connection;
  self->pBlob=blob;
  self->curoffset=0;
}

static void
APSWBlob_dealloc(APSWBlob *self)
{
  if(self->pBlob)
    {
      int res=sqlite3_blob_close(self->pBlob);
      if(res!=SQLITE_OK)
        {
          make_exception(res, self->connection->db);
          apsw_write_unraiseable();
        }
      self->pBlob=0;
    }
  if(self->connection)
    {
      Py_DECREF(self->connection);
      self->connection=0;
    }
  self->ob_type->tp_free((PyObject*)self);
}

/* If the blob is closed, we return the same error as normal python files */
#define CHECK_BLOB_CLOSED \
  if(!self->pBlob) \
    { \
      PyErr_Format(PyExc_ValueError, "I/O operation on closed blob"); \
      return NULL; \
    }

static PyObject *
APSWBlob_length(APSWBlob *self)
{
  CHECK_BLOB_CLOSED;
  return PyInt_FromLong(sqlite3_blob_bytes(self->pBlob));
}

static PyObject *
APSWBlob_read(APSWBlob *self, PyObject *args)
{
  int length=-1;
  int res;
  PyObject *buffy=0;
  char *thebuffer;

  CHECK_BLOB_CLOSED;

   
  /* The python file read routine treats negative numbers as read till
     end of file, which I think is rather silly.  (Try reading -3
     bytes from /dev/zero on a 64 bit machine with lots of swap to see
     why).  In any event we remain consistent with Python file
     objects */
  if(!PyArg_ParseTuple(args, "|i:read(numbytes=remaining)", &length))
    return NULL;

  /* eof? */
  if(self->curoffset==sqlite3_blob_bytes(self->pBlob))
    Py_RETURN_NONE;

  if(length==0)
    return PyString_FromString("");

  if(length<0)
    length=sqlite3_blob_bytes(self->pBlob)-self->curoffset;

  /* trying to read more than is in the blob? */
  if(self->curoffset+length>sqlite3_blob_bytes(self->pBlob))
    length=sqlite3_blob_bytes(self->pBlob)-self->curoffset;

  buffy=PyString_FromStringAndSize(NULL, length);
  if(!buffy) return NULL;

  thebuffer= PyString_AS_STRING(buffy);
  Py_BEGIN_ALLOW_THREADS
    res=sqlite3_blob_read(self->pBlob, thebuffer, length, self->curoffset);
  Py_END_ALLOW_THREADS;
  if(res!=SQLITE_OK)
    {
      Py_DECREF(buffy);
      SET_EXC(self->connection->db, res);
      return NULL;
    }
  else
    self->curoffset+=length;
  assert(self->curoffset<=sqlite3_blob_bytes(self->pBlob));
  return buffy;
}

static PyObject *
APSWBlob_seek(APSWBlob *self, PyObject *args)
{
  CHECK_BLOB_CLOSED;
  int offset, whence=0;
  
  if(!PyArg_ParseTuple(args, "i|i:seek(offset,whence=0)", &offset, &whence))
    return NULL;
  
  switch(whence)
    {
    default:
      PyErr_Format(PyExc_ValueError, "whence parameter should be 0, 1 or 2");
      return NULL;
    case 0: /* relative to begining of file */
      if(offset<0 || offset>sqlite3_blob_bytes(self->pBlob))
        goto out_of_range;
      self->curoffset=offset;
      break;
    case 1: /* relative to current position */
      if(self->curoffset+offset<0 || self->curoffset+offset>sqlite3_blob_bytes(self->pBlob))
        goto out_of_range;
      self->curoffset+=offset;
      break;
    case 2: /* relative to end of file */
      if(sqlite3_blob_bytes(self->pBlob)+offset<0 || sqlite3_blob_bytes(self->pBlob)+offset>sqlite3_blob_bytes(self->pBlob))
        goto out_of_range;
      self->curoffset=sqlite3_blob_bytes(self->pBlob)+offset;
      break;
    }
  Py_RETURN_NONE;
 out_of_range:
  PyErr_Format(PyExc_ValueError, "The resulting offset would be less than zero or past the end of the blob");
  return NULL;
}

static PyObject *
APSWBlob_tell(APSWBlob *self)
{
  CHECK_BLOB_CLOSED;
  return PyInt_FromLong(self->curoffset);
}

static PyObject *
APSWBlob_write(APSWBlob *self, PyObject *obj)
{
  const void *buffer=0;
  Py_ssize_t size;
  int res;
  CHECK_BLOB_CLOSED;

  /* we support buffers and string for the object */
  if(PyBuffer_Check(obj))
    {
      if(PyObject_AsReadBuffer(obj, &buffer, &size))
        return NULL;
    }
  else if(PyString_Check(obj))
    {
      buffer=PyString_AS_STRING(obj);
      size=PyString_GET_SIZE(obj);
    }
  else
    {
      PyErr_Format(PyExc_TypeError, "Parameter should be string or buffer");
      return NULL;
    }

  if( ((int)(size+self->curoffset))<self->curoffset)
    {
      PyErr_Format(PyExc_ValueError, "Data is too large (integer wrap)");
      return NULL;
    }
  if( ((int)(size+self->curoffset))>sqlite3_blob_bytes(self->pBlob))
    {
      PyErr_Format(PyExc_ValueError, "Data would go beyond end of blob");
      return NULL;
    }
  res=sqlite3_blob_write(self->pBlob, buffer, (int)size, self->curoffset);
  if(res!=SQLITE_OK)
    {
      SET_EXC(self->connection->db, res);
      return NULL;
    }
  else
    self->curoffset+=size;
  assert(self->curoffset<=sqlite3_blob_bytes(self->pBlob));
  Py_RETURN_NONE;
}

static PyObject *
APSWBlob_close(APSWBlob *self)
{
  int res;
  /* we allow close to be called multiple times */
  if(!self->pBlob) goto end;
  res=sqlite3_blob_close(self->pBlob);
  SET_EXC(self->connection->db, res);
  self->pBlob=0; /* sqlite ticket #2815 */
  Py_DECREF(self->connection);
  self->connection=0;
  if(res!=SQLITE_OK)
    return NULL;   
 end:
  Py_RETURN_NONE;
}


static PyMethodDef APSWBlob_methods[]={
  {"length", (PyCFunction)APSWBlob_length, METH_NOARGS,
   "Returns length in bytes of the blob"},
  {"read", (PyCFunction)APSWBlob_read, METH_VARARGS,
   "Reads data from the blob"},
  {"seek", (PyCFunction)APSWBlob_seek, METH_VARARGS,
   "Seeks to a position in the blob"},
  {"tell", (PyCFunction)APSWBlob_tell, METH_NOARGS,
   "Returns current blob offset"},
  {"write", (PyCFunction)APSWBlob_write, METH_O,
   "Writes data to blob"},
  {"close", (PyCFunction)APSWBlob_close, METH_NOARGS,
   "Closes blob"},
  {0,0,0,0} /* Sentinel */
};

static PyTypeObject APSWBlobType = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "apsw.blob",               /*tp_name*/
    sizeof(APSWBlob),          /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)APSWBlob_dealloc, /*tp_dealloc*/ 
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
    Py_TPFLAGS_DEFAULT,        /*tp_flags*/
    "APSW blob object",        /* tp_doc */
    0,		               /* tp_traverse */
    0,		               /* tp_clear */
    0,		               /* tp_richcompare */
    0,		               /* tp_weaklistoffset */
    0,		               /* tp_iter */
    0,		               /* tp_iternext */
    APSWBlob_methods,          /* tp_methods */
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    0,                         /* tp_init */
    0,                         /* tp_alloc */
    APSWBlob_new,              /* tp_new */
    0,                         /* tp_free */
    0,                         /* tp_is_gc */
    0,                         /* tp_bases */
    0,                         /* tp_mro */
    0,                         /* tp_cache */
    0,                         /* tp_subclasses */
    0,                         /* tp_weaklist */
    0,                         /* tp_del */
};



/* CURSOR CODE */

/* Do finalization and free resources.  Returns the SQLITE error code */
static int
resetcursor(APSWCursor *self, int force)
{
  int res=SQLITE_OK;

  Py_XDECREF(self->bindings);
  self->bindings=NULL;
  self->bindingsoffset=-1;

  if(self->statement)
    {
      res=statementcache_finalize(self->connection->stmtcache, self->statement);
      if(!force) /* we don't care about errors when forcing */
	SET_EXC(self->connection->db, res);
      self->statement=0;
    }

  if(!force && (self->status!=C_DONE && self->zsqlnextpos))
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
  
  if(!force && self->status!=C_DONE && self->emiter)
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
    {
      assert(res);
      AddTraceBackHere(__FILE__, __LINE__, "resetcursor", NULL);
    }

  return res;
}

static void
APSWCursor_dealloc(APSWCursor * self)
{
  /* thread check - we can't use macro as it returns*/
  PyObject *err_type, *err_value, *err_traceback;
  int have_error=PyErr_Occurred()?1:0;

  if( (self->status!=C_DONE || self->statement || self->zsqlnextpos || self->emiter) &&  /* only whine if there was anything left in the APSWCursor */
      self->connection->thread_ident!=PyThread_get_thread_ident())
    {
      if (have_error)
        PyErr_Fetch(&err_type, &err_value, &err_traceback);
      PyErr_Format(ExcThreadingViolation, "The destructor for APSWCursor is called in a different thread than it"
                   "was created in.  All calls must be in the same thread.  It was created in thread %d " 
                   "and this is %d.  SQLite is not being closed as a result.",
                   (int)(self->connection->thread_ident), (int)(PyThread_get_thread_ident()));            
      apsw_write_unraiseable();
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

  resetcursor(self, 1);
  if(PyErr_Occurred())
    PyErr_Clear(); /* clear out any exceptions from resetcursor since we don't care */

  if (have_error)
    /* restore earlier error if there was one */
    PyErr_Restore(err_type, err_value, err_traceback);

  /* we no longer need connection */
  if(self->connection)
    {
      pointerlist_remove(&self->connection->cursors, self);
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
APSWCursor_init(APSWCursor *self, Connection *connection)
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
APSWCursor_getdescription(APSWCursor *self)
{
  int ncols,i;
  PyObject *result=NULL;
  PyObject *pair=NULL;

  CHECK_THREAD(self->connection,NULL);
  CHECK_CLOSED(self->connection,NULL);

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
      pair=Py_BuildValue("(O&O&)", 
			 convertutf8string, sqlite3_column_name(self->statement, i),
			 convertutf8string, sqlite3_column_decltype(self->statement, i));
			 
      if(!pair) goto error;

      PyTuple_SET_ITEM(result, i, pair);
      /* owned by result now */
      pair=0;
    }
  
  return result;

 error:
  Py_XDECREF(result);
  Py_XDECREF(pair);
  return NULL;
}

/* internal function - returns SQLite error code (ie SQLITE_OK if all is well) */
static int
APSWCursor_dobinding(APSWCursor *self, int arg, PyObject *obj)
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
                SET_EXC(NULL, SQLITE_TOOBIG);
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
                    SET_EXC(NULL, SQLITE_TOOBIG);
                    res=SQLITE_TOOBIG;
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
                SET_EXC(NULL, SQLITE_TOOBIG);
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
          SET_EXC(NULL, SQLITE_TOOBIG);
	  return -1;
	}
      res=sqlite3_bind_blob(self->statement, arg, buffer, (int)buflen, SQLITE_TRANSIENT);
    }
  else if(PyObject_TypeCheck(obj, &ZeroBlobBindType)==1)
    {
      res=sqlite3_bind_zeroblob(self->statement, arg, ((ZeroBlobBind*)obj)->blobsize);
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
APSWCursor_dobindings(APSWCursor *self)
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
	  if(!keyo) return -1;

	  obj=PyDict_GetItem(self->bindings, keyo);
	  Py_DECREF(keyo);

          if(!obj)
            /* this is where we could error on missing keys */
            continue;
          if(APSWCursor_dobinding(self,arg,obj))
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
      if(APSWCursor_dobinding(self, arg, obj))
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
APSWCursor_doexectrace(APSWCursor *self, exectrace_oldstate *etos)
{
  PyObject *retval=NULL;
  PyObject *sqlcmd=NULL;
  PyObject *bindings=NULL;
  int result;

  assert(self->exectrace);

  /* make a string of the command */
  sqlcmd=convertutf8stringsize(etos->previouszsqlpos, self->zsqlnextpos-etos->previouszsqlpos);

  if(!sqlcmd) return -1;

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
	      /* I couldn't work anything into the test suite to actually make this fail! */
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

  retval=PyObject_CallFunction(self->exectrace, "OO", sqlcmd, bindings);
  Py_DECREF(sqlcmd);
  Py_DECREF(bindings);
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
APSWCursor_dorowtrace(APSWCursor *self, PyObject *retval)
{
  assert(self->rowtrace);

  retval=PyEval_CallObject(self->rowtrace, retval);
  if(!retval) 
    return NULL;
  
  return retval;
}

/* Returns a borrowed reference to self if all is ok, else NULL on error */
static PyObject *
APSWCursor_step(APSWCursor *self)
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
        res=(self->statement)?(sqlite3_step(self->statement)):(SQLITE_DONE);
      Py_END_ALLOW_THREADS;

      switch(res&0xff)
        {
        case SQLITE_MISUSE:
          /* this would be an error in apsw itself */
          self->status=C_DONE;
          SET_EXC(self->connection->db,res);
          resetcursor(self, 0);
          return NULL;

	case SQLITE_ROW:
          self->status=C_ROW;
          return (PyErr_Occurred())?(NULL):((PyObject*)self);

        case SQLITE_DONE:
	  if (PyErr_Occurred())
	    {
	      self->status=C_DONE;
	      return NULL;
	    }
          break;


	case SQLITE_SCHEMA:
	  /* This is typically because a cached statement has become
	     invalid.  We figure this out by re-preparing.  See SQLite
	     ticket 2158.
	   */
	  {
	    sqlite3_stmt *newstmt;
	    res=statementcache_dup(self->connection->stmtcache, self->statement, &newstmt);
	    if(newstmt)
	      {
		assert(res==SQLITE_OK);
		sqlite3_transfer_bindings(self->statement, newstmt);
		statementcache_finalize(self->connection->stmtcache, self->statement);
		self->statement=newstmt;
		continue;
	      }
	    SET_EXC(self->connection->db,res);
	    self->status=C_DONE;
	    resetcursor(self, 0); /* we have handled error */
	    return NULL;
	  }

        default: /* sqlite3_prepare_v2 introduced in 3.3.9 means the
		    error code is returned from step as well as
		    finalize/reset */
          /* FALLTHRU */
        case SQLITE_ERROR:  /* SQLITE_BUSY is handled here as well */
          /* there was an error - we need to get actual error code from sqlite3_finalize */
          self->status=C_DONE;
          res=resetcursor(self, 0);  /* this will get the error code for us */
          assert(res!=SQLITE_OK);
          return NULL;

          
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
              if(resetcursor(self, 0)!=SQLITE_OK)
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
              if(resetcursor(self, 0)!=SQLITE_OK)
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
      res=statementcache_finalize(self->connection->stmtcache, self->statement);
      self->statement=0;
      SET_EXC(self->connection->db,res);
      if (res!=SQLITE_OK)
        {
          assert((res&0xff)!=SQLITE_BUSY); /* finalize shouldn't be returning busy, only step */
          return NULL;
        }

      assert(!self->statement);
      if(self->exectrace)
        {
          etos.previouszsqlpos=self->zsqlnextpos;
          etos.savedbindingsoffset=self->bindingsoffset;
        }
      assert(!PyErr_Occurred());
      res=statementcache_prepare(self->connection->stmtcache, self->connection->db, self->zsqlnextpos, -1, &self->statement, &self->zsqlnextpos);
      SET_EXC(self->connection->db,res);
      if (res!=SQLITE_OK)
        {
          assert((res&0xff)!=SQLITE_BUSY); /* prepare definitely shouldn't be returning busy */
          return NULL;
        }
      assert(!PyErr_Occurred());
      if(APSWCursor_dobindings(self))
        {
          assert(PyErr_Occurred());
          return NULL;
        }

      if(self->exectrace)
        {
          if(APSWCursor_doexectrace(self, &etos))
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
APSWCursor_execute(APSWCursor *self, PyObject *args)
{
  int res;
  PyObject *retval=NULL;
  exectrace_oldstate etos;

  CHECK_THREAD(self->connection, NULL);
  CHECK_CLOSED(self->connection, NULL);

  res=resetcursor(self, 0);
  if(res!=SQLITE_OK)
    {
      assert(PyErr_Occurred());
      return NULL;
    }
  
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
  assert(!PyErr_Occurred());
  res=statementcache_prepare(self->connection->stmtcache, self->connection->db, self->zsql, -1, &self->statement, &self->zsqlnextpos);
  SET_EXC(self->connection->db,res);
  if (res!=SQLITE_OK)
    {
      AddTraceBackHere(__FILE__, __LINE__, "APSWCursor_execute.sqlite3_prepare_v2", "{s: O, s: N}", 
		       "Connection", self->connection, 
		       "statement", PyUnicode_DecodeUTF8(self->zsql, strlen(self->zsql), "strict"));
      return NULL;
    }
  assert(!PyErr_Occurred());

  self->bindingsoffset=0;
  if(APSWCursor_dobindings(self))
    {
      assert(PyErr_Occurred());
      return NULL;
    }

  if(self->exectrace)
    {
      if(APSWCursor_doexectrace(self, &etos))
        {
          assert(PyErr_Occurred());
          return NULL;  
        }
    }

  self->status=C_BEGIN;

  retval=APSWCursor_step(self);
  if (!retval) 
    {
      assert(PyErr_Occurred());
      return NULL;
    }
  Py_INCREF(retval);
  return retval;
}

static PyObject *
APSWCursor_executemany(APSWCursor *self, PyObject *args)
{
  int res;
  PyObject *retval=NULL;
  PyObject *theiterable=NULL;
  PyObject *next=NULL;
  exectrace_oldstate etos;

  CHECK_THREAD(self->connection, NULL);
  CHECK_CLOSED(self->connection, NULL);

  res=resetcursor(self, 0);
  if(res!=SQLITE_OK)
    {
      assert(PyErr_Occurred());
      return NULL;
    }
  
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
  assert(!PyErr_Occurred());
  res=statementcache_prepare(self->connection->stmtcache, self->connection->db, self->zsql, -1, &self->statement, &self->zsqlnextpos);
  SET_EXC(self->connection->db,res);
  if (res!=SQLITE_OK)
    return NULL;
  assert(!PyErr_Occurred());

  self->bindingsoffset=0;
  if(APSWCursor_dobindings(self))
    {
      assert(PyErr_Occurred());
      return NULL;
    }

  if(self->exectrace)
    {
      if(APSWCursor_doexectrace(self, &etos))
        {
          assert(PyErr_Occurred());
          return NULL;  
        }
    }

  self->status=C_BEGIN;

  retval=APSWCursor_step(self);
  if (!retval) 
    {
      assert(PyErr_Occurred());
      return NULL;
    }
  Py_INCREF(retval);
  return retval;
}

static PyObject *
APSWCursor_close(APSWCursor *self, PyObject *args)
{
  int res;
  int force=0;

  CHECK_THREAD(self->connection, NULL);
  if (!self->connection->db) /* if connection is closed, then we must also be closed */
    Py_RETURN_NONE;

  if(!PyArg_ParseTuple(args, "|i:close(force=False)", &force))
    return NULL;

  res=resetcursor(self, force);
  if(res!=SQLITE_OK)
    {
      assert(PyErr_Occurred());
      return NULL;
    }
  
  Py_RETURN_NONE;
}

static PyObject *
APSWCursor_next(APSWCursor *self)
{
  PyObject *retval;
  PyObject *item;
  int numcols=-1;
  int i;

  CHECK_THREAD(self->connection, NULL);
  CHECK_CLOSED(self->connection, NULL);

 again:
  if(self->status==C_BEGIN)
    if(!APSWCursor_step(self))
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
      PyObject *r2=APSWCursor_dorowtrace(self, retval);
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
APSWCursor_iter(APSWCursor *self)
{
  CHECK_THREAD(self->connection, NULL);
  CHECK_CLOSED(self->connection, NULL);

  Py_INCREF(self);
  return (PyObject*)self;
}

static PyObject *
APSWCursor_setexectrace(APSWCursor *self, PyObject *func)
{
  CHECK_THREAD(self->connection, NULL);
  CHECK_CLOSED(self->connection, NULL);

  if(func!=Py_None && !PyCallable_Check(func))
    {
      PyErr_SetString(PyExc_TypeError, "parameter must be callable");
      return NULL;
    }

  if(func!=Py_None)
    Py_INCREF(func);

  Py_XDECREF(self->exectrace);
  self->exectrace=(func!=Py_None)?func:NULL;

  Py_RETURN_NONE;
}

static PyObject *
APSWCursor_setrowtrace(APSWCursor *self, PyObject *func)
{
  CHECK_THREAD(self->connection, NULL);
  CHECK_CLOSED(self->connection, NULL);

  if(func!=Py_None && !PyCallable_Check(func))
    {
      PyErr_SetString(PyExc_TypeError, "parameter must be callable");
      return NULL;
    }

  if(func!=Py_None)
    Py_INCREF(func);

  Py_XDECREF(self->rowtrace);
  self->rowtrace=(func!=Py_None)?func:NULL;

  Py_RETURN_NONE;
}

static PyObject *
APSWCursor_getexectrace(APSWCursor *self)
{
  PyObject *ret;

  CHECK_THREAD(self->connection, NULL);
  CHECK_CLOSED(self->connection, NULL);

  ret=(self->exectrace)?(self->exectrace):Py_None;
  Py_INCREF(ret);
  return ret;
}

static PyObject *
APSWCursor_getrowtrace(APSWCursor *self)
{
  PyObject *ret;
  CHECK_THREAD(self->connection, NULL);
  CHECK_CLOSED(self->connection, NULL);
  ret =(self->rowtrace)?(self->rowtrace):Py_None;
  Py_INCREF(ret);
  return ret;
}

static PyObject *
APSWCursor_getconnection(APSWCursor *self)
{
  CHECK_THREAD(self->connection, NULL);
  CHECK_CLOSED(self->connection, NULL);

  Py_INCREF(self->connection);
  return (PyObject*)self->connection;
}

static PyMethodDef APSWCursor_methods[] = {
  {"execute", (PyCFunction)APSWCursor_execute, METH_VARARGS,
   "Executes one or more statements" },
  {"executemany", (PyCFunction)APSWCursor_executemany, METH_VARARGS,
   "Repeatedly executes statements on sequence" },
  {"next", (PyCFunction)APSWCursor_next, METH_NOARGS,
   "Returns next row returned from query"},
  {"setexectrace", (PyCFunction)APSWCursor_setexectrace, METH_O,
   "Installs a function called for every statement executed"},
  {"setrowtrace", (PyCFunction)APSWCursor_setrowtrace, METH_O,
   "Installs a function called for every row returned"},
  {"getexectrace", (PyCFunction)APSWCursor_getexectrace, METH_NOARGS,
   "Returns the current exec tracer function"},
  {"getrowtrace", (PyCFunction)APSWCursor_getrowtrace, METH_NOARGS,
   "Returns the current row tracer function"},
  {"getrowtrace", (PyCFunction)APSWCursor_getrowtrace, METH_NOARGS,
   "Returns the current row tracer function"},
  {"getconnection", (PyCFunction)APSWCursor_getconnection, METH_NOARGS,
   "Returns the connection object for this cursor"},
  {"getdescription", (PyCFunction)APSWCursor_getdescription, METH_NOARGS,
   "Returns the description for the current row"},
  {"close", (PyCFunction)APSWCursor_close, METH_VARARGS,
   "Closes the cursor" },
  {0, 0, 0, 0}  /* Sentinel */
};


static PyTypeObject APSWCursorType = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "apsw.Cursor",             /*tp_name*/
    sizeof(APSWCursor),            /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)APSWCursor_dealloc, /*tp_dealloc*/
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
    (getiterfunc)APSWCursor_iter,  /* tp_iter */
    (iternextfunc)APSWCursor_next, /* tp_iternext */
    APSWCursor_methods,            /* tp_methods */
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
    0,                         /* tp_del */
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

  Py_RETURN_NONE;
}

static PyMethodDef module_methods[] = {
  {"sqlitelibversion", (PyCFunction)getsqliteversion, METH_NOARGS,
   "Return the version of the SQLite library"},
  {"apswversion", (PyCFunction)getapswversion, METH_NOARGS,
   "Return the version of the APSW wrapper"},
  {"enablesharedcache", (PyCFunction)enablesharedcache, METH_VARARGS,
   "Sets shared cache semantics for this thread"},

    {0, 0, 0, 0}  /* Sentinel */
};



#ifndef PyMODINIT_FUNC	/* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif
PyMODINIT_FUNC
initapsw(void) 
{
    PyObject *m;
    PyObject *thedict;
    PyObject *hooks;

    assert(sizeof(int)==4);             /* we expect 32 bit ints */
    assert(sizeof(long long)==8);             /* we expect 64 bit long long */

    if (PyType_Ready(&ConnectionType) < 0)
      return;

    if (PyType_Ready(&APSWCursorType) < 0)
      return;

    if (PyType_Ready(&ZeroBlobBindType) <0)
      return;

    if (PyType_Ready(&APSWBlobType) <0)
      return;

    /* ensure threads are available */
    PyEval_InitThreads();


    m = apswmodule = Py_InitModule3("apsw", module_methods,
                       "Another Python SQLite Wrapper.");

    if (m == NULL)  return;

    if(init_exceptions(m)) return;

    Py_INCREF(&ConnectionType);
    PyModule_AddObject(m, "Connection", (PyObject *)&ConnectionType);
    /* we don't add cursor to the module since users shouldn't be able to instantiate them directly */
    Py_INCREF(&ZeroBlobBindType);
    PyModule_AddObject(m, "zeroblob", (PyObject *)&ZeroBlobBindType);

    hooks=PyList_New(0);
    if(!hooks) return;
    PyModule_AddObject(m, "connection_hooks", hooks);
    

    /* add in some constants and also put them in a corresponding mapping dictionary */

#define ADDINT(v) PyModule_AddObject(m, #v, Py_BuildValue("i", v)); \
         PyDict_SetItemString(thedict, #v, Py_BuildValue("i", v));  \
         PyDict_SetItem(thedict, Py_BuildValue("i", v), Py_BuildValue("s", #v));

    thedict=PyDict_New();
    if(!thedict) return;

    ADDINT(SQLITE_DENY);
    ADDINT(SQLITE_IGNORE);
    ADDINT(SQLITE_OK);
    
    PyModule_AddObject(m, "mapping_authorizer_return", thedict);

    /* authorizer functions */
    thedict=PyDict_New();
    if(!thedict) return;

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
    ADDINT(SQLITE_FUNCTION);

    PyModule_AddObject(m, "mapping_authorizer_function", thedict);

    /* Version number */
    PyModule_AddObject(m, "SQLITE_VERSION_NUMBER", Py_BuildValue("i", SQLITE_VERSION_NUMBER));

    /* vtable best index constraints */
#if defined(SQLITE_INDEX_CONSTRAINT_EQ) && defined(SQLITE_INDEX_CONSTRAINT_MATCH)

    thedict=PyDict_New();
    if(!thedict) return;

    ADDINT(SQLITE_INDEX_CONSTRAINT_EQ);
    ADDINT(SQLITE_INDEX_CONSTRAINT_GT);
    ADDINT(SQLITE_INDEX_CONSTRAINT_LE);
    ADDINT(SQLITE_INDEX_CONSTRAINT_LT);
    ADDINT(SQLITE_INDEX_CONSTRAINT_GE);
    ADDINT(SQLITE_INDEX_CONSTRAINT_MATCH);

    PyModule_AddObject(m, "mapping_bestindex_constraints", thedict);

#endif /* constraints */

    /* extendended result codes */
    thedict=PyDict_New();
    if(!thedict) return;

    ADDINT(SQLITE_IOERR_READ);
    ADDINT(SQLITE_IOERR_SHORT_READ);
    ADDINT(SQLITE_IOERR_WRITE);
    ADDINT(SQLITE_IOERR_FSYNC);
    ADDINT(SQLITE_IOERR_DIR_FSYNC);
    ADDINT(SQLITE_IOERR_TRUNCATE);
    ADDINT(SQLITE_IOERR_FSTAT);
    ADDINT(SQLITE_IOERR_UNLOCK);
    ADDINT(SQLITE_IOERR_RDLOCK);
    ADDINT(SQLITE_IOERR_DELETE);
    ADDINT(SQLITE_IOERR_BLOCKED);
    ADDINT(SQLITE_IOERR_NOMEM);
    PyModule_AddObject(m, "mapping_extended_result_codes", thedict);

    /* error codes */
    thedict=PyDict_New();
    if(!thedict) return;

    ADDINT(SQLITE_OK);
    ADDINT(SQLITE_ERROR);
    ADDINT(SQLITE_INTERNAL);
    ADDINT(SQLITE_PERM);
    ADDINT(SQLITE_ABORT);
    ADDINT(SQLITE_BUSY);
    ADDINT(SQLITE_LOCKED);
    ADDINT(SQLITE_NOMEM);
    ADDINT(SQLITE_READONLY);
    ADDINT(SQLITE_INTERRUPT);
    ADDINT(SQLITE_IOERR);
    ADDINT(SQLITE_CORRUPT);
    ADDINT(SQLITE_FULL);
    ADDINT(SQLITE_CANTOPEN);
    ADDINT(SQLITE_PROTOCOL);
    ADDINT(SQLITE_EMPTY);
    ADDINT(SQLITE_SCHEMA);
    ADDINT(SQLITE_CONSTRAINT);
    ADDINT(SQLITE_MISMATCH);
    ADDINT(SQLITE_MISUSE);
    ADDINT(SQLITE_NOLFS);
    ADDINT(SQLITE_AUTH);
    ADDINT(SQLITE_FORMAT);
    ADDINT(SQLITE_RANGE);
    ADDINT(SQLITE_NOTADB);

    PyModule_AddObject(m, "mapping_result_codes", thedict);

}

