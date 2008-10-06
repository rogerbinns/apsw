/*
  Another Python Sqlite Wrapper

  This wrapper aims to be the minimum necessary layer over SQLite 3
  itself.

  It assumes we are running as 32 bit int with a 64 bit long long type
  available.

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

/* Fight with setuptools over ndebug */
#ifdef APSW_NO_NDEBUG
#ifdef NDEBUG
#undef NDEBUG
#endif
#endif

/* SQLite amalgamation */
#ifdef APSW_USE_SQLITE_AMALGAMATION
/* See SQLite ticket 2554 */
#define SQLITE_API static
#define SQLITE_EXTERN static
#include APSW_USE_SQLITE_AMALGAMATION

/* Fight with SQLite over ndebug */
#ifdef APSW_NO_NDEBUG
#ifdef NDEBUG
#undef NDEBUG
#endif
#endif

#else
/* SQLite 3 headers */
#include "sqlite3.h"
#endif

#if SQLITE_VERSION_NUMBER < 3006002
#error Your SQLite version is too old.  It must be at least 3.6.2
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

#ifdef __GNUC__
#define APSW_ARGUNUSED __attribute__ ((unused))
#else
#define APSW_ARGUNUSED
#endif

/* Python 2.5 compatibility when size_t types become 64 bit.
   SQLite3 is limited to 32 bit sizes even on a 64 bit machine. */
#if PY_VERSION_HEX < 0x02050000
typedef int Py_ssize_t;
#endif

/* Python 2.3 doesn't have these */
#ifndef Py_RETURN_NONE
#define Py_RETURN_NONE return Py_INCREF(Py_None), Py_None
#endif
#ifndef Py_RETURN_TRUE
#define Py_RETURN_TRUE return Py_INCREF(Py_True), Py_True
#define Py_RETURN_FALSE return Py_INCREF(Py_False), Py_False
#endif

/* fun with objects - this is defined in Python 3 */
#ifndef Py_TYPE
#define Py_TYPE(x) ((x)->ob_type)
#endif

/* How to make a string from a utf8 constant */
#if PY_VERSION_HEX < 0x03000000
#define MAKESTR  PyString_FromString
#else
#define MAKESTR  PyUnicode_FromString
#endif

#if PY_VERSION_HEX < 0x03000000
#define PyBytes_FromStringAndSize PyString_FromStringAndSize
#define PyBytes_AsString          PyString_AsString
#define PyBytes_AS_STRING         PyString_AS_STRING
#define PyBytes_GET_SIZE          PyString_GET_SIZE
#define _PyBytes_Resize           _PyString_Resize
#define PyIntLong_Check(x)        (PyInt_Check((x)) || PyLong_Check((x)))
#define PyIntLong_AsLong(x)       ( (PyInt_Check((x))) ? ( PyInt_AsLong((x)) ) : ( (PyLong_AsLong((x)))))
#else
#define PyIntLong_Check           PyLong_Check
#define PyIntLong_AsLong          PyLong_AsLong
#define PyInt_FromLong            PyLong_FromLong
#endif

/* A module to augment tracebacks */
#include "traceback.c"

/* A list of pointers (used by Connection to keep track of Cursors) */
#include "pointerlist.c"

/* Prepared statement caching */
/* #define SCSTATS */
#define STATEMENTCACHE_LINKAGE static
#include "statementcache.c"

/* used to decide if we will use int or long long, sqlite limit tests due to it not being 64 bit correct */
#define APSW_INT32_MIN (-2147483647-1)
#define APSW_INT32_MAX 2147483647


#ifdef APSW_TESTFIXTURES
/* Fault injection */
#define APSW_FAULT_INJECT(name,good,bad)          \
do {                                              \
  if(APSW_Should_Fault(#name))                    \
    {                                             \
      do { bad ; } while(0);                      \
    }                                             \
  else                                            \
    {                                             \
      do { good ; } while(0);                     \
    }                                             \
 } while(0)

static int APSW_Should_Fault(const char *);

/* Are we Python 2.x (x>=5) and doing 64 bit? - _LP64 is best way I can find as sizeof isn't valid in cpp #if */
#if  PY_VERSION_HEX>=0x02050000 && defined(_LP64) && _LP64
#define APSW_TEST_LARGE_OBJECTS
#endif

#else /* APSW_TESTFIXTURES */
#define APSW_FAULT_INJECT(name,good,bad)        \
  do { good ; } while(0)

#endif


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

#define CHECK_USE(e)                                                \
  { if(self->inuse)                                                                                 \
      {    /* raise exception if we aren't already in one */                                                                         \
           if (!PyErr_Occurred())                                                                                                    \
             PyErr_Format(ExcThreadingViolation, "You are trying to use the same object concurrently in two threads which is not allowed."); \
           return e;                                                                                                                 \
      }                                                                                                                              \
  }

#define CHECK_CLOSED(connection,e) \
{ if(!connection->db) { PyErr_Format(ExcConnectionClosed, "The connection has been closed"); return e; } }

#define APSW_BEGIN_ALLOW_THREADS \
  do { \
      assert(self->inuse==0); self->inuse=1; \
      Py_BEGIN_ALLOW_THREADS

#define APSW_END_ALLOW_THREADS \
     Py_END_ALLOW_THREADS; \
     assert(self->inuse==1); self->inuse=0; \
  } while(0)

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
static PyObject *ExcVFSNotImplemented; /* base vfs doesn't implment function */
static PyObject *ExcVFSFileClosed;     /* attempted operation on closed file */

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
  unsigned int i;
  PyObject *obj;

  /* PyModule_AddObject uses borrowed reference so we incref whatever
     we give to it, so we still have a copy to use */

  /* custom ones first */

  APSWException=PyErr_NewException("apsw.Error", NULL, NULL);
  if(!APSWException) return -1;
  Py_INCREF(APSWException);
  if(PyModule_AddObject(m, "Error", (PyObject *)APSWException))
    return -1;

  struct {PyObject **var; const char *name; } apswexceptions[]={
    {&ExcThreadingViolation, "ThreadingViolationError"},
    {&ExcIncomplete, "IncompleteExecutionError"},
    {&ExcBindings, "BindingsError"},
    {&ExcComplete, "ExecutionCompleteError"},
    {&ExcTraceAbort, "ExecTraceAbort"},
    {&ExcExtensionLoading, "ExtensionLoadingError"},
    {&ExcConnectionNotClosed, "ConnectionNotClosedError"},
    {&ExcConnectionClosed, "ConnectionClosedError"},
    {&ExcVFSNotImplemented, "VFSNotImplementedError"},
    {&ExcVFSFileClosed, "VFSFileClosedError"}
  };

  for(i=0; i<sizeof(apswexceptions)/sizeof(apswexceptions[0]); i++)
    {
      sprintf(buffy, "apsw.%s", apswexceptions[i].name);
      *apswexceptions[i].var=PyErr_NewException(buffy, APSWException, NULL);
      if(!*apswexceptions[i].var) return -1;                                      
      Py_INCREF(*apswexceptions[i].var);                                          
      if(PyModule_AddObject(m, apswexceptions[i].name, *apswexceptions[i].var))         
        return -1;
    }

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

  APSW_FAULT_INJECT(UnknownSQLiteErrorCode,,res=0xfe);
  
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
#define SET_EXC(res,db)  { if(res != SQLITE_OK && !PyErr_Occurred()) make_exception(res,db); }

/* 
   The default Python PyErr_WriteUnraiseable is almost useless.  It
   only prints the str() of the exception and the str() of the object
   passed in.  This gives the developer no clue whatsoever where in
   the code it is happening.  It also does funky things to the passed
   in object which can cause the destructor to fire twice.
   Consequently we use our version here.  It makes the traceback
   complete, and then tries the following, going to the next if
   the hook isn't found or returns an error:

   * excepthook of hookobject (if not NULL)
   * excepthook of sys module
   * PyErr_Display

   If any return an error then then the next one is tried.  When we
   return, any error will be cleared.
*/
static void 
apsw_write_unraiseable(PyObject *hookobject)
{
  PyObject *err_type=NULL, *err_value=NULL, *err_traceback=NULL;
  PyObject *excepthook=NULL;
  PyObject *result=NULL;
  PyFrameObject *frame=NULL;

  /* fill in the rest of the traceback */
  frame = PyThreadState_GET()->frame;
  while(frame)
    {
      PyTraceBack_Here(frame);
      frame=frame->f_back;
    }
  
  /* Get the exception details */
  PyErr_Fetch(&err_type, &err_value, &err_traceback);
  PyErr_NormalizeException(&err_type, &err_value, &err_traceback);

  if(hookobject)
    {
      excepthook=PyObject_GetAttrString(hookobject, "excepthook");
      PyErr_Clear();
      if(excepthook)
        {
          result=PyEval_CallFunction(excepthook, "(OOO)", err_type?err_type:Py_None, err_value?err_value:Py_None, err_traceback?err_traceback:Py_None);
          if(result)
            goto finally;
        }
      Py_XDECREF(excepthook);
    }

  excepthook=PySys_GetObject("excepthook");
  if(excepthook)
    {
      Py_INCREF(excepthook); /* borrowed reference from PySys_GetObject so we increment */
      PyErr_Clear();
      result=PyEval_CallFunction(excepthook, "(OOO)", err_type?err_type:Py_None, err_value?err_value:Py_None, err_traceback?err_traceback:Py_None);
      if(result) 
        goto finally;
    }

  /* remove any error from callback failure */
  PyErr_Clear();
  PyErr_Display(err_type, err_value, err_traceback);

  finally:
  Py_XDECREF(excepthook);
  Py_XDECREF(result);
  Py_XDECREF(err_traceback);
  Py_XDECREF(err_value);
  Py_XDECREF(err_type);
  PyErr_Clear(); /* being paranoid - make sure no errors on return */
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
  int i;
  PyObject *str=NULL;
  PyObject *etype=NULL, *evalue=NULL, *etraceback=NULL;

  assert(PyErr_Occurred());

  PyErr_Fetch(&etype, &evalue, &etraceback);

  /* find out if the exception corresponds to an apsw exception descriptor */
  for(i=0;exc_descriptors[i].code!=-1;i++)
    if(PyErr_GivenExceptionMatches(etype, exc_descriptors[i].cls))
      {
        res=exc_descriptors[i].code;
        /* do we have extended information available? */
        if(PyObject_HasAttrString(evalue, "extendedresult"))
          {
            /* extract it */
            PyObject *extended=PyObject_GetAttrString(evalue, "extendedresult");
            if(extended && PyIntLong_Check(extended))
              /* Any errors in this will be swallowed */
              res=(PyIntLong_AsLong(extended) & 0xffffff00u)|res;
            Py_XDECREF(extended);
          }
        break;
      }

  if(errmsg)
    {
      /* I just want a string of the error! */      
      if(!str && evalue)
	str=PyObject_Str(evalue);
      if(!str && etype)
	str=PyObject_Str(etype);
      if(!str) str=MAKESTR("python exception with no information");
      if(*errmsg)
	sqlite3_free(*errmsg);
      *errmsg=sqlite3_mprintf("%s",PyBytes_AsString(str));

      Py_XDECREF(str);
    }

  PyErr_Restore(etype, evalue, etraceback);
  assert(PyErr_Occurred());
  return res;
}

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
  assert(method!=obj);
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
  if(!pyerralreadyoccurred && PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "Call_PythonMethod", "{s: s, s: i, s: O, s: O}", 
                     "methodname", methodname,
                     "mandatory", mandatory,
                     "args", args,
                     "method", method);

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

typedef struct Connection Connection; /* forward declaration */

typedef struct _vtableinfo
{
  PyObject *datasource;           /* object with create/connect methods */
  Connection *connection;         /* the Connection this is registered against so we don't
				     have to have a global table mapping sqlite3_db* to
				     Connection* */
} vtableinfo;


/* CONNECTION TYPE */

struct Connection { 
  PyObject_HEAD
  sqlite3 *db;                    /* the actual database connection */
  const char *filename;           /* utf8 filename of the database */
  int co_linenumber;              /* line number of allocation */
  PyObject *co_filename;          /* filename of allocation */

  unsigned inuse;                 /* track if we are in use preventing concurrent thread mangling */

  pointerlist dependents;         /* tracking cursors & blobs belonging to this connection */
  StatementCache *stmtcache;      /* prepared statement cache */

  funccbinfo *functions;          /* linked list of registered functions */

  /* registered hooks/handlers (NULL or callable) */
  PyObject *busyhandler;     
  PyObject *rollbackhook;
  PyObject *profile;
  PyObject *updatehook;
  PyObject *commithook;           
  PyObject *progresshandler;      
  PyObject *authorizer;
  PyObject *collationneeded;

  /* if we are using one of our VFS since sqlite doesn't reference count them */
  PyObject *vfs;
};

static PyTypeObject ConnectionType;

/* CURSOR TYPE */

typedef struct {
  PyObject_HEAD
  Connection *connection;          /* pointer to parent connection */
  sqlite3_stmt *statement;         /* current compiled statement */

  unsigned inuse;                 /* track if we are in use preventing concurrent thread mangling */

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
  unsigned inuse;                 /* track if we are in use preventing concurrent thread mangling */
  int curoffset;                  /* SQLite only supports 32 bit signed int offsets */
} APSWBlob;

static PyTypeObject APSWBlobType;



/* CONVENIENCE FUNCTIONS */

/* Return a PyBuffer (py2) or PyBytes (py3) */
#if PY_VERSION_HEX<0x03000000
static PyObject *
converttobytes(const void *ptr, Py_ssize_t size)
{

  PyObject *item;
  item=PyBuffer_New(size);
  if(item)
    {
      void *buffy=0;
      Py_ssize_t size2=size;
      int aswb=PyObject_AsWriteBuffer(item, &buffy, &size2);

      APSW_FAULT_INJECT(AsWriteBufferFails,,(PyErr_NoMemory(),aswb=-1));

      if(aswb==0)
        memcpy(buffy, ptr, size);
      else
        {
          Py_DECREF(item);
          item=NULL;
        }
    }
  return item;
}
#else
#define converttobytes PyBytes_FromStringAndSize
#endif



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

/* Returns a PyBytes/String encoded in UTF8 - new reference.
   Use PyBytes/String_AsString on the return value to get a
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
  size_t strbytes=2*PyUnicode_GET_SIZE(obj);             \
  const void *strdata=PyUnicode_AS_DATA(obj);            

#define UNIDATAEND(obj)                                  \
}

#define USE16(x) x##16

#else  /* Py_UNICODE_SIZE!=2 */

#define UNIDATABEGIN(obj) \
{                                                        \
  Py_ssize_t strbytes=0;				 \
  const char *strdata=NULL;                              \
  PyObject *_utf8=NULL;                                  \
  _utf8=PyUnicode_AsUTF8String(obj);                     \
  if(_utf8)                                              \
    {                                                    \
      strbytes=PyBytes_GET_SIZE(_utf8);                  \
      strdata=PyBytes_AS_STRING(_utf8);                  \
    } 

#define UNIDATAEND(obj)                                  \
  Py_XDECREF(_utf8);                                     \
}

#define USE16(x) x

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

  Py_XDECREF(self->busyhandler);
  self->busyhandler=0;

  Py_XDECREF(self->rollbackhook);
  self->rollbackhook=0;

  Py_XDECREF(self->profile);
  self->profile=0;

  Py_XDECREF(self->updatehook);
  self->updatehook=0;

  Py_XDECREF(self->commithook);
  self->commithook=0;

  Py_XDECREF(self->progresshandler);
  self->progresshandler=0;
  
  Py_XDECREF(self->authorizer);
  self->authorizer=0;

  Py_XDECREF(self->collationneeded);
  self->collationneeded=0;

  Py_XDECREF(self->vfs);
  self->vfs=0;
}

/* Closes cursors and blobs belonging to this connection */
static PyObject *
Connection_close(Connection *self, PyObject *args)
{
  int res;
  pointerlist_visit plv;
  int force=0;

  if(!self->db)
    goto finally;

  CHECK_USE(NULL);

  assert(!PyErr_Occurred());

  if(!PyArg_ParseTuple(args, "|i:close(force=False)", &force))
    return NULL;

  for(pointerlist_visit_begin(&self->dependents, &plv);
      pointerlist_visit_finished(&plv);
      pointerlist_visit_next(&plv))
    {
      PyObject *closeres=NULL;
      PyObject *obj=(PyObject*)pointerlist_visit_get(&plv);

      closeres=Call_PythonMethodV(obj, "close", 1, "(i)", force);
      Py_XDECREF(closeres);
      if(!closeres)
        return NULL;
    }

  res=statementcache_free(self->stmtcache);
  assert(res==0);
  self->stmtcache=0;

  APSW_BEGIN_ALLOW_THREADS
    APSW_FAULT_INJECT(ConnectionCloseFail, res=sqlite3_close(self->db), res=SQLITE_IOERR);
  APSW_END_ALLOW_THREADS;

  if (res!=SQLITE_OK) 
    {
      SET_EXC(res, self->db);
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
          res=statementcache_free(self->stmtcache);
          assert(res==0);
          self->stmtcache=0;
        }

      APSW_BEGIN_ALLOW_THREADS
        APSW_FAULT_INJECT(DestructorCloseFail, res=sqlite3_close(self->db), res=SQLITE_IOERR);
      APSW_END_ALLOW_THREADS;
      self->db=0;

      if(res!=SQLITE_OK)
        {
          /* not allowed to clobber existing exception */
          PyObject *etype=NULL, *evalue=NULL, *etraceback=NULL, *utf8filename=NULL;
          PyErr_Fetch(&etype, &evalue, &etraceback);

          utf8filename=getutf8string(self->co_filename);
          
          PyErr_Format(ExcConnectionNotClosed, 
                       "apsw.Connection on \"%s\" at address %p, allocated at %s:%d. The destructor "
                       "has encountered an error %d closing the connection, but cannot raise an exception.",
                       self->filename?self->filename:"NULL", self,
                       PyBytes_AsString(utf8filename), self->co_linenumber,
                       res);
          
          apsw_write_unraiseable(NULL);
          Py_XDECREF(utf8filename);
          PyErr_Restore(etype, evalue, etraceback);
        }
    }

  /* Our dependents all hold a refcount on us, so they must have all
     released before this destructor could be called */
  assert(self->dependents.numentries==0);
  pointerlist_free(&self->dependents);

  Connection_internal_cleanup(self);

  Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject*
Connection_new(PyTypeObject *type, APSW_ARGUNUSED PyObject *args, APSW_ARGUNUSED PyObject *kwds)
{
    Connection *self;

    self = (Connection *)type->tp_alloc(type, 0);
    if (self != NULL) {
      /* Strictly speaking the memory was already zeroed.  This is
         just defensive coding. */
      self->db=0;
      self->inuse=0;
      self->filename=0;
      self->co_linenumber=0;
      self->co_filename=0;
      memset(&self->dependents, 0, sizeof(self->dependents));
      pointerlist_init(&self->dependents);
      self->stmtcache=0;
      self->functions=0;
      self->busyhandler=0;
      self->rollbackhook=0;
      self->profile=0;
      self->updatehook=0;
      self->commithook=0;
      self->progresshandler=0;
      self->authorizer=0;
      self->collationneeded=0;
      self->vfs=0;
    }

    return (PyObject *)self;
}

/* forward declaration so we can tell if it is one of ours */
static int apswvfs_xAccess(sqlite3_vfs *vfs, const char *zName, int flags, int *pResOut);

static int
Connection_init(Connection *self, PyObject *args, PyObject *kwds)
{
  static char *kwlist[]={"filename", "flags", "vfs", "statementcachesize", NULL};
  PyObject *hooks=NULL, *hook=NULL, *iterator=NULL, *hookargs=NULL, *hookresult=NULL;
  PyFrameObject *frame;
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
     Don't do that! */
  APSW_BEGIN_ALLOW_THREADS
    vfsused=sqlite3_vfs_find(vfs);
    res=sqlite3_open_v2(filename, &self->db, flags, vfs);
  APSW_END_ALLOW_THREADS;
  SET_EXC(res, self->db);  /* nb sqlite3_open always allocates the db even on error */
  
  if(res!=SQLITE_OK)
      goto pyexception;
    
  if(vfsused && vfsused->xAccess==apswvfs_xAccess)
    {
      PyObject *pyvfsused=(PyObject*)(vfsused->pAppData);
      Py_INCREF(pyvfsused);
      self->vfs=pyvfsused;
    }

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
      self->stmtcache=statementcache_init(self->db, statementcachesize);
      goto finally;
    }

 pyexception:
  /* clean up db since it is useless - no need for user to call close */
  res= -1;
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

  CHECK_USE(NULL);
  CHECK_CLOSED(self, NULL);
  
  if(!PyArg_ParseTuple(args, "esesesLi:blobopen(database, table, column, rowid, rd_wr)", 
                       STRENCODING, &dbname, STRENCODING, &tablename, STRENCODING, &column, &rowid, &writing))
    return NULL;

  APSW_BEGIN_ALLOW_THREADS
    res=sqlite3_blob_open(self->db, dbname, tablename, column, rowid, writing, &blob);
  APSW_END_ALLOW_THREADS;

  PyMem_Free((void*)dbname);
  PyMem_Free((void*)tablename);
  PyMem_Free((void*)column);
  SET_EXC(res, self->db);
  if(res!=SQLITE_OK)
    return NULL;
  
  APSW_FAULT_INJECT(BlobAllocFails,apswblob=PyObject_New(APSWBlob, &APSWBlobType), (PyErr_NoMemory(), apswblob=NULL));
  if(!apswblob)
    {
      sqlite3_blob_close(blob);
      return NULL;
    }

  pointerlist_add(&self->dependents, apswblob);
  APSWBlob_init(apswblob, self, blob);
  return (PyObject*)apswblob;
}

static void APSWCursor_init(APSWCursor *, Connection *);

static PyObject *
Connection_cursor(Connection *self)
{
  APSWCursor* cursor = NULL;

  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);

  APSW_FAULT_INJECT(CursorAllocFails,cursor = PyObject_New(APSWCursor, &APSWCursorType), (PyErr_NoMemory(), cursor=NULL));
  if(!cursor)
    return NULL;

  /* incref me since cursor holds a pointer */
  Py_INCREF((PyObject*)self);
  pointerlist_add(&self->dependents, cursor);
  APSWCursor_init(cursor, self);
  
  return (PyObject*)cursor;
}

static PyObject *
Connection_setbusytimeout(Connection *self, PyObject *args)
{
  int ms=0;
  int res;

  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);

  if(!PyArg_ParseTuple(args, "i:setbusytimeout(millseconds)", &ms))
    return NULL;

  res=sqlite3_busy_timeout(self->db, ms);
  SET_EXC(res, self->db);
  if(res!=SQLITE_OK) return NULL;
  
  /* free any explicit busyhandler we may have had */
  Py_XDECREF(self->busyhandler);
  self->busyhandler=0;

  Py_RETURN_NONE;
}

static PyObject *
Connection_changes(Connection *self)
{
  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);
  return PyLong_FromLong(sqlite3_changes(self->db));
}

static PyObject *
Connection_totalchanges(Connection *self)
{
  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);
  return PyLong_FromLong(sqlite3_total_changes(self->db));
}

static PyObject *
Connection_getautocommit(Connection *self)
{
  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);
  if (sqlite3_get_autocommit(self->db))
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

static PyObject *
Connection_last_insert_rowid(Connection *self)
{
  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);

  return PyLong_FromLongLong(sqlite3_last_insert_rowid(self->db));
}

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

static PyObject *
Connection_interrupt(Connection *self)
{
  CHECK_CLOSED(self, NULL);

  sqlite3_interrupt(self->db);  /* no return value */
  Py_RETURN_NONE;
}

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

static PyObject *
Connection_setupdatehook(Connection *self, PyObject *callable)
{
  /* sqlite3_update_hook doesn't return an error code */
  
  CHECK_USE(NULL);
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

  APSW_FAULT_INJECT(RollbackHookExistingError,,PyErr_NoMemory());

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
  
  CHECK_USE(NULL);
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
  
  CHECK_USE(NULL);
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

static PyObject *
Connection_setcommithook(Connection *self, PyObject *callable)
{
  /* sqlite3_commit_hook doesn't return an error code */
  
  CHECK_USE(NULL);
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
  
  CHECK_USE(NULL);
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

static PyObject *
Connection_setauthorizer(Connection *self, PyObject *callable)
{
  int res;

  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);

  if(callable==Py_None)
    {
      APSW_FAULT_INJECT(SetAuthorizerNullFail,res=sqlite3_set_authorizer(self->db, NULL, NULL),res=SQLITE_IOERR);
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

  APSW_FAULT_INJECT(SetAuthorizerFail,res=sqlite3_set_authorizer(self->db, authorizercb, self),res=SQLITE_IOERR);
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

static PyObject *
Connection_collationneeded(Connection *self, PyObject *callable)
{
  int res;

  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);

  if(callable==Py_None)
    {
      APSW_FAULT_INJECT(CollationNeededNullFail,res=sqlite3_collation_needed(self->db, NULL, NULL),res=SQLITE_IOERR);
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

  APSW_FAULT_INJECT(CollationNeededFail,res=sqlite3_collation_needed(self->db, self, collationneeded_cb), res=SQLITE_IOERR);
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

#if defined(EXPERIMENTAL) && !defined(SQLITE_OMIT_LOAD_EXTENSION)  /* extension loading */
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
  APSW_FAULT_INJECT(EnableLoadExtensionFail, res=sqlite3_enable_load_extension(self->db, enabledp), res=SQLITE_IOERR);
  SET_EXC(res, self->db);

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

  CHECK_USE(NULL);
  CHECK_CLOSED(self, NULL);
  
  if(!PyArg_ParseTuple(args, "es|z:loadextension(filename, entrypoint=None)", STRENCODING, &zfile, &zproc))
    return NULL;

  APSW_BEGIN_ALLOW_THREADS
    res=sqlite3_load_extension(self->db, zfile, zproc, &errmsg);
  APSW_END_ALLOW_THREADS;
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

static PyObject *
Connection_setbusyhandler(Connection *self, PyObject *callable)
{
  int res=SQLITE_OK;

  CHECK_USE(NULL);
  CHECK_CLOSED(self,NULL);

  if(callable==Py_None)
    {
      APSW_FAULT_INJECT(SetBusyHandlerNullFail,res=sqlite3_busy_handler(self->db, NULL, NULL), res=SQLITE_IOERR);
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

  APSW_FAULT_INJECT(SetBusyHandlerFail,res=sqlite3_busy_handler(self->db, busyhandlercb, self), res=SQLITE_IOERR);
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
  int coltype=sqlite3_value_type(value);

  APSW_FAULT_INJECT(UnknownValueType,,coltype=123456);

  switch(coltype)
    {
    case SQLITE_INTEGER:
      {
        sqlite3_int64 val=sqlite3_value_int64(value);
#if Py_VERSION_MAJOR<3
        if (val>=APSW_INT32_MIN && val<=APSW_INT32_MAX)
          return PyInt_FromLong((long)val);
#endif
        return PyLong_FromLongLong(val);
      }

    case SQLITE_FLOAT:
      return PyFloat_FromDouble(sqlite3_value_double(value));
      
    case SQLITE_TEXT:
      return convertutf8stringsize((const char*)sqlite3_value_text(value), sqlite3_value_bytes(value));

    case SQLITE_NULL:
      Py_RETURN_NONE;

    case SQLITE_BLOB:
      return converttobytes(sqlite3_value_blob(value), sqlite3_value_bytes(value));

    default:
      PyErr_Format(APSWException, "Unknown sqlite column type %d!", coltype);
      return NULL;
    }
  /* can't get here */
  assert(0);
  return NULL;
}

/* Converts column to PyObject.  Returns a new reference. Almost identical to above 
   but we cannot just use sqlite3_column_value and then call the above function as 
   SQLite doesn't allow that ("unprotected values") */
static PyObject *
convert_column_to_pyobject(sqlite3_stmt *stmt, int col)
{
  int coltype=sqlite3_column_type(stmt, col);

  APSW_FAULT_INJECT(UnknownColumnType,,coltype=12348);

  switch(coltype)
    {
    case SQLITE_INTEGER:
      {
        sqlite3_int64 val=sqlite3_column_int64(stmt, col);
#if Py_VERSION_MAJOR<3
        if (val>=APSW_INT32_MIN && val<=APSW_INT32_MAX)
          return PyInt_FromLong((long)val);
#endif
        return PyLong_FromLongLong(val);
      }

    case SQLITE_FLOAT:
      return PyFloat_FromDouble(sqlite3_column_double(stmt, col));
      
    case SQLITE_TEXT:
      return convertutf8stringsize((const char*)sqlite3_column_text(stmt, col), sqlite3_column_bytes(stmt, col));

    case SQLITE_NULL:
      Py_RETURN_NONE;

    case SQLITE_BLOB:
      return converttobytes(sqlite3_column_blob(stmt, col), sqlite3_column_bytes(stmt, col));

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
#if PY_VERSION_HEX<0x03000000
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
#if PY_VERSION_HEX < 0x03000000
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
  funccbinfo *cbinfo=(funccbinfo*)sqlite3_user_data(context);
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
      SET_EXC(res, self->db);
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
      SET_EXC(res, self->db);
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

  res=sqlite3_create_collation_v2(self->db,
                                  name,
                                  SQLITE_UTF8,
                                  (callable!=Py_None)?callable:NULL,
                                  (callable!=Py_None)?collation_cb:NULL,
                                  (callable!=Py_None)?collation_destroy:NULL);
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

  res=sqlite3_file_control(self->db, dbname, op, ptr);

  SET_EXC(res, self->db);

 finally:
  if(dbname) PyMem_Free(dbname);
  
  if(PyErr_Occurred())
    return NULL;

  Py_RETURN_NONE;
}

/* Virtual table code */

#ifdef EXPERIMENTAL

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
      PyObject *str;

      APSW_FAULT_INJECT(VtabCreateBadString,str=convertutf8string(argv[i]), str=PyErr_NoMemory());
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
    sqliteres=sqlite3_declare_vtab(db, PyBytes_AsString(utf8schema));
    Py_DECREF(utf8schema);
    if(sqliteres!=SQLITE_OK)
      {
	SET_EXC(sqliteres, db);
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


static void
vtabFree(void *context)
{
  vtableinfo *vti=(vtableinfo*)context;
  PyGILState_STATE gilstate;
  gilstate=PyGILState_Ensure();

  Py_XDECREF(vti->datasource);
  /* connection was a borrowed reference so no decref needed */
  PyMem_Free(vti);

  PyGILState_Release(gilstate);
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
	  if(PyErr_Occurred() || !constraint) goto pyexception;
	  j++;
	  /* it can be None */
	  if(constraint==Py_None)
	    {
	      Py_DECREF(constraint);
	      continue;
	    }
	  /* or an integer */
	  if(PyIntLong_Check(constraint))
	    {
	      indexinfo->aConstraintUsage[i].argvIndex=PyIntLong_AsLong(constraint);
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
	  if(!PyIntLong_Check(argvindex))
	    {
	      PyErr_Format(PyExc_TypeError, "argvindex for constraint #%d should be an integer", j);
	      AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xBestIndex.result_constraint_argvindex", "{s: O, s: O, s: O, s: O, s: O}", 
			       "self", vtable, "result", res, "indices", indices, "constraint", constraint, "argvindex", argvindex);
	      goto constraintfail;
	    }
	  omitv=PyObject_IsTrue(omit);
	  if(omitv==-1) 
            goto constraintfail;
          indexinfo->aConstraintUsage[i].argvIndex=PyIntLong_AsLong(argvindex);
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
	if(!PyIntLong_Check(idxnum))
	  {
	    PyErr_Format(PyExc_TypeError, "idxnum must be an integer");
	      AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xBestIndex.result_indexnum", "{s: O, s: O, s: O}", "self", vtable, "result", res, "indexnum", idxnum);
	    Py_DECREF(idxnum);
	    goto pyexception;
	  }
        indexinfo->idxNum=PyIntLong_AsLong(idxnum);
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
	indexinfo->idxStr=sqlite3_mprintf("%s", PyBytes_AsString(utf8str));
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

static struct {
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
vtabRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid)
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
vtabUpdate(sqlite3_vtab *pVtab, int argc, sqlite3_value **argv, sqlite3_int64 *pRowid)
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
      APSW_FAULT_INJECT(VtabUpdateChangeRowFail,newrowid=convert_value_to_pyobject(argv[1]), newrowid=PyErr_NoMemory());
      if(!args || !oldrowid || !newrowid)
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
	  PyObject *field;
          APSW_FAULT_INJECT(VtabUpdateBadField,field=convert_value_to_pyobject(argv[i+2]), field=PyErr_NoMemory());
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

static int
vtabRename(sqlite3_vtab *pVtab, const char *zNew)
{
  PyGILState_STATE gilstate;
  PyObject *vtable, *res=NULL, *newname=NULL;
  int sqliteres=SQLITE_OK;

  gilstate=PyGILState_Ensure();
  vtable=((apsw_vtable*)pVtab)->vtable;

  APSW_FAULT_INJECT(VtabRenameBadName, newname=convertutf8string(zNew), newname=PyErr_NoMemory());
  if(!newname)
    {
      sqliteres=SQLITE_ERROR;
      goto finally;
    }
  /* Marked as optional since sqlite does the actual renaming */
  res=Call_PythonMethodV(vtable, "Rename", 0, "(N)", newname);
  if(!res)
    {
      sqliteres=MakeSqliteMsgFromPyException(NULL);
      AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xRename", "{s: O, s: s}", "self", vtable, "newname", zNew);
    }
  
 finally:
  Py_XDECREF(res);
  PyGILState_Release(gilstate);
  return sqliteres;
}


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
    vtabRename 
  };

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
  APSW_FAULT_INJECT(CreateModuleFail, res=sqlite3_create_module_v2(self->db, name, &apsw_vtable_module, vti, vtabFree), res=SQLITE_IOERR);
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
  {0, 0, 0, 0}  /* Sentinel */
};


static PyTypeObject ConnectionType = 
  {
#if PY_VERSION_HEX<0x03000000
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
#else
    PyVarObject_HEAD_INIT(NULL,0)
#endif
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
#if PY_VERSION_HEX>=0x03000000
    0                          /* tp_version_tag */
#endif
};


/* ZEROBLOB CODE */

/*  Zeroblob is used for binding and results - takes a single integer
   in constructor and has no other methods */

typedef struct {
  PyObject_HEAD
  int blobsize;
} ZeroBlobBind;

static PyObject*
ZeroBlobBind_new(PyTypeObject *type, APSW_ARGUNUSED PyObject *args, APSW_ARGUNUSED PyObject *kwargs)
{
  ZeroBlobBind *self;
  self=(ZeroBlobBind*)type->tp_alloc(type, 0);
  if(self) self->blobsize=0;
  return (PyObject*)self;
}

static int
ZeroBlobBind_init(ZeroBlobBind *self, PyObject *args, PyObject *kwargs)
{
  int n;
  if(kwargs && PyDict_Size(kwargs)!=0)
    {
      PyErr_Format(PyExc_TypeError, "Zeroblob constructor does not take keyword arguments");
      return -1;
    }
  
  if(!PyArg_ParseTuple(args, "i", &n))
    return -1;

  if(n<0)
    {
      PyErr_Format(PyExc_TypeError, "zeroblob size must be >= 0");
      return -1;
    }
  self->blobsize=n;

  return 0;
}

static PyTypeObject ZeroBlobBindType = {
#if PY_VERSION_HEX<0x03000000
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
#else
    PyVarObject_HEAD_INIT(NULL,0)
#endif
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
    (initproc)ZeroBlobBind_init, /* tp_init */
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
#if PY_VERSION_HEX>=0x03000000
    0                          /* tp_version_tag */
#endif
};


/* BLOB CODE */

static void
APSWBlob_init(APSWBlob *self, Connection *connection, sqlite3_blob *blob)
{
  Py_INCREF(connection);
  self->connection=connection;
  self->pBlob=blob;
  self->curoffset=0;
  self->inuse=0;
}

static void
APSWBlob_dealloc(APSWBlob *self)
{
  if(self->pBlob)
    {
      int res=sqlite3_blob_close(self->pBlob);
      if(res!=SQLITE_OK)
        {
          PyObject *err_type, *err_value, *err_traceback;
          int have_error;

          APSW_FAULT_INJECT(BlobDeallocException,,PyErr_NoMemory());

          have_error=PyErr_Occurred()?1:0;
          if(have_error)
            PyErr_Fetch(&err_type, &err_value, &err_traceback);
          SET_EXC(res, self->connection->db);
          apsw_write_unraiseable(NULL);
          if(have_error)
            PyErr_Restore(err_type, err_value, err_traceback);
          /* destructors can't throw exceptions */
          PyErr_Clear();
        }
      self->pBlob=0;
      pointerlist_remove(&self->connection->dependents, self);
    }
  if(self->connection)
    {
      Py_DECREF(self->connection);
      self->connection=0;
    }
  Py_TYPE(self)->tp_free((PyObject*)self);
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
  CHECK_USE(NULL);
  CHECK_BLOB_CLOSED;
  return PyLong_FromLong(sqlite3_blob_bytes(self->pBlob));
}

static PyObject *
APSWBlob_read(APSWBlob *self, PyObject *args)
{
  int length=-1;
  int res;
  PyObject *buffy=0;
  char *thebuffer;

  CHECK_USE(NULL);
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
    return PyBytes_FromStringAndSize(NULL, 0);

  if(length<0)
    length=sqlite3_blob_bytes(self->pBlob)-self->curoffset;

  /* trying to read more than is in the blob? */
  if(self->curoffset+length>sqlite3_blob_bytes(self->pBlob))
    length=sqlite3_blob_bytes(self->pBlob)-self->curoffset;

  buffy=PyBytes_FromStringAndSize(NULL, length);

  if(!buffy) return NULL;

  thebuffer= PyBytes_AS_STRING(buffy);
  APSW_BEGIN_ALLOW_THREADS
    res=sqlite3_blob_read(self->pBlob, thebuffer, length, self->curoffset);
  APSW_END_ALLOW_THREADS;

  if(res!=SQLITE_OK)
    {
      Py_DECREF(buffy);
      SET_EXC(res, self->connection->db);
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
  int offset, whence=0;
  CHECK_USE(NULL);
  CHECK_BLOB_CLOSED;
  
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
  CHECK_USE(NULL);
  CHECK_BLOB_CLOSED;
  return PyLong_FromLong(self->curoffset);
}

static PyObject *
APSWBlob_write(APSWBlob *self, PyObject *obj)
{
  const void *buffer=0;
  Py_ssize_t size;
  int res;
  CHECK_USE(NULL);
  CHECK_BLOB_CLOSED;

  /* we support buffers and string for the object */
  if(!PyUnicode_Check(obj) && PyObject_CheckReadBuffer(obj))
    {
      int asrb=PyObject_AsReadBuffer(obj, &buffer, &size);

      APSW_FAULT_INJECT(BlobWriteAsReadBufFails,,(PyErr_NoMemory(), asrb=-1));

      if(asrb!=0)
        return NULL;
    }
  else
    {
      PyErr_Format(PyExc_TypeError, "Parameter should be bytes/string or buffer");
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

  APSW_BEGIN_ALLOW_THREADS
    res=sqlite3_blob_write(self->pBlob, buffer, size, self->curoffset);
  APSW_END_ALLOW_THREADS;

  if(res!=SQLITE_OK)
    {
      SET_EXC(res, self->connection->db);
      return NULL;
    }
  else
    self->curoffset+=size;
  assert(self->curoffset<=sqlite3_blob_bytes(self->pBlob));
  Py_RETURN_NONE;
}

static PyObject *
APSWBlob_close(APSWBlob *self, PyObject *args)
{
  int res;
  int force=0;
  /* we allow close to be called multiple times */
  if(!self->pBlob) goto end;
  CHECK_USE(NULL);

  if(!PyArg_ParseTuple(args, "|i:close(force=False)", &force))
    return NULL;

  APSW_BEGIN_ALLOW_THREADS
    res=sqlite3_blob_close(self->pBlob);
  APSW_END_ALLOW_THREADS;

  SET_EXC(res, self->connection->db);
  pointerlist_remove(&self->connection->dependents, self);
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
  {"close", (PyCFunction)APSWBlob_close, METH_VARARGS,
   "Closes blob"},
  {0,0,0,0} /* Sentinel */
};

static PyTypeObject APSWBlobType = {
#if PY_VERSION_HEX<0x03000000
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
#else
    PyVarObject_HEAD_INIT(NULL,0)
#endif
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
    0,                         /* tp_new */
    0,                         /* tp_free */
    0,                         /* tp_is_gc */
    0,                         /* tp_bases */
    0,                         /* tp_mro */
    0,                         /* tp_cache */
    0,                         /* tp_subclasses */
    0,                         /* tp_weaklist */
    0,                         /* tp_del */
#if PY_VERSION_HEX>=0x03000000
    0                          /* tp_version_tag */
#endif
};



/* CURSOR CODE */

/* Do finalization and free resources.  Returns the SQLITE error code */
static int
resetcursor(APSWCursor *self, int force)
{
  int res=SQLITE_OK;

  Py_XDECREF(self->bindings);
  self->bindings=NULL;
  self->bindingsoffset= -1;

  if(self->statement)
    {
      res=statementcache_finalize(self->connection->stmtcache, self->statement);
      if(!force) /* we don't care about errors when forcing */
	SET_EXC(res, self->connection->db);
      self->statement=0;
    }

  if(!force && (self->status!=C_DONE && self->zsqlnextpos))
    {
      if (*self->zsqlnextpos && res==SQLITE_OK)
        {
          /* We still have more, so this is actually an abort. */
          res=SQLITE_ERROR;
          if(!PyErr_Occurred())
            {
              PyErr_Format(ExcIncomplete, "Error: there are still remaining sql statements to execute");
              AddTraceBackHere(__FILE__, __LINE__, "resetcursor", "{s: s}", "remaining", self->zsqlnextpos);
            }
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
      AddTraceBackHere(__FILE__, __LINE__, "resetcursor", "{s: i}", "res", res);
    }

  return res;
}

static void
APSWCursor_dealloc(APSWCursor * self)
{
  PyObject *err_type, *err_value, *err_traceback;
  int have_error=PyErr_Occurred()?1:0;

  /* do our finalisation ... */

  if (have_error)
    {
      /* remember the existing error so that resetcursor won't immediately return */
      PyErr_Fetch(&err_type, &err_value, &err_traceback);
      PyErr_Clear();
    }

  resetcursor(self, /* force = */ 1);
  assert(!PyErr_Occurred());

  if (have_error)
    /* restore earlier error if there was one */
    PyErr_Restore(err_type, err_value, err_traceback);

  /* we no longer need connection */
  if(self->connection)
    {
      pointerlist_remove(&self->connection->dependents, self);
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
  
  Py_TYPE(self)->tp_free((PyObject*)self);
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
  self->inuse=0;
}

static PyObject *
APSWCursor_getdescription(APSWCursor *self)
{
  int ncols,i;
  PyObject *result=NULL;
  PyObject *pair=NULL;

  CHECK_USE(NULL);
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
      APSW_FAULT_INJECT(GetDescriptionFail,
      pair=Py_BuildValue("(O&O&)", 
			 convertutf8string, sqlite3_column_name(self->statement, i),
			 convertutf8string, sqlite3_column_decltype(self->statement, i)),
      pair=PyErr_NoMemory()
      );                  
			 
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

  APSW_FAULT_INJECT(DoBindingFail,,PyErr_NoMemory());

  if(PyErr_Occurred()) 
    return -1;

  if(obj==Py_None)
    res=sqlite3_bind_null(self->statement, arg);
  /* Python uses a 'long' for storage of PyInt.  This could
     be a 32bit or 64bit quantity depending on the platform. */
#if PY_VERSION_HEX<0x03000000
  else if(PyInt_Check(obj))
    res=sqlite3_bind_int64(self->statement, arg, PyInt_AS_LONG(obj));
#endif
  else if (PyLong_Check(obj))
    /* nb: PyLong_AsLongLong can cause Python level error */
    res=sqlite3_bind_int64(self->statement, arg, PyLong_AsLongLong(obj));
  else if (PyFloat_Check(obj))
    res=sqlite3_bind_double(self->statement, arg, PyFloat_AS_DOUBLE(obj));
  else if (PyUnicode_Check(obj))
    {
      const void *badptr=NULL;
      UNIDATABEGIN(obj)
        APSW_FAULT_INJECT(DoBindingUnicodeConversionFails,,strdata=(char*)PyErr_NoMemory());
        badptr=strdata;
#ifdef APSW_TEST_LARGE_OBJECTS
        APSW_FAULT_INJECT(DoBindingLargeUnicode,,strbytes=0x001234567890L);
#endif
        if(strdata)
          {
	    if(strbytes>APSW_INT32_MAX)
	      {
                SET_EXC(SQLITE_TOOBIG, NULL);
	      }
	    else
              res=USE16(sqlite3_bind_text)(self->statement, arg, strdata, strbytes, SQLITE_TRANSIENT);
          }
      UNIDATAEND(obj);
      if(!badptr) 
        {
          assert(PyErr_Occurred());
          return -1;
        }
    }
#if PY_VERSION_HEX < 0x03000000
  else if (PyString_Check(obj))
    {
      const char *val=PyString_AS_STRING(obj);
      const size_t lenval=PyString_GET_SIZE(obj);
      const char *chk=val;

      if(lenval<10000)
        for(;chk<val+lenval && !((*chk)&0x80); chk++);
      if(chk<val+lenval)
        {
          const void *badptr=NULL;
          PyObject *str2=PyUnicode_FromObject(obj);
          if(!str2)
            return -1;
          UNIDATABEGIN(str2)
            APSW_FAULT_INJECT(DoBindingStringConversionFails,,strdata=(char*)PyErr_NoMemory());
#ifdef APSW_TEST_LARGE_OBJECTS
            APSW_FAULT_INJECT(DoBindingLargeString,,strbytes=0x001234567890L);
#endif
            badptr=strdata;
            if(strdata)
              {
		if(strbytes>APSW_INT32_MAX)
		  {
                    SET_EXC(SQLITE_TOOBIG, NULL);
                    res=SQLITE_TOOBIG;
		  }
		else
                  res=USE16(sqlite3_bind_text)(self->statement, arg, strdata, strbytes, SQLITE_TRANSIENT);
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
	  assert(lenval<APSW_INT32_MAX);
	  res=sqlite3_bind_text(self->statement, arg, val, lenval, SQLITE_TRANSIENT);
	}
    }
#endif
  else if (PyObject_CheckReadBuffer(obj))
    {
      const void *buffer;
      Py_ssize_t buflen;
      int asrb;
      
      APSW_FAULT_INJECT(DoBindingAsReadBufferFails,asrb=PyObject_AsReadBuffer(obj, &buffer, &buflen), (PyErr_NoMemory(), asrb=-1));
      if(asrb!=0)
        return -1;

      if (buflen>APSW_INT32_MAX)
	{
          SET_EXC(SQLITE_TOOBIG, NULL);
	  return -1;
	}
      res=sqlite3_bind_blob(self->statement, arg, buffer, buflen, SQLITE_TRANSIENT);
    }
  else if(PyObject_TypeCheck(obj, &ZeroBlobBindType)==1)
    {
      res=sqlite3_bind_zeroblob(self->statement, arg, ((ZeroBlobBind*)obj)->blobsize);
    }
  else 
    {
      PyErr_Format(PyExc_TypeError, "Bad binding argument type supplied - argument #%d: type %s", (int)(arg+self->bindingsoffset), Py_TYPE(obj)->tp_name);
      return -1;
    }
  if(res!=SQLITE_OK)
    {
      SET_EXC(res, self->connection->db);
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

  APSW_FAULT_INJECT(DoBindingExistingError,,PyErr_NoMemory());
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
          if(APSWCursor_dobinding(self,arg,obj)!=SQLITE_OK)
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
          APSW_FAULT_INJECT(DoExecTraceBadSlice,
          bindings=PySequence_GetSlice(self->bindings, etos->savedbindingsoffset, self->bindingsoffset),
          bindings=PyErr_NoMemory());

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

  for(;;)
    {
      assert(!PyErr_Occurred());
      APSW_BEGIN_ALLOW_THREADS
        res=(self->statement)?(sqlite3_step(self->statement)):(SQLITE_DONE);
      APSW_END_ALLOW_THREADS;

      switch(res&0xff)
        {
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
	  /* We used to call statementcache_dup which did a reprepare.
             To avoid race conditions with the statement cache (we
             release the GIL around prepare now) we now just return
             the error.  See SQLite ticket 2158.
	   */

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
      SET_EXC(res, self->connection->db);
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
      res=statementcache_prepare(self->connection->stmtcache, self->connection->db, self->zsqlnextpos, -1, &self->statement, &self->zsqlnextpos, &self->inuse);
      SET_EXC(res, self->connection->db);
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

  CHECK_USE(NULL);
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
  res=statementcache_prepare(self->connection->stmtcache, self->connection->db, self->zsql, -1, &self->statement, &self->zsqlnextpos, &self->inuse);
  SET_EXC(res, self->connection->db);
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

  CHECK_USE(NULL);
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
  res=statementcache_prepare(self->connection->stmtcache, self->connection->db, self->zsql, -1, &self->statement, &self->zsqlnextpos, &self->inuse);
  SET_EXC(res, self->connection->db);
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

  CHECK_USE(NULL);
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

  CHECK_USE(NULL);
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
  
  /* return the row of data */
  numcols=sqlite3_data_count(self->statement);
  retval=PyTuple_New(numcols);
  if(!retval) goto error;

  for(i=0;i<numcols;i++)
    {
      item=convert_column_to_pyobject(self->statement, i);
      if(!item) goto error;
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
 error:
  Py_XDECREF(retval);
  return NULL;
}

static PyObject *
APSWCursor_iter(APSWCursor *self)
{
  CHECK_USE(NULL);
  CHECK_CLOSED(self->connection, NULL);

  Py_INCREF(self);
  return (PyObject*)self;
}

static PyObject *
APSWCursor_setexectrace(APSWCursor *self, PyObject *func)
{
  CHECK_USE(NULL);
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
  CHECK_USE(NULL);
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

  CHECK_USE(NULL);
  CHECK_CLOSED(self->connection, NULL);

  ret=(self->exectrace)?(self->exectrace):Py_None;
  Py_INCREF(ret);
  return ret;
}

static PyObject *
APSWCursor_getrowtrace(APSWCursor *self)
{
  PyObject *ret;
  CHECK_USE(NULL);
  CHECK_CLOSED(self->connection, NULL);
  ret =(self->rowtrace)?(self->rowtrace):Py_None;
  Py_INCREF(ret);
  return ret;
}

static PyObject *
APSWCursor_getconnection(APSWCursor *self)
{
  CHECK_USE(NULL);
  CHECK_CLOSED(self->connection, NULL);

  Py_INCREF(self->connection);
  return (PyObject*)self->connection;
}

static PyMethodDef APSWCursor_methods[] = {
  {"execute", (PyCFunction)APSWCursor_execute, METH_VARARGS,
   "Executes one or more statements" },
  {"executemany", (PyCFunction)APSWCursor_executemany, METH_VARARGS,
   "Repeatedly executes statements on sequence" },
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
#if PY_VERSION_HEX<0x03000000
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
#else
    PyVarObject_HEAD_INIT(NULL,0)
#endif
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
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE
#if PY_VERSION_HEX<0x03000000
 | Py_TPFLAGS_HAVE_ITER
#endif
 , /*tp_flags*/
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
#if PY_VERSION_HEX>=0x03000000
    0                          /* tp_version_tag */
#endif
};



/* VFS CODE */

/* Naming convention prefixes.  Since sqlite3.c is #included into this
   file we have to ensure there is no clash with its names.  There are
   two objects - the VFS itself and a VFSFile as returned from xOpen.
   For each there are both C and Python methods.  The C methods are
   what SQLite calls and effectively turns a C call into a Python
   call.  The Python methods turn a Python call into the C call of the
   (SQLite C) object we are inheriting from and wouldn't be necessary
   if we didn't implement the inheritance feature.

   Methods:

   apswvfs_         sqlite3_vfs* functions http://sqlite.org/c3ref/vfs.html
   apswvfspy_       Python implementations of those same functions
   apswvfsfile_     io methods http://sqlite.org/c3ref/io_methods.html
   apswvfsfilepy_   Python implementations of those same functions

   Structures:

   APSWVFS          Python object for vfs (sqlite3_vfs * is used for sqlite object)
   APSWVFSType      Type object for above
   APSWVFSFile      Python object for vfs file
   APSWVFSFileType  Type object for above
   APSWSQLite3File  sqlite object for vfs file ("subclass" of sqlite3_file)
*/

/* what error code do we do for not implemented? */
#define VFSNOTIMPLEMENTED(x)              \
  if(!self->basevfs || !self->basevfs->x) \
  { PyErr_Format(ExcVFSNotImplemented, "VFSNotImplementedError: Method " #x " is not implemented"); return NULL; }

#define VFSFILENOTIMPLEMENTED(x)              \
  if(!self->base || !self->base->pMethods->x) \
  { PyErr_Format(ExcVFSNotImplemented, "VFSNotImplementedError: File method " #x " is not implemented"); return NULL; }

/* various checks */
#define CHECKVFS \
   assert(vfs->pAppData);

#define CHECKVFSPY   \
   assert(self->containingvfs->pAppData==self)

#define CHECKVFSFILE \
   assert(apswfile->file); 

#define CHECKVFSFILEPY \
  if(!self->base) { PyErr_Format(ExcVFSFileClosed, "VFSFileClosed: Attempting operation on closed file"); return NULL; }

#define VFSPREAMBLE                         \
  PyObject *etype, *eval, *etb;             \
  PyGILState_STATE gilstate;                \
  gilstate=PyGILState_Ensure();             \
  PyErr_Fetch(&etype, &eval, &etb);         \
  CHECKVFS;

#define VFSPOSTAMBLE                        \
  if(PyErr_Occurred())                      \
    apsw_write_unraiseable((PyObject*)(vfs->pAppData)); \
  PyErr_Restore(etype, eval, etb);          \
  PyGILState_Release(gilstate);

#define FILEPREAMBLE                        \
  APSWSQLite3File *apswfile=(APSWSQLite3File*)(void*)file; \
  PyObject *etype, *eval, *etb;             \
  PyGILState_STATE gilstate;                \
  gilstate=PyGILState_Ensure();             \
  PyErr_Fetch(&etype, &eval, &etb);         \
  CHECKVFSFILE;

#define FILEPOSTAMBLE                       \
  if(PyErr_Occurred())                      \
    apsw_write_unraiseable(apswfile->file); \
  PyErr_Restore(etype, eval, etb);          \
  PyGILState_Release(gilstate);

typedef struct 
{
  PyObject_HEAD;
  sqlite3_vfs *basevfs;         /* who we inherit from (might be null) */
  sqlite3_vfs *containingvfs;   /* pointer given to sqlite for this instance */
  int registered;               /* are we currently registered? */
} APSWVFS;

static PyTypeObject APSWVFSType;

typedef struct /* inherits */
{
  const struct sqlite3_io_methods *pMethods;  /* structure sqlite needs */
  PyObject *file;                             
} APSWSQLite3File;

/* this is only used if there is inheritance */
typedef struct
{
  PyObject_HEAD;
  struct sqlite3_file *base;
} APSWVFSFile;

static PyTypeObject APSWVFSFileType;

static struct sqlite3_io_methods apsw_io_methods;

/* This function only needs to call sys.excepthook.  If things mess up
   then whoever called us will fallback on PyErr_Display etc */
static PyObject*
apswvfs_excepthook(APSW_ARGUNUSED PyObject *donotuseself, PyObject *args)
{
  /* NOTE: do not use the self argument as this function is used for
     both apswvfs and apswvfsfile.  If you need to use self then make
     two versions of the function. */
  PyObject *excepthook; 

  excepthook=PySys_GetObject("excepthook"); /* NB borrowed reference */
  if(!excepthook) return NULL;

  return PyEval_CallObject(excepthook, args);
}

static int
apswvfs_xDelete(sqlite3_vfs *vfs, const char *zName, int syncDir)
{
  PyObject *pyresult=NULL;
  int result=SQLITE_OK;

  VFSPREAMBLE;

  pyresult=Call_PythonMethodV((PyObject*)(vfs->pAppData), "xDelete", 1, "(Ni)", convertutf8string(zName), syncDir);
  if(!pyresult)
    {
      result=MakeSqliteMsgFromPyException(NULL);
      AddTraceBackHere(__FILE__, __LINE__, "vfs.xDelete", "{s: s, s: i}", "zName", zName, "syncDir", syncDir);
    }

  VFSPOSTAMBLE;
  return result;
}

static PyObject *
apswvfspy_xDelete(APSWVFS *self, PyObject *args)
{
  char *zName=NULL;
  int syncDir, res;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xDelete);

  if(!PyArg_ParseTuple(args, "esi", STRENCODING, &zName, &syncDir))
    return NULL;

  res=self->basevfs->xDelete(self->basevfs, zName, syncDir);
  PyMem_Free(zName);

  if(res==SQLITE_OK)
    Py_RETURN_NONE;

  SET_EXC(res, NULL);
  return NULL;
}

static int
apswvfs_xAccess(sqlite3_vfs *vfs, const char *zName, int flags, int *pResOut)
{
  PyObject *pyresult=NULL;
  int result=SQLITE_OK;

  VFSPREAMBLE;

  pyresult=Call_PythonMethodV((PyObject*)(vfs->pAppData), "xAccess", 1, "(Ni)", convertutf8string(zName), flags);
  if(!pyresult)
    goto finally;

  if(PyIntLong_Check(pyresult))
    *pResOut=!!PyIntLong_AsLong(pyresult);
  else
    PyErr_Format(PyExc_TypeError, "xAccess should return a number");

 finally:
  if(PyErr_Occurred())
    {
      *pResOut=0;
      result=MakeSqliteMsgFromPyException(NULL);
      AddTraceBackHere(__FILE__, __LINE__, "vfs.xAccess", "{s: s, s: i}", "zName", zName, "flags", flags);
    }

  VFSPOSTAMBLE;
  return result;
}

static PyObject *
apswvfspy_xAccess(APSWVFS *self, PyObject *args)
{
  char *zName=NULL;
  int res, flags, resout=0;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xAccess);

  if(!PyArg_ParseTuple(args, "esi", STRENCODING, &zName, &flags))
    return NULL;

  res=self->basevfs->xAccess(self->basevfs, zName, flags, &resout);
  PyMem_Free(zName);

  if(res==SQLITE_OK)
    {
      if(resout)
        Py_RETURN_TRUE;
      Py_RETURN_FALSE;
    }

  SET_EXC(res, NULL);
  return NULL;
}


static int
apswvfs_xFullPathname(sqlite3_vfs *vfs, const char *zName, int nOut, char *zOut)
{
  PyObject *pyresult=NULL, *utf8=NULL;
  int result=SQLITE_OK;

  VFSPREAMBLE;

  pyresult=Call_PythonMethodV((PyObject*)(vfs->pAppData), "xFullPathname", 1, "(N)", convertutf8string(zName));
  if(!pyresult)
    {
      result=MakeSqliteMsgFromPyException(NULL);
      AddTraceBackHere(__FILE__, __LINE__, "vfs.xFullPathname", "{s: s, s: i}", "zName", zName, "nOut", nOut);
    }
  else
    {
      utf8=getutf8string(pyresult);
      if(!utf8)
        {
          result=SQLITE_ERROR;
          AddTraceBackHere(__FILE__, __LINE__, "vfs.xFullPathname", "{s: s, s: O}", "zName", zName, "result_from_python", pyresult);
          goto finally;
        }
      /* nOut includes null terminator space (ie is mxPathname+1) */
      if(PyBytes_GET_SIZE(utf8)+1>nOut)
        {
          result=SQLITE_TOOBIG;
          SET_EXC(result, NULL);
          AddTraceBackHere(__FILE__, __LINE__, "vfs.xFullPathname", "{s: s, s: O, s: i}", "zName", zName, "result_from_python", utf8, "nOut", nOut);
          goto finally;
        }
      memcpy(zOut, PyBytes_AS_STRING(utf8), PyBytes_GET_SIZE(utf8)+1); /* Python always null terminates hence +1 */
    }

 finally:
  Py_XDECREF(utf8);
  Py_XDECREF(pyresult);

  VFSPOSTAMBLE;
  return result;
}

static PyObject *
apswvfspy_xFullPathname(APSWVFS *self, PyObject *name)
{
  char *resbuf=NULL;
  PyObject *result=NULL, *utf8=NULL;
  int res=SQLITE_NOMEM;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xFullPathname);

  utf8=getutf8string(name);
  if(!utf8)
    {
      AddTraceBackHere(__FILE__, __LINE__, "vfspy.xFullPathname", "{s: O}", "name", name);
      goto finally;
    }

  resbuf=PyMem_Malloc(self->basevfs->mxPathname+1);
  memset(resbuf, 0, self->basevfs->mxPathname+1); /* make sure it is null terminated */
  if(resbuf)
    res=self->basevfs->xFullPathname(self->basevfs, PyBytes_AsString(utf8), self->basevfs->mxPathname+1, resbuf);

  if(res==SQLITE_OK)
    APSW_FAULT_INJECT(xFullPathnameConversion,result=convertutf8string(resbuf),result=PyErr_NoMemory());

  if(!result)
    res=SQLITE_CANTOPEN;

  if(res!=SQLITE_OK)
    {
      SET_EXC(res, NULL);
      AddTraceBackHere(__FILE__, __LINE__, "vfspy.xFullPathname", "{s: O, s: i, s: O}", "name", name, "res", res, "result", result?result:Py_None);
    }

 finally:
  Py_XDECREF(utf8);
  if(resbuf) PyMem_Free(resbuf);
  
  return result;
}

static int
apswvfs_xOpen(sqlite3_vfs *vfs, const char *zName, sqlite3_file *file, int inflags, int *pOutFlags)
{
  int result=SQLITE_CANTOPEN;
  PyObject *flags=NULL;
  PyObject *pyresult=NULL;
  APSWSQLite3File *apswfile=(APSWSQLite3File*)(void*)file;

  VFSPREAMBLE;

  flags=PyList_New(2);
  if(!flags) goto finally;

  PyList_SET_ITEM(flags, 0, PyInt_FromLong(inflags));
  PyList_SET_ITEM(flags, 1, PyInt_FromLong(pOutFlags?*pOutFlags:0));
  if(PyErr_Occurred()) goto finally;

  pyresult=Call_PythonMethodV((PyObject*)(vfs->pAppData), "xOpen", 1, "(NO)", convertutf8string(zName), flags);
  if(!pyresult)
    {
      result=MakeSqliteMsgFromPyException(NULL);
      goto finally;
    }

  if(!PyList_Check(flags) || PyList_GET_SIZE(flags)!=2 || !PyIntLong_Check(PyList_GET_ITEM(flags, 1)))
    {
      PyErr_Format(PyExc_TypeError, "Flags should be two item list with item zero being integer input and item one being integer output");
      AddTraceBackHere(__FILE__, __LINE__, "vfs.xOpen", "{s: s, s: i, s: i}", "zName", zName, "inflags", inflags, "flags", flags);
      goto finally;
    }

  if(pOutFlags)
    *pOutFlags=(int)PyIntLong_AsLong(PyList_GET_ITEM(flags, 1));
  if(PyErr_Occurred()) goto finally;

  apswfile->pMethods=&apsw_io_methods;
  apswfile->file=pyresult;
  pyresult=NULL;
  result=SQLITE_OK;

 finally:
  assert(PyErr_Occurred()?result!=SQLITE_OK:1);
  Py_XDECREF(pyresult);
  Py_XDECREF(flags);

  VFSPOSTAMBLE;

  return result;
}

static PyObject *
apswvfspy_xOpen(APSWVFS *self, PyObject *args)
{
  sqlite3_file *file=NULL;
  int flagsout=0;
  int flagsin=0;
  int res;
  PyObject *result=NULL, *flags;
  PyObject *pyname=NULL, *utf8name=NULL;
  APSWVFSFile *apswfile=NULL;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xOpen);

  if(!PyArg_ParseTuple(args, "OO", &pyname, &flags))
    return NULL;

  if(pyname==Py_None)
    {
      utf8name=Py_None;
      Py_INCREF(Py_None);
    }
  else
    utf8name=getutf8string(pyname);
  if(!utf8name) 
    goto finally;


  if(!PyList_Check(flags) || PyList_GET_SIZE(flags)!=2 || !PyIntLong_Check(PyList_GET_ITEM(flags, 0)) || !PyIntLong_Check(PyList_GET_ITEM(flags, 1)))
    {
      PyErr_Format(PyExc_TypeError, "Flags argument needs to be a list of two integers");
      goto finally;
    }
  
  flagsout=PyIntLong_AsLong(PyList_GET_ITEM(flags, 1));
  flagsin=PyIntLong_AsLong(PyList_GET_ITEM(flags, 0));
  /* check for overflow */
  if(flagsout!=PyIntLong_AsLong(PyList_GET_ITEM(flags, 1)) || flagsin!=PyIntLong_AsLong(PyList_GET_ITEM(flags, 0)))
    PyErr_Format(PyExc_OverflowError, "Flags arguments need to fit in 32 bits");
  if(PyErr_Occurred()) goto finally;

  file=PyMem_Malloc(self->basevfs->szOsFile);
  if(!file) goto finally;

  res=self->basevfs->xOpen(self->basevfs, (utf8name==Py_None)?NULL:PyBytes_AS_STRING(utf8name), file, flagsin, &flagsout);
  if(PyErr_Occurred()) goto finally;
  if(res!=SQLITE_OK)
    {
      SET_EXC(res, NULL);
      goto finally;
    }

  PyList_SetItem(flags, 1, PyInt_FromLong(flagsout));
  if(PyErr_Occurred()) goto finally;

  apswfile=PyObject_New(APSWVFSFile, &APSWVFSFileType);
  if(!apswfile) goto finally;

  apswfile->base=file;
  file=NULL;
  result=(PyObject*)(void*)apswfile;
                                                                       
 finally:
  if(file) PyMem_Free(file);
  Py_XDECREF(utf8name);
  return result;
}

static void*
apswvfs_xDlOpen(sqlite3_vfs *vfs, const char *zName)
{
  PyObject *pyresult=NULL;
  void *result=NULL;

  VFSPREAMBLE;

  pyresult=Call_PythonMethodV((PyObject*)(vfs->pAppData), "xDlOpen", 1, "(N)", convertutf8string(zName));
  if(pyresult)
    {
      if(PyIntLong_Check(pyresult))
        result=PyLong_AsVoidPtr(pyresult);
      else
        PyErr_Format(PyExc_TypeError, "Pointer returned must be int/long");
    }
  if(PyErr_Occurred())
    {
      result=NULL;
      AddTraceBackHere(__FILE__, __LINE__, "vfs.xDlOpen", "{s: s, s: O}", "zName", zName, "result", pyresult?pyresult:Py_None);
    }

  Py_XDECREF(pyresult);
  VFSPOSTAMBLE;
  return result;
}

static PyObject *
apswvfspy_xDlOpen(APSWVFS *self, PyObject *args)
{
  char *zName=NULL;
  void *res;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xDlOpen);

  if(!PyArg_ParseTuple(args, "es", STRENCODING, &zName))
    return NULL;

  res=self->basevfs->xDlOpen(self->basevfs, zName);
  PyMem_Free(zName);

  return PyLong_FromVoidPtr(res);
}

static void*
apswvfs_xDlSym(sqlite3_vfs *vfs, void *handle, const char *zName)
{
  PyObject *pyresult=NULL;
  void *result=NULL;
  
  VFSPREAMBLE;

  pyresult=Call_PythonMethodV((PyObject*)(vfs->pAppData), "xDlSym", 1, "(NN)", PyLong_FromVoidPtr(handle), convertutf8string(zName));
  if(pyresult)
    {
      if(PyIntLong_Check(pyresult))
        result=PyLong_AsVoidPtr(pyresult);
      else
        PyErr_Format(PyExc_TypeError, "Pointer returned must be int/long");
    }
  if(PyErr_Occurred())
    {
      result=NULL;
      AddTraceBackHere(__FILE__, __LINE__, "vfs.xDlSym", "{s: s, s: O}", "zName", zName, "result", pyresult?pyresult:Py_None);
    }

  Py_XDECREF(pyresult);
  VFSPOSTAMBLE;
  return result;
}

static PyObject *
apswvfspy_xDlSym(APSWVFS *self, PyObject *args)
{
  char *zName=NULL;
  void *res=NULL;
  PyObject *pyptr;
  void *ptr=NULL;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xDlSym);

  if(!PyArg_ParseTuple(args, "Oes", &pyptr, STRENCODING, &zName))
    return NULL;

  if(PyIntLong_Check(pyptr))
    ptr=PyLong_AsVoidPtr(pyptr);
  else
    PyErr_Format(PyExc_TypeError, "Pointer must be int/long");

  if(PyErr_Occurred())
    goto finally;

  res=self->basevfs->xDlSym(self->basevfs, ptr, zName);

 finally:
  PyMem_Free(zName);

  if(PyErr_Occurred())
    {
      AddTraceBackHere(__FILE__, __LINE__, "vfspy.xDlSym", "{s: O}", "args", args);
      return NULL;
    }

  return PyLong_FromVoidPtr(res);
}

static void
apswvfs_xDlClose(sqlite3_vfs *vfs, void *handle)
{
  PyObject *pyresult=NULL;
  VFSPREAMBLE;

  pyresult=Call_PythonMethodV((PyObject*)(vfs->pAppData), "xDlClose", 1, "(N)", PyLong_FromVoidPtr(handle));

  if(PyErr_Occurred())
      AddTraceBackHere(__FILE__, __LINE__, "vfs.xDlClose", "{s: N}", "ptr", PyLong_FromVoidPtr(handle));

  Py_XDECREF(pyresult);
  VFSPOSTAMBLE;
}

static PyObject *
apswvfspy_xDlClose(APSWVFS *self, PyObject *pyptr)
{
  void *ptr=NULL;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xDlClose);

  if(PyIntLong_Check(pyptr))
    ptr=PyLong_AsVoidPtr(pyptr);
  else
    PyErr_Format(PyExc_TypeError, "Argument is not number (pointer)");

  if(PyErr_Occurred())
    goto finally;

  self->basevfs->xDlClose(self->basevfs, ptr);

 finally:

  if(PyErr_Occurred())
    {
      AddTraceBackHere(__FILE__, __LINE__, "vfspy.xDlClose", "{s: O}", "ptr", pyptr);
      return NULL;
    }

  Py_RETURN_NONE;
}

static void
apswvfs_xDlError(sqlite3_vfs *vfs, int nByte, char *zErrMsg)
{
  PyObject *pyresult=NULL, *utf8=NULL;
  VFSPREAMBLE;

  pyresult=Call_PythonMethodV((PyObject*)(vfs->pAppData), "xDlError", 0, "()");

  if(pyresult && pyresult!=Py_None)
    {
      utf8=getutf8string(pyresult);
      if(utf8)
        {
          /* Get size includes trailing null */
          size_t len=PyBytes_GET_SIZE(utf8);
          if(len>(size_t)nByte) len=(size_t)nByte;
          memcpy(zErrMsg, PyBytes_AS_STRING(utf8), len);
        }

    }

  if(PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "vfs.xDlError", NULL);

  Py_XDECREF(pyresult);
  Py_XDECREF(utf8);
  VFSPOSTAMBLE;
}

static PyObject *
apswvfspy_xDlError(APSWVFS *self)
{
  PyObject *res=NULL;
  PyObject *unicode=NULL;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xDlError);

  APSW_FAULT_INJECT(xDlErrorAllocFail,
                    res=PyBytes_FromStringAndSize(NULL, 512+self->basevfs->mxPathname),
                    res=PyErr_NoMemory());
  if(res)
    {
      memset(PyBytes_AS_STRING(res), 0, PyBytes_GET_SIZE(res));
      self->basevfs->xDlError(self->basevfs, PyBytes_GET_SIZE(res), PyBytes_AS_STRING(res));
    }

  if(PyErr_Occurred())
    {
      AddTraceBackHere(__FILE__, __LINE__, "vfspy.xDlError", NULL);
      Py_XDECREF(res);
      return NULL;
    }

  /* did they make a message? */
  if(strlen(PyBytes_AS_STRING(res))==0)
    {
      Py_DECREF(res);
      Py_RETURN_NONE;
    }

  /* turn into unicode */
  APSW_FAULT_INJECT(xDlErrorUnicodeFail,
                    unicode=convertutf8string(PyBytes_AS_STRING(res)),
                    unicode=PyErr_NoMemory());
  if(unicode)
    {
      Py_DECREF(res);
      return unicode;
    }

  AddTraceBackHere(__FILE__, __LINE__, "vfspy.xDlError", "{s: O, s: N}", "self", self, "res", PyBytes_FromStringAndSize(PyBytes_AS_STRING(res), strlen(PyBytes_AS_STRING(res))));
  Py_DECREF(res);
  return NULL;
}

static int
apswvfs_xRandomness(sqlite3_vfs *vfs, int nByte, char *zOut)
{
  PyObject *pyresult=NULL;
  int result=0;
  VFSPREAMBLE;

  pyresult=Call_PythonMethodV((PyObject*)(vfs->pAppData), "xRandomness", 1, "(i)", nByte);

  if(pyresult && PyUnicode_Check(pyresult))
    PyErr_Format(PyExc_TypeError, "Randomness object must be data/bytes not unicode");
  else if(pyresult && pyresult!=Py_None)
    {
      const void *buffer;
      Py_ssize_t buflen;
      int asrb=PyObject_AsReadBuffer(pyresult, &buffer, &buflen);
      if(asrb==0)
        {
          if(buflen>nByte)
            buflen=nByte;
          memcpy(zOut, buffer, buflen);
          result=buflen;
        }
      else
        assert(PyErr_Occurred());
    }

  if(PyErr_Occurred())
      AddTraceBackHere(__FILE__, __LINE__, "vfs.xRandomness", "{s: i, s: O}", "nByte", nByte, "result", pyresult?pyresult:Py_None);

  Py_XDECREF(pyresult);
  VFSPOSTAMBLE;
  return result;
}

static PyObject *
apswvfspy_xRandomness(APSWVFS *self, PyObject *args)
{
  PyObject *res=NULL;
  int nbyte=0;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xRandomness);

  if(!PyArg_ParseTuple(args, "i", &nbyte))
    return NULL;

  if(nbyte<0)
    {
      PyErr_Format(PyExc_ValueError, "You can't have negative amounts of randomness!");
      return NULL;
    }

  APSW_FAULT_INJECT(xRandomnessAllocFail,
                    res=PyBytes_FromStringAndSize(NULL, nbyte),
                    res=PyErr_NoMemory());
  if(res)
    {
      int amt=self->basevfs->xRandomness(self->basevfs, PyBytes_GET_SIZE(res), PyBytes_AS_STRING(res));
      if(amt<nbyte)
        _PyBytes_Resize(&res, amt);
    }

  if(PyErr_Occurred())
    {
      AddTraceBackHere(__FILE__, __LINE__, "vfspy.xRandomness", "{s: i}", "nbyte", nbyte);
      Py_XDECREF(res);
      return NULL;
    }

  return res;
}

/* return the number of microseconds that the underlying OS was requested to sleep for. */
static int
apswvfs_xSleep(sqlite3_vfs *vfs, int microseconds)
{
  PyObject *pyresult=NULL;
  int result=0;

  VFSPREAMBLE;

  pyresult=Call_PythonMethodV((PyObject*)(vfs->pAppData), "xSleep", 1, "(i)", microseconds);

  if(pyresult)
    {
      if(PyIntLong_Check(pyresult))
        {
          long actual=PyIntLong_AsLong(pyresult);
          if(actual!=(int)actual)
            PyErr_Format(PyExc_OverflowError, "Result is too big for integer");
          result=actual;
        }
      else
        PyErr_Format(PyExc_TypeError, "You should return a number from sleep");
    }

  if(PyErr_Occurred())
      AddTraceBackHere(__FILE__, __LINE__, "vfs.xSleep", "{s: i, s: O}", "microseconds", microseconds, "result", pyresult?pyresult:Py_None);

  Py_XDECREF(pyresult);
  VFSPOSTAMBLE;
  return result;
}

static PyObject *
apswvfspy_xSleep(APSWVFS *self, PyObject *args)
{
  int microseconds=0;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xSleep);

  if(!PyArg_ParseTuple(args, "i", &microseconds))
    return NULL;

  return PyLong_FromLong(self->basevfs->xSleep(self->basevfs, microseconds));
}

/* See http://www.sqlite.org/cvstrac/tktview?tn=3394 for SQLite implementation issues */
static int
apswvfs_xCurrentTime(sqlite3_vfs *vfs, double *julian)
{
  PyObject *pyresult=NULL;
  /* note returns zero or one.  Details in sqlite ticket 3394*/
  int result=0; 
  VFSPREAMBLE;

  pyresult=Call_PythonMethodV((PyObject*)(vfs->pAppData), "xCurrentTime", 1, "()");

  if(pyresult)
    *julian=PyFloat_AsDouble(pyresult);

  if(PyErr_Occurred())
    {
      AddTraceBackHere(__FILE__, __LINE__, "vfs.xCurrentTime", "{s: O}", "result", pyresult?pyresult:Py_None);
      result=1;
    }

  Py_XDECREF(pyresult);
  VFSPOSTAMBLE;
  return result;
}

static PyObject *
apswvfspy_xCurrentTime(APSWVFS *self)
{
  int res;
  double julian=0;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xCurrentTime);

  res=self->basevfs->xCurrentTime(self->basevfs, &julian);

  APSW_FAULT_INJECT(xCurrentTimeFail, ,res=1);

  if(res!=0)
    {
      /* routines are documented to return zero or one - see ticket 3394 info above */
      SET_EXC(SQLITE_ERROR, NULL);   /* general sqlite error code */
      AddTraceBackHere(__FILE__, __LINE__, "vfspy.xCurrentTime", NULL);
      return NULL;
    }

  return PyFloat_FromDouble(julian);
}

static int
apswvfs_xGetLastError(sqlite3_vfs *vfs, int nByte, char *zErrMsg)
{
  PyObject *pyresult=NULL, *utf8=NULL;
  int buffertoosmall=0;

  VFSPREAMBLE;

  pyresult=Call_PythonMethodV((PyObject*)(vfs->pAppData), "xGetLastError", 0, "()");

  if(pyresult && pyresult!=Py_None)
    {
      utf8=getutf8string(pyresult);
      if(utf8)
        {
          /* Get size includes trailing null */
          size_t len=PyBytes_GET_SIZE(utf8);
          if(len>(size_t)nByte)
            {
              len=(size_t)nByte;
              buffertoosmall=1;
            }
          memcpy(zErrMsg, PyBytes_AS_STRING(utf8), len);
        }

    }

  if(PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "vfs.xGetLastError", NULL);

  Py_XDECREF(pyresult);
  Py_XDECREF(utf8);
  VFSPOSTAMBLE;
  return buffertoosmall;
}

static PyObject *
apswvfspy_xGetLastError(APSWVFS *self)
{
  PyObject *res=NULL;
  int toobig=1;
  Py_ssize_t size=256; /* start small */

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xGetLastError);

  res=PyBytes_FromStringAndSize(NULL, size);
  if(!res) goto error;
  while(toobig)
    {
      int resizeresult;

      memset(PyBytes_AS_STRING(res), 0, PyBytes_GET_SIZE(res));
      toobig=self->basevfs->xGetLastError(self->basevfs, PyBytes_GET_SIZE(res), PyBytes_AS_STRING(res));
      if(!toobig)
        break;
      size*=2; /* double size and try again */
      APSW_FAULT_INJECT(xGetLastErrorAllocFail,
                        resizeresult=_PyBytes_Resize(&res, size),
                        resizeresult=(PyErr_NoMemory(), -1));
      if(resizeresult!=0)
        goto error;
    }

  /* did they make a message? */
  if(strlen(PyBytes_AS_STRING(res))==0)
    {
      Py_XDECREF(res);
      Py_RETURN_NONE;
    }

  _PyBytes_Resize(&res, strlen(PyBytes_AS_STRING(res)));
  return res;

 error:
  assert(PyErr_Occurred());
  AddTraceBackHere(__FILE__, __LINE__, "vfspy.xGetLastError", "{s: O, s: i}", "self", self, "size", (int)size);
  Py_XDECREF(res);
  return NULL;
}

static PyObject *
apswvfspy_unregister(APSWVFS *self)
{
  int res;

  CHECKVFSPY;

  if(self->registered)
    {
      /* although it is undocumented by sqlite, we assume that an
         unregister failure always results in an unregister and so
         continue freeing the data structures.  we memset everything
         to zero so there will be a coredump should this behaviour
         change.  as of 3.6.3 the sqlite code doesn't return
         anything except ok anyway. */
      res=sqlite3_vfs_unregister(self->containingvfs);
      self->registered=0;
      APSW_FAULT_INJECT(APSWVFSDeallocFail, ,res=SQLITE_IOERR);

      SET_EXC(res, NULL);
      if(res!=SQLITE_OK)
        return NULL;
    }
  Py_RETURN_NONE;
}

static void
APSWVFS_dealloc(APSWVFS *self)
{
  if(self->basevfs && self->basevfs->xAccess==apswvfs_xAccess)
    {
      Py_DECREF((PyObject*)self->basevfs->pAppData);
    }

  if(self->containingvfs)
    {
      PyObject *xx;

      /* not allowed to clobber existing exception */
      PyObject *etype=NULL, *evalue=NULL, *etraceback=NULL;
      PyErr_Fetch(&etype, &evalue, &etraceback);
        
      xx=apswvfspy_unregister(self);
      Py_XDECREF(xx);

      if(PyErr_Occurred())
        apsw_write_unraiseable(NULL);
      PyErr_Restore(etype, evalue, etraceback);

      /* some cleanups */
      self->containingvfs->pAppData=NULL;
      PyMem_Free((void*)(self->containingvfs->zName));
      /* zero it out so any attempt to use results in core dump */
      memset(self->containingvfs, 0, sizeof(sqlite3_vfs));
      PyMem_Free(self->containingvfs);
      self->containingvfs=NULL;

    }
  
  self->basevfs=self->containingvfs=NULL;

  Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject *
APSWVFS_new(PyTypeObject *type, APSW_ARGUNUSED PyObject *args, APSW_ARGUNUSED PyObject *kwds)
{
  APSWVFS *self;
  self= (APSWVFS*)type->tp_alloc(type, 0);
  if(self)
    {
      self->basevfs=NULL;
      self->containingvfs=NULL;
      self->registered=0;
    }
  return (PyObject*)self;
}

static int
APSWVFS_init(APSWVFS *self, PyObject *args, PyObject *kwds)
{
  static char *kwlist[]={"name", "base", "makedefault", "maxpathname", NULL};
  char *base=NULL, *name=NULL;
  int makedefault=0, maxpathname=0, res;

  if(!PyArg_ParseTupleAndKeywords(args, kwds, "es|esii:init(name, base=None, makedefault=False, maxpathname=1024)", kwlist,
                                  STRENCODING, &name, STRENCODING, &base, &makedefault, &maxpathname))
    return -1;

  if(base)
    {
      int baseversion;
      if(!strlen(base))
        {
          PyMem_Free(base);
          base=NULL;
        }
      self->basevfs=sqlite3_vfs_find(base);
      if(!self->basevfs)
        {
          PyErr_Format(PyExc_ValueError, "Base vfs named \"%s\" not found", base?base:"<default>");
          goto error;
        }
      baseversion=self->basevfs->iVersion;
      APSW_FAULT_INJECT(APSWVFSBadVersion, , baseversion=-789426);
      if(baseversion!=1)
        {
          PyErr_Format(PyExc_ValueError, "Base vfs implements version %d of vfs spec, but apsw only supports version 1", baseversion);
          goto error;
        }
      if(base) PyMem_Free(base);
    }
  
  self->containingvfs=(sqlite3_vfs *)PyMem_Malloc(sizeof(sqlite3_vfs));
  if(!self->containingvfs) return -1;
  memset(self->containingvfs, 0, sizeof(sqlite3_vfs)); 
  self->containingvfs->iVersion=1;
  self->containingvfs->szOsFile=sizeof(APSWSQLite3File);
  if(self->basevfs && !maxpathname)
    self->containingvfs->mxPathname=self->basevfs->mxPathname;
  else 
    self->containingvfs->mxPathname=maxpathname?maxpathname:1024;
  self->containingvfs->zName=name;
  name=NULL;
  self->containingvfs->pAppData=self;
#define METHOD(meth) \
  self->containingvfs->x##meth=apswvfs_x##meth;

  METHOD(Delete);
  METHOD(FullPathname);
  METHOD(Open);
  METHOD(Access);
  METHOD(DlOpen);
  METHOD(DlSym);
  METHOD(DlClose);
  METHOD(DlError);
  METHOD(Randomness);
  METHOD(Sleep);
  METHOD(CurrentTime);
  METHOD(GetLastError);
#undef METHOD
  /* not implemented in SQLite anyway */


  APSW_FAULT_INJECT(APSWVFSRegistrationFails,
                    res=sqlite3_vfs_register(self->containingvfs, makedefault),
                    res=SQLITE_NOMEM);

  if(res==SQLITE_OK)
    {
      self->registered=1;
      if(self->basevfs && self->basevfs->xAccess==apswvfs_xAccess)
        {
          Py_INCREF((PyObject*)self->basevfs->pAppData);
        }
      return 0;
    }

  SET_EXC(res, NULL);
    
 error:
  if(name) PyMem_Free(name);
  if(base) PyMem_Free(base);
  if(self->containingvfs && self->containingvfs->zName) PyMem_Free((void*)(self->containingvfs->zName));
  if(self->containingvfs) PyMem_Free(self->containingvfs);
  self->containingvfs=NULL;
  return -1;
}

static PyMethodDef APSWVFS_methods[]={
  {"xDelete", (PyCFunction)apswvfspy_xDelete, METH_VARARGS, "xDelete"},
  {"xFullPathname", (PyCFunction)apswvfspy_xFullPathname, METH_O, "xFullPathname"},
  {"xOpen", (PyCFunction)apswvfspy_xOpen, METH_VARARGS, "xOpen"},
  {"xAccess", (PyCFunction)apswvfspy_xAccess, METH_VARARGS, "xAccess"},
  {"xDlOpen", (PyCFunction)apswvfspy_xDlOpen, METH_VARARGS, "xDlOpen"},
  {"xDlSym", (PyCFunction)apswvfspy_xDlSym, METH_VARARGS, "xDlSym"},
  {"xDlClose", (PyCFunction)apswvfspy_xDlClose, METH_O, "xDlClose"},
  {"xDlError", (PyCFunction)apswvfspy_xDlError, METH_NOARGS, "xDlError"},
  {"xRandomness", (PyCFunction)apswvfspy_xRandomness, METH_VARARGS, "xRandomness"},
  {"xSleep", (PyCFunction)apswvfspy_xSleep, METH_VARARGS, "xSleep"},
  {"xCurrentTime", (PyCFunction)apswvfspy_xCurrentTime, METH_NOARGS, "xCurrentTime"},
  {"xGetLastError", (PyCFunction)apswvfspy_xGetLastError, METH_NOARGS, "xGetLastError"},
  {"unregister", (PyCFunction)apswvfspy_unregister, METH_NOARGS, "Unregisters the vfs"},
  {"excepthook", (PyCFunction)apswvfs_excepthook, METH_VARARGS, "Exception hook"},
  /* Sentinel */
  {0, 0, 0, 0}
  };

static PyTypeObject APSWVFSType =
  {
#if PY_VERSION_HEX<0x03000000
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
#else
    PyVarObject_HEAD_INIT(NULL,0)
#endif
    "apsw.VFS",                /*tp_name*/
    sizeof(APSWVFS),           /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)APSWVFS_dealloc, /*tp_dealloc*/ 
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
    "VFS object",              /* tp_doc */
    0,		               /* tp_traverse */
    0,		               /* tp_clear */
    0,		               /* tp_richcompare */
    0,		               /* tp_weaklistoffset */
    0,		               /* tp_iter */
    0,		               /* tp_iternext */
    APSWVFS_methods,           /* tp_methods */
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)APSWVFS_init,    /* tp_init */
    0,                         /* tp_alloc */
    APSWVFS_new,               /* tp_new */
    0,                         /* tp_free */
    0,                         /* tp_is_gc */
    0,                         /* tp_bases */
    0,                         /* tp_mro */
    0,                         /* tp_cache */
    0,                         /* tp_subclasses */
    0,                         /* tp_weaklist */
    0,                         /* tp_del */
#if PY_VERSION_HEX>=0x03000000
    0,                         /* tp_version */
#endif
  };

static PyObject *apswvfsfilepy_xClose(APSWVFSFile *self);

static void
APSWVFSFile_dealloc(APSWVFSFile *self)
{
  PyObject *a,*b,*c;

  PyErr_Fetch(&a, &b, &c);

  if(self->base)
    {
      /* close it */
      PyObject *x=apswvfsfilepy_xClose(self);
      Py_XDECREF(x);
    }
  if(PyErr_Occurred())
    {
      AddTraceBackHere(__FILE__, __LINE__, "APSWVFS File destructor", NULL);
      apsw_write_unraiseable(NULL);
    }
  Py_TYPE(self)->tp_free((PyObject*)self);

  PyErr_Restore(a,b,c);
}

/*ARGSUSED*/
static PyObject *
APSWVFSFile_new(PyTypeObject *type, APSW_ARGUNUSED PyObject *args, APSW_ARGUNUSED PyObject *kwds)
{
  APSWVFSFile *self;
  self= (APSWVFSFile*)type->tp_alloc(type, 0);
  if(self)
    self->base=NULL;

  return (PyObject*)self;
}

static int
APSWVFSFile_init(APSWVFSFile *self, PyObject *args, PyObject *kwds)
{
  static char *kwlist[]={"vfs", "name", "flags", NULL};
  char *vfs=NULL;
  PyObject *flags=NULL, *pyname=NULL, *utf8name=NULL;
  int xopenresult;
  int flagsout=0;
  int res=-1; /* error */

  PyObject *itemzero=NULL, *itemone=NULL, *zero=NULL, *pyflagsout=NULL;
  sqlite3_vfs *vfstouse=NULL;
  sqlite3_file *file;

  if(!PyArg_ParseTupleAndKeywords(args, kwds, "esOO:init(vfs, name, flags)", kwlist, STRENCODING, &vfs, &pyname, &flags))
    return -1;

  if(pyname==Py_None)
    {
      utf8name=Py_None;
      Py_INCREF(utf8name);
    }
  else
    utf8name=getutf8string(pyname);

  if(!utf8name) goto finally;

  /* type checking */
  if(strlen(vfs)==0)
    {
      /* sqlite uses null for default vfs - we use empty string */
      PyMem_Free(vfs);
      vfs=NULL;
    }
  /* flags need to be a list of two integers */
  if(!PySequence_Check(flags) || PySequence_Size(flags)!=2)
    {
      PyErr_Format(PyExc_TypeError, "Flags should be a sequence of two integers");
      goto finally;
    }
  itemzero=PySequence_GetItem(flags, 0);
  itemone=PySequence_GetItem(flags, 1);
  if(!itemzero || !itemone || !PyIntLong_Check(itemzero) || !PyIntLong_Check(itemone))
    {
      PyErr_Format(PyExc_TypeError, "Flags should contain two integers");
      goto finally;
    }
  /* check we can change item 1 */
  zero=PyInt_FromLong(0);
  if(!zero) goto finally;
  if(-1==PySequence_SetItem(flags, 1, zero))
    goto finally;
  
  vfstouse=sqlite3_vfs_find(vfs);
  if(!vfstouse)
    {
      PyErr_Format(PyExc_ValueError, "Unknown vfs \"%s\"", vfs);
      goto finally;
    }
  file=PyMem_Malloc(vfstouse->szOsFile);
  if(!file) goto finally;
  xopenresult=vfstouse->xOpen(vfstouse, (utf8name==Py_None)?NULL:PyBytes_AS_STRING(utf8name), file, (int)PyIntLong_AsLong(itemzero), &flagsout);
  SET_EXC(xopenresult, NULL);
  if(PyErr_Occurred())
    {
      /* just in case the result was ok, but there was a python level exception ... */
      if(xopenresult==SQLITE_OK) file->pMethods->xClose(file);
      PyMem_Free(file);
      goto finally;
    }
  
  pyflagsout=PyInt_FromLong(flagsout);
  
  if(-1==PySequence_SetItem(flags, 1, pyflagsout))
    {
      file->pMethods->xClose(file);
      PyMem_Free(file);
      goto finally;
    }
  
  if(PyErr_Occurred()) goto finally;
  
  self->base=(sqlite3_file*)(void*)file;
  res=0;

 finally:
  assert(res==0 || PyErr_Occurred());
  if(PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "vfsfile.init", "{s: O, s: O}", "args", args, "kwargs", kwds);

  Py_XDECREF(pyflagsout);
  Py_XDECREF(itemzero);
  Py_XDECREF(itemone);
  Py_XDECREF(zero);
  Py_XDECREF(utf8name);
  if(vfs) PyMem_Free(vfs);
  return res;
}

static int
apswvfsfile_xRead(sqlite3_file *file, void *bufout, int amount, sqlite3_int64 offset)
{
  int result=SQLITE_ERROR;
  PyObject *pybuf=NULL;
  int asrb;
  Py_ssize_t size;
  const void *buffer;
  
  FILEPREAMBLE;

  pybuf=Call_PythonMethodV(apswfile->file, "xRead", 1, "(iL)", amount, offset);
  if(!pybuf)
    {
      assert(PyErr_Occurred());
      result=MakeSqliteMsgFromPyException(NULL);
      goto finally;
    }
  if(pybuf==Py_None)
    {
      result=SQLITE_IOERR_SHORT_READ;
      goto finally;
    }
  if(PyUnicode_Check(pybuf) || !PyObject_CheckReadBuffer(pybuf))
    {
      PyErr_Format(PyExc_TypeError, "Object returned from xRead should be bytes/buffer/string");
      goto finally;
    }
  asrb=PyObject_AsReadBuffer(pybuf, &buffer, &size);
  
  APSW_FAULT_INJECT(xReadReadBufferFail,,(PyErr_NoMemory(),asrb=-1));

  if(asrb!=0)
    {
      PyErr_Format(PyExc_TypeError, "Object returned from xRead doesn't do read buffer");
      goto finally;
    }

  if(size<amount)
      result=SQLITE_IOERR_SHORT_READ;
  else
    {
      memcpy(bufout, buffer, amount);
      result=SQLITE_OK;
    }
  
 finally:
  if(PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile_xRead", "{s: i, s: L, s: O}", "amount", amount, "offset", offset, "result", pybuf);

  Py_XDECREF(pybuf);
  FILEPOSTAMBLE;
  return result;
}

static PyObject *
apswvfsfilepy_xRead(APSWVFSFile *self, PyObject *args)
{
  int amount;
  sqlite3_int64 offset;
  int res;
  PyObject *buffy=NULL;

  CHECKVFSFILEPY;
  VFSFILENOTIMPLEMENTED(xRead);

  if(!PyArg_ParseTuple(args, "iL", &amount, &offset))
    {
      assert(PyErr_Occurred());
      return NULL;
    }
  
  buffy=PyBytes_FromStringAndSize(NULL, amount);
  if(!buffy) return NULL;

  res=self->base->pMethods->xRead(self->base, PyBytes_AS_STRING(buffy), amount, offset);

  if(res==SQLITE_OK)
    return buffy;
    
  Py_DECREF(buffy);

  if(res==SQLITE_IOERR_SHORT_READ)
    Py_RETURN_NONE;

  SET_EXC(res, NULL);
  return NULL;
}

static int
apswvfsfile_xWrite(sqlite3_file *file, const void *buffer, int amount, sqlite3_int64 offset)
{
  PyObject *pyresult=NULL, *pybuf=NULL;
  int result=SQLITE_OK;
  FILEPREAMBLE;

  /* I could instead use PyBuffer_New here which avoids duplicating
     the memory.  But if the developer keeps a reference on it then
     the underlying memory goes away on return of this function and
     all hell would break lose on next access.  It is very unlikely
     someone would hang on to them but I'd rather there not be any
     possibility of problems.  In any event the data sizes are usually
     very small - typically the SQLite default page size of 1kb */
  pybuf=PyBytes_FromStringAndSize(buffer, amount);
  if(!pybuf) goto finally;

  pyresult=Call_PythonMethodV(apswfile->file, "xWrite", 1, "(OL)", pybuf, offset);
  
 finally:
  if(PyErr_Occurred())
    {
      result=MakeSqliteMsgFromPyException(NULL);
      AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile_xWrite", "{s: i, s: L, s: O}", "amount", amount, "offset", offset, "data", pybuf?pybuf:Py_None);
    }
  Py_XDECREF(pybuf);
  Py_XDECREF(pyresult);
  FILEPOSTAMBLE;
  return result;
}

static PyObject *
apswvfsfilepy_xWrite(APSWVFSFile *self, PyObject *args)
{
  sqlite3_int64 offset;
  int res;
  PyObject *buffy=NULL;
  const void *buffer;
  Py_ssize_t size;
  int asrb;

  CHECKVFSFILEPY;
  VFSFILENOTIMPLEMENTED(xWrite);

  if(!PyArg_ParseTuple(args, "OL", &buffy, &offset))
    {
      assert(PyErr_Occurred());
      return NULL;
    }
  
  asrb=PyObject_AsReadBuffer(buffy, &buffer, &size);
  if(asrb!=0 || PyUnicode_Check(buffy))
    {
      PyErr_Format(PyExc_TypeError, "Object passed to xWrite doesn't do read buffer");
      AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile_xWrite", "{s: L, s: O}", "offset", offset, "buffer", buffy);
      return NULL;
    }

  res=self->base->pMethods->xWrite(self->base, buffer, size, offset);

  if(res==SQLITE_OK)
    Py_RETURN_NONE;
    
  SET_EXC(res, NULL);
  return NULL;
}

static int
apswvfsfile_xUnlock(sqlite3_file *file, int flag)
{
  int result=SQLITE_ERROR;
  PyObject *pyresult=NULL;
  FILEPREAMBLE;

  pyresult=Call_PythonMethodV(apswfile->file, "xUnlock", 1, "(i)", flag);
  if(!pyresult)
    result=MakeSqliteMsgFromPyException(NULL);
  else
    result=SQLITE_OK;

  if(PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile.xUnlock", "{s: i}", "flag", flag);
  Py_XDECREF(pyresult);
  FILEPOSTAMBLE;
  return result;
}

static PyObject *
apswvfsfilepy_xUnlock(APSWVFSFile *self, PyObject *args)
{
  int flag, res;

  CHECKVFSFILEPY;
  VFSFILENOTIMPLEMENTED(xUnlock);

  if(!PyArg_ParseTuple(args, "i", &flag))
    return NULL;
  
  res=self->base->pMethods->xUnlock(self->base, flag);
  
  APSW_FAULT_INJECT(xUnlockFails,,res=SQLITE_IOERR);

  if(res==SQLITE_OK)
    Py_RETURN_NONE;
    
  SET_EXC(res, NULL);
  return NULL;
}

static int
apswvfsfile_xLock(sqlite3_file *file, int flag)
{
  int result=SQLITE_ERROR;
  PyObject *pyresult=NULL;
  FILEPREAMBLE;

  pyresult=Call_PythonMethodV(apswfile->file, "xLock", 1, "(i)", flag);
  if(!pyresult)
    {
      result=MakeSqliteMsgFromPyException(NULL);
      /* a busy exception is normal so we clear it */
      if(SQLITE_BUSY==(result&0xff))
        PyErr_Clear();
    }
  else
    result=SQLITE_OK;

  Py_XDECREF(pyresult);
  if(PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile.xLock", "{s: i}", "level", flag);

  FILEPOSTAMBLE;
  return result;
}

static PyObject *
apswvfsfilepy_xLock(APSWVFSFile *self, PyObject *args)
{
  int flag, res;

  CHECKVFSFILEPY;
  VFSFILENOTIMPLEMENTED(xLock);

  if(!PyArg_ParseTuple(args, "i", &flag))
    return NULL;
  
  res=self->base->pMethods->xLock(self->base, flag);

  if(res==SQLITE_OK)
    Py_RETURN_NONE;
    
  SET_EXC(res, NULL);
  return NULL;
}

static int
apswvfsfile_xTruncate(sqlite3_file *file, sqlite3_int64 size)
{
  int result=SQLITE_ERROR;
  PyObject *pyresult=NULL;
  FILEPREAMBLE;

  pyresult=Call_PythonMethodV(apswfile->file, "xTruncate", 1, "(L)", size);
  if(!pyresult)
    result=MakeSqliteMsgFromPyException(NULL);
  else
    result=SQLITE_OK;

  Py_XDECREF(pyresult);
  if(PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile.xTruncate", "{s: L}", "size", size);
  FILEPOSTAMBLE;
  return result;
}

static PyObject *
apswvfsfilepy_xTruncate(APSWVFSFile *self, PyObject *args)
{
  int res;
  sqlite3_int64 size;

  CHECKVFSFILEPY;
  VFSFILENOTIMPLEMENTED(xTruncate);

  if(!PyArg_ParseTuple(args, "L", &size))
    return NULL;
  
  res=self->base->pMethods->xTruncate(self->base, size);

  if(res==SQLITE_OK)
    Py_RETURN_NONE;
    
  SET_EXC(res, NULL);
  return NULL;
}

static int
apswvfsfile_xSync(sqlite3_file *file, int flags)
{
  int result=SQLITE_ERROR;
  PyObject *pyresult=NULL;
  FILEPREAMBLE;

  pyresult=Call_PythonMethodV(apswfile->file, "xSync", 1, "(i)", flags);
  if(!pyresult)
    result=MakeSqliteMsgFromPyException(NULL);
  else
    result=SQLITE_OK;

  Py_XDECREF(pyresult);
  if (PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile.xSync", "{s: i}", "flags", flags);
  FILEPOSTAMBLE;
  return result;
}

static PyObject *
apswvfsfilepy_xSync(APSWVFSFile *self, PyObject *args)
{
  int flags, res;

  CHECKVFSFILEPY;
  VFSFILENOTIMPLEMENTED(xSync);

  if(!PyArg_ParseTuple(args, "i", &flags))
    return NULL;
  
  res=self->base->pMethods->xSync(self->base, flags);

  APSW_FAULT_INJECT(xSyncFails, ,res=SQLITE_IOERR);

  if(res==SQLITE_OK)
    Py_RETURN_NONE;
    
  SET_EXC(res, NULL);
  return NULL;
}


static int
apswvfsfile_xSectorSize(sqlite3_file *file)
{
  int result=512;
  PyObject *pyresult=NULL;
  FILEPREAMBLE;

  pyresult=Call_PythonMethodV(apswfile->file, "xSectorSize", 0, "()");
  if(!pyresult)
    result=MakeSqliteMsgFromPyException(NULL);
  else if(pyresult!=Py_None)
    {
      if(PyIntLong_Check(pyresult))
        result=PyIntLong_AsLong(pyresult); /* returns -1 on error/overflow */
      else
        PyErr_Format(PyExc_TypeError, "xSectorSize should return a number");
    }

  /* We can't return errors so use unraiseable */
  if(PyErr_Occurred())
    {
      AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile_xSectorSize", NULL);
      result=512; /* could be -1 as stated above */
    }

  Py_XDECREF(pyresult);
  FILEPOSTAMBLE;
  return result;
}

static PyObject *
apswvfsfilepy_xSectorSize(APSWVFSFile *self)
{
  int res=512;

  CHECKVFSFILEPY;
  VFSFILENOTIMPLEMENTED(xSectorSize);

  res=self->base->pMethods->xSectorSize(self->base);

  return PyInt_FromLong(res);
}

static int
apswvfsfile_xDeviceCharacteristics(sqlite3_file *file)
{
  int result=0;
  PyObject *pyresult=NULL;
  FILEPREAMBLE;

  pyresult=Call_PythonMethodV(apswfile->file, "xDeviceCharacteristics", 0, "()");
  if(!pyresult)
    result=MakeSqliteMsgFromPyException(NULL);
  else if(pyresult!=Py_None)
    {
      if(PyIntLong_Check(pyresult))
        result=PyIntLong_AsLong(pyresult); /* sets to -1 on error */
      else
        PyErr_Format(PyExc_TypeError, "xDeviceCharacteristics should return a number");
    }

  /* We can't return errors so use unraiseable */
  if(PyErr_Occurred())
    {
      AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile_xDeviceCharacteristics", NULL);
      result=0; /* harmless value for error cases */
    }

  Py_XDECREF(pyresult);
  FILEPOSTAMBLE;
  return result;
}

static PyObject *
apswvfsfilepy_xDeviceCharacteristics(APSWVFSFile *self)
{
  int res=0;

  CHECKVFSFILEPY;
  VFSFILENOTIMPLEMENTED(xDeviceCharacteristics);

  res=self->base->pMethods->xDeviceCharacteristics(self->base);

  return PyInt_FromLong(res);
}


static int
apswvfsfile_xFileSize(sqlite3_file *file, sqlite3_int64 *pSize)
{
  int result=SQLITE_OK;
  PyObject *pyresult=NULL;
  FILEPREAMBLE;
  
  pyresult=Call_PythonMethodV(apswfile->file, "xFileSize", 1, "()");
  if(!pyresult)
    result=MakeSqliteMsgFromPyException(NULL);
  else if(PyLong_Check(pyresult))
    *pSize=PyLong_AsLongLong(pyresult);
  else if(PyIntLong_Check(pyresult))
    *pSize=PyIntLong_AsLong(pyresult);
  else
    PyErr_Format(PyExc_TypeError, "xFileSize should return a number");

  if(PyErr_Occurred())
    {
      result=MakeSqliteMsgFromPyException(NULL);
      AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile_xFileSize", "{s: O}", "result", pyresult?pyresult:Py_None);
    }

  Py_XDECREF(pyresult);
  FILEPOSTAMBLE;
  return result;
}

static PyObject *
apswvfsfilepy_xFileSize(APSWVFSFile *self)
{
  sqlite3_int64 size;
  int res;

  CHECKVFSFILEPY;
  VFSFILENOTIMPLEMENTED(xFileSize);
  res=self->base->pMethods->xFileSize(self->base, &size);

  APSW_FAULT_INJECT(xFileSizeFails, ,res=SQLITE_IOERR);

  if(res!=SQLITE_OK)
    {
      SET_EXC(res, NULL);
      return NULL;
    }
  return PyLong_FromLongLong(size);
}

static int
apswvfsfile_xCheckReservedLock(sqlite3_file *file, int *pResOut)
{
  int result=SQLITE_OK;
  PyObject *pyresult=NULL;
  FILEPREAMBLE;

  pyresult=Call_PythonMethodV(apswfile->file, "xCheckReservedLock", 1, "()");
  if(!pyresult)
    result=MakeSqliteMsgFromPyException(NULL);
  else if(PyIntLong_Check(pyresult))
    *pResOut=!!PyIntLong_AsLong(pyresult);
  else
    PyErr_Format(PyExc_TypeError, "xCheckReservedLock should return a boolean/number");

  if(PyErr_Occurred())
    {
      result=MakeSqliteMsgFromPyException(NULL);
      AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile_xCheckReservedLock", "{s: O}", "result", pyresult?pyresult:Py_None);
    }

  Py_XDECREF(pyresult);
  FILEPOSTAMBLE;
  return result;
}

static PyObject *
apswvfsfilepy_xCheckReservedLock(APSWVFSFile *self)
{
  int islocked;
  int res;

  CHECKVFSFILEPY;
  VFSFILENOTIMPLEMENTED(xCheckReservedLock);

  res=self->base->pMethods->xCheckReservedLock(self->base, &islocked);

  APSW_FAULT_INJECT(xCheckReservedLockFails,, res=SQLITE_IOERR);

  if(res!=SQLITE_OK)
    {
      SET_EXC(res, NULL);
      return NULL;
    }

  APSW_FAULT_INJECT(xCheckReservedLockIsTrue,,islocked=1);

  if(islocked)
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

static int
apswvfsfile_xFileControl(sqlite3_file *file, int op, void *pArg)
{
  int result=SQLITE_ERROR;
  PyObject *pyresult=NULL;
  FILEPREAMBLE;

  pyresult=Call_PythonMethodV(apswfile->file, "xFileControl", 1, "(iN)", op, PyLong_FromVoidPtr(pArg));
  if(!pyresult)
    result=MakeSqliteMsgFromPyException(NULL);
  else
    result=SQLITE_OK;

  Py_XDECREF(pyresult);
  FILEPOSTAMBLE;
  return result;
}

static PyObject *
apswvfsfilepy_xFileControl(APSWVFSFile *self, PyObject *args)
{
  int op, res=SQLITE_ERROR;
  PyObject *pyptr;
  void *ptr=NULL;

  CHECKVFSFILEPY;
  VFSFILENOTIMPLEMENTED(xFileControl);

  if(!PyArg_ParseTuple(args, "iO", &op, &pyptr))
    return NULL;

  if(PyIntLong_Check(pyptr))
    ptr=PyLong_AsVoidPtr(pyptr);
  else
    PyErr_Format(PyExc_TypeError, "Argument is not number (pointer)");

  if(PyErr_Occurred())
    goto finally;
  
  res=self->base->pMethods->xFileControl(self->base, op, ptr);

  if(res==SQLITE_OK)
    Py_RETURN_NONE;
 finally:
  SET_EXC(res, NULL);
  return NULL;
}

static int
apswvfsfile_xClose(sqlite3_file *file)
{
  int result=SQLITE_ERROR;
  PyObject *pyresult=NULL;
  FILEPREAMBLE;

  pyresult=Call_PythonMethodV(apswfile->file, "xClose", 1, "()");
  if(!pyresult)
    result=MakeSqliteMsgFromPyException(NULL);
  else
    result=SQLITE_OK;

  if(PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile.xClose", NULL);

  Py_XDECREF(apswfile->file);
  apswfile->file=NULL;
  Py_XDECREF(pyresult);
  FILEPOSTAMBLE;
  return result;
}

static PyObject *
apswvfsfilepy_xClose(APSWVFSFile *self)
{
  int res;

  if(!self->base) /* already closed */
    Py_RETURN_NONE;

  res=self->base->pMethods->xClose(self->base);
  
  APSW_FAULT_INJECT(xCloseFails,, res=SQLITE_IOERR);

  /* we set pMethods to NULL after xClose callback so xClose can call other operations
     such as read or write during close */
  self->base->pMethods=NULL;

  PyMem_Free(self->base);
  self->base=NULL;

  if(res==SQLITE_OK)
    Py_RETURN_NONE;
    
  SET_EXC(res, NULL);
  return NULL;
}

static struct sqlite3_io_methods apsw_io_methods=
  {
    1,                                 /* version */
    apswvfsfile_xClose,                /* close */
    apswvfsfile_xRead,                 /* read */
    apswvfsfile_xWrite,                /* write */
    apswvfsfile_xTruncate,             /* truncate */
    apswvfsfile_xSync,                 /* sync */
    apswvfsfile_xFileSize,             /* filesize */
    apswvfsfile_xLock,                 /* lock */
    apswvfsfile_xUnlock,               /* unlock */
    apswvfsfile_xCheckReservedLock,    /* checkreservedlock */
    apswvfsfile_xFileControl,          /* filecontrol */
    apswvfsfile_xSectorSize,           /* sectorsize */
    apswvfsfile_xDeviceCharacteristics /* device characteristics */
  };


static PyMethodDef APSWVFSFile_methods[]={
  {"xRead", (PyCFunction)apswvfsfilepy_xRead, METH_VARARGS, "xRead"},
  {"xUnlock", (PyCFunction)apswvfsfilepy_xUnlock, METH_VARARGS, "xUnlock"},
  {"xLock", (PyCFunction)apswvfsfilepy_xLock, METH_VARARGS, "xLock"},
  {"xClose", (PyCFunction)apswvfsfilepy_xClose, METH_NOARGS, "xClose"},
  {"xSectorSize", (PyCFunction)apswvfsfilepy_xSectorSize, METH_NOARGS, "xSectorSize"},
  {"xFileSize", (PyCFunction)apswvfsfilepy_xFileSize, METH_NOARGS, "xFileSize"},
  {"xDeviceCharacteristics", (PyCFunction)apswvfsfilepy_xDeviceCharacteristics, METH_NOARGS, "xDeviceCharacteristics"},
  {"xCheckReservedLock", (PyCFunction)apswvfsfilepy_xCheckReservedLock, METH_NOARGS, "xCheckReservedLock"},
  {"xWrite", (PyCFunction)apswvfsfilepy_xWrite, METH_VARARGS, "xWrite"},
  {"xSync", (PyCFunction)apswvfsfilepy_xSync, METH_VARARGS, "xSync"},
  {"xTruncate", (PyCFunction)apswvfsfilepy_xTruncate, METH_VARARGS, "xTruncate"},
  {"xFileControl", (PyCFunction)apswvfsfilepy_xFileControl, METH_VARARGS, "xFileControl"},
  {"excepthook", (PyCFunction)apswvfs_excepthook, METH_VARARGS, "Exception hook"},
  /* Sentinel */
  {0, 0, 0, 0}
  };

static PyTypeObject APSWVFSFileType =
  {
#if PY_VERSION_HEX<0x03000000
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
#else
    PyVarObject_HEAD_INIT(NULL,0)
#endif
    "apsw.VFSFile",            /*tp_name*/
    sizeof(APSWVFSFile),       /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)APSWVFSFile_dealloc, /*tp_dealloc*/ 
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
    "VFSFile object",          /* tp_doc */
    0,		               /* tp_traverse */
    0,		               /* tp_clear */
    0,		               /* tp_richcompare */
    0,		               /* tp_weaklistoffset */
    0,		               /* tp_iter */
    0,		               /* tp_iternext */
    APSWVFSFile_methods,       /* tp_methods */
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)APSWVFSFile_init, /* tp_init */
    0,                         /* tp_alloc */
    APSWVFSFile_new,           /* tp_new */
    0,                         /* tp_free */
    0,                         /* tp_is_gc */
    0,                         /* tp_bases */
    0,                         /* tp_mro */
    0,                         /* tp_cache */
    0,                         /* tp_subclasses */
    0,                         /* tp_weaklist */
    0,                         /* tp_del */
#if PY_VERSION_HEX>=0x03000000
    0                          /* tp_version_tag */
#endif
  };



/* END OF VFS CODE */




/* MODULE METHODS */
static PyObject *
getsqliteversion(void)
{
  return MAKESTR(sqlite3_libversion());
}

static PyObject *
getapswversion(void)
{
  return MAKESTR(APSW_VERSION);
}

static PyObject *
enablesharedcache(APSW_ARGUNUSED PyObject *self, PyObject *args)
{
  int setting,res;
  if(!PyArg_ParseTuple(args, "i:enablesharedcache(boolean)", &setting))
    return NULL;

  APSW_FAULT_INJECT(EnableSharedCacheFail,res=sqlite3_enable_shared_cache(setting),res=SQLITE_NOMEM);
  SET_EXC(res, NULL);

  if(res!=SQLITE_OK)
    return NULL;

  Py_RETURN_NONE;
}

static PyObject *
initialize(void)
{
  int res;

  res=sqlite3_initialize();
  APSW_FAULT_INJECT(InitializeFail, ,res=SQLITE_NOMEM);
  SET_EXC(res, NULL);

  if(res!=SQLITE_OK)
    return NULL;

  Py_RETURN_NONE;
}

static PyObject *
sqliteshutdown(void)
{
  int res;
  
  APSW_FAULT_INJECT(ShutdownFail, res=sqlite3_shutdown(), res=SQLITE_NOMEM);
  SET_EXC(res, NULL);

  if(res!=SQLITE_OK)
    return NULL;

  Py_RETURN_NONE;
}

#ifdef EXPERIMENTAL
static PyObject *
config(APSW_ARGUNUSED PyObject *self, PyObject *args)
{
  int res, optdup;
  long opt;

  if(PyTuple_GET_SIZE(args)<1 || !PyIntLong_Check(PyTuple_GET_ITEM(args, 0)))
    {
      PyErr_Format(PyExc_TypeError, "There should be at least one argument with the first being a number");
      return NULL;
    }
  opt=PyIntLong_AsLong(PyTuple_GET_ITEM(args,0));
  if(PyErr_Occurred())
    return NULL;

  switch(opt)
    {
    case SQLITE_CONFIG_SINGLETHREAD:
    case SQLITE_CONFIG_MULTITHREAD:
    case SQLITE_CONFIG_SERIALIZED:
      if(!PyArg_ParseTuple(args, "i", &optdup))
        return NULL;
      assert(opt==optdup);
      res=sqlite3_config( (int)opt );
      break;
      
    case SQLITE_CONFIG_MEMSTATUS:
      {
        int boolval;
        if(!PyArg_ParseTuple(args, "ii", &optdup, &boolval))
          return NULL;
        assert(opt==optdup);
        res=sqlite3_config( (int)opt, boolval);
        break;
      }
      
    default:
      PyErr_Format(PyExc_TypeError, "Unknown config type %d", (int)opt);
      return NULL;
    }

  SET_EXC(res, NULL);

  if(res!=SQLITE_OK)
    return NULL;

  Py_RETURN_NONE;
}
#endif /* EXPERIMENTAL */

static PyObject*
memoryused(void)
{
  return PyLong_FromLongLong(sqlite3_memory_used());
}

static PyObject*
memoryhighwater(APSW_ARGUNUSED PyObject *self, PyObject *args)
{
  int reset=0;

  if(!PyArg_ParseTuple(args, "|i:memoryhighwater(reset=False)", &reset))
    return NULL;

  return PyLong_FromLongLong(sqlite3_memory_highwater(reset));
}

static PyObject *
status(APSW_ARGUNUSED PyObject *self, PyObject *args)
{
  int res, op, current=0, highwater=0, reset=0;

  if(!PyArg_ParseTuple(args, "i|i:status(op, reset=False)", &op, &reset))
    return NULL;

  res=sqlite3_status(op, &current, &highwater, reset);
  SET_EXC(res, NULL);

  if(res!=SQLITE_OK)
    return NULL;

  return Py_BuildValue("(ii)", current, highwater);
}

static PyObject *
vfsnames(APSW_ARGUNUSED PyObject *self)
{
  PyObject *result=NULL, *str=NULL;
  sqlite3_vfs *vfs=sqlite3_vfs_find(0);

  result=PyList_New(0);
  if(!result) goto error;

  while(vfs)
    {
      APSW_FAULT_INJECT(vfsnamesfails, 
                        str=convertutf8string(vfs->zName),
                        str=PyErr_NoMemory());
      if(!str) goto error;
      if(PyList_Append(result, str)) goto error;
      Py_DECREF(str);
      vfs=vfs->pNext;
    }
  return result;

 error:
  Py_XDECREF(str);
  Py_XDECREF(result);
  return NULL;
}

static PyObject *
getapswexceptionfor(APSW_ARGUNUSED PyObject *self, PyObject *pycode)
{
  int code, i;
  PyObject *result=NULL;

  if(!PyIntLong_Check(pycode))
    {
      PyErr_Format(PyExc_TypeError, "Argument should be an integer");
      return NULL;
    }
  code=PyIntLong_AsLong(pycode);
  if(PyErr_Occurred()) return NULL;

  for(i=0;exc_descriptors[i].name;i++)
    if (exc_descriptors[i].code==(code&0xff))
      {
        result=PyObject_CallObject(exc_descriptors[i].cls, NULL);
        if(!result) return result;
        break;
      }
  if(!result)
    {
      PyErr_Format(PyExc_ValueError, "%d is not a known error code", code);
      return result;
    }

  PyObject_SetAttrString(result, "extendedresult", PyInt_FromLong(code));
  PyObject_SetAttrString(result, "result", PyInt_FromLong(code&0xff));
  return result;
}

#if defined(APSW_TESTFIXTURES) && defined(APSW_USE_SQLITE_AMALGAMATION)
/* a routine to reset the random number generator so that we can test xRandomness */
static PyObject *
apsw_test_reset_rng(APSW_ARGUNUSED PyObject *self)
{
  /* See sqlite3PrngResetState in sqlite's random.c which is above us if using the amalgamation */
  GLOBAL(struct sqlite3PrngType, sqlite3Prng).isInit = 0;

  Py_RETURN_NONE;
}
#endif

#ifdef APSW_TESTFIXTURES
/* xGetLastError isn't actually called anywhere by SQLite so add a
   manual way of doing so
   http://www.sqlite.org/cvstrac/tktview?tn=3337 */

static PyObject *
apsw_call_xGetLastError(APSW_ARGUNUSED PyObject *self, PyObject *args)
{
  char *vfsname;
  int bufsize;
  PyObject *resultbuffer=NULL;
  sqlite3_vfs *vfs;
  int res=-1;

  if(!PyArg_ParseTuple(args, "esi", STRENCODING, &vfsname, &bufsize))
    return NULL;

  vfs=sqlite3_vfs_find(vfsname);
  if(!vfs) goto finally;

  resultbuffer=PyBytes_FromStringAndSize(NULL, bufsize);
  if(!resultbuffer) goto finally;

  memset(PyBytes_AS_STRING(resultbuffer), 0, PyBytes_GET_SIZE(resultbuffer));

  res=vfs->xGetLastError(vfs, bufsize, PyBytes_AS_STRING(resultbuffer));

 finally:
  if(vfsname)
    PyMem_Free(vfsname);

  return resultbuffer?Py_BuildValue("Ni", resultbuffer, res):NULL;
}
#endif


static PyMethodDef module_methods[] = {
  {"sqlitelibversion", (PyCFunction)getsqliteversion, METH_NOARGS,
   "Return the version of the SQLite library"},
  {"apswversion", (PyCFunction)getapswversion, METH_NOARGS,
   "Return the version of the APSW wrapper"},
  {"vfsnames", (PyCFunction)vfsnames, METH_NOARGS,
   "Returns list of vfs names"},
  {"enablesharedcache", (PyCFunction)enablesharedcache, METH_VARARGS,
   "Sets shared cache semantics for this thread"},
  {"initialize", (PyCFunction)initialize, METH_NOARGS,
   "Initialize SQLite library"},
  {"shutdown", (PyCFunction)sqliteshutdown, METH_NOARGS,
   "Shutdown SQLite library"},
#ifdef EXPERIMENTAL
  {"config", (PyCFunction)config, METH_VARARGS,
   "Calls sqlite3_config"},
#endif
  {"memoryused", (PyCFunction)memoryused, METH_NOARGS,
   "Current SQLite memory in use"},
  {"memoryhighwater", (PyCFunction)memoryhighwater, METH_VARARGS,
   "Most amount of memory used"},
  {"status", (PyCFunction)status, METH_VARARGS,
   "Gets various SQLite counters"},
  {"exceptionfor", (PyCFunction)getapswexceptionfor, METH_O,
   "Returns exception instance corresponding to supplied sqlite error code"},
#if defined(APSW_TESTFIXTURES) && defined(APSW_USE_SQLITE_AMALGAMATION)
  {"test_reset_rng", (PyCFunction)apsw_test_reset_rng, METH_NOARGS,
   "Resets random number generator so we can test vfs xRandomness"},
#endif
#ifdef APSW_TESTFIXTURES
  {"test_call_xGetLastError", (PyCFunction)apsw_call_xGetLastError, METH_VARARGS,
   "Calls xGetLastError routine"},
#endif
  {0, 0, 0, 0}  /* Sentinel */
};



#if PY_VERSION_HEX>=0x03000000
static struct PyModuleDef apswmoduledef={
  PyModuleDef_HEAD_INIT,
  "apsw", 
  NULL,
  -1,
  module_methods,
  0,
  0,
  0,
  0,
};
#endif


PyMODINIT_FUNC
#if PY_VERSION_HEX<0x03000000
initapsw(void) 
#else
PyInit_apsw(void)
#endif
{
    PyObject *m=NULL;
    PyObject *thedict=NULL;
    const char *mapping_name=NULL;
    PyObject *hooks;
    unsigned int i;

    assert(sizeof(int)==4);             /* we expect 32 bit ints */
    assert(sizeof(long long)==8);             /* we expect 64 bit long long */

    /* Check SQLite was compiled with thread safety */
    if(!sqlite3_threadsafe())
      {
        PyErr_Format(PyExc_EnvironmentError, "SQLite was compiled without thread safety and cannot be used.");
        goto fail;
      }

    if (PyType_Ready(&ConnectionType) < 0
        || PyType_Ready(&APSWCursorType) < 0
        || PyType_Ready(&ZeroBlobBindType) <0
        || PyType_Ready(&APSWBlobType) <0
        || PyType_Ready(&APSWVFSType) <0
        || PyType_Ready(&APSWVFSFileType) <0
        )
      goto fail;

    /* ensure threads are available */
    PyEval_InitThreads();

#if PY_VERSION_HEX<0x03000000
    m = apswmodule = Py_InitModule3("apsw", module_methods,
                       "Another Python SQLite Wrapper.");
#else
    m = apswmodule = PyModule_Create(&apswmoduledef);
#endif

    if (m == NULL)  goto fail;

    if(init_exceptions(m)) goto fail;

    Py_INCREF(&ConnectionType);
    PyModule_AddObject(m, "Connection", (PyObject *)&ConnectionType);
    
    /* we don't add cursor to the module since users shouldn't be able to instantiate them directly */
    
    Py_INCREF(&ZeroBlobBindType);
    PyModule_AddObject(m, "zeroblob", (PyObject *)&ZeroBlobBindType);

    Py_INCREF(&APSWVFSType);
    PyModule_AddObject(m, "VFS", (PyObject*)&APSWVFSType);
    Py_INCREF(&APSWVFSFileType);
    PyModule_AddObject(m, "VFSFile", (PyObject*)&APSWVFSFileType);
    
    hooks=PyList_New(0);
    if(!hooks) goto fail;
    PyModule_AddObject(m, "connection_hooks", hooks);

    /* Version number */
    PyModule_AddIntConstant(m, "SQLITE_VERSION_NUMBER", SQLITE_VERSION_NUMBER);
    

    /* add in some constants and also put them in a corresponding mapping dictionary */

    /* sentinel should be a number that doesn't exist */
#define SENTINEL -786343
#define DICT(n) {n, SENTINEL}
#define END {NULL, 0}
#define ADDINT(n) {#n, n}

    struct { const char *name; int value; } integers[]={
      DICT("mapping_authorizer_return"),
      ADDINT(SQLITE_DENY),
      ADDINT(SQLITE_IGNORE),
      ADDINT(SQLITE_OK),
      END,
      
      DICT("mapping_authorizer_function"),
      ADDINT(SQLITE_CREATE_INDEX),
      ADDINT(SQLITE_CREATE_TABLE),
      ADDINT(SQLITE_CREATE_TEMP_INDEX),
      ADDINT(SQLITE_CREATE_TEMP_TABLE),
      ADDINT(SQLITE_CREATE_TEMP_TRIGGER),
      ADDINT(SQLITE_CREATE_TEMP_VIEW),
      ADDINT(SQLITE_CREATE_TRIGGER),
      ADDINT(SQLITE_CREATE_VIEW),
      ADDINT(SQLITE_DELETE),
      ADDINT(SQLITE_DROP_INDEX),
      ADDINT(SQLITE_DROP_TABLE),
      ADDINT(SQLITE_DROP_TEMP_INDEX),
      ADDINT(SQLITE_DROP_TEMP_TABLE),
      ADDINT(SQLITE_DROP_TEMP_TRIGGER),
      ADDINT(SQLITE_DROP_TEMP_VIEW),
      ADDINT(SQLITE_DROP_TRIGGER),
      ADDINT(SQLITE_DROP_VIEW),
      ADDINT(SQLITE_INSERT),
      ADDINT(SQLITE_PRAGMA),
      ADDINT(SQLITE_READ),
      ADDINT(SQLITE_SELECT),
      ADDINT(SQLITE_TRANSACTION),
      ADDINT(SQLITE_UPDATE),
      ADDINT(SQLITE_ATTACH),
      ADDINT(SQLITE_DETACH),
      ADDINT(SQLITE_ALTER_TABLE),
      ADDINT(SQLITE_REINDEX),
      ADDINT(SQLITE_COPY),
      ADDINT(SQLITE_ANALYZE),
      ADDINT(SQLITE_CREATE_VTABLE),
      ADDINT(SQLITE_DROP_VTABLE),
      ADDINT(SQLITE_FUNCTION),
      END,

      /* vtable best index constraints */
#if defined(SQLITE_INDEX_CONSTRAINT_EQ) && defined(SQLITE_INDEX_CONSTRAINT_MATCH)
      DICT("mapping_bestindex_constraints"),
      ADDINT(SQLITE_INDEX_CONSTRAINT_EQ),
      ADDINT(SQLITE_INDEX_CONSTRAINT_GT),
      ADDINT(SQLITE_INDEX_CONSTRAINT_LE),
      ADDINT(SQLITE_INDEX_CONSTRAINT_LT),
      ADDINT(SQLITE_INDEX_CONSTRAINT_GE),
      ADDINT(SQLITE_INDEX_CONSTRAINT_MATCH),
      END,
#endif /* constraints */

    /* extendended result codes */
      DICT("mapping_extended_result_codes"),
      ADDINT(SQLITE_IOERR_READ),
      ADDINT(SQLITE_IOERR_SHORT_READ),
      ADDINT(SQLITE_IOERR_WRITE),
      ADDINT(SQLITE_IOERR_FSYNC),
      ADDINT(SQLITE_IOERR_DIR_FSYNC),
      ADDINT(SQLITE_IOERR_TRUNCATE),
      ADDINT(SQLITE_IOERR_FSTAT),
      ADDINT(SQLITE_IOERR_UNLOCK),
      ADDINT(SQLITE_IOERR_RDLOCK),
      ADDINT(SQLITE_IOERR_DELETE),
      ADDINT(SQLITE_IOERR_BLOCKED),
      ADDINT(SQLITE_IOERR_NOMEM),
      ADDINT(SQLITE_IOERR_ACCESS),
      ADDINT(SQLITE_IOERR_CHECKRESERVEDLOCK),
      ADDINT(SQLITE_IOERR_LOCK),
      END,

    /* error codes */
      DICT("mapping_result_codes"),
      ADDINT(SQLITE_OK),
      ADDINT(SQLITE_ERROR),
      ADDINT(SQLITE_INTERNAL),
      ADDINT(SQLITE_PERM),
      ADDINT(SQLITE_ABORT),
      ADDINT(SQLITE_BUSY),
      ADDINT(SQLITE_LOCKED),
      ADDINT(SQLITE_NOMEM),
      ADDINT(SQLITE_READONLY),
      ADDINT(SQLITE_INTERRUPT),
      ADDINT(SQLITE_IOERR),
      ADDINT(SQLITE_CORRUPT),
      ADDINT(SQLITE_FULL),
      ADDINT(SQLITE_CANTOPEN),
      ADDINT(SQLITE_PROTOCOL),
      ADDINT(SQLITE_EMPTY),
      ADDINT(SQLITE_SCHEMA),
      ADDINT(SQLITE_CONSTRAINT),
      ADDINT(SQLITE_MISMATCH),
      ADDINT(SQLITE_MISUSE),
      ADDINT(SQLITE_NOLFS),
      ADDINT(SQLITE_AUTH),
      ADDINT(SQLITE_FORMAT),
      ADDINT(SQLITE_RANGE),
      ADDINT(SQLITE_NOTADB),
      END,

      /* open flags */
      DICT("mapping_open_flags"),
      ADDINT(SQLITE_OPEN_READONLY),
      ADDINT(SQLITE_OPEN_READWRITE),
      ADDINT(SQLITE_OPEN_CREATE),
      ADDINT(SQLITE_OPEN_DELETEONCLOSE),
      ADDINT(SQLITE_OPEN_EXCLUSIVE),
      ADDINT(SQLITE_OPEN_MAIN_DB),
      ADDINT(SQLITE_OPEN_TEMP_DB),
      ADDINT(SQLITE_OPEN_TRANSIENT_DB),
      ADDINT(SQLITE_OPEN_MAIN_JOURNAL),
      ADDINT(SQLITE_OPEN_TEMP_JOURNAL),
      ADDINT(SQLITE_OPEN_SUBJOURNAL),
      ADDINT(SQLITE_OPEN_MASTER_JOURNAL),
      ADDINT(SQLITE_OPEN_NOMUTEX),
      ADDINT(SQLITE_OPEN_FULLMUTEX),
      END,

      /* limits */
      DICT("mapping_limits"),
      ADDINT(SQLITE_LIMIT_LENGTH),
      ADDINT(SQLITE_LIMIT_SQL_LENGTH),
      ADDINT(SQLITE_LIMIT_COLUMN),
      ADDINT(SQLITE_LIMIT_EXPR_DEPTH),
      ADDINT(SQLITE_LIMIT_COMPOUND_SELECT),
      ADDINT(SQLITE_LIMIT_VDBE_OP),
      ADDINT(SQLITE_LIMIT_FUNCTION_ARG),
      ADDINT(SQLITE_LIMIT_ATTACHED),
      ADDINT(SQLITE_LIMIT_LIKE_PATTERN_LENGTH),
      ADDINT(SQLITE_LIMIT_VARIABLE_NUMBER),
      /* We don't include the MAX limits - see http://code.google.com/p/apsw/issues/detail?id=17 */
      END,

      DICT("mapping_config"),
      ADDINT(SQLITE_CONFIG_SINGLETHREAD),
      ADDINT(SQLITE_CONFIG_MULTITHREAD),
      ADDINT(SQLITE_CONFIG_SERIALIZED),
      ADDINT(SQLITE_CONFIG_MALLOC),
      ADDINT(SQLITE_CONFIG_GETMALLOC),
      ADDINT(SQLITE_CONFIG_SCRATCH),
      ADDINT(SQLITE_CONFIG_PAGECACHE),
      ADDINT(SQLITE_CONFIG_HEAP),
      ADDINT(SQLITE_CONFIG_MEMSTATUS),
      ADDINT(SQLITE_CONFIG_MUTEX),
      ADDINT(SQLITE_CONFIG_GETMUTEX),
      ADDINT(SQLITE_CONFIG_CHUNKALLOC),
      ADDINT(SQLITE_CONFIG_LOOKASIDE),
      END,

      DICT("mapping_db_config"),
      ADDINT(SQLITE_DBCONFIG_LOOKASIDE),
      END,

      DICT("mapping_status"),
      ADDINT(SQLITE_STATUS_MEMORY_USED),
      ADDINT(SQLITE_STATUS_PAGECACHE_USED),
      ADDINT(SQLITE_STATUS_PAGECACHE_OVERFLOW),
      ADDINT(SQLITE_STATUS_SCRATCH_USED),
      ADDINT(SQLITE_STATUS_SCRATCH_OVERFLOW),
      ADDINT(SQLITE_STATUS_MALLOC_SIZE),
      ADDINT(SQLITE_STATUS_PARSER_STACK),
      ADDINT(SQLITE_STATUS_PAGECACHE_SIZE),
      ADDINT(SQLITE_STATUS_SCRATCH_SIZE),
      END,

      DICT("mapping_db_status"),
      ADDINT(SQLITE_DBSTATUS_LOOKASIDE_USED),
      END,

      DICT("mapping_locking_level"),
      ADDINT(SQLITE_LOCK_NONE),
      ADDINT(SQLITE_LOCK_SHARED),
      ADDINT(SQLITE_LOCK_RESERVED),
      ADDINT(SQLITE_LOCK_PENDING),
      ADDINT(SQLITE_LOCK_EXCLUSIVE),
      END,

      DICT("mapping_access"),
      ADDINT(SQLITE_ACCESS_EXISTS),
      ADDINT(SQLITE_ACCESS_READWRITE),
      ADDINT(SQLITE_ACCESS_READ),
      END,

      DICT("mapping_device_characteristics"),
      ADDINT(SQLITE_IOCAP_ATOMIC),
      ADDINT(SQLITE_IOCAP_ATOMIC512),
      ADDINT(SQLITE_IOCAP_ATOMIC1K),
      ADDINT(SQLITE_IOCAP_ATOMIC2K),
      ADDINT(SQLITE_IOCAP_ATOMIC4K),
      ADDINT(SQLITE_IOCAP_ATOMIC8K),
      ADDINT(SQLITE_IOCAP_ATOMIC16K),
      ADDINT(SQLITE_IOCAP_ATOMIC32K),
      ADDINT(SQLITE_IOCAP_ATOMIC64K),
      ADDINT(SQLITE_IOCAP_SAFE_APPEND),
      ADDINT(SQLITE_IOCAP_SEQUENTIAL),
      END,

      DICT("mapping_sync"),
      ADDINT(SQLITE_SYNC_NORMAL),
      ADDINT(SQLITE_SYNC_FULL),
      ADDINT(SQLITE_SYNC_DATAONLY),
      END};
 
 
 for(i=0;i<sizeof(integers)/sizeof(integers[0]); i++)
   {
     const char *name=integers[i].name;
     int value=integers[i].value;
     PyObject *pyname;
     PyObject *pyvalue;

     /* should be at dict */
     if(!thedict)
       {
         assert(value==SENTINEL);
         assert(mapping_name==NULL);
         mapping_name=name;
         thedict=PyDict_New();
         continue;
       }
     /* at END? */
     if(!name)
       {
         assert(thedict);
         PyModule_AddObject(m, mapping_name, thedict);
         thedict=NULL;
         mapping_name=NULL;
         continue;
       }
     /* regular ADDINT */
     PyModule_AddIntConstant(m, name, value);
     pyname=MAKESTR(name);
     pyvalue=PyInt_FromLong(value);
     if(!pyname || !pyvalue) goto fail;
     PyDict_SetItem(thedict, pyname, pyvalue);
     PyDict_SetItem(thedict, pyvalue, pyname);
     Py_DECREF(pyname);
     Py_DECREF(pyvalue);
   }
 /* should have ended with END so thedict should be NULL */
 assert(thedict==NULL);

 if(!PyErr_Occurred())
      {
        return
#if PY_VERSION_HEX>=0x03000000
          m
#endif
          ;
      }

 fail:
    Py_XDECREF(m);
    return 
#if PY_VERSION_HEX>=0x03000000
          NULL
#endif
          ;
}

#ifdef APSW_TESTFIXTURES
static int
APSW_Should_Fault(const char *name)
{
  PyGILState_STATE gilstate;
  PyObject *faultdict=NULL, *truthval=NULL, *value=NULL;
  int res=0;

  gilstate=PyGILState_Ensure();

  if(!PyObject_HasAttrString(apswmodule, "faultdict"))
    PyObject_SetAttrString(apswmodule, "faultdict", PyDict_New());

  value=MAKESTR(name);
  
  faultdict=PyObject_GetAttrString(apswmodule, "faultdict");
  
  truthval=PyDict_GetItem(faultdict, value);
  if(!truthval)
    goto finally;

  /* set false if present - one shot firing */
  PyDict_SetItem(faultdict, value, Py_False);
  res=PyObject_IsTrue(truthval);

 finally:
  Py_XDECREF(value);
  Py_XDECREF(faultdict);

  PyGILState_Release(gilstate);
  return res;
}
#endif
