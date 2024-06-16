/*
  VFS code

  See the accompanying LICENSE file.
*/

/**

.. _vfs:

Virtual File System (VFS)
*************************

`VFS <https://sqlite.org/c3ref/vfs.html>`__ defines
the interface between the SQLite core and the underlying operating
system. The majority of the functionality deals with files. APSW
exposes this functionality letting you provide your own routines.
You can also *inherit* from an existing vfs making it easy to augment
or override specific routines.

You specify which VFS to use as a parameter to the :class:`Connection`
constructor.

.. code-block:: python

  db=apsw.Connection("file", vfs="myvfs")

The easiest way to get started is to make a :class:`VFS` derived class
that inherits from the default vfs.  Then override methods you want to
change behaviour of.  If you want to just change how file operations
are done then you have to override :meth:`VFS.xOpen` to return a file
instance that has your overridden :class:`VFSFile` methods.  The
:ref:`example <example_vfs>` demonstrates obfuscating the database
file contents.

Exceptions and errors
=====================

To return an error from any routine you should raise an exception. The
exception will be translated into the corresponding SQLite error code.
To return a specific SQLite error code use :meth:`exception_for`.  If
the exception does not map to any specific error code then
*SQLITE_ERROR* which corresponds to :exc:`SQLError` is returned to SQLite.

The SQLite code that deals with VFS errors behaves in varying
ways. Some routines have no way to return an error: eg `xDlOpen
<https://sqlite.org/c3ref/vfs.html>`_ just returns zero/NULL on
being unable to load a library, `xSleep
<https://sqlite.org/c3ref/vfs.html>`_ has no error return
parameter), others are unified (eg almost any
error in xWrite will be returned to the user as disk full
error). Sometimes errors are ignored as they are harmless such as when
a journal can't be deleted after a commit (the journal is marked as
obsolete before being deleted).  Simple operations such as opening a
database can result in many different VFS function calls such as hot
journals being detected, locking, and read/writes for
playback/rollback.

If multiple exceptions occur during the same SQLite control flow, then
they will be :doc:`chained <exceptions>` together.
:ref:`Augmented stack traces <augmentedstacktraces>` are available
which significantly increase detail about the exceptions and help with
debugging.

*/

/* make working with file control pragma easier */

/** .. class:: VFSFcntlPragma

A helper class to work with `SQLITE_FCNTL_PRAGMA
<https://sqlite.org/c3ref/c_fcntl_begin_atomic_write.html#sqlitefcntlpragma>`__
in :meth:`VFSFile.xFileControl`. The :ref:`example <example_vfs>`
shows usage of this class.

It is only valid while in :meth:`VFSFile.xFileControl`, and using
outside of that will result in memory corruption and crashes.

*/
typedef struct apswfcntl_pragma
{
  PyObject_HEAD char **strings;
  int init_was_called;
} apswfcntl_pragma;

static PyObject *
apswfcntl_pragma_new(PyTypeObject *type, PyObject *Py_UNUSED(args), PyObject *Py_UNUSED(kwds))
{
  apswfcntl_pragma *self = (apswfcntl_pragma *)type->tp_alloc(type, 0);
  if (self != NULL)
  {
    self->strings = NULL;
    self->init_was_called = 0;
  }
  return (PyObject *)self;
}

/** .. method:: __init__(pointer: int)

The pointer must be what your xFileControl method received.

*/
static int
apswfcntl_pragma_init(apswfcntl_pragma *self, PyObject *args, PyObject *kwargs)
{
  void *pointer = NULL;
  {
    VFSFcntlPragma_init_CHECK;
    PREVENT_INIT_MULTIPLE_CALLS;
    ARG_CONVERT_VARARGS_TO_FASTCALL;
    ARG_PROLOG(1, VFSFcntlPragma_init_KWNAMES);
    ARG_MANDATORY ARG_pointer(pointer);
    ARG_EPILOG(-1, VFSFcntlPragma_init_USAGE, Py_XDECREF(fast_kwnames));
  }
  self->strings = pointer;
  return 0;
}

/** .. attribute:: result
    :type: str | None

    The first element which becomes the result or error message
*/
static PyObject *
apswfcntl_pragma_get_result(apswfcntl_pragma *self)
{
  return convertutf8string(self->strings[0]);
}

static int
apswfcntl_pragma_set_result(apswfcntl_pragma *self, PyObject *value)
{
  if (!Py_IsNone(value) && !PyUnicode_Check(value))
  {
    PyErr_Format(PyExc_TypeError, "Expected None or str, not %s", Py_TypeName(value));
    return -1;
  }
  if (self->strings[0])
  {
    sqlite3_free(self->strings[0]);
    self->strings[0] = NULL;
  }
  if (!Py_IsNone(value))
  {
    const char *cstr = PyUnicode_AsUTF8(value);
    if (!cstr)
      return -1;
    self->strings[0] = sqlite3_mprintf("%s", cstr);
    if (!self->strings[0])
    {
      PyErr_NoMemory();
      return -1;
    }
  }
  return 0;
}

/** .. attribute:: name
    :type: str

    The name of the pragma
*/
static PyObject *
apswfcntl_pragma_get_name(apswfcntl_pragma *self)
{
  return convertutf8string(self->strings[1]);
}

/** .. attribute:: value
    :type: str | None

    The value for the pragma, if provided else None,
*/
static PyObject *
apswfcntl_pragma_get_value(apswfcntl_pragma *self)
{
  return convertutf8string(self->strings[2]);
}

static PyGetSetDef apswfcntl_pragma_getsetters[] = {
    {"result", (getter)apswfcntl_pragma_get_result, (setter)apswfcntl_pragma_set_result, VFSFcntlPragma_result_DOC},
    {"name", (getter)apswfcntl_pragma_get_name, NULL, VFSFcntlPragma_name_DOC},
    {"value", (getter)apswfcntl_pragma_get_value, NULL, VFSFcntlPragma_value_DOC},
    /* sentinel */
    {
        NULL, NULL, NULL, NULL}};

static PyTypeObject apswfcntl_pragma_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "apsw.VFSFcntlPragma",
    .tp_doc = VFSFcntlPragma_class_DOC,
    .tp_basicsize = sizeof(apswfcntl_pragma),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = apswfcntl_pragma_new,
    .tp_init = (initproc)apswfcntl_pragma_init,
    .tp_getset = apswfcntl_pragma_getsetters,
};

/* Naming convention prefixes.  Since sqlite3.c is #included alongside
   this file we have to ensure there is no clash with its names.
   There are two objects - the VFS itself and a VFSFile as returned
   from xOpen.  For each there are both C and Python methods.  The C
   methods are what SQLite calls and effectively turns a C call into a
   Python call.  The Python methods turn a Python call into the C call
   of the (SQLite C) object we are inheriting from and wouldn't be
   necessary if we didn't implement the inheritance feature.

   Methods:

   apswvfs_         sqlite3_vfs* functions https://sqlite.org/c3ref/vfs.html
   apswvfspy_       Python implementations of those same functions
   apswvfsfile_     io methods https://sqlite.org/c3ref/io_methods.html
   apswvfsfilepy_   Python implementations of those same functions

   Structures:

   APSWVFS          Python object for vfs (sqlite3_vfs * is used for sqlite object)
   APSWVFSType      Type object for above
   APSWVFSFile      Python object for vfs file
   APSWVFSFileType  Type object for above
   APSWSQLite3File  sqlite object for vfs file ("subclass" of sqlite3_file)
*/

/* what error code do we do for not implemented? */
#define VFSNOTIMPLEMENTED(x, v)                                                                            \
  if (!self->basevfs || self->basevfs->iVersion < v || !self->basevfs->x)                                  \
  {                                                                                                        \
    return PyErr_Format(ExcVFSNotImplemented, "VFSNotImplementedError: Method " #x " is not implemented"); \
  }

#define VFSFILENOTIMPLEMENTED(x, v)                                                                             \
  if (!self->base || self->base->pMethods->iVersion < v || !self->base->pMethods->x)                            \
  {                                                                                                             \
    return PyErr_Format(ExcVFSNotImplemented, "VFSNotImplementedError: File method " #x " is not implemented"); \
  }

/* various checks */
#define CHECKVFS \
  assert(vfs->pAppData);

#define CHECKVFSPY \
  assert(self->containingvfs->pAppData == self)

#define CHECKVFSFILE \
  assert(apswfile->file);

#define CHECKVFSFILEPY                                                                           \
  if (!self->base)                                                                               \
  {                                                                                              \
    return PyErr_Format(ExcVFSFileClosed, "VFSFileClosed: Attempting operation on closed file"); \
  }

#define VFSPREAMBLE               \
  PyGILState_STATE gilstate;      \
  gilstate = PyGILState_Ensure(); \
  MakeExistingException();        \
  CHAIN_EXC_BEGIN                 \
  CHECKVFS;

#define VFSPOSTAMBLE \
  CHAIN_EXC_END;     \
  PyGILState_Release(gilstate);

#define FILEPREAMBLE                                           \
  APSWSQLite3File *apswfile = (APSWSQLite3File *)(void *)file; \
  PyGILState_STATE gilstate;                                   \
  gilstate = PyGILState_Ensure();                              \
  MakeExistingException();                                     \
  CHAIN_EXC_BEGIN                                              \
  CHECKVFSFILE;

#define FILEPOSTAMBLE \
  CHAIN_EXC_END;      \
  PyGILState_Release(gilstate);

typedef struct
{
  PyObject_HEAD
      sqlite3_vfs *basevfs;   /* who we inherit from (might be null) */
  sqlite3_vfs *containingvfs; /* pointer given to sqlite for this instance */
  int registered;             /* are we currently registered? */
  int init_was_called;
} APSWVFS;

static PyTypeObject APSWVFSType;

typedef struct /* inherits */
{
  const struct sqlite3_io_methods *pMethods; /* structure sqlite needs */
  PyObject *file;
} APSWSQLite3File;

/* this is only used if there is inheritance */
typedef struct
{
  PyObject_HEAD struct sqlite3_file *base;
  /* filename has to be around for lifetime of base.  This will
     either be utf8 text (a string was passed in) or point
     to the filename in APSWURIFilename.  The former needs
     to be freed, the latter not.

     The format is a utf8 bytes, NULL, uri parameters, NULL */
  const char *filename;
  int free_filename; /* should filename be freed in destructor */
  /* If you add any new members then also initialize them in
     apswvfspy_xOpen() as that function does not call init because it
     has values already */
  int init_was_called;
} APSWVFSFile;

static PyTypeObject APSWVFSFileType;
static PyTypeObject APSWURIFilenameType;

static const struct sqlite3_io_methods apsw_io_methods_v1;
static const struct sqlite3_io_methods apsw_io_methods_v2;

typedef struct
{
  PyObject_HEAD const char *filename;
} APSWURIFilename;

/** .. class:: VFS

    Provides operating system access.  You can get an overview in the
    `SQLite documentation <https://sqlite.org/c3ref/vfs.html>`_.  To
    create a VFS your Python class must inherit from :class:`VFS`.

*/

/** .. method:: excepthook(etype: type[BaseException], evalue: BaseException, etraceback: Optional[types.TracebackType]) -> Any

    Called when there has been an exception in a :class:`VFS` routine,
    and it can't be reported to the caller as usual.

    The default implementation passes the exception information
    to sqlite3_log, and the first non-error of
    :func:`sys.unraisablehook` and :func:`sys.excepthook`, falling back to
    `PyErr_Display`.
*/
static PyObject *
apswvfs_excepthook(PyObject *Py_UNUSED(donotuseself), PyObject *args)
{
  /* NOTE: do not use the self argument as this function is used for
     both apswvfs and apswvfsfile.  If you need to use self then make
     two versions of the function. */
  assert(!PyErr_Occurred());
  PyObject *one = NULL, *two = NULL, *three = NULL;

  if (!PySequence_Check(args) || 3 != PySequence_Length(args))
    goto error;

  one = PySequence_GetItem(args, 0);
  if (!one)
    goto error;
  two = PySequence_GetItem(args, 1);
  if (!two)
    goto error;
  three = PySequence_GetItem(args, 2);
  if (!three)
    goto error;

  PyErr_Restore(one, two, three);

  apsw_write_unraisable(NULL);
  Py_RETURN_NONE;
error:
  PyErr_Clear();
  Py_XDECREF(one);
  Py_XDECREF(two);
  Py_XDECREF(three);
  return PyErr_Format(PyExc_ValueError, "Failed to process exception in excepthook");
}

static int
apswvfs_xDelete(sqlite3_vfs *vfs, const char *zName, int syncDir)
{
  PyObject *pyresult = NULL;
  int result = SQLITE_OK;

  VFSPREAMBLE;

  PyObject *vargs[] = {NULL, (PyObject *)(vfs->pAppData), PyUnicode_FromString(zName), PyBool_FromLong(syncDir)};
  if (vargs[2] && vargs[3])
    pyresult = PyObject_VectorcallMethod(apst.xDelete, vargs + 1, 3 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[2]);
  Py_XDECREF(vargs[3]);
  if (!pyresult)
  {
    result = MakeSqliteMsgFromPyException(NULL);
    if (result == SQLITE_IOERR_DELETE_NOENT)
      PyErr_Clear();
    else
      AddTraceBackHere(__FILE__, __LINE__, "vfs.xDelete", "{s: s, s: i}", "zName", zName, "syncDir", syncDir);
  }

  VFSPOSTAMBLE;
  return result;
}

/** .. method:: xDelete(filename: str, syncdir: bool) -> None

    Delete the named file. If the file is missing then raise an
    :exc:`IOError` exception with extendedresult
    *SQLITE_IOERR_DELETE_NOENT*

    :param filename: File to delete

    :param syncdir: If True then the directory should be synced
      ensuring that the file deletion has been recorded on the disk
      platters.  ie if there was an immediate power failure after this
      call returns, on a reboot the file would still be deleted.
*/
static PyObject *
apswvfspy_xDelete(APSWVFS *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *filename = NULL;
  int syncdir, res;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xDelete, 1);

  {
    VFS_xDelete_CHECK;
    ARG_PROLOG(2, VFS_xDelete_KWNAMES);
    ARG_MANDATORY ARG_str(filename);
    ARG_MANDATORY ARG_bool(syncdir);
    ARG_EPILOG(NULL, VFS_xDelete_USAGE, );
  }
  res = self->basevfs->xDelete(self->basevfs, filename, syncdir);

  if (res == SQLITE_OK)
    Py_RETURN_NONE;

  SET_EXC(res, NULL);
  return NULL;
}

static int
apswvfs_xAccess(sqlite3_vfs *vfs, const char *zName, int flags, int *pResOut)
{
  PyObject *pyresult = NULL;
  int result = SQLITE_OK;

  VFSPREAMBLE;

  PyObject *vargs[] = {NULL, (PyObject *)(vfs->pAppData), PyUnicode_FromString(zName), PyLong_FromLong(flags)};
  if (vargs[2] && vargs[3])
    pyresult = PyObject_VectorcallMethod(apst.xAccess, vargs + 1, 3 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[2]);
  Py_XDECREF(vargs[3]);
  if (!pyresult)
    goto finally;

  if (PyLong_Check(pyresult))
    *pResOut = !!PyLong_AsInt(pyresult);
  else
    PyErr_Format(PyExc_TypeError, "xAccess should return a number");

finally:
  if (PyErr_Occurred())
  {
    *pResOut = 0;
    result = MakeSqliteMsgFromPyException(NULL);
    AddTraceBackHere(__FILE__, __LINE__, "vfs.xAccess", "{s: s, s: i}", "zName", zName, "flags", flags);
  }

  VFSPOSTAMBLE;
  return result;
}

/** .. method:: xAccess(pathname: str, flags: int) -> bool

    SQLite wants to check access permissions.  Return True or False
    accordingly.

    :param pathname: File or directory to check
    :param flags: One of the `access flags <https://sqlite.org/c3ref/c_access_exists.html>`_
*/
static PyObject *
apswvfspy_xAccess(APSWVFS *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *pathname = NULL;
  int res, flags, resout = 0;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xAccess, 1);

  {
    VFS_xAccess_CHECK;
    ARG_PROLOG(2, VFS_xAccess_KWNAMES);
    ARG_MANDATORY ARG_str(pathname);
    ARG_MANDATORY ARG_int(flags);
    ARG_EPILOG(NULL, VFS_xAccess_USAGE, );
  }

  res = self->basevfs->xAccess(self->basevfs, pathname, flags, &resout);

  if (res == SQLITE_OK)
  {
    if (resout)
      Py_RETURN_TRUE;
    Py_RETURN_FALSE;
  }

  SET_EXC(res, NULL);
  return NULL;
}

static int
apswvfs_xFullPathname(sqlite3_vfs *vfs, const char *zName, int nOut, char *zOut)
{
  PyObject *pyresult = NULL;
  int result = SQLITE_ERROR;

  VFSPREAMBLE;

  PyObject *vargs[] = {NULL, (PyObject *)(vfs->pAppData), PyUnicode_FromString(zName)};
  if (vargs[2])
    pyresult = PyObject_VectorcallMethod(apst.xFullPathname, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[2]);

  if (!pyresult || !PyUnicode_Check(pyresult))
  {
    if (pyresult)
      PyErr_Format(PyExc_TypeError, "Expected a string");
    result = MakeSqliteMsgFromPyException(NULL);
    AddTraceBackHere(__FILE__, __LINE__, "vfs.xFullPathname", "{s: s, s: i}", "zName", zName, "nOut", nOut);
  }
  else if (PyUnicode_Check(pyresult))
  {
    const char *utf8;
    Py_ssize_t utf8len;

    utf8 = PyUnicode_AsUTF8AndSize(pyresult, &utf8len);
    if (!utf8)
    {
      result = SQLITE_ERROR;
      AddTraceBackHere(__FILE__, __LINE__, "vfs.xFullPathname", "{s: s, s: O}", "zName", zName, "result_from_python", OBJ(pyresult));
      goto finally;
    }
    /* nOut includes null terminator space (ie is mxPathname+1) */
    if (utf8len + 1 > nOut)
    {
      result = SQLITE_TOOBIG;
      SET_EXC(result, NULL);
      AddTraceBackHere(__FILE__, __LINE__, "vfs.xFullPathname", "{s: s, s: O, s: i}", "zName", zName, "result_from_python", OBJ(pyresult), "nOut", nOut);
      goto finally;
    }
    memcpy(zOut, utf8, utf8len + 1); /* Python always null terminates hence +1 */
    result = SQLITE_OK;
  }

finally:
  Py_XDECREF(pyresult);

  VFSPOSTAMBLE;
  return result;
}

/** .. method:: xFullPathname(name: str) -> str

  Return the absolute pathname for name.  You can use ``os.path.abspath`` to do this.
*/
static PyObject *
apswvfspy_xFullPathname(APSWVFS *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  char *resbuf = NULL;
  const char *name;
  PyObject *result = NULL;
  int res = SQLITE_NOMEM;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xFullPathname, 1);

  {
    VFS_xFullPathname_CHECK;
    ARG_PROLOG(1, VFS_xFullPathname_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_EPILOG(NULL, VFS_xFullPathname_USAGE, );
  }

  resbuf = PyMem_Calloc(1, self->basevfs->mxPathname + 1);
  if (resbuf)
  {
    res = self->basevfs->xFullPathname(self->basevfs, name, self->basevfs->mxPathname + 1, resbuf);
    if (PyErr_Occurred())
      res = MakeSqliteMsgFromPyException(NULL);
  }

  if (res == SQLITE_OK)
    result = convertutf8string(resbuf);

  if (!result)
    res = SQLITE_CANTOPEN;

  if (res != SQLITE_OK)
  {
    SET_EXC(res, NULL);
    AddTraceBackHere(__FILE__, __LINE__, "vfspy.xFullPathname", "{s: s, s: i, s: O}", "name", name, "res", res, "result", OBJ(result));
  }

  if (resbuf)
    PyMem_Free(resbuf);

  return result;
}

static int
apswvfs_xOpen(sqlite3_vfs *vfs, const char *zName, sqlite3_file *file, int inflags, int *pOutFlags)
{
  int result = SQLITE_CANTOPEN;
  PyObject *flags = NULL;
  PyObject *pyresult = NULL;
  APSWSQLite3File *apswfile = (APSWSQLite3File *)(void *)file;
  /* how we pass the name */
  PyObject *nameobject = NULL;

  VFSPREAMBLE;

  flags = PyList_New(2);
  if (!flags)
    goto finally;

  PyList_SET_ITEM(flags, 0, PyLong_FromLong(inflags));
  PyList_SET_ITEM(flags, 1, PyLong_FromLong(pOutFlags ? *pOutFlags : 0));
  if (PyErr_Occurred())
    goto finally;

  if (inflags & (SQLITE_OPEN_URI | SQLITE_OPEN_MAIN_DB))
  {
    nameobject = _PyObject_New(&APSWURIFilenameType);
    if (nameobject)
      ((APSWURIFilename *)nameobject)->filename = (char *)zName;
  }
  else
    nameobject = convertutf8string(zName);

  PyObject *vargs[] = {NULL, (PyObject *)(vfs->pAppData), nameobject, flags};
  if (vargs[2] && vargs[3])
    pyresult = PyObject_VectorcallMethod(apst.xOpen, vargs + 1, 3 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  /* issue 501 */
  if (inflags & (SQLITE_OPEN_URI | SQLITE_OPEN_MAIN_DB) && nameobject)
    ((APSWURIFilename *)nameobject)->filename = 0;
  if (!pyresult)
  {
    result = MakeSqliteMsgFromPyException(NULL);
    goto finally;
  }

  if (!PyList_Check(flags) || PyList_GET_SIZE(flags) != 2 || !PyLong_Check(PyList_GET_ITEM(flags, 1)))
  {
    PyErr_Format(PyExc_TypeError, "Flags should be two item list with item zero being integer input and item one being integer output");
    AddTraceBackHere(__FILE__, __LINE__, "vfs.xOpen", "{s: s, s: i, s: i}", "zName", zName, "inflags", inflags, "flags", flags);
    goto finally;
  }

  if (pOutFlags)
    *pOutFlags = PyLong_AsInt(PyList_GET_ITEM(flags, 1));
  if (PyErr_Occurred())
    goto finally;

  /* If we are inheriting from another file object, and that file
     object supports version 2 io_methods (Shm* family of functions)
     then we need to allocate an io_methods dupe of our own and fill
     in their shm methods. */
  if (PyObject_IsInstance(pyresult, (PyObject *)&APSWVFSFileType))
  {
    APSWVFSFile *f = (APSWVFSFile *)pyresult;
    if (!f->base || !f->base->pMethods || !f->base->pMethods->xShmMap)
      goto version1;
    apswfile->pMethods = &apsw_io_methods_v2;
  }
  else
  {
  version1:
    apswfile->pMethods = &apsw_io_methods_v1;
  }

  apswfile->file = Py_NewRef(pyresult);
  result = SQLITE_OK;

finally:
  assert(PyErr_Occurred() ? (result != SQLITE_OK) : 1);
  Py_XDECREF(pyresult);
  Py_XDECREF(flags);
  Py_XDECREF(nameobject);

  VFSPOSTAMBLE;

  return result;
}

/** .. method:: xOpen(name: Optional[str | URIFilename], flags: list[int,int]) -> VFSFile

    This method should return a new file object based on name.  You
    can return a :class:`VFSFile` from a completely different VFS.

    :param name: File to open.  Note that *name* may be *None* in which
        case you should open a temporary file with a name of your
        choosing.  May be an instance of :class:`URIFilename`.

    :param flags: A list of two integers ``[inputflags,
      outputflags]``.  Each integer is one or more of the `open flags
      <https://sqlite.org/c3ref/c_open_autoproxy.html>`_ binary orred
      together.  The ``inputflags`` tells you what SQLite wants.  For
      example *SQLITE_OPEN_DELETEONCLOSE* means the file should
      be automatically deleted when closed.  The ``outputflags``
      describes how you actually did open the file.  For example if you
      opened it read only then *SQLITE_OPEN_READONLY* should be
      set.


*/
static PyObject *
apswvfspy_xOpen(APSWVFS *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  sqlite3_file *file = NULL;
  int flagsout = 0;
  int flagsin = 0;
  int res;

  PyObject *name = NULL, *flags = NULL, *result = NULL;
  APSWVFSFile *apswfile = NULL;
  const char *filename = NULL;
  int free_filename = 1;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xOpen, 1);

  {
    VFS_xOpen_CHECK;
    ARG_PROLOG(2, VFS_xOpen_KWNAMES);
    ARG_MANDATORY ARG_optional_str_URIFilename(name);
    ARG_MANDATORY ARG_List_int_int(flags);
    ARG_EPILOG(NULL, VFS_xOpen_USAGE, );
  }

  if (Py_IsNone(name))
  {
    filename = NULL;
  }
  else if (name->ob_type == &APSWURIFilenameType)
  {
    filename = ((APSWURIFilename *)name)->filename;
    free_filename = 0;
  }
  else
  {
    const char *utf8 = PyUnicode_AsUTF8(name);
    if (!utf8)
      goto finally;
    filename = apsw_strdup(utf8);
    if (!filename)
      goto finally;
  }

  flagsout = PyLong_AsInt(PyList_GET_ITEM(flags, 1));
  flagsin = PyLong_AsInt(PyList_GET_ITEM(flags, 0));
  if (PyErr_Occurred())
    goto finally;

  file = PyMem_Calloc(1, self->basevfs->szOsFile);
  if (!file)
    goto finally;

  res = self->basevfs->xOpen(self->basevfs, filename, file, flagsin, &flagsout);

  MakeExistingException();

  if (PyErr_Occurred())
    goto finally;
  if (res != SQLITE_OK)
  {
    SET_EXC(res, NULL);
    goto finally;
  }

  PyList_SetItem(flags, 1, PyLong_FromLong(flagsout));
  if (PyErr_Occurred())
    goto finally;

  apswfile = (APSWVFSFile *)_PyObject_New(&APSWVFSFileType);
  if (!apswfile)
    goto finally;
  apswfile->base = file;
  apswfile->filename = filename;
  apswfile->free_filename = free_filename;
  filename = NULL;
  file = NULL;
  result = (PyObject *)apswfile;

finally:
  if (file)
    PyMem_Free(file);
  if (free_filename)
    PyMem_Free((void *)filename);
  return result;
}

static void *
apswvfs_xDlOpen(sqlite3_vfs *vfs, const char *zName)
{
  PyObject *pyresult = NULL;
  void *result = NULL;

  VFSPREAMBLE;

  PyObject *vargs[] = {NULL, (PyObject *)(vfs->pAppData), PyUnicode_FromString(zName)};
  if (vargs[2])
    pyresult = PyObject_VectorcallMethod(apst.xDlOpen, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[2]);
  if (pyresult)
  {
    if (PyLong_Check(pyresult) && PyLong_AsDouble(pyresult) >= 0)
      result = PyLong_AsVoidPtr(pyresult);
    else
      PyErr_Format(PyExc_TypeError, "Pointer returned must be int and non-negative");
  }
  if (PyErr_Occurred())
  {
    result = NULL;
    AddTraceBackHere(__FILE__, __LINE__, "vfs.xDlOpen", "{s: s, s: O}", "zName", zName, "result", OBJ(pyresult));
  }

  Py_XDECREF(pyresult);
  VFSPOSTAMBLE;
  return result;
}

/** .. method:: xDlOpen(filename: str) -> int

   Load the shared library. You should return a number which will be
   treated as a void pointer at the C level. On error you should
   return 0 (NULL). The number is passed as is to
   :meth:`~VFS.xDlSym`/:meth:`~VFS.xDlClose` so it can represent
   anything that is convenient for you (eg an index into an
   array). You can use ctypes to load a library::

     def xDlOpen(name: str):
        return ctypes.cdll.LoadLibrary(name)._handle

*/
static PyObject *
apswvfspy_xDlOpen(APSWVFS *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *filename = NULL;
  void *res;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xDlOpen, 1);
  {
    VFS_xDlOpen_CHECK;
    ARG_PROLOG(1, VFS_xDlOpen_KWNAMES);
    ARG_MANDATORY ARG_str(filename);
    ARG_EPILOG(NULL, VFS_xDlOpen_USAGE, );
  }
  res = self->basevfs->xDlOpen(self->basevfs, filename);

  return PyErr_Occurred() ? NULL : PyLong_FromVoidPtr(res);
}

static void (*apswvfs_xDlSym(sqlite3_vfs *vfs, void *handle, const char *zName))(void)
{
  PyObject *pyresult = NULL;
  void *result = NULL;

  VFSPREAMBLE;

  PyObject *vargs[] = {NULL, (PyObject *)(vfs->pAppData), PyLong_FromVoidPtr(handle), PyUnicode_FromString(zName)};
  if (vargs[2] && vargs[3])
    pyresult = PyObject_VectorcallMethod(apst.xDlSym, vargs + 1, 3 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[2]);
  Py_XDECREF(vargs[3]);
  if (pyresult)
  {
    if (PyLong_Check(pyresult))
      result = PyLong_AsVoidPtr(pyresult);
    else
      PyErr_Format(PyExc_TypeError, "Pointer returned must be int");
  }
  if (PyErr_Occurred())
  {
    result = NULL;
    AddTraceBackHere(__FILE__, __LINE__, "vfs.xDlSym", "{s: s, s: O}", "zName", zName, "result", OBJ(pyresult));
  }

  Py_XDECREF(pyresult);
  VFSPOSTAMBLE;
  return result;
}

/** .. method:: xDlSym(handle: int, symbol: str) -> int

    Returns the address of the named symbol which will be called by
    SQLite. On error you should return 0 (NULL). You can use ctypes::

      def xDlSym(ptr: int, name: str):
         return _ctypes.dlsym (ptr, name)  # Linux/Unix/Mac etc (note leading underscore)
         return ctypes.win32.kernel32.GetProcAddress (ptr, name)  # Windows

    :param handle: The value returned from an earlier :meth:`~VFS.xDlOpen` call
    :param symbol: A string
*/
static PyObject *
apswvfspy_xDlSym(APSWVFS *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *symbol = NULL;
  void *res = NULL;
  void *handle = NULL;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xDlSym, 1);

  {
    VFS_xDlSym_CHECK;
    ARG_PROLOG(2, VFS_xDlSym_KWNAMES);
    ARG_MANDATORY ARG_pointer(handle);
    ARG_MANDATORY ARG_str(symbol);
    ARG_EPILOG(NULL, VFS_xDlSym_USAGE, );
  }
  res = self->basevfs->xDlSym(self->basevfs, handle, symbol);

  MakeExistingException();

  if (PyErr_Occurred())
  {
    AddTraceBackHere(__FILE__, __LINE__, "vfspy.xDlSym", "{s: s}", "symbol", symbol);
    return NULL;
  }

  return PyLong_FromVoidPtr(res);
}

static void
apswvfs_xDlClose(sqlite3_vfs *vfs, void *handle)
{
  PyObject *pyresult = NULL;
  VFSPREAMBLE;

  PyObject *vargs[] = {NULL, (PyObject *)(vfs->pAppData), PyLong_FromVoidPtr(handle)};
  if (vargs[2])
    pyresult = PyObject_VectorcallMethod(apst.xDlClose, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);

  if (PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "vfs.xDlClose", "{s: O}", "ptr", OBJ(vargs[2]));
  Py_XDECREF(vargs[2]);
  Py_XDECREF(pyresult);
  VFSPOSTAMBLE;
}

/** .. method:: xDlClose(handle: int) -> None

    Close and unload the library corresponding to the handle you
    returned from :meth:`~VFS.xDlOpen`.  You can use ctypes to do
    this::

      def xDlClose(handle: int):
         # Note leading underscore in _ctypes
         _ctypes.dlclose(handle)       # Linux/Mac/Unix
         _ctypes.FreeLibrary(handle)   # Windows
*/
static PyObject *
apswvfspy_xDlClose(APSWVFS *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  void *handle = NULL;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xDlClose, 1);

  {
    VFS_xDlClose_CHECK;
    ARG_PROLOG(1, VFS_xDlClose_KWNAMES);
    ARG_MANDATORY ARG_pointer(handle);
    ARG_EPILOG(NULL, VFS_xDlClose_USAGE, );
  }
  self->basevfs->xDlClose(self->basevfs, handle);

  MakeExistingException();

  if (PyErr_Occurred())
  {
    AddTraceBackHere(__FILE__, __LINE__, "vfspy.xDlClose", "{s: K}", "handle", (unsigned long long)handle);
    return NULL;
  }

  Py_RETURN_NONE;
}

static void
apswvfs_xDlError(sqlite3_vfs *vfs, int nByte, char *zErrMsg)
{
  PyObject *pyresult = NULL;
  VFSPREAMBLE;

  PyObject *vargs[] = {NULL, (PyObject *)(vfs->pAppData)};
  if (PyObject_HasAttr(vargs[1], apst.xDlError))
    pyresult = PyObject_VectorcallMethod(apst.xDlError, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);

  if (pyresult && !Py_IsNone(pyresult))
  {
    if (PyUnicode_Check(pyresult))
    {
      const char *utf8;
      Py_ssize_t utf8len;

      utf8 = PyUnicode_AsUTF8AndSize(pyresult, &utf8len);
      if (utf8)
      {
        if (utf8len > (Py_ssize_t)nByte - 1)
          utf8len = (Py_ssize_t)nByte - 1;
        memcpy(zErrMsg, utf8, utf8len);
        zErrMsg[utf8len] = 0;
      }
    }
    else
      PyErr_Format(PyExc_TypeError, "xDlError must return a string");
  }

  if (PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "vfs.xDlError", NULL);

  Py_XDECREF(pyresult);
  VFSPOSTAMBLE;
}

/** .. method:: xDlError() -> str

    Return an error string describing the last error of
    :meth:`~VFS.xDlOpen` or :meth:`~VFS.xDlSym` (ie they returned
    zero/NULL). If you do not supply this routine then SQLite provides
    a generic message. To implement this method, catch exceptions in
    :meth:`~VFS.xDlOpen` or :meth:`~VFS.xDlSym`, turn them into
    strings, save them, and return them in this routine.  If you have
    an error in this routine or return None then SQLite's generic
    message will be used.
*/
static PyObject *
apswvfspy_xDlError(APSWVFS *self)
{
  PyObject *res = NULL;
  PyObject *unicode = NULL;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xDlError, 1);

  res = PyBytes_FromStringAndSize(NULL, 512 + self->basevfs->mxPathname);
  if (res)
  {
    memset(PyBytes_AS_STRING(res), 0, PyBytes_GET_SIZE(res));
    self->basevfs->xDlError(self->basevfs, PyBytes_GET_SIZE(res), PyBytes_AS_STRING(res));
  }

  if (PyErr_Occurred())
  {
    AddTraceBackHere(__FILE__, __LINE__, "vfspy.xDlError", NULL);
    Py_XDECREF(res);
    return NULL;
  }

  /* did they make a message? */
  if (strlen(PyBytes_AS_STRING(res)) == 0)
  {
    Py_DECREF(res);
    Py_RETURN_NONE;
  }

  /* turn into unicode */
  unicode = convertutf8string(PyBytes_AS_STRING(res));
  if (unicode)
  {
    Py_DECREF(res);
    return unicode;
  }

  AddTraceBackHere(__FILE__, __LINE__, "vfspy.xDlError", "{s: O, s: O}", "self", self, "res", OBJ(res));
  Py_DECREF(res);
  return NULL;
}

static int
apswvfs_xRandomness(sqlite3_vfs *vfs, int nByte, char *zOut)
{
  PyObject *pyresult = NULL;
  int result = 0;
  VFSPREAMBLE;

  PyObject *vargs[] = {NULL, (PyObject *)(vfs->pAppData), PyLong_FromLong(nByte)};
  if (vargs[2])
    pyresult = PyObject_VectorcallMethod(apst.xRandomness, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[2]);

  if (pyresult && !Py_IsNone(pyresult))
  {
    int asrb;
    Py_buffer py3buffer;
    Py_ssize_t len;

    asrb = PyObject_GetBufferContiguous(pyresult, &py3buffer, PyBUF_SIMPLE);
    if (asrb == 0)
    {
      len = py3buffer.len;
      if (len > nByte)
        len = nByte;
      memcpy(zOut, py3buffer.buf, len);
      result = len;
      PyBuffer_Release(&py3buffer);
    }
    else
      assert(PyErr_Occurred());
  }

  if (PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "vfs.xRandomness", "{s: i, s: O}", "nByte", nByte, "result", OBJ(pyresult));

  Py_XDECREF(pyresult);
  VFSPOSTAMBLE;
  return result;
}

/** .. method:: xRandomness(numbytes: int) -> bytes

  This method is called once on the default VFS when SQLite needs to
  seed the random number generator.  You can return less than the
  number of bytes requested including None. If you return more then
  the surplus is ignored.

*/
static PyObject *
apswvfspy_xRandomness(APSWVFS *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *res = NULL;
  int numbytes = 0;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xRandomness, 1);

  {
    VFS_xRandomness_CHECK;
    ARG_PROLOG(1, VFS_xRandomness_KWNAMES);
    ARG_MANDATORY ARG_int(numbytes);
    ARG_EPILOG(NULL, VFS_xRandomness_USAGE, );
  }
  if (numbytes < 0)
    return PyErr_Format(PyExc_ValueError, "You can't have negative amounts of randomness!");

  res = PyBytes_FromStringAndSize(NULL, numbytes);
  if (res)
  {
    int amt = self->basevfs->xRandomness(self->basevfs, PyBytes_GET_SIZE(res), PyBytes_AS_STRING(res));
    if (amt < numbytes)
      _PyBytes_Resize(&res, amt);
    MakeExistingException();
  }

  if (PyErr_Occurred())
  {
    AddTraceBackHere(__FILE__, __LINE__, "vfspy.xRandomness", "{s: i}", "numbytes", numbytes);
    Py_XDECREF(res);
    return NULL;
  }

  return res;
}

/* return the number of microseconds that the underlying OS was requested to sleep for. */
static int
apswvfs_xSleep(sqlite3_vfs *vfs, int microseconds)
{
  PyObject *pyresult = NULL;
  int result = 0;

  VFSPREAMBLE;
  PyObject *vargs[] = {NULL, (PyObject *)(vfs->pAppData), PyLong_FromLong(microseconds)};
  if (vargs[2])
    pyresult = PyObject_VectorcallMethod(apst.xSleep, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[2]);
  if (pyresult)
  {
    if (PyLong_Check(pyresult))
      result = PyLong_AsInt(pyresult);
    else
      PyErr_Format(PyExc_TypeError, "You should return a number from sleep");
  }

  if (PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "vfs.xSleep", "{s: i, s: O}", "microseconds", microseconds, "result", OBJ(pyresult));

  Py_XDECREF(pyresult);
  VFSPOSTAMBLE;
  return result;
}

/** .. method:: xSleep(microseconds: int) -> int

    Pause execution of the thread for at least the specified number of
    microseconds (millionths of a second).  This routine is typically called from the busy handler.

    :returns: How many microseconds you actually requested the
      operating system to sleep for. For example if your operating
      system sleep call only takes seconds then you would have to have
      rounded the microseconds number up to the nearest second and
      should return that rounded up value.
*/
static PyObject *
apswvfspy_xSleep(APSWVFS *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int microseconds = 0;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xSleep, 1);

  {
    VFS_xSleep_CHECK;
    ARG_PROLOG(1, VFS_xSleep_KWNAMES);
    ARG_MANDATORY ARG_int(microseconds);
    ARG_EPILOG(NULL, VFS_xSleep_USAGE, );
  }
  return PyLong_FromLong(self->basevfs->xSleep(self->basevfs, microseconds));
}

static int
apswvfs_xCurrentTime(sqlite3_vfs *vfs, double *julian)
{
  PyObject *pyresult = NULL;
  int result = 0;
  VFSPREAMBLE;

  PyObject *vargs[] = {NULL, (PyObject *)(vfs->pAppData)};
  pyresult = PyObject_VectorcallMethod(apst.xCurrentTime, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);

  if (pyresult)
    *julian = PyFloat_AsDouble(pyresult);

  if (PyErr_Occurred())
  {
    AddTraceBackHere(__FILE__, __LINE__, "vfs.xCurrentTime", "{s: O}", "result", OBJ(pyresult));
    result = 1;
  }

  Py_XDECREF(pyresult);
  VFSPOSTAMBLE;
  return result;
}

/** .. method:: xCurrentTime()  -> float

  Return the `Julian Day Number
  <https://en.wikipedia.org/wiki/Julian_day>`_ as a floating point
  number where the integer portion is the day and the fractional part
  is the time.
*/
static PyObject *
apswvfspy_xCurrentTime(APSWVFS *self)
{
  int res;
  double julian = 0;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xCurrentTime, 1);

  res = self->basevfs->xCurrentTime(self->basevfs, &julian);

  if (res != 0)
  {
    SET_EXC(SQLITE_ERROR, NULL); /* general sqlite error code */
    AddTraceBackHere(__FILE__, __LINE__, "vfspy.xCurrentTime", NULL);
    return NULL;
  }

  return PyFloat_FromDouble(julian);
}

static int
apswvfs_xCurrentTimeInt64(sqlite3_vfs *vfs, sqlite3_int64 *time)
{
  PyObject *pyresult = NULL;
  int result = 0;
  VFSPREAMBLE;

  PyObject *vargs[] = {NULL, (PyObject *)(vfs->pAppData)};
  pyresult = PyObject_VectorcallMethod(apst.xCurrentTimeInt64, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);

  if (pyresult)
    *time = PyLong_AsLongLong(pyresult);

  if (PyErr_Occurred())
  {
    AddTraceBackHere(__FILE__, __LINE__, "vfs.xCurrentTimeInt64", "{s: O}", "result", OBJ(pyresult));
    result = 1;
  }

  Py_XDECREF(pyresult);
  VFSPOSTAMBLE;
  return result;
}

/** .. method:: xCurrentTimeInt64()  -> int

  Returns as an integer the `Julian Day Number
  <https://en.wikipedia.org/wiki/Julian_day>`__ multiplied by 86400000
  (the number of milliseconds in a 24-hour day).
*/
static PyObject *
apswvfspy_xCurrentTimeInt64(APSWVFS *self)
{
  int res;
  sqlite3_int64 time;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xCurrentTimeInt64, 1);

  res = self->basevfs->xCurrentTimeInt64(self->basevfs, &time);

  if (res != 0)
  {
    SET_EXC(SQLITE_ERROR, NULL); /* general sqlite error code */
    AddTraceBackHere(__FILE__, __LINE__, "vfspy.xCurrentTimeInt64", NULL);
    return NULL;
  }

  return PyLong_FromLongLong(time);
}

static int
apswvfs_xGetLastError(sqlite3_vfs *vfs, int nByte, char *zErrMsg)
{
  PyObject *pyresult = NULL, *item0 = NULL, *item1 = NULL;
  int res = -1;

  VFSPREAMBLE;

  /* Ensure null termination */
  if (nByte > 0 && zErrMsg)
    *zErrMsg = 0;

  PyObject *vargs[] = {NULL, (PyObject *)(vfs->pAppData)};
  if (PyObject_HasAttr(vargs[1], apst.xGetLastError))
    pyresult = PyObject_VectorcallMethod(apst.xGetLastError, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);

  if (!pyresult || !PySequence_Check(pyresult) || 2 != PySequence_Length(pyresult))
  {
    if (!PyErr_Occurred())
      PyErr_Format(PyExc_TypeError, "xGetLastError must return two item sequence (int, None or str)");
    goto end;
  }

  item0 = PySequence_GetItem(pyresult, 0);
  if (item0)
    item1 = PySequence_GetItem(pyresult, 1);

  if (!item0 || !item1)
  {
    assert(PyErr_Occurred());
    goto end;
  }

  if (!PyLong_Check(item0))
  {
    PyErr_Format(PyExc_TypeError, "First last error item must be a number");
    goto end;
  }

  res = PyLong_AsInt(item0);
  if (PyErr_Occurred())
    goto end;

  if (Py_IsNone(item1))
    goto end;

  if (!PyUnicode_Check(item1))
  {
    PyErr_Format(PyExc_TypeError, "xGetLastError return second item must be None or str");
    goto end;
  }
  {
    const char *utf8;
    Py_ssize_t utf8len;
    utf8 = PyUnicode_AsUTF8AndSize(item1, &utf8len);
    if (utf8)
    {
      /* Get size includes trailing null */
      size_t len = utf8len;
      if (zErrMsg && len > 0)
      {
        if (len > (size_t)nByte)
          len = (size_t)nByte;
        memcpy(zErrMsg, utf8, len);
        zErrMsg[len - 1] = 0;
      }
    }
  }

end:
  if (PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "vfs.xGetLastError", "{s:O}", "pyresult", OBJ(pyresult));

  Py_XDECREF(pyresult);
  Py_XDECREF(item0);
  Py_XDECREF(item1);
  VFSPOSTAMBLE;
  return res;
}

/** .. method:: xGetLastError() -> tuple[int, str]

  Return an integer error code and (optional) text describing
  the last error code and message that happened in this thread.

*/
static PyObject *
apswvfspy_xGetLastError(APSWVFS *self)
{
  PyObject *res = NULL, *text = NULL;
  int errval;
  size_t msglen;
  const size_t size = 1024;
  char *buffer = NULL;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xGetLastError, 1);

  /* the plus one is to ensure it is always null terminated */
  buffer = (char *)sqlite3_malloc64(size + 1);
  if (!buffer)
  {
    PyErr_NoMemory();
    goto error;
  }
  memset(buffer, 0, size + 1);

  errval = self->basevfs->xGetLastError(self->basevfs, (int)size, buffer);

  msglen = strnlen(buffer, size);
  if (msglen > 0)
  {
    text = PyUnicode_FromStringAndSize(buffer, msglen);
    if (!text)
      goto error;
  }
  else
  {
    text = Py_NewRef(Py_None);
  }

  res = PyTuple_New(2);
  if (!res)
    goto error;

  PyTuple_SET_ITEM(res, 0, PyLong_FromLong(errval));
  PyTuple_SET_ITEM(res, 1, text);
  if (PyErr_Occurred())
    goto error;

  sqlite3_free(buffer);

  return res;

error:
  assert(PyErr_Occurred());
  sqlite3_free(buffer);
  AddTraceBackHere(__FILE__, __LINE__, "vfspy.xGetLastError", "{s: O, s: i}", "self", self, "size", (int)size);
  Py_XDECREF(text);
  Py_XDECREF(res);
  return NULL;
}

static int
apswvfs_xSetSystemCall(sqlite3_vfs *vfs, const char *zName, sqlite3_syscall_ptr call)
{
  int res = SQLITE_OK;
  PyObject *pyresult = NULL;

  VFSPREAMBLE;

  PyObject *vargs[] = {NULL, (PyObject *)(vfs->pAppData), PyUnicode_FromString(zName), PyLong_FromVoidPtr(call)};
  if (vargs[2] && vargs[3])
    pyresult = PyObject_VectorcallMethod(apst.xSetSystemCall, vargs + 1, 3 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[2]);
  Py_XDECREF(vargs[3]);
  if (!pyresult)
    res = MakeSqliteMsgFromPyException(NULL);

  if (res == SQLITE_NOTFOUND)
    PyErr_Clear();

  if (PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "vfs.xSetSystemCall", "{s: O}", "pyresult", OBJ(pyresult));

  Py_XDECREF(pyresult);
  VFSPOSTAMBLE;
  return res;
}

/** .. method:: xSetSystemCall(name: Optional[str], pointer: int) -> bool

    Change a system call used by the VFS.  This is useful for testing
    and some other scenarios such as sandboxing.

    :param name: The string name of the system call

    :param pointer: A pointer provided as an int.  There is no
      reference counting or other memory tracking of the pointer.  If
      you provide one you need to ensure it is around for the lifetime
      of this and any other related VFS.

    Raise an exception to return an error.  If the system call does
    not exist then raise :exc:`NotFoundError`.

    If `name` is None, then all systemcalls are reset to their defaults.

    :returns: True if the system call was set.  False if the system
      call is not known.
*/
static PyObject *
apswvfspy_xSetSystemCall(APSWVFS *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *name = 0;
  void *pointer = NULL;
  int res = -7; /* initialization to stop compiler whining */

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xSetSystemCall, 3);

  {
    VFS_xSetSystemCall_CHECK;
    ARG_PROLOG(2, VFS_xSetSystemCall_KWNAMES);
    ARG_MANDATORY ARG_optional_str(name);
    ARG_MANDATORY ARG_pointer(pointer);
    ARG_EPILOG(NULL, VFS_xSetSystemCall_USAGE, );
  }

  res = self->basevfs->xSetSystemCall(self->basevfs, name, pointer);
  if (res != SQLITE_OK && res != SQLITE_NOTFOUND)
    SET_EXC(res, NULL);

  if (PyErr_Occurred())
  {
    AddTraceBackHere(__FILE__, __LINE__, "vfspy.xSetSystemCall", "{s: s, s: i}", "name", name, "res", res);
    return NULL;
  }

  assert(res == SQLITE_OK || res == SQLITE_NOTFOUND);

  if (res == SQLITE_OK)
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

static sqlite3_syscall_ptr
apswvfs_xGetSystemCall(sqlite3_vfs *vfs, const char *zName)
{
  sqlite3_syscall_ptr ptr = NULL;
  PyObject *pyresult = NULL;

  VFSPREAMBLE;
  PyObject *vargs[] = {NULL, (PyObject *)(vfs->pAppData), PyUnicode_FromString(zName)};
  if (vargs[2])
  {
    pyresult = PyObject_VectorcallMethod(apst.xGetSystemCall, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
    Py_DECREF(vargs[2]);
    if (pyresult)
    {
      if (PyLong_Check(pyresult))
        ptr = PyLong_AsVoidPtr(pyresult);
      else
        PyErr_Format(PyExc_TypeError, "Pointer must be int/long");
    }
  }
  if (PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "vfs.xGetSystemCall", "{s:O}", "pyresult", OBJ(pyresult));

  Py_XDECREF(pyresult);
  VFSPOSTAMBLE;
  return ptr;
}

/** .. method:: xGetSystemCall(name: str) -> Optional[int]

    Returns a pointer for the current method implementing the named
    system call.  Return None if the call does not exist.

*/
static PyObject *
apswvfspy_xGetSystemCall(APSWVFS *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *name;
  sqlite3_syscall_ptr ptr;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xGetSystemCall, 3);
  {
    VFS_xGetSystemCall_CHECK;
    ARG_PROLOG(1, VFS_xGetSystemCall_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_EPILOG(NULL, VFS_xGetSystemCall_USAGE, );
  }
  ptr = self->basevfs->xGetSystemCall(self->basevfs, name);

  if (ptr)
    return PyLong_FromVoidPtr(ptr);
  if (PyErr_Occurred())
    return NULL;
  Py_RETURN_NONE;
}

static const char *
apswvfs_xNextSystemCall(sqlite3_vfs *vfs, const char *zName)
{
  PyObject *pyresult = NULL;
  const char *res = NULL;

  VFSPREAMBLE;
  PyObject *vargs[] = {NULL, (PyObject *)(vfs->pAppData), PyUnicode_FromString(zName)};
  if (vargs[2])
    pyresult = PyObject_VectorcallMethod(apst.xNextSystemCall, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[2]);
  if (pyresult && !Py_IsNone(pyresult))
  {
    if (PyUnicode_Check(pyresult))
    {
      PyUnicode_InternInPlace(&pyresult);
      res = PyUnicode_AsUTF8(pyresult);
    }
    else
      PyErr_Format(PyExc_TypeError, "You must return a string or None");
  }

  if (PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "vfs.xNextSystemCall", "{s:O}", "pyresult", OBJ(pyresult));

  Py_XDECREF(pyresult);
  VFSPOSTAMBLE;
  return res;
}

/** .. method:: xNextSystemCall(name: Optional[str]) -> Optional[str]

    This method is repeatedly called to iterate over all of the system
    calls in the vfs.  When called with None you should return the
    name of the first system call.  In subsequent calls return the
    name after the one passed in.  If name is the last system call
    then return None.

*/
static PyObject *
apswvfspy_xNextSystemCall(APSWVFS *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *name = NULL, *zName;
  PyObject *res = NULL;

  CHECKVFSPY;
  VFSNOTIMPLEMENTED(xNextSystemCall, 3);

  {
    VFS_xNextSystemCall_CHECK;
    ARG_PROLOG(1, VFS_xNextSystemCall_KWNAMES);
    ARG_MANDATORY ARG_optional_str(name);
    ARG_EPILOG(NULL, VFS_xNextSystemCall_USAGE, );
  }

  zName = self->basevfs->xNextSystemCall(self->basevfs, name);
  if (PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "vfspy.xNextSystemCall", "{s:s}", "name", name);
  else if (zName)
    res = convertutf8string(zName);
  else
    res = Py_NewRef(Py_None);

  return res;
}

/** .. method:: unregister() -> None

   Unregisters the VFS making it unavailable to future database
   opens. You do not need to call this as the VFS is automatically
   unregistered by when the VFS has no more references or open
   databases using it. It is however useful to call if you have made
   your VFS be the default and wish to immediately make it be
   unavailable. It is safe to call this routine multiple times.

   -* sqlite3_vfs_unregister
*/
static PyObject *
apswvfspy_unregister(APSWVFS *self)
{
  CHECKVFSPY;

  if (self->registered)
  {
    /* although it is undocumented by sqlite, we assume that an
         unregister failure always results in an unregister and so
         continue freeing the data structures.  we memset everything
         to zero so there will be a coredump should this behaviour
         change.  the sqlite code doesn't return
         anything except ok anyway. */
    int res = sqlite3_vfs_unregister(self->containingvfs);
    self->registered = 0;
    if (res)
    {
      SET_EXC(res, NULL);
      return NULL;
    }
  }
  Py_RETURN_NONE;
}

static void
APSWVFS_dealloc(APSWVFS *self)
{
  if (self->basevfs && self->basevfs->xAccess == apswvfs_xAccess)
  {
    Py_DECREF((PyObject *)self->basevfs->pAppData);
  }

  if (self->containingvfs)
  {
    PyObject *xx;

    /* not allowed to clobber existing exception */
    PY_ERR_FETCH(exc_save);
    xx = apswvfspy_unregister(self);
    Py_XDECREF(xx);

    if (PyErr_Occurred())
      apsw_write_unraisable(NULL);
    PY_ERR_RESTORE(exc_save);

    /* some cleanups */
    self->containingvfs->pAppData = NULL;
    PyMem_Free((void *)(self->containingvfs->zName));
    /* zero it out so any attempt to use results in core dump */
    memset(self->containingvfs, 0, sizeof(sqlite3_vfs));
    PyMem_Free(self->containingvfs);
    self->containingvfs = NULL;
  }

  self->basevfs = self->containingvfs = NULL;

  Py_TpFree((PyObject *)self);
}

static PyObject *
APSWVFS_new(PyTypeObject *type, PyObject *Py_UNUSED(args), PyObject *Py_UNUSED(kwds))
{
  APSWVFS *self;
  self = (APSWVFS *)type->tp_alloc(type, 0);
  if (self)
  {
    self->basevfs = NULL;
    self->containingvfs = NULL;
    self->registered = 0;
  }
  return (PyObject *)self;
}

/** .. method:: __init__(name: str, base: Optional[str] = None, makedefault: bool = False, maxpathname: int = 1024, *, iVersion: int = 3, exclude: Optional[set[str]] = None)

    :param name: The name to register this vfs under.  If the name
        already exists then this vfs will replace the prior one of the
        same name.  Use :meth:`apsw.vfs_names` to get a list of
        registered vfs names.

    :param base: If you would like to inherit behaviour from an already registered vfs then give
        their name.  To inherit from the default vfs, use a zero
        length string ``""`` as the name.

    :param makedefault: If true then this vfs will be registered as the default, and will be
        used by any opens that don't specify a vfs.

    :param maxpathname: The maximum length of database name in bytes when
        represented in UTF-8.  If a pathname is passed in longer than
        this value then SQLite will not`be able to open it.  If you are
        using a base, then a value of zero will use the value from base.

    :param iVersion: Version number for the `sqlite3_vfs <https://sqlite.org/c3ref/vfs.html>`__
        structure.

    :param exclude: A set of strings, naming the methods that will be filled in with ``NULL`` in the `sqlite3_vfs
        <https://sqlite.org/c3ref/vfs.html>`__  structure to indicate to SQLite that they are
        not supported.

    -* sqlite3_vfs_register sqlite3_vfs_find
*/
static int
APSWVFS_init(APSWVFS *self, PyObject *args, PyObject *kwargs)
{
  const char *base = NULL, *name = NULL;
  int makedefault = 0, maxpathname = 1024, res, iVersion = 3;
  PyObject *exclude = NULL;

  {
    VFS_init_CHECK;
    PREVENT_INIT_MULTIPLE_CALLS;
    ARG_CONVERT_VARARGS_TO_FASTCALL;
    ARG_PROLOG(4, VFS_init_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_OPTIONAL ARG_optional_str(base);
    ARG_OPTIONAL ARG_bool(makedefault);
    ARG_OPTIONAL ARG_int(maxpathname);
    ARG_OPTIONAL ARG_int(iVersion);
    ARG_OPTIONAL ARG_optional_set(exclude);
    ARG_EPILOG(-1, VFS_init_USAGE, Py_XDECREF(fast_kwnames));
  }

  if (iVersion < 1 || iVersion > 3)
  {
    PyErr_Format(PyExc_ValueError, "apsw only supports VFS iVersion of 1, 2 and 3, not %d", iVersion);
    goto error;
  }

  if (base)
  {
    int baseversion;
    if (!strlen(base))
    {
      base = NULL;
    }
    self->basevfs = sqlite3_vfs_find(base);
    if (!self->basevfs)
    {
      PyErr_Format(PyExc_ValueError, "Base vfs named \"%s\" not found", base ? base : "<default>");
      goto error;
    }
    baseversion = self->basevfs->iVersion;
    APSW_FAULT_INJECT(APSWVFSBadVersion, , baseversion = -789426);
    if (baseversion < 1 || baseversion > 3)
    {
      PyErr_Format(PyExc_ValueError, "Base vfs implements version %d of vfs spec, but apsw only supports versions 1, 2 and 3", baseversion);
      goto error;
    }
  }

  self->containingvfs = (sqlite3_vfs *)PyMem_Calloc(1, sizeof(sqlite3_vfs));
  if (!self->containingvfs)
    return -1;
  self->containingvfs->iVersion = iVersion;
  self->containingvfs->szOsFile = sizeof(APSWSQLite3File);
  if (self->basevfs && !maxpathname)
    self->containingvfs->mxPathname = self->basevfs->mxPathname;
  else
    self->containingvfs->mxPathname = maxpathname;
  self->containingvfs->zName = apsw_strdup(name);
  if (!self->containingvfs->zName)
    goto error;
  self->containingvfs->pAppData = self;
#define METHOD(meth)                                  \
  do                                                  \
  {                                                   \
    int include = 1;                                  \
    if (exclude)                                      \
    {                                                 \
      if (1 == PySet_Contains(exclude, apst.x##meth)) \
        include = 0;                                  \
    }                                                 \
    if (include)                                      \
      self->containingvfs->x##meth = apswvfs_x##meth; \
  } while (0)

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
  METHOD(CurrentTimeInt64);
  METHOD(SetSystemCall);
  METHOD(GetSystemCall);
  METHOD(NextSystemCall);
#undef METHOD
  res = sqlite3_vfs_register(self->containingvfs, makedefault);

  if (res == SQLITE_OK)
  {
    self->registered = 1;
    if (self->basevfs && self->basevfs->xAccess == apswvfs_xAccess)
    {
      Py_INCREF((PyObject *)self->basevfs->pAppData);
    }
    return 0;
  }

  SET_EXC(res, NULL);

error:
  if (self->containingvfs && self->containingvfs->zName)
    PyMem_Free((void *)(self->containingvfs->zName));
  if (self->containingvfs)
    PyMem_Free(self->containingvfs);
  self->containingvfs = NULL;
  return -1;
}

static PyObject *
APSWVFS_tp_str(APSWVFS *self)
{
  if (!self->containingvfs)
    return PyUnicode_FromFormat("<apsw.VFS object at %p>", self);
  if (self->basevfs)
    return PyUnicode_FromFormat("<apsw.VFS object \"%s\" inherits from \"%s\" at %p>", self->containingvfs->zName, self->basevfs->zName, self);
  return PyUnicode_FromFormat("<apsw.VFS object \"%s\" at %p>", self->containingvfs->zName, self);
}

static PyMethodDef APSWVFS_methods[] = {
    {"xDelete", (PyCFunction)apswvfspy_xDelete, METH_FASTCALL | METH_KEYWORDS, VFS_xDelete_DOC},
    {"xFullPathname", (PyCFunction)apswvfspy_xFullPathname, METH_FASTCALL | METH_KEYWORDS, VFS_xFullPathname_DOC},
    {"xOpen", (PyCFunction)apswvfspy_xOpen, METH_FASTCALL | METH_KEYWORDS, VFS_xOpen_DOC},
    {"xAccess", (PyCFunction)apswvfspy_xAccess, METH_FASTCALL | METH_KEYWORDS, VFS_xAccess_DOC},
    {"xDlOpen", (PyCFunction)apswvfspy_xDlOpen, METH_FASTCALL | METH_KEYWORDS, VFS_xDlOpen_DOC},
    {"xDlSym", (PyCFunction)apswvfspy_xDlSym, METH_FASTCALL | METH_KEYWORDS, VFS_xDlSym_DOC},
    {"xDlClose", (PyCFunction)apswvfspy_xDlClose, METH_FASTCALL | METH_KEYWORDS, VFS_xDlClose_DOC},
    {"xDlError", (PyCFunction)apswvfspy_xDlError, METH_NOARGS, VFS_xDlError_DOC},
    {"xRandomness", (PyCFunction)apswvfspy_xRandomness, METH_FASTCALL | METH_KEYWORDS, VFS_xRandomness_DOC},
    {"xSleep", (PyCFunction)apswvfspy_xSleep, METH_FASTCALL | METH_KEYWORDS, VFS_xSleep_DOC},
    {"xCurrentTime", (PyCFunction)apswvfspy_xCurrentTime, METH_NOARGS, VFS_xCurrentTime_DOC},
    {"xCurrentTimeInt64", (PyCFunction)apswvfspy_xCurrentTimeInt64, METH_NOARGS, VFS_xCurrentTimeInt64_DOC},
    {"xGetLastError", (PyCFunction)apswvfspy_xGetLastError, METH_NOARGS, VFS_xGetLastError_DOC},
    {"xSetSystemCall", (PyCFunction)apswvfspy_xSetSystemCall, METH_FASTCALL | METH_KEYWORDS, VFS_xSetSystemCall_DOC},
    {"xGetSystemCall", (PyCFunction)apswvfspy_xGetSystemCall, METH_FASTCALL | METH_KEYWORDS, VFS_xGetSystemCall_DOC},
    {"xNextSystemCall", (PyCFunction)apswvfspy_xNextSystemCall, METH_FASTCALL | METH_KEYWORDS, VFS_xNextSystemCall_DOC},
    {"unregister", (PyCFunction)apswvfspy_unregister, METH_NOARGS, VFS_unregister_DOC},
    {"excepthook", (PyCFunction)apswvfs_excepthook, METH_VARARGS, VFS_excepthook_DOC},
    /* Sentinel */
    {0, 0, 0, 0}};

static PyTypeObject APSWVFSType =
    {
        PyVarObject_HEAD_INIT(NULL, 0)
            .tp_name = "apsw.VFS",
        .tp_basicsize = sizeof(APSWVFS),
        .tp_dealloc = (destructor)APSWVFS_dealloc,
        .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
        .tp_doc = VFS_class_DOC,
        .tp_methods = APSWVFS_methods,
        .tp_init = (initproc)APSWVFS_init,
        .tp_new = APSWVFS_new,
        .tp_str = (reprfunc)APSWVFS_tp_str,
};

static int is_apsw_vfs(sqlite3_vfs *vfs)
{
#define M(n) (vfs->n == NULL || vfs->n == apswvfs_##n)
  return vfs->iVersion >= 1 && M(xOpen) && M(xDelete) && M(xAccess) && M(xFullPathname) && M(xDlOpen) && M(xDlError) && M(xDlSym) && M(xDlClose) && M(xRandomness) && M(xSleep) && M(xCurrentTime) && M(xGetLastError);
#undef M
}

/** .. class:: VFSFile

    Wraps access to a file.  You only need to derive from this class
    if you want the file object returned from :meth:`VFS.xOpen` to
    inherit from an existing VFS implementation.

*/

/** .. method:: excepthook(etype: type[BaseException], evalue: BaseException, etraceback: Optional[types.TracebackType]) ->None

    Called when there has been an exception in a :class:`VFSFile`
    routine, and it can't be reported to the caller as usual.

    The default implementation passes the exception information
    to sqlite3_log, and the first non-error of
    :func:`sys.unraisablehook` and :func:`sys.excepthook`, falling back to
    `PyErr_Display`.
*/

static PyObject *apswvfsfilepy_xClose(APSWVFSFile *self);

static void
APSWVFSFile_dealloc(APSWVFSFile *self)
{
  PY_ERR_FETCH(exc_save);

  if (self->base)
  {
    /* close it */
    PyObject *x = apswvfsfilepy_xClose(self);
    Py_XDECREF(x);
  }
  if (self->free_filename)
    PyMem_Free((void *)(self->filename));

  if (PyErr_Occurred())
  {
    AddTraceBackHere(__FILE__, __LINE__, "APSWVFS File destructor", NULL);
    apsw_write_unraisable(NULL);
  }
  Py_TpFree((PyObject *)self);

  PY_ERR_RESTORE(exc_save);
}

static PyObject *
APSWVFSFile_new(PyTypeObject *type, PyObject *Py_UNUSED(args), PyObject *Py_UNUSED(kwds))
{
  APSWVFSFile *self;
  self = (APSWVFSFile *)type->tp_alloc(type, 0);
  if (self)
  {
    self->base = NULL;
    self->filename = NULL;
    self->free_filename = 1;
  }

  return (PyObject *)self;
}

/** .. method:: __init__(vfs: str, filename: str | URIFilename | None, flags: list[int,int])

    :param vfs: The vfs you want to inherit behaviour from.  You can
       use an empty string ``""`` to inherit from the default vfs.
    :param name: The name of the file being opened.  May be an instance of :class:`URIFilename`.
    :param flags: A two item list ``[inflags, outflags]`` as detailed in :meth:`VFS.xOpen`.

    :raises ValueError: If the named VFS is not registered.

    .. note::

      If the VFS that you inherit from supports :ref:`write ahead
      logging <wal>` then your :class:`VFSFile` will also support the
      xShm methods necessary to implement wal.

    .. seealso::

      :meth:`VFS.xOpen`
*/
static int
APSWVFSFile_init(APSWVFSFile *self, PyObject *args, PyObject *kwargs)
{
  const char *vfs = NULL;
  PyObject *flags = NULL, *pyflagsin = NULL, *pyflagsout = NULL, *filename = NULL;
  int xopenresult = -1;
  int res = -1; /* error */
  int flagsin;
  int flagsout = 0;

  sqlite3_vfs *vfstouse = NULL;
  sqlite3_file *file = NULL;

  {
    VFSFile_init_CHECK;
    PREVENT_INIT_MULTIPLE_CALLS;
    ARG_CONVERT_VARARGS_TO_FASTCALL;
    ARG_PROLOG(3, VFSFile_init_KWNAMES);
    ARG_MANDATORY ARG_str(vfs);
    ARG_MANDATORY ARG_pyobject(filename);
    ARG_MANDATORY ARG_List_int_int(flags);
    ARG_EPILOG(-1, VFSFile_init_USAGE, Py_XDECREF(fast_kwnames));
  }

  if (Py_TYPE(filename) == &APSWURIFilenameType)
  {
    self->filename = ((APSWURIFilename *)filename)->filename;
    self->free_filename = 0;
  }
  else if (PyUnicode_Check(filename))
  {
    const char *text = PyUnicode_AsUTF8(filename);
    if (!text)
      return -1;
    self->filename = apsw_strdup(text);
    if (!self->filename)
      return -1;
  }
  else if (Py_IsNone(filename))
  {
    self->filename = NULL;
  }
  else
  {
    PyErr_Format(PyExc_TypeError, "filename should be a string");
    return -1;
  }

  if (0 == strlen(vfs))
  {
    /* sqlite uses null for default vfs - we use empty string */
    vfs = NULL;
  }

  pyflagsin = PyList_GetItem(flags, 0);
  if (!pyflagsin)
    goto finally;

  flagsin = PyLong_AsInt(pyflagsin);
  if (PyErr_Occurred())
    goto finally;

  vfstouse = sqlite3_vfs_find(vfs);
  if (!vfstouse)
  {
    PyErr_Format(PyExc_ValueError, "Unknown vfs \"%s\"", vfs);
    goto finally;
  }
  file = PyMem_Calloc(1, vfstouse->szOsFile);
  if (!file)
    goto finally;

  if (0 != Py_EnterRecursiveCall(" instantiating APSWVFSFile"))
    goto finally;
  xopenresult = vfstouse->xOpen(vfstouse, self->filename, file, (int)flagsin, &flagsout);
  Py_LeaveRecursiveCall();

  SET_EXC(xopenresult, NULL);
  MakeExistingException();

  if (PyErr_Occurred())
    goto finally;

  pyflagsout = PyLong_FromLong(flagsout);
  if (!pyflagsout)
    goto finally;

  if (-1 == PyList_SetItem(flags, 1, pyflagsout))
  {
    Py_DECREF(pyflagsout);
    goto finally;
  }

  if (PyErr_Occurred())
    goto finally;

  self->base = (sqlite3_file *)(void *)file;
  res = 0;

finally:
  if (PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "vfsfile.init", "{s: s, s: O, s: O}", "vfs", vfs, "filename", filename, "flags", flags);

  if (res != 0 && file)
  {
    if (xopenresult == SQLITE_OK)
      CHAIN_EXC(file->pMethods->xClose(file));

    PyMem_Free(file);
  }

  assert((res == 0 && !PyErr_Occurred()) || (res != 0 && PyErr_Occurred()));
  return res;
}

static int
apswvfsfile_xRead(sqlite3_file *file, void *bufout, int amount, sqlite3_int64 offset)
{
  int result = SQLITE_ERROR;
  PyObject *pybuf = NULL;
  int asrb = -1;
  Py_buffer py3buffer;

  FILEPREAMBLE;

  PyObject *vargs[] = {NULL, apswfile->file, PyLong_FromLong(amount), PyLong_FromLongLong(offset)};
  if (vargs[2] && vargs[3])
    pybuf = PyObject_VectorcallMethod(apst.xRead, vargs + 1, 3 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[2]);
  Py_XDECREF(vargs[3]);
  if (!pybuf)
  {
    assert(PyErr_Occurred());
    result = MakeSqliteMsgFromPyException(NULL);
    goto finally;
  }
  if (!PyObject_CheckBuffer(pybuf))
  {
    PyErr_Format(PyExc_TypeError, "Object returned from xRead should be buffer (bytes etc)");
    goto finally;
  }

  asrb = PyObject_GetBufferContiguous(pybuf, &py3buffer, PyBUF_SIMPLE);
  if (asrb != 0)
  {
    assert(PyErr_Occurred());
    goto finally;
  }

  if (py3buffer.len < amount)
  {
    result = SQLITE_IOERR_SHORT_READ;
    memset(bufout, 0, amount);
    memcpy(bufout, py3buffer.buf, py3buffer.len);
  }
  else
  {
    memcpy(bufout, py3buffer.buf, amount);
    result = SQLITE_OK;
  }

finally:
  if (PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile_xRead", "{s: i, s: L, s: O}", "amount", amount, "offset", offset, "result", OBJ(pybuf));
  if (asrb == 0)
    PyBuffer_Release(&py3buffer);
  Py_XDECREF(pybuf);
  FILEPOSTAMBLE;
  return result;
}

/** .. method:: xRead(amount: int, offset: int) -> bytes

    Read the specified *amount* of data starting at *offset*. You
    should make every effort to read all the data requested, or return
    an error. If you have the file open for non-blocking I/O or if
    signals happen then it is possible for the underlying operating
    system to do a partial read. You will need to request the
    remaining data. Except for empty files SQLite considers short
    reads to be a fatal error.

    :param amount: Number of bytes to read
    :param offset: Where to start reading.
*/
static PyObject *
apswvfsfilepy_xRead(APSWVFSFile *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int amount;
  sqlite3_int64 offset;
  int res;
  PyObject *buffy = NULL;

  CHECKVFSFILEPY;
  VFSFILENOTIMPLEMENTED(xRead, 1);

  {
    VFSFile_xRead_CHECK;
    ARG_PROLOG(2, VFSFile_xRead_KWNAMES);
    ARG_MANDATORY ARG_int(amount);
    ARG_MANDATORY ARG_int64(offset);
    ARG_EPILOG(NULL, VFSFile_xRead_USAGE, );
  }

  buffy = PyBytes_FromStringAndSize(NULL, amount);
  if (!buffy)
    return NULL;

  res = self->base->pMethods->xRead(self->base, PyBytes_AS_STRING(buffy), amount, offset);

  if (res == SQLITE_OK)
    return buffy;

  if (res == SQLITE_IOERR_SHORT_READ)
  {
    /* We don't know how short the read was, so look for first
         non-trailing null byte.  */
    while (amount && PyBytes_AS_STRING(buffy)[amount - 1] == 0)
      amount--;
    if (_PyBytes_Resize(&buffy, amount))
      return NULL;

    return buffy;
  }

  Py_DECREF(buffy);

  SET_EXC(res, NULL);
  return NULL;
}

static int
apswvfsfile_xWrite(sqlite3_file *file, const void *buffer, int amount, sqlite3_int64 offset)
{
  PyObject *pyresult = NULL, *pybuf = NULL;
  int result = SQLITE_OK;
  FILEPREAMBLE;

  /* Performance opportunity: We currently duplicate the buffer passed
     by SQLite which involvces a memory copy.  A memoryview could be used
     instead but the underlying buffer passed by SQLite goes out of scope
     after this function returns.  Sp we'd have to detect the callee
     hanging on to the memoryview. */
  PyObject *vargs[] = {NULL, apswfile->file, PyBytes_FromStringAndSize(buffer, amount), PyLong_FromLongLong(offset)};
  if (vargs[2] && vargs[3])
    pyresult = PyObject_VectorcallMethod(apst.xWrite, vargs + 1, 3 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[2]);
  Py_XDECREF(vargs[3]);

  if (!pyresult)
  {
    assert(PyErr_Occurred());
    result = MakeSqliteMsgFromPyException(NULL);
    AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile_xWrite", "{s: i, s: L, s: O}", "amount", amount, "offset", offset, "data", OBJ(pybuf));
  }
  Py_XDECREF(pyresult);
  FILEPOSTAMBLE;
  return result;
}

/** .. method:: xWrite(data: bytes, offset: int) -> None

  Write the *data* starting at absolute *offset*. You must write all the data
  requested, or return an error. If you have the file open for
  non-blocking I/O or if signals happen then it is possible for the
  underlying operating system to do a partial write. You will need to
  write the remaining data.

  :param offset: Where to start writing.
*/

static PyObject *
apswvfsfilepy_xWrite(APSWVFSFile *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  sqlite3_int64 offset;
  int res;
  Py_buffer data_buffer;
  PyObject *data;

  CHECKVFSFILEPY;
  VFSFILENOTIMPLEMENTED(xWrite, 1);

  {
    VFSFile_xWrite_CHECK;
    ARG_PROLOG(2, VFSFile_xWrite_KWNAMES);
    ARG_MANDATORY ARG_py_buffer(data);
    ARG_MANDATORY ARG_int64(offset);
    ARG_EPILOG(NULL, VFSFile_xWrite_USAGE, );
  }

  if (0 != PyObject_GetBufferContiguous(data, &data_buffer, PyBUF_SIMPLE))
  {
    assert(PyErr_Occurred());
    return NULL;
  }

  res = self->base->pMethods->xWrite(self->base, data_buffer.buf, data_buffer.len, offset);

  PyBuffer_Release(&data_buffer);

  if (res == SQLITE_OK)
    Py_RETURN_NONE;

  SET_EXC(res, NULL);
  return NULL;
}

static int
apswvfsfile_xUnlock(sqlite3_file *file, int flag)
{
  int result = SQLITE_ERROR;
  PyObject *pyresult = NULL;
  FILEPREAMBLE;

  PyObject *vargs[] = {NULL, apswfile->file, PyLong_FromLong(flag)};
  if (vargs[2])
    pyresult = PyObject_VectorcallMethod(apst.xUnlock, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[2]);
  if (!pyresult)
  {
    result = MakeSqliteMsgFromPyException(NULL);
    AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile.xUnlock", "{s: i}", "flag", flag);
  }
  else
    result = SQLITE_OK;

  Py_XDECREF(pyresult);
  FILEPOSTAMBLE;
  return result;
}

/** .. method:: xUnlock(level: int) -> None

    Decrease the lock to the level specified which is one of the
    `SQLITE_LOCK <https://sqlite.org/c3ref/c_lock_exclusive.html>`_
    family of constants.
*/
static PyObject *
apswvfsfilepy_xUnlock(APSWVFSFile *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int level, res;

  CHECKVFSFILEPY;
  VFSFILENOTIMPLEMENTED(xUnlock, 1);

  {
    VFSFile_xUnlock_CHECK;
    ARG_PROLOG(1, VFSFile_xUnlock_KWNAMES);
    ARG_MANDATORY ARG_int(level);
    ARG_EPILOG(NULL, VFSFile_xUnlock_USAGE, );
  }
  res = self->base->pMethods->xUnlock(self->base, level);

  APSW_FAULT_INJECT(xUnlockFails, , res = SQLITE_IOERR);

  if (res == SQLITE_OK)
    Py_RETURN_NONE;

  SET_EXC(res, NULL);
  return NULL;
}

static int
apswvfsfile_xLock(sqlite3_file *file, int flag)
{
  int result = SQLITE_ERROR;
  PyObject *pyresult = NULL;
  FILEPREAMBLE;

  PyObject *vargs[] = {NULL, apswfile->file, PyLong_FromLong(flag)};
  if (vargs[2])
    pyresult = PyObject_VectorcallMethod(apst.xLock, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[2]);
  if (!pyresult)
  {
    result = MakeSqliteMsgFromPyException(NULL);
    /* a busy exception is normal so we clear it */
    if (SQLITE_BUSY == (result & 0xff))
      PyErr_Clear();
    else
      AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile.xLock", "{s: i}", "level", flag);
  }
  else
    result = SQLITE_OK;

  Py_XDECREF(pyresult);
  FILEPOSTAMBLE;
  return result;
}

/** .. method:: xLock(level: int) -> None

  Increase the lock to the level specified which is one of the
  `SQLITE_LOCK <https://sqlite.org/c3ref/c_lock_exclusive.html>`_
  family of constants. If you can't increase the lock level because
  someone else has locked it, then raise :exc:`BusyError`.
*/
static PyObject *
apswvfsfilepy_xLock(APSWVFSFile *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int level, res;

  CHECKVFSFILEPY;
  VFSFILENOTIMPLEMENTED(xLock, 1);

  {
    VFSFile_xLock_CHECK;
    ARG_PROLOG(1, VFSFile_xLock_KWNAMES);
    ARG_MANDATORY ARG_int(level);
    ARG_EPILOG(NULL, VFSFile_xLock_USAGE, );
  }

  res = self->base->pMethods->xLock(self->base, level);

  if (res == SQLITE_OK)
    Py_RETURN_NONE;

  SET_EXC(res, NULL);
  return NULL;
}

static int
apswvfsfile_xTruncate(sqlite3_file *file, sqlite3_int64 size)
{
  int result = SQLITE_ERROR;
  PyObject *pyresult = NULL;
  FILEPREAMBLE;
  PyObject *vargs[] = {NULL, apswfile->file, PyLong_FromLongLong(size)};
  if (vargs[2])
    pyresult = PyObject_VectorcallMethod(apst.xTruncate, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[2]);
  if (!pyresult)
  {
    result = MakeSqliteMsgFromPyException(NULL);
    AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile.xTruncate", "{s: L}", "size", size);
  }
  else
    result = SQLITE_OK;

  Py_XDECREF(pyresult);
  FILEPOSTAMBLE;
  return result;
}

/** .. method:: xTruncate(newsize: int) -> None

  Set the file length to *newsize* (which may be more or less than the
  current length).
*/
static PyObject *
apswvfsfilepy_xTruncate(APSWVFSFile *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int res;
  sqlite3_int64 newsize;

  CHECKVFSFILEPY;
  VFSFILENOTIMPLEMENTED(xTruncate, 1);

  {
    VFSFile_xTruncate_CHECK;
    ARG_PROLOG(1, VFSFile_xTruncate_KWNAMES);
    ARG_MANDATORY ARG_int64(newsize);
    ARG_EPILOG(NULL, VFSFile_xTruncate_USAGE, );
  }
  res = self->base->pMethods->xTruncate(self->base, newsize);

  if (res == SQLITE_OK)
    Py_RETURN_NONE;

  SET_EXC(res, NULL);
  return NULL;
}

static int
apswvfsfile_xSync(sqlite3_file *file, int flags)
{
  int result = SQLITE_ERROR;
  PyObject *pyresult = NULL;
  FILEPREAMBLE;

  PyObject *vargs[] = {NULL, apswfile->file, PyLong_FromLong(flags)};
  if (vargs[2])
    pyresult = PyObject_VectorcallMethod(apst.xSync, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  if (!pyresult)
  {
    result = MakeSqliteMsgFromPyException(NULL);
    AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile.xSync", "{s: i}", "flags", flags);
  }
  else
    result = SQLITE_OK;

  Py_XDECREF(pyresult);
  FILEPOSTAMBLE;
  return result;
}

/** .. method:: xSync(flags: int) -> None

  Ensure data is on the disk platters (ie could survive a power
  failure immediately after the call returns) with the `sync flags
  <https://sqlite.org/c3ref/c_sync_dataonly.html>`_ detailing what
  needs to be synced.
*/
static PyObject *
apswvfsfilepy_xSync(APSWVFSFile *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int flags, res;

  CHECKVFSFILEPY;
  VFSFILENOTIMPLEMENTED(xSync, 1);
  {
    VFSFile_xSync_CHECK;
    ARG_PROLOG(1, VFSFile_xSync_KWNAMES);
    ARG_MANDATORY ARG_int(flags);
    ARG_EPILOG(NULL, VFSFile_xSync_USAGE, );
  }
  res = self->base->pMethods->xSync(self->base, flags);

  APSW_FAULT_INJECT(xSyncFails, , res = SQLITE_IOERR);

  if (res == SQLITE_OK)
    Py_RETURN_NONE;

  SET_EXC(res, NULL);
  return NULL;
}

static int
apswvfsfile_xSectorSize(sqlite3_file *file)
{
  int result = 4096;
  PyObject *pyresult = NULL;
  FILEPREAMBLE;

  PyObject *vargs[] = {NULL, apswfile->file};
  pyresult = PyObject_VectorcallMethod(apst.xSectorSize, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  if (!pyresult)
    result = MakeSqliteMsgFromPyException(NULL);
  else if (!Py_IsNone(pyresult))
  {
    if (PyLong_Check(pyresult))
      result = PyLong_AsInt(pyresult); /* returns -1 on error/overflow */
    else
      PyErr_Format(PyExc_TypeError, "xSectorSize should return a number");
  }

  /* We can't return errors so use unraisable */
  if (PyErr_Occurred())
  {
    AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile_xSectorSize", NULL);
    result = 4096; /* could be -1 as stated above */
  }

  Py_XDECREF(pyresult);
  FILEPOSTAMBLE;
  return result;
}

/** .. method:: xSectorSize() -> int

    Return the native underlying sector size. SQLite uses the value
    returned in determining the default database page size. If you do
    not implement the function or have an error then 4096 (the SQLite
    default) is returned.
*/
static PyObject *
apswvfsfilepy_xSectorSize(APSWVFSFile *self)
{
  int res = 4096;

  CHECKVFSFILEPY;
  VFSFILENOTIMPLEMENTED(xSectorSize, 1);

  res = self->base->pMethods->xSectorSize(self->base);

  return PyErr_Occurred() ? NULL : PyLong_FromLong(res);
}

static int
apswvfsfile_xDeviceCharacteristics(sqlite3_file *file)
{
  int result = 0;
  PyObject *pyresult = NULL;
  FILEPREAMBLE;

  if (PyObject_HasAttr(apswfile->file, apst.xDeviceCharacteristics))
  {
    PyObject *vargs[] = {NULL, apswfile->file};
    pyresult = PyObject_VectorcallMethod(apst.xDeviceCharacteristics, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
    if (!pyresult)
      result = MakeSqliteMsgFromPyException(NULL);
    else if (!Py_IsNone(pyresult))
    {
      if (PyLong_Check(pyresult))
        result = PyLong_AsInt(pyresult); /* sets to -1 on error */
      else
        PyErr_Format(PyExc_TypeError, "xDeviceCharacteristics should return a number");
    }

    /* We can't return errors so use unraisable */
    if (PyErr_Occurred())
    {
      AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile_xDeviceCharacteristics", "{s: O}", "result", OBJ(pyresult));
      apsw_write_unraisable(apswfile->file);
      result = 0; /* harmless value for error cases */
    }

    Py_XDECREF(pyresult);
  }
  FILEPOSTAMBLE;
  return result;
}

/** .. method:: xDeviceCharacteristics() -> int

  Return `I/O capabilities
  <https://sqlite.org/c3ref/c_iocap_atomic.html>`_ (bitwise or of
  appropriate values). If you do not implement the function or have an
  error then 0 (the SQLite default) is returned.
*/
static PyObject *
apswvfsfilepy_xDeviceCharacteristics(APSWVFSFile *self)
{
  int res = 0;

  CHECKVFSFILEPY;
  VFSFILENOTIMPLEMENTED(xDeviceCharacteristics, 1);

  res = self->base->pMethods->xDeviceCharacteristics(self->base);

  return PyLong_FromLong(res);
}

static int
apswvfsfile_xFileSize(sqlite3_file *file, sqlite3_int64 *pSize)
{
  int result = SQLITE_OK;
  PyObject *pyresult = NULL;
  FILEPREAMBLE;

  PyObject *vargs[] = {NULL, apswfile->file};
  pyresult = PyObject_VectorcallMethod(apst.xFileSize, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);

  if (!pyresult)
    result = MakeSqliteMsgFromPyException(NULL);
  else if (PyLong_Check(pyresult))
    *pSize = PyLong_AsLongLong(pyresult);
  else
    PyErr_Format(PyExc_TypeError, "xFileSize should return a number");

  if (PyErr_Occurred())
  {
    result = MakeSqliteMsgFromPyException(NULL);
    AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile_xFileSize", "{s: O}", "result", OBJ(pyresult));
  }

  Py_XDECREF(pyresult);
  FILEPOSTAMBLE;
  return result;
}

/** .. method:: xFileSize() -> int

  Return the size of the file in bytes.
*/
static PyObject *
apswvfsfilepy_xFileSize(APSWVFSFile *self)
{
  sqlite3_int64 size;
  int res;

  CHECKVFSFILEPY;
  VFSFILENOTIMPLEMENTED(xFileSize, 1);
  res = self->base->pMethods->xFileSize(self->base, &size);

  APSW_FAULT_INJECT(xFileSizeFails, , res = SQLITE_IOERR);

  if (res != SQLITE_OK)
  {
    SET_EXC(res, NULL);
    return NULL;
  }
  return PyLong_FromLongLong(size);
}

static int
apswvfsfile_xCheckReservedLock(sqlite3_file *file, int *pResOut)
{
  int result = SQLITE_OK;
  PyObject *pyresult = NULL;
  FILEPREAMBLE;

  PyObject *vargs[] = {NULL, apswfile->file};
  pyresult = PyObject_VectorcallMethod(apst.xCheckReservedLock, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  if (!pyresult)
    result = MakeSqliteMsgFromPyException(NULL);
  else if (PyLong_Check(pyresult))
    *pResOut = !!PyLong_AsInt(pyresult);
  else
    PyErr_Format(PyExc_TypeError, "xCheckReservedLock should return a boolean/number");

  if (PyErr_Occurred())
  {
    result = MakeSqliteMsgFromPyException(NULL);
    AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile_xCheckReservedLock", "{s: O}", "result", OBJ(pyresult));
  }

  Py_XDECREF(pyresult);
  FILEPOSTAMBLE;
  return result;
}

/** .. method:: xCheckReservedLock() -> bool

  Returns True if any database connection (in this or another process)
  has a lock other than `SQLITE_LOCK_NONE or SQLITE_LOCK_SHARED
  <https://sqlite.org/c3ref/c_lock_exclusive.html>`_.
*/
static PyObject *
apswvfsfilepy_xCheckReservedLock(APSWVFSFile *self)
{
  int islocked;
  int res;

  CHECKVFSFILEPY;
  VFSFILENOTIMPLEMENTED(xCheckReservedLock, 1);

  res = self->base->pMethods->xCheckReservedLock(self->base, &islocked);

  APSW_FAULT_INJECT(xCheckReservedLockFails, , res = SQLITE_IOERR);

  if (res != SQLITE_OK)
  {
    SET_EXC(res, NULL);
    return NULL;
  }

  APSW_FAULT_INJECT(xCheckReservedLockIsTrue, , islocked = 1);

  if (islocked)
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

static int
apswvfsfile_xFileControl(sqlite3_file *file, int op, void *pArg)
{
  int result = SQLITE_ERROR;
  PyObject *pyresult = NULL;
  FILEPREAMBLE;

  /* Special handling of SQLITE_FCNTL_VFSNAME */
  if (op == SQLITE_FCNTL_VFSNAME)
  {
    /* see if there is a base to call first */
    if (PyObject_TypeCheck(apswfile->file, &APSWVFSFileType))
    {
      sqlite3_file *base = ((APSWVFSFile *)apswfile->file)->base;
      result = base->pMethods->xFileControl(base, op, pArg);
    }
    /* Use the classname */
    const char *name = Py_TYPE(apswfile->file)->tp_name;
    const char *modname = NULL;
    PyObject *qualname = NULL;

#if PY_VERSION_HEX >= 0x030b0000
    qualname = PyType_GetQualName(Py_TYPE(apswfile->file));
    if (qualname && PyUnicode_Check(qualname))
    {
      const char *tmp_name = PyUnicode_AsUTF8(qualname);
      if (tmp_name)
        name = tmp_name;
    }
#endif

    PyErr_Clear();

    PyObject *module = PyObject_GetAttrString((PyObject *)Py_TYPE(apswfile->file), "__module__");
    if (module && PyUnicode_Check(module))
    {
      modname = PyUnicode_AsUTF8(module);
      PyErr_Clear();
    }

    /* the above calls could have exceptions but they aren't useful,
       so ignore */
    PyErr_Clear();

    char *new_val = sqlite3_mprintf("%s%s%s%s%s",
                                    modname ? modname : "",
                                    modname ? "." : "",
                                    name,
                                    (*(char **)pArg) ? "/" : "",
                                    (*(char **)pArg) ? *(char **)pArg : "");

    /* done with the strings, so can free now */
    Py_XDECREF(module);
    Py_XDECREF(qualname);

    if (new_val)
    {
      if (*(char **)pArg)
        sqlite3_free(*(char **)pArg);

      *(char **)pArg = new_val;
    }
    result = SQLITE_OK;
    goto end;
  }

  PyObject *vargs[] = {NULL, apswfile->file, PyLong_FromLong(op), PyLong_FromVoidPtr(pArg)};
  if (vargs[2] && vargs[3])
    pyresult = PyObject_VectorcallMethod(apst.xFileControl, vargs + 1, 3 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[2]);
  Py_XDECREF(vargs[3]);
  if (!pyresult)
    result = MakeSqliteMsgFromPyException(NULL);
  else
  {
    if (!Py_IsTrue(pyresult) && !Py_IsFalse(pyresult))
    {
      PyErr_Format(PyExc_TypeError, "xFileControl must return True or False");
      result = SQLITE_ERROR;
    }
    else
      result = Py_IsTrue(pyresult) ? SQLITE_OK : SQLITE_NOTFOUND;
  }

  Py_XDECREF(pyresult);
end:
  FILEPOSTAMBLE;
  return result;
}

/** .. method:: xFileControl(op: int, ptr: int) -> bool

   Receives `file control
   <https://sqlite.org/c3ref/file_control.html>`_ request typically
   issued by :meth:`Connection.file_control`.  See
   :meth:`Connection.file_control` for an example of how to pass a
   Python object to this routine.

   :param op: A numeric code.  Codes below 100 are reserved for SQLite
     internal use.
   :param ptr: An integer corresponding to a pointer at the C level.

   :returns: A boolean indicating if the op was understood

   Ensure you pass any unrecognised codes through to your super class.
   For example::

       def xFileControl(self, op: int, ptr: int) -> bool:
           if op == 1027:
               process_quick(ptr)
           elif op == 1028:
               obj=ctypes.py_object.from_address(ptr).value
           else:
               # this ensures superclass implementation is called
               return super().xFileControl(op, ptr)
          # we understood the op
          return True

  .. note::

    `SQLITE_FCNTL_VFSNAME
    <https://sqlite.org/c3ref/c_fcntl_begin_atomic_write.html#sqlitefcntlvfsname>`__
    is automatically handled for you dealing with the necessary memory allocation
    and listing all the VFS if you are inheriting.  It includes the fully qualified
    class name for this object.

*/
static PyObject *
apswvfsfilepy_xFileControl(APSWVFSFile *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int op, res = SQLITE_ERROR;
  void *ptr = NULL;

  CHECKVFSFILEPY;
  VFSFILENOTIMPLEMENTED(xFileControl, 1);

  {
    VFSFile_xFileControl_CHECK;
    ARG_PROLOG(2, VFSFile_xFileControl_KWNAMES);
    ARG_MANDATORY ARG_int(op);
    ARG_MANDATORY ARG_pointer(ptr);
    ARG_EPILOG(NULL, VFSFile_xFileControl_USAGE, );
  }
  res = self->base->pMethods->xFileControl(self->base, op, ptr);

  if (res == SQLITE_OK)
    Py_RETURN_TRUE;
  if (res == SQLITE_NOTFOUND)
    Py_RETURN_FALSE;

  SET_EXC(res, NULL);
  return NULL;
}

static int
apswvfsfile_xClose(sqlite3_file *file)
{
  int result = SQLITE_ERROR;
  PyObject *pyresult = NULL;
  FILEPREAMBLE;

  PyObject *vargs[] = {NULL, apswfile->file};
  pyresult = PyObject_VectorcallMethod(apst.xClose, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);

  if (!pyresult || PyErr_Occurred())
  {
    result = MakeSqliteMsgFromPyException(NULL);
    AddTraceBackHere(__FILE__, __LINE__, "apswvfsfile.xClose", NULL);
  }
  else
    result = SQLITE_OK;

  Py_XDECREF(apswfile->file);
  apswfile->file = NULL;
  Py_XDECREF(pyresult);
  FILEPOSTAMBLE;
  return result;
}

/** .. method:: xClose() -> None

  Close the database. Note that even if you return an error you should
  still close the file.  It is safe to call this method multiple
  times.
*/
static PyObject *
apswvfsfilepy_xClose(APSWVFSFile *self)
{
  int res;

  if (!self->base) /* already closed */
    Py_RETURN_NONE;

  res = self->base->pMethods->xClose(self->base);

  APSW_FAULT_INJECT(xCloseFails, , res = SQLITE_IOERR);

  /* we set pMethods to NULL after xClose callback so xClose can call other operations
     such as read or write during close */
  self->base->pMethods = NULL;

  PyMem_Free(self->base);
  self->base = NULL;

  if (res == SQLITE_OK)
    Py_RETURN_NONE;

  SET_EXC(res, NULL);
  return NULL;
}

static PyObject *
APSWVFSFile_tp_str(APSWVFSFile *self)
{
  return PyUnicode_FromFormat("<apsw.VFSFile object filename \"%s\" at %p>", self->filename ? self->filename : "(nil)", self);
}

#define APSWPROXYBASE                                          \
  APSWSQLite3File *apswfile = (APSWSQLite3File *)(void *)file; \
  APSWVFSFile *f = (APSWVFSFile *)(apswfile->file);            \
  assert(PyObject_TypeCheck(f, &APSWVFSFileType));

static int
apswproxyxShmLock(sqlite3_file *file, int offset, int n, int flags)
{
  APSWPROXYBASE;
  return f->base->pMethods->xShmLock(f->base, offset, n, flags);
}

static int
apswproxyxShmMap(sqlite3_file *file, int iPage, int pgsz, int isWrite, void volatile **pp)
{
  APSWPROXYBASE;
  return f->base->pMethods->xShmMap(f->base, iPage, pgsz, isWrite, pp);
}

static void
apswproxyxShmBarrier(sqlite3_file *file)
{
  APSWPROXYBASE;
  f->base->pMethods->xShmBarrier(f->base);
}

static int
apswproxyxShmUnmap(sqlite3_file *file, int deleteFlag)
{
  APSWPROXYBASE;
  return f->base->pMethods->xShmUnmap(f->base, deleteFlag);
}

static const struct sqlite3_io_methods apsw_io_methods_v1 =
    {
        1,                                  /* version */
        apswvfsfile_xClose,                 /* close */
        apswvfsfile_xRead,                  /* read */
        apswvfsfile_xWrite,                 /* write */
        apswvfsfile_xTruncate,              /* truncate */
        apswvfsfile_xSync,                  /* sync */
        apswvfsfile_xFileSize,              /* filesize */
        apswvfsfile_xLock,                  /* lock */
        apswvfsfile_xUnlock,                /* unlock */
        apswvfsfile_xCheckReservedLock,     /* checkreservedlock */
        apswvfsfile_xFileControl,           /* filecontrol */
        apswvfsfile_xSectorSize,            /* sectorsize */
        apswvfsfile_xDeviceCharacteristics, /* device characteristics */
        0,                                  /* shmmap */
        0,                                  /* shmlock */
        0,                                  /* shmbarrier */
        0                                   /* shmunmap */
};

static const struct sqlite3_io_methods apsw_io_methods_v2 =
    {
        2,                                  /* version */
        apswvfsfile_xClose,                 /* close */
        apswvfsfile_xRead,                  /* read */
        apswvfsfile_xWrite,                 /* write */
        apswvfsfile_xTruncate,              /* truncate */
        apswvfsfile_xSync,                  /* sync */
        apswvfsfile_xFileSize,              /* filesize */
        apswvfsfile_xLock,                  /* lock */
        apswvfsfile_xUnlock,                /* unlock */
        apswvfsfile_xCheckReservedLock,     /* checkreservedlock */
        apswvfsfile_xFileControl,           /* filecontrol */
        apswvfsfile_xSectorSize,            /* sectorsize */
        apswvfsfile_xDeviceCharacteristics, /* device characteristics */
        apswproxyxShmMap,                   /* shmmap */
        apswproxyxShmLock,                  /* shmlock */
        apswproxyxShmBarrier,               /* shmbarrier */
        apswproxyxShmUnmap                  /* shmunmap */
};

static PyMethodDef APSWVFSFile_methods[] = {
    {"xRead", (PyCFunction)apswvfsfilepy_xRead, METH_FASTCALL | METH_KEYWORDS, VFSFile_xRead_DOC},
    {"xUnlock", (PyCFunction)apswvfsfilepy_xUnlock, METH_FASTCALL | METH_KEYWORDS, VFSFile_xUnlock_DOC},
    {"xLock", (PyCFunction)apswvfsfilepy_xLock, METH_FASTCALL | METH_KEYWORDS, VFSFile_xLock_DOC},
    {"xClose", (PyCFunction)apswvfsfilepy_xClose, METH_NOARGS, VFSFile_xClose_DOC},
    {"xSectorSize", (PyCFunction)apswvfsfilepy_xSectorSize, METH_NOARGS, VFSFile_xSectorSize_DOC},
    {"xFileSize", (PyCFunction)apswvfsfilepy_xFileSize, METH_NOARGS, VFSFile_xFileSize_DOC},
    {"xDeviceCharacteristics", (PyCFunction)apswvfsfilepy_xDeviceCharacteristics, METH_NOARGS, VFSFile_xDeviceCharacteristics_DOC},
    {"xCheckReservedLock", (PyCFunction)apswvfsfilepy_xCheckReservedLock, METH_NOARGS, VFSFile_xCheckReservedLock_DOC},
    {"xWrite", (PyCFunction)apswvfsfilepy_xWrite, METH_FASTCALL | METH_KEYWORDS, VFSFile_xWrite_DOC},
    {"xSync", (PyCFunction)apswvfsfilepy_xSync, METH_FASTCALL | METH_KEYWORDS, VFSFile_xSync_DOC},
    {"xTruncate", (PyCFunction)apswvfsfilepy_xTruncate, METH_FASTCALL | METH_KEYWORDS, VFSFile_xTruncate_DOC},
    {"xFileControl", (PyCFunction)apswvfsfilepy_xFileControl, METH_FASTCALL | METH_KEYWORDS, VFSFile_xFileControl_DOC},
    {"excepthook", (PyCFunction)apswvfs_excepthook, METH_VARARGS, VFSFile_excepthook_DOC},
    /* Sentinel */
    {0, 0, 0, 0}};

static PyTypeObject APSWVFSFileType =
    {
        PyVarObject_HEAD_INIT(NULL, 0)
            .tp_name = "apsw.VFSFile",
        .tp_basicsize = sizeof(APSWVFSFile),
        .tp_dealloc = (destructor)APSWVFSFile_dealloc,
        .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
        .tp_doc = VFSFile_class_DOC,
        .tp_methods = APSWVFSFile_methods,
        .tp_init = (initproc)APSWVFSFile_init,
        .tp_new = APSWVFSFile_new,
        .tp_str = (reprfunc)APSWVFSFile_tp_str,
};

/** .. class:: URIFilename

    SQLite packs `uri parameters
    <https://sqlite.org/uri.html>`__ and the filename together   This class
    encapsulates that packing.  The :ref:`example <example_vfs>` shows
    usage of this class.

    Your :meth:`VFS.xOpen` method will generally be passed one of
    these instead of a string as the filename if the URI flag was used
    or the main database flag is set.

    You can safely pass it on to the :class:`VFSFile` constructor
    which knows how to get the name back out.  The URIFilename is
    only valid for the duration of the xOpen call.  If you save
    and use the object later you will get an exception.
*/

#define CHECK_SCOPE                                                          \
  do                                                                         \
  {                                                                          \
    if (!self->filename)                                                     \
      return PyErr_Format(ExcInvalidContext, "URIFilename is out of scope"); \
  } while (0)

/** .. method:: filename() -> str

    Returns the filename.
*/
static PyObject *
apswurifilename_filename(APSWURIFilename *self)
{
  CHECK_SCOPE;
  return convertutf8string(self->filename);
}

/** .. attribute:: parameters
    :type: tuple[str, ...]

    A tuple of the parameter names present.

    -* sqlite3_uri_key
*/
static PyObject *
apswurifilename_parameters(APSWURIFilename *self)
{
  CHECK_SCOPE;
  int i, count = 0;
  for (i = 0;; i++)
    if (!sqlite3_uri_key(self->filename, i))
      break;
  count = i;

  PyObject *res = PyTuple_New(count);
  if (!res)
    goto fail;

  for (i = 0; i < count; i++)
  {
    PyObject *tmpstring = PyUnicode_FromString(sqlite3_uri_key(self->filename, i));
    if (!tmpstring)
      goto fail;
    PyTuple_SET_ITEM(res, i, tmpstring);
  }

  return res;

fail:
  Py_XDECREF(res);
  return NULL;
}

/** .. method:: uri_parameter(name: str) -> Optional[str]

    Returns the value of parameter `name` or None.

    -* sqlite3_uri_parameter
*/
static PyObject *
apswurifilename_uri_parameter(APSWURIFilename *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  CHECK_SCOPE;
  const char *res, *name;
  {
    URIFilename_uri_parameter_CHECK;
    ARG_PROLOG(1, URIFilename_uri_parameter_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_EPILOG(NULL, URIFilename_uri_parameter_USAGE, );
  }
  res = sqlite3_uri_parameter(self->filename, name);
  return convertutf8string(res);
}

/** .. method:: uri_int(name: str, default: int) -> int

    Returns the integer value for parameter `name` or `default` if not
    present.

    -* sqlite3_uri_int64
*/
static PyObject *
apswurifilename_uri_int(APSWURIFilename *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  CHECK_SCOPE;
  const char *name = NULL;
  long long res = 0, default_;

  {
    URIFilename_uri_int_CHECK;
    ARG_PROLOG(2, URIFilename_uri_int_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_MANDATORY ARG_int64(default_);
    ARG_EPILOG(NULL, URIFilename_uri_int_USAGE, );
  }
  res = sqlite3_uri_int64(self->filename, name, default_);

  return PyLong_FromLongLong(res);
}

/** .. method:: uri_boolean(name: str, default: bool) -> bool

    Returns the boolean value for parameter `name` or `default` if not
    present.

    -* sqlite3_uri_boolean
 */
static PyObject *
apswurifilename_uri_boolean(APSWURIFilename *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  CHECK_SCOPE;
  const char *name = NULL;
  int default_ = 0, res;

  {
    URIFilename_uri_boolean_CHECK;
    ARG_PROLOG(2, URIFilename_uri_boolean_KWNAMES);
    ARG_MANDATORY ARG_str(name);
    ARG_MANDATORY ARG_bool(default_);
    ARG_EPILOG(NULL, URIFilename_uri_boolean_USAGE, );
  }

  res = sqlite3_uri_boolean(self->filename, name, default_);

  if (res)
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

static PyObject *
apswurifilename_tp_str(APSWURIFilename *self)
{
  /* CHECK_SCOPE not needed since we manually check */
  if (!self->filename)
    return PyUnicode_FromFormat("<apsw.URIFilename object (out of scope) at %p>", self);
  return PyUnicode_FromFormat("<apsw.URIFilename object \"%s\" at %p>", self->filename, self);
}

static PyMethodDef APSWURIFilenameMethods[] = {
    {"filename", (PyCFunction)apswurifilename_filename, METH_NOARGS, URIFilename_filename_DOC},
    {"uri_parameter", (PyCFunction)apswurifilename_uri_parameter, METH_FASTCALL | METH_KEYWORDS, URIFilename_uri_parameter_DOC},
    {"uri_int", (PyCFunction)apswurifilename_uri_int, METH_FASTCALL | METH_KEYWORDS, URIFilename_uri_int_DOC},
    {"uri_boolean", (PyCFunction)apswurifilename_uri_boolean, METH_FASTCALL | METH_KEYWORDS, URIFilename_uri_boolean_DOC},
    /* Sentinel */
    {0, 0, 0, 0}};

static PyGetSetDef APSWURIFilename_getset[] = {
    {"parameters", (getter)apswurifilename_parameters, NULL, URIFilename_parameters_DOC},
    {0, 0, 0, 0},
};

static PyTypeObject APSWURIFilenameType =
    {
        PyVarObject_HEAD_INIT(NULL, 0)
            .tp_name = "apsw.URIFilename",
        .tp_basicsize = sizeof(APSWURIFilename),
        .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
        .tp_doc = URIFilename_class_DOC,
        .tp_methods = APSWURIFilenameMethods,
        .tp_str = (reprfunc)apswurifilename_tp_str,
        .tp_getset = APSWURIFilename_getset,
};

#undef CHECK_SCOPE
