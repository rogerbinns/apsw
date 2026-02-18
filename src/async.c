
/* some forward declarations we can't have here because they don't know the
   contents of struct Connection */
static PyObject *async_get_controller_from_connection(PyObject *connection);

static PyObject *async_cursor_prefetch_context_var;
static PyObject *async_controller_context_var;

/* used for getting call details im a non-worker thread that can be invoked in the worker thread */
typedef struct BoxedCall
{
  PyObject_VAR_HEAD

      /* discriminated union */
      enum {
        Dormant = 0,
        ConnectionInit,
        FastCallWithKeywords,
        Unary,
        Binary,
        AttrGet,
      } call_type;

  /* PyContext to run call in */
  PyObject *context;

  union
  {
    struct
    {
      PyObject *connection;
      PyObject *args;
      PyObject *kwargs;
    } ConnectionInit;

    /* note this must be the largest member of the union because
       we allocate more after fast_args*/
    struct
    {
      PyCFunctionFastWithKeywords function;
      PyObject *object;
      PyObject *fast_kwnames;
      Py_ssize_t fast_nargs; /* length of args ignoring first entry */
      /* entry 1 is not used so we can PY_VECTORCALL_ARGUMENTS_OFFSET
         and is followed by fast_nargs additional pointers */
      PyObject *fast_args[1];
    } FastCallWithKeywords;

    struct
    {
      unaryfunc function;
      PyObject *arg;
    } Unary;

    struct
    {
      binaryfunc function;
      PyObject *args[2];
    } Binary;

    struct
    {
      getter function;
      PyObject *arg1;
      void *arg2;
    } AttrGet;
  };
} BoxedCall;

static void
BoxedCall_clear(PyObject *self_)
{
  BoxedCall *self = (BoxedCall *)self_;

  switch (self->call_type)
  {
  case Dormant:
    return;

  case ConnectionInit:
    Py_DECREF(self->ConnectionInit.connection);
    Py_DECREF(self->ConnectionInit.args);
    Py_XDECREF(self->ConnectionInit.kwargs);
    break;

  case FastCallWithKeywords: {
    Py_ssize_t total_args
        = PyVectorcall_NARGS(self->FastCallWithKeywords.fast_nargs)
          + (self->FastCallWithKeywords.fast_kwnames ? PyTuple_GET_SIZE(self->FastCallWithKeywords.fast_kwnames) : 0);
    Py_XDECREF(self->FastCallWithKeywords.object);
    Py_XDECREF(self->FastCallWithKeywords.fast_kwnames);
    for (Py_ssize_t i = 0; i < total_args; i++)
      Py_DECREF(self->FastCallWithKeywords.fast_args[1 + i]);
    break;
  }
  case Unary:
    Py_DECREF(self->Unary.arg);
    break;

  case Binary:
    Py_DECREF(self->Binary.args[0]);
    Py_XDECREF(self->Binary.args[1]);
    break;

  case AttrGet:
    Py_DECREF(self->AttrGet.arg1);
    break;
  }

  Py_CLEAR(((BoxedCall *)self)->context);

  self->call_type = Dormant;
}

static void
BoxedCall_dealloc(PyObject *self)
{
  BoxedCall_clear(self);
  Py_TpFree(self);
}

static PyObject *
BoxedCall_internal_call(BoxedCall *self)
{
  assert(self->call_type != Dormant);

  PyObject *result = NULL;

  if (0 == PyContext_Enter(self->context))
  {

    switch (self->call_type)
    {
    case ConnectionInit:
      if (0
          == Py_TYPE(self->ConnectionInit.connection)
                 ->tp_init(self->ConnectionInit.connection, self->ConnectionInit.args, self->ConnectionInit.kwargs))
        result = Py_NewRef(self->ConnectionInit.connection);
      break;
    case FastCallWithKeywords:
      result = self->FastCallWithKeywords.function(
          self->FastCallWithKeywords.object, self->FastCallWithKeywords.fast_args + 1,
          self->FastCallWithKeywords.fast_nargs | PY_VECTORCALL_ARGUMENTS_OFFSET,
          self->FastCallWithKeywords.fast_kwnames);
      break;
    case Unary:
      result = self->Unary.function(self->Unary.arg);
      break;

    case Binary:
      result = self->Binary.function(self->Binary.args[0], self->Binary.args[1]);
      break;

    case AttrGet:
      result = self->AttrGet.function(self->AttrGet.arg1, self->AttrGet.arg2);
      break;

    case Dormant:
      Py_UNREACHABLE();
    }

    PyContext_Exit(self->context);
  }

  if (!result && PyErr_Occurred() && !PyErr_ExceptionMatches(PyExc_StopAsyncIteration)
      && !PyErr_ExceptionMatches(PyExc_StopIteration))
  {
    if (self->call_type == ConnectionInit)
      /* this causes close on init failure so threads don't get leaked */
      Py_DECREF(self->ConnectionInit.connection);

    AddTraceBackHere(__FILE__, __LINE__, "apsw.aio.BoxedCall.__call__", "{s:i}", "call_type", (int)self->call_type);
  }

  BoxedCall_clear((PyObject *)self);

  return result;
}

static PyObject *
BoxedCall_call(PyObject *self_, PyObject *args, PyObject *kwargs)
{
  BoxedCall *self = (BoxedCall *)self_;

  if (kwargs || (args && PyTuple_GET_SIZE(args)))
    return PyErr_Format(PyExc_RuntimeError, "BoxedCall takes no parameters");

  if (self->call_type == Dormant)
  {
    PyErr_SetString(PyExc_RuntimeError, "Can only be called once");
    return NULL;
  }

  return BoxedCall_internal_call(self);
}

static PyTypeObject BoxedCallType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.aio.BoxedCall",
  .tp_basicsize = sizeof(BoxedCall),
  .tp_dealloc = BoxedCall_dealloc,
  .tp_itemsize = sizeof(PyObject *),
  .tp_free = PyObject_Free,
  .tp_call = BoxedCall_call,
};

#undef make_boxed_call
static BoxedCall *
make_boxed_call(Py_ssize_t total_args)
{
#include "faultinject.h"

  BoxedCall *box = (BoxedCall *)_PyObject_NewVar(&BoxedCallType, total_args);
  if (box)
  {
    box->call_type = Dormant;

    box->context = PyContext_CopyCurrent();
    if (!box->context)
    {
      BoxedCall_dealloc((PyObject *)box);
      return NULL;
    }

    /* verify union member size constraints */
    assert(sizeof(box->FastCallWithKeywords) >= sizeof(box->ConnectionInit));
    assert(sizeof(box->FastCallWithKeywords) >= sizeof(box->Unary));
    assert(sizeof(box->FastCallWithKeywords) >= sizeof(box->Binary));
    assert(sizeof(box->FastCallWithKeywords) >= sizeof(box->AttrGet));
  }

  return box;
}

static void
async_shutdown_controller(PyObject *controller)
{
  /* exceptions are always done as unraisable */
  if (controller)
  {
    PY_ERR_FETCH(saved_err);
    PyObject *vargs[] = { NULL, controller };
    PyObject *result
        = PyObject_VectorcallMethod_NoAsync(apst.close, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
    if (!result)
    {
      AddTraceBackHere(__FILE__, __LINE__, "apsw.aio.controller_shutdown", "{s: O}", "controller", controller);
      apsw_write_unraisable(NULL);
    }
    else
      Py_DECREF(result);
    PY_ERR_RESTORE(saved_err);
  }
}

/* Note this steals the ref from boxed_call */
static PyObject *
async_send_boxed_call(PyObject *connection, PyObject *boxed_call)
{
  PyObject *vargs[] = { NULL, async_get_controller_from_connection(connection), boxed_call };
  PyObject *result = PyObject_VectorcallMethod_NoAsync(apst.send, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_DECREF(boxed_call);

  return result;
}

static PyObject *coro_for_value;

static PyObject *
async_return_value(PyObject *value)
{
  if (!coro_for_value)
  {
    coro_for_value = PyImport_ImportModuleAttr(apst.apsw_aio, apst._coro_for_value);
    if (!coro_for_value)
      return NULL;
  }

  PyObject *vargs[] = { NULL, value };
  return PyObject_Vectorcall_NoAsync(coro_for_value, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
}

static PyObject *coro_for_exception;

static PyObject *
async_return_exception(PyObject *exc)
{
  if (!coro_for_exception)
  {
    coro_for_exception = PyImport_ImportModuleAttr(apst.apsw_aio, apst._coro_for_exception);
    if (!coro_for_exception)
      return NULL;
  }

  PyObject *vargs[] = { NULL, exc };
  return PyObject_Vectorcall_NoAsync(coro_for_exception, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
}

static PyObject *coro_for_stopasynciteration;

static PyObject *
async_return_stopasynciteration(void)
{
  if (!coro_for_stopasynciteration)
  {
    coro_for_stopasynciteration = PyImport_ImportModuleAttr(apst.apsw_aio, apst._coro_for_stopasynciteration);
    if (!coro_for_stopasynciteration)
      return NULL;
  }

  return PyObject_CallNoArgs(coro_for_stopasynciteration);
}

static PyObject *
make_boxed_fastcall(PyCFunctionFastWithKeywords function, PyObject *object, PyObject *const *fast_args,
                    Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  Py_ssize_t total_args = PyVectorcall_NARGS(fast_nargs) + (fast_kwnames ? PyTuple_GET_SIZE(fast_kwnames) : 0);

  BoxedCall *boxed_call = make_boxed_call(total_args);
  if (!boxed_call)
    return NULL;

  boxed_call->call_type = FastCallWithKeywords;
  boxed_call->FastCallWithKeywords.function = function;
  boxed_call->FastCallWithKeywords.object = Py_XNewRef(object);
  boxed_call->FastCallWithKeywords.fast_kwnames = Py_XNewRef(fast_kwnames);
  boxed_call->FastCallWithKeywords.fast_nargs = fast_nargs;
  memcpy(boxed_call->FastCallWithKeywords.fast_args + 1, fast_args, sizeof(PyObject *) * total_args);
  for (Py_ssize_t i = 0; i < total_args; i++)
    Py_INCREF(boxed_call->FastCallWithKeywords.fast_args[1 + i]);

  return (PyObject *)boxed_call;
}

static PyObject *
do_async_fastcall(PyObject *connection, PyCFunctionFastWithKeywords function, PyObject *object,
                  PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *boxed = make_boxed_fastcall(function, object, fast_args, fast_nargs, fast_kwnames);
  return boxed ? async_send_boxed_call(connection, boxed) : NULL;
}

static PyObject *
do_async_unary(PyObject *connection, unaryfunc function, PyObject *arg)
{
  BoxedCall *boxed_call = make_boxed_call(0);
  if (!boxed_call)
    return NULL;

  boxed_call->call_type = Unary;
  boxed_call->Unary.function = function;
  boxed_call->Unary.arg = Py_NewRef(arg);

  return async_send_boxed_call(connection, (PyObject *)boxed_call);
}

static PyObject *
do_async_binary(PyObject *connection, binaryfunc function, PyObject *arg1, PyObject *arg2)
{
  BoxedCall *boxed_call = make_boxed_call(0);
  if (!boxed_call)
    return NULL;

  boxed_call->call_type = Binary;
  boxed_call->Binary.function = function;
  boxed_call->Binary.args[0] = Py_NewRef(arg1);
  boxed_call->Binary.args[1] = Py_XNewRef(arg2);

  return async_send_boxed_call(connection, (PyObject *)boxed_call);
}

static PyObject *
do_async_attr_get(PyObject *connection, getter function, PyObject *arg1, void *arg2)
{
  BoxedCall *boxed_call = make_boxed_call(0);
  if (!boxed_call)
    return NULL;

  boxed_call->call_type = AttrGet;
  boxed_call->AttrGet.function = function;
  boxed_call->AttrGet.arg1 = Py_NewRef(arg1);
  boxed_call->AttrGet.arg2 = arg2;

  return async_send_boxed_call(connection, (PyObject *)boxed_call);
}

/* all threads are workers in sync mode, else check threadid */
#define IN_WORKER_THREAD(CONN) (!(CONN)->async_controller || PyThread_get_thread_ident() == (((CONN)->async_thread_id)))

#define ASYNC_FASTCALL(CONN, FUNCTION)                                                                                 \
  do                                                                                                                   \
  {                                                                                                                    \
    if (!IN_WORKER_THREAD(CONN))                                                                                       \
      return do_async_fastcall((PyObject *)(CONN), FUNCTION, self_, fast_args, fast_nargs, fast_kwnames);              \
  } while (0)

#define ASYNC_UNARY(CONN, FUNCTION, ARG)                                                                               \
  do                                                                                                                   \
  {                                                                                                                    \
    if (!IN_WORKER_THREAD(CONN))                                                                                       \
      return do_async_unary((PyObject *)(CONN), FUNCTION, (ARG));                                                      \
  } while (0)

#define ASYNC_BINARY(CONN, FUNCTION, ARG1, ARG2)                                                                       \
  do                                                                                                                   \
  {                                                                                                                    \
    if (!IN_WORKER_THREAD(CONN))                                                                                       \
      return do_async_binary((PyObject *)(CONN), FUNCTION, (ARG1), (ARG2));                                            \
  } while (0)

#define ASYNC_ATTR_GET(CONN, FUNCTION, ARG1, ARG2)                                                                     \
  do                                                                                                                   \
  {                                                                                                                    \
    if (!IN_WORKER_THREAD(CONN))                                                                                       \
      return do_async_attr_get((PyObject *)(CONN), FUNCTION, (ARG1), (ARG2));                                          \
  } while (0)

/* standard error routines - these return NULL so they can be returned themselves */
static PyObject *
error_async_in_sync_context(void)
{
  PyErr_SetString(PyExc_TypeError, "Using async in sync context");
  return NULL;
}

static PyObject *
error_sync_in_async_context(void)
{
  PyErr_SetString(PyExc_TypeError, "Using sync in async context");
  return NULL;
}