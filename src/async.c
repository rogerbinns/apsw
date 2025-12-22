
/* some forward declarations we can't have here because they don't know the
   contents of struct Connection */
static PyObject *async_get_controller_from_connection(PyObject *connection);

static PyObject *async_cursor_prefetch_context_var;
static PyObject *async_controller_context_var;

/* used to return values and exceptions.  I originally did this by
   calling into the controller which was time consuming and fragile */
typedef struct
{
  PyObject_HEAD

  enum
  {
    Value,
    Exception,
    StopAsyncIteration
  } value_type;

  PyObject *one;
#if PY_VERSION_HEX < 0x030c0000
  PyObject *two;
  PyObject *three;
#endif
} AwaitableWrapper;

static PyObject *
AwaitableWrapper_await(PyObject *self)
{
  return Py_NewRef(self);
}

static PyObject *
AwaitableWrapper_next(PyObject *self_)
{
  AwaitableWrapper *self = (AwaitableWrapper *)self_;
  switch (self->value_type)
  {
  case Exception: {
    /* exception restoring steals references */
#if PY_VERSION_HEX < 0x030c0000
    PyErr_Restore(self->one, self->two, self->three);
    self->one = self->two = self->three = NULL;
#else
    PyErr_SetRaisedException(self->one);
    self->one = NULL;
#endif
    break;
  }
  case Value:
  {
    /* PyErr_SetObject has more complex code to instantiate the exception */
    PyObject *real_value = PyObject_CallOneArg(PyExc_StopIteration, self->one);
    if (real_value)
    {
      PyErr_SetObject(PyExc_StopIteration, real_value);
      Py_DECREF(real_value);
    }
    break;
  }
  case StopAsyncIteration:
    PyErr_SetNone(PyExc_StopAsyncIteration);
  }
  self->value_type = StopAsyncIteration;
  return NULL;
}

/* these methods are just to make it smell like a Future but do nothing */

static PyObject *
AwaitableWrapper_cancel(PyObject *Py_UNUSED(unused1), PyObject *Py_UNUSED(unused21))
{
  Py_RETURN_FALSE;
}

static PyObject *
AwaitableWrapper_cancelled(PyObject *Py_UNUSED(unused1), PyObject *Py_UNUSED(unused21))
{
  Py_RETURN_FALSE;
}

static PyObject *
AwaitableWrapper_done(PyObject *Py_UNUSED(unused1), PyObject *Py_UNUSED(unused21))
{
  Py_RETURN_TRUE;
}

static void
AwaitableWrapper_dealloc(PyObject *self_)
{
  AwaitableWrapper *self = (AwaitableWrapper *)self_;
  Py_CLEAR(self->one);
#if PY_VERSION_HEX < 0x030c0000
  Py_CLEAR(self->two);
  Py_CLEAR(self->three);
#endif
  Py_TpFree(self_);
}

static PyAsyncMethods AwaitableWrapper_async_methods = {
  .am_await = AwaitableWrapper_await,
};

static PyMethodDef AwaitableMethods[] = {
  { "cancel", (PyCFunction)AwaitableWrapper_cancel, METH_NOARGS },
  { "cancelled", (PyCFunction)AwaitableWrapper_cancelled, METH_NOARGS },
  { "done", (PyCFunction)AwaitableWrapper_done, METH_NOARGS },
  { 0 },
};

static PyTypeObject AwaitableWrapperType = {
  .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.aio.AwaitableWrapper",
  .tp_basicsize = sizeof(AwaitableWrapper),
  .tp_flags = Py_TPFLAGS_DEFAULT,
  .tp_new = PyType_GenericNew,
  .tp_iter = PyObject_SelfIter,
  .tp_iternext = AwaitableWrapper_next,
  .tp_dealloc = AwaitableWrapper_dealloc,
  .tp_methods = AwaitableMethods,
  .tp_as_async = &AwaitableWrapper_async_methods,
};

/* used for getting call details im a non-worker thread that can be invoked in the worker thread */
typedef struct BoxedCall
{
  PyObject_VAR_HEAD

  /* discriminated union */
  enum
  {
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
    Py_DECREF(self->FastCallWithKeywords.object);
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
  self->call_type = Dormant;
}

static void
BoxedCall_dealloc(PyObject *self)
{
  BoxedCall_clear(self);
  Py_CLEAR(((BoxedCall *)self)->context);
  Py_TpFree(self);
}

static PyObject *
BoxedCall_internal_call(BoxedCall *self)
{
  PyObject *result = NULL;

  switch (self->call_type)
  {
  case ConnectionInit:
    if (0
        == Py_TYPE(self->ConnectionInit.connection)
               ->tp_init(self->ConnectionInit.connection, self->ConnectionInit.args, self->ConnectionInit.kwargs))
      result = Py_NewRef(self->ConnectionInit.connection);
    break;
  case FastCallWithKeywords:
    result = self->FastCallWithKeywords.function(self->FastCallWithKeywords.object,
                                                 self->FastCallWithKeywords.fast_args + 1,
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
    PyErr_SetString(PyExc_RuntimeError, "Can only be called once");
  }

  if (!result && PyErr_Occurred() && !PyErr_ExceptionMatches(PyExc_StopAsyncIteration)
      && !PyErr_ExceptionMatches(PyExc_StopIteration))
    AddTraceBackHere(__FILE__, __LINE__, "apsw.aio.BoxedCall.__call__", "{s:i}", "call_type", (int)self->call_type);

  BoxedCall_clear((PyObject *)self);

  return result;
}

static PyObject *
BoxedCall_call(PyObject *self_, PyObject *args, PyObject *kwargs)
{
  BoxedCall *self = (BoxedCall *)self_;

  if (kwargs || (args && PyTuple_GET_SIZE(args)))
    return PyErr_Format(PyExc_RuntimeError, "BoxedCall takes no parameters");

  return BoxedCall_internal_call(self);
}

static PyObject *
BoxedCall_enter(PyObject *self_, PyObject *Py_UNUSED(unused))
{
  BoxedCall *self = (BoxedCall *)self_;
  if (self->call_type == Dormant)
  {
    PyErr_SetString(PyExc_RuntimeError, "BoxedCall has already been called");
    return NULL;
  }
  if (0 == PyContext_Enter(self->context))
    return Py_NewRef(self_);
  return NULL;
}

static PyObject *
BoxedCall_exit(PyObject *self_, PyObject *const *Py_UNUSED(fast_args), Py_ssize_t Py_UNUSED(fast_nargs),
               PyObject *Py_UNUSED(fast_kwnames))
{
  BoxedCall *self = (BoxedCall *)self_;

  PyContext_Exit(self->context);

  Py_RETURN_NONE;
}

static PyMethodDef BoxedCall_methods[] = {
  { "__enter__", (PyCFunction)BoxedCall_enter, METH_NOARGS },
  { "__exit__", (PyCFunction)BoxedCall_exit, METH_FASTCALL | METH_KEYWORDS },
  { NULL },
};

static PyTypeObject BoxedCallType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.aio.BoxedCall",
  .tp_basicsize = sizeof(BoxedCall),
  .tp_dealloc = BoxedCall_dealloc,
  .tp_itemsize = sizeof(PyObject *),
  .tp_free = PyObject_Free,
  .tp_call = BoxedCall_call,
  .tp_methods = BoxedCall_methods,
};

static BoxedCall *
make_boxed_call(Py_ssize_t total_args)
{
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

static PyObject *
async_return_value(PyObject *value)
{
  AwaitableWrapper *wrap = (AwaitableWrapper *)_PyObject_New(&AwaitableWrapperType);
  if (wrap)
  {
    wrap->value_type = Value;
    wrap->one = Py_NewRef(value);
#if PY_VERSION_HEX < 0x030c0000
    wrap->two = NULL;
    wrap->three = NULL;
#endif
  }
  return (PyObject *)wrap;
}

static PyObject *
async_return_exception(PyObject *value)
{
  AwaitableWrapper *wrap = (AwaitableWrapper *)_PyObject_New(&AwaitableWrapperType);
  if (wrap)
  {
    wrap->value_type = Exception;
#if PY_VERSION_HEX < 0x030c0000
    wrap->one = Py_XNewRef(PyTuple_GET_ITEM(value, 0));
    wrap->two = Py_XNewRef(PyTuple_GET_ITEM(value, 1));
    wrap->three = Py_XNewRef(PyTuple_GET_ITEM(value, 2));
#else
    wrap->one = Py_NewRef(value);
#endif
  }
  return (PyObject *)wrap;
}

static PyObject *
async_return_stopasynciteration(void)
{
  AwaitableWrapper *wrap = (AwaitableWrapper *)_PyObject_New(&AwaitableWrapperType);
  if (wrap)
  {
    wrap->value_type = StopAsyncIteration;
    wrap->one = NULL;
#if PY_VERSION_HEX < 0x030c0000
    wrap->two = NULL;
    wrap->three = NULL;
#endif
  }
  return (PyObject *)wrap;
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
  boxed_call->FastCallWithKeywords.object = Py_NewRef(object);
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
  PyErr_SetString(PyExc_TypeError,
                  "Using async in sync context");
  return NULL;
}

static PyObject *
error_sync_in_async_context(void)
{
  PyErr_SetString(PyExc_TypeError, "Using sync in async context");
  return NULL;
}