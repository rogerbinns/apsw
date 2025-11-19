
/* some forward declarations we can't have here because they don't know the
   contents of struct Connection */
static PyObject *async_get_controller_from_connection(PyObject *connection);
static int Connection_init(PyObject *self_, PyObject *args, PyObject *kwargs);

#ifdef APSW_DEBUG
static void async_fake_worker_thread(PyObject *connection_, int value);
#endif

/* used for getting call details im a non-worker thread that can be invoked in the worker thread */
typedef struct BoxedCall
{
  PyObject_HEAD

  /* discriminated union */
  enum
  {
    Dormant = 0,
    ConnectionInit,
    FastCallWithKeywords,
    Unary,
    Binary,
  } call_type;

  union
  {
    struct
    {
      PyObject *connection;
      PyObject *args;
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
  };
} BoxedCall;

static void
BoxedCall_clear(PyObject *self_)
{
  BoxedCall *self = (BoxedCall *)self_;

  switch (self->call_type)
  {
  case Dormant:
    break;

  case ConnectionInit:
    Py_DECREF(self->ConnectionInit.connection);
    Py_DECREF(self->ConnectionInit.args);
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
    Py_DECREF(self->Binary.args[1]);
    break;

  default:
    // ::TODO:: delete this default once the code is complete
    assert(0);
  }
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
  PyObject *result = NULL;

  switch (self->call_type)
  {
  case ConnectionInit:
    if (0 == Connection_init(self->ConnectionInit.connection, self->ConnectionInit.args, NULL))
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

  default:
    // ::TODO:: delete this default once the code is complete
    assert(0);
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

  if (kwargs || (args && PyTuple_GET_SIZE(args)) || self->call_type == Dormant)
    return PyErr_Format(PyExc_RuntimeError, "BoxedCall takes no parameters and can only be called once");

  return BoxedCall_internal_call(self);
}

static PyTypeObject BoxedCallType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.aio.BoxedCall",
  .tp_basicsize = sizeof(BoxedCall),
  .tp_dealloc = BoxedCall_dealloc,
  .tp_itemsize = sizeof(PyObject *),
  .tp_free = PyObject_Free,
  .tp_call = BoxedCall_call,
  // ::TODO:: add tp_traverse
};

static BoxedCall *
make_boxed_call(Py_ssize_t total_args)
{
  BoxedCall *box = (BoxedCall *)_PyObject_NewVar(&BoxedCallType, total_args);
  if (box)
  {
    box->call_type = Dormant;

    /* verify union member size constraints */
    assert(sizeof(box->FastCallWithKeywords) >= sizeof(box->ConnectionInit));
    assert(sizeof(box->FastCallWithKeywords) >= sizeof(box->Unary));
    assert(sizeof(box->FastCallWithKeywords) >= sizeof(box->Binary));
  }

  return box;
}

static void
async_shutdown_controller(PyObject *controller)
{
#ifdef APSW_DEBUG
  if (controller == async_dummy_controller)
    return;
#endif
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
#ifdef APSW_DEBUG
  if (async_get_controller_from_connection(connection) == async_dummy_controller)
  {

    async_fake_worker_thread(connection, 1);
    PyObject *myresult = BoxedCall_internal_call((BoxedCall *)boxed_call);
    async_fake_worker_thread(connection, 0);
    Py_DECREF(boxed_call);
    return myresult;
  }
#endif

  PyObject *vargs[] = { NULL, async_get_controller_from_connection(connection), boxed_call };
  PyObject *result = PyObject_VectorcallMethod_NoAsync(apst.send, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_DECREF(boxed_call);

  return result;
}

static PyObject *
async_return_value(PyObject *connection, PyObject *value)
{
  PyObject *vargs[] = { NULL, async_get_controller_from_connection(connection), value };
  return PyObject_VectorcallMethod_NoAsync(apst.async_value, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
}

static PyObject *
async_return_exception(PyObject *connection, PyObject *tuple)
{
  assert(PyTuple_CheckExact(tuple) && PyTuple_GET_SIZE(tuple) == 2);
  PyObject *exc = PyTuple_GetItem(tuple, 0), *traceback = PyTuple_GetItem(tuple, 1);
  if (PyErr_Occurred())
    return NULL;
  PyObject *vargs[] = { NULL, async_get_controller_from_connection(connection), exc, traceback };
  return PyObject_VectorcallMethod_NoAsync(apst.async_exception, vargs + 1, 3 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
}

void
async_send_discard(PyObject *connection, PyObject *object)
{
  PyObject *vargs[] = { NULL, async_get_controller_from_connection(connection), object };
#ifdef APSW_DEBUG
  if (vargs[1] == async_dummy_controller)
    return;
#endif
  PY_ERR_FETCH(saved_err);

  PyObject *result
      = PyObject_VectorcallMethod_NoAsync(apst.cancel, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  if (!result)
  {
    AddTraceBackHere(__FILE__, __LINE__, "apsw.aio.controller_discard", "{s: O,s:O}", "controller", vargs[1], "object",
                     object);
    apsw_write_unraisable(NULL);
  }
  else
    Py_DECREF(result);
  PY_ERR_RESTORE(saved_err);
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
  boxed_call->Binary.args[1] = Py_NewRef(arg2);

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
