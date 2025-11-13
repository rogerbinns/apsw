
/* some forward declarations we can't have here because they don't know the
   contents of struct Connection */
static PyObject *async_get_controller_from_connection(PyObject *connection);
static int Connection_init(PyObject *self_, PyObject *args, PyObject *kwargs);

/* used for getting call details im a non-worker thread that can be invoked in the worker thread */
typedef struct
{
  PyObject_HEAD

  /* discriminated union */
  enum
  {
    Dormant = 0,
    ConnectionInit,
    FastCallWithKeywords,
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

  case FastCallWithKeywords:
    Py_DECREF(self->FastCallWithKeywords.object);
    Py_XDECREF(self->FastCallWithKeywords.fast_kwnames);
    for (Py_ssize_t i = 0; i < self->FastCallWithKeywords.fast_nargs; i++)
      Py_DECREF(self->FastCallWithKeywords.fast_args[1 + i]);
    break;

  default:
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
BoxedCall_call(PyObject *self_, PyObject *args, PyObject *kwargs)
{
  BoxedCall *self = (BoxedCall *)self_;
  PyObject *result = NULL;

  if (kwargs || (args && PyTuple_GET_SIZE(args)) || self->call_type == Dormant)
  {
    PyErr_Format(PyExc_RuntimeError, "BoxedCall takes no parameters and can only be called once");
    goto finally;
  }

  switch (self->call_type)
  {
  case ConnectionInit:
    if(0==Connection_init(self->ConnectionInit.connection, self->ConnectionInit.args, NULL))
      result = Py_NewRef(self->ConnectionInit.connection);
    break;
  case FastCallWithKeywords:
    result = self->FastCallWithKeywords.function(self->FastCallWithKeywords.object,
                                                 self->FastCallWithKeywords.fast_args + 1,
                                                 self->FastCallWithKeywords.fast_nargs | PY_VECTORCALL_ARGUMENTS_OFFSET,
                                                 self->FastCallWithKeywords.fast_kwnames);
    break;
  default:
    assert(0);
  }

finally:
  if (!result)
    AddTraceBackHere(__FILE__, __LINE__, "apsw.aio.BoxedCall.__call__", "{s:i}", "call_type", (int)self->call_type);
  BoxedCall_clear(self_);
  return result;
}

static PyTypeObject BoxedCallType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.aio.BoxedCall",
  .tp_basicsize = sizeof(BoxedCall),
  .tp_dealloc = BoxedCall_dealloc,
  .tp_itemsize = sizeof(PyObject *),
  .tp_free = PyObject_Free,
  .tp_call = BoxedCall_call,
};

static BoxedCall *
make_boxed_call(Py_ssize_t fast_nargs)
{
  BoxedCall *box = (BoxedCall *)_PyObject_NewVar(&BoxedCallType, fast_nargs);
  if (box)
    box->call_type = Dormant;

  /* verify union member size constraints */
  assert(sizeof(box->FastCallWithKeywords) >= sizeof(box->ConnectionInit));

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
    PY_ERR_RESTORE(saved_err);
  }
}

static PyObject *
do_async_fastcall(PyObject *connection, PyCFunctionFastWithKeywords function, PyObject *object,
                  PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  Py_ssize_t actual_nargs = PyVectorcall_NARGS(fast_nargs);
  BoxedCall *boxed_call = make_boxed_call(actual_nargs);
  if (!boxed_call)
    return NULL;

  boxed_call->call_type = FastCallWithKeywords;
  boxed_call->FastCallWithKeywords.function = function;
  boxed_call->FastCallWithKeywords.object = Py_NewRef(object);
  boxed_call->FastCallWithKeywords.fast_kwnames = Py_XNewRef(fast_kwnames);
  boxed_call->FastCallWithKeywords.fast_nargs = actual_nargs;
  memcpy(boxed_call->FastCallWithKeywords.fast_args + 1, fast_args, sizeof(PyObject *) * actual_nargs);
  for (size_t i = 0; i < actual_nargs; i++)
    Py_INCREF(boxed_call->FastCallWithKeywords.fast_args[1 + i]);

  PyObject *vargs[] = { NULL, async_get_controller_from_connection(connection), (PyObject *)boxed_call };
  PyObject *result = PyObject_VectorcallMethod_NoAsync(apst.send, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_DECREF(boxed_call);

  return result;
}

/* all threads are workers in sync mode, else check tss key */
#define IN_WORKER_THREAD(CONN) (!(CONN)->async_controller || PyThread_tss_get((&(CONN)->async_tss_key)))

#define ASYNC_FASTCALL(CONN, FUNCTION)                                                                                 \
  do                                                                                                                   \
  {                                                                                                                    \
    if (!IN_WORKER_THREAD(CONN))                                                                                       \
      return do_async_fastcall((PyObject *)(CONN), FUNCTION, self_, fast_args, fast_nargs, fast_kwnames);              \
  } while (0)
