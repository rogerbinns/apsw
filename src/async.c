
/* used for getting call details im a non-worker thread that can be invoked in the worker thread */
typedef struct
{
  PyObject_HEAD

  /* discriminated union */
  enum
  {
    Dormant = 0,
    ConnectionInit,
    VectorCall,
  } call_type;

  union
  {
    struct
    {
      PyObject *connection;
      PyObject *args;
    } ConnectionInit;

    struct
    {
      PyObject *callable;
      PyObject *fast_kwnames;
      Py_ssize_t fast_nargs; /* length of args ignoring first entry */
      /* entry 1 is not used so we can PY_VECTORCALL_ARGUMENTS_OFFSET
         and is followed by fast_nargs additional pointers */
      PyObject *fast_args[1];
    } VectorCall;
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
    Py_CLEAR(self->ConnectionInit.connection);
    Py_CLEAR(self->ConnectionInit.args);
    break;

  case VectorCall:
    Py_CLEAR(self->VectorCall.callable);
    Py_CLEAR(self->VectorCall.fast_kwnames);
    for (Py_ssize_t i = 0; i < self->VectorCall.fast_nargs; i++)
      Py_CLEAR(self->VectorCall.fast_args[1 + i]) ;
  }
  self->call_type = Dormant;
}

static void
BoxedCall_dealloc(PyObject *self)
{
  BoxedCall_clear(self);
  Py_TpFree(self);
}

static PyTypeObject BoxedCallType = {
  PyVarObject_HEAD_INIT(NULL, 0).tp_name = "apsw.aio.BoxedCall",
  .tp_basicsize = sizeof(BoxedCall),
  .tp_dealloc = BoxedCall_dealloc,
  .tp_itemsize = sizeof(PyObject *),
  .tp_free = PyObject_Free,
};

static BoxedCall *
make_boxed_call(Py_ssize_t fast_nargs)
{
  BoxedCall *box = (BoxedCall *)_PyObject_NewVar(&BoxedCallType, fast_nargs);
  assert(!box || box->call_type == Dormant);
  return box;
}

static void
async_shutdown_controller(PyObject *controller)
{
  /* exceptions are always done as unraiseable */
  if (controller)
  {
    PY_ERR_FETCH(saved_err);
    PyObject *vargs[] = { NULL, controller };
    PyObject *result
        = PyObject_VectorcallMethod_NoAsync(apst.shutdown, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
    if (!result)
    {
      AddTraceBackHere(__FILE__, __LINE__, "apsw.aio.controller_shutdown", "{s: O}", "controller", controller);
      apsw_write_unraisable(NULL);
    }
    PY_ERR_RESTORE(saved_err);
  }
}