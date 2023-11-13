/*
   Virtual table code

   See the accompanying LICENSE file.
*/

/**

.. _virtualtables:

Virtual Tables
**************

`Virtual Tables <https://sqlite.org/vtab.html>`__ let a developer
provide an underlying table implementations, while still presenting
a normal SQL interface to the user. The person writing SQL doesn't
need to know or care that some of the tables come from elsewhere.

Some examples of how you might use this:

* Translating to/from information stored in other formats

* Accessing the data remotely (eg you could make a table that backends into the cloud)

* Dynamic information (eg currently running processes, files and directories, objects in your program)

* There are other examples on the `SQLite page <https://sqlite.org/vtab.html>`__

.. tip::

  You'll find initial development a lot quicker by using
  :meth:`apsw.ext.make_virtual_module` which lets you
  export a Python function as a virtual table, being
  able to provide positional and keyword arguments as
  part of your query.

  See :ref:`the example <example_virtual_tables>`.

To write a virtual table, you need to have 3 types of object. A
:class:`module <VTModule>` providing the module, a :class:`virtual table <VTTable>`,
and a :class:`cursor <VTCursor>` that moves through a table.

*/

/** .. class:: IndexInfo

  IndexInfo represents the `sqlite3_index_info
  <https://www.sqlite.org/c3ref/index_info.html>`__ and associated
  methods used in the :meth:`VTTable.BestIndexObject` method.

  Naming is identical to the C structure rather than Pythonic.  You can
  access members directly while needing to use get/set methods for array
  members.

  You will get :exc:`InvalidContextError` if you use the object outside of an
  BestIndex method.

  :meth:`apsw.ext.index_info_to_dict` provides a convenient
  representation of this object as a :class:`dict`.

*/
typedef struct SqliteIndexInfo
{
  PyObject_HEAD
      sqlite3_index_info *index_info;
} SqliteIndexInfo;

#define CHECK_INDEX(ret)                                                                         \
  do                                                                                             \
  {                                                                                              \
    if (!self->index_info)                                                                       \
    {                                                                                            \
      PyErr_Format(ExcInvalidContext, "IndexInfo is out of scope (BestIndex call has finished)"); \
      return ret;                                                                                \
    }                                                                                            \
  } while (0)

#define CHECK_RANGE(against)                                                                                                                     \
  do                                                                                                                                             \
  {                                                                                                                                              \
    if (which < 0 || which >= (self->index_info->against))                                                                                       \
      return PyErr_Format(PyExc_IndexError, "which parameter (%i) is out of range - should be >=0 and <%i", which, (self->index_info->against)); \
  } while (0)

/** .. attribute:: nConstraint
  :type: int

  (Read-only) Number of constraint entries
*/
static PyObject *
SqliteIndexInfo_get_nConstraint(SqliteIndexInfo *self)
{
  CHECK_INDEX(NULL);

  return PyLong_FromLong(self->index_info->nConstraint);
}

/** .. attribute:: nOrderBy
  :type: int

  (Read-only) Number of order by  entries
*/
static PyObject *
SqliteIndexInfo_get_nOrderBy(SqliteIndexInfo *self)
{
  CHECK_INDEX(NULL);

  return PyLong_FromLong(self->index_info->nOrderBy);
}

/** .. method:: get_aConstraint_iColumn(which: int) -> int

 Returns *iColumn* for *aConstraint[which]*

*/
static PyObject *
SqliteIndexInfo_get_aConstraint_iColumn(SqliteIndexInfo *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int which;

  CHECK_INDEX(NULL);

  {
    IndexInfo_get_aConstraint_iColumn_CHECK;
    ARG_PROLOG(1, IndexInfo_get_aConstraint_iColumn_KWNAMES);
    ARG_MANDATORY ARG_int(which);
    ARG_EPILOG(NULL, IndexInfo_get_aConstraint_iColumn_USAGE, );
  }

  CHECK_RANGE(nConstraint);

  return PyLong_FromLong(self->index_info->aConstraint[which].iColumn);
}

/** .. method:: get_aConstraint_op(which: int) -> int

 Returns *op* for *aConstraint[which]*

*/
static PyObject *
SqliteIndexInfo_get_aConstraint_op(SqliteIndexInfo *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int which;

  CHECK_INDEX(NULL);

  {
    IndexInfo_get_aConstraint_op_CHECK;
    ARG_PROLOG(1, IndexInfo_get_aConstraint_op_KWNAMES);
    ARG_MANDATORY ARG_int(which);
    ARG_EPILOG(NULL, IndexInfo_get_aConstraint_op_USAGE, );
  }

  CHECK_RANGE(nConstraint);

  return PyLong_FromLong(self->index_info->aConstraint[which].op);
}

/** .. method:: get_aConstraint_usable(which: int) -> bool

 Returns *usable* for *aConstraint[which]*

*/
static PyObject *
SqliteIndexInfo_get_aConstraint_usable(SqliteIndexInfo *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int which;

  CHECK_INDEX(NULL);

  {
    IndexInfo_get_aConstraint_usable_CHECK;
    ARG_PROLOG(1, IndexInfo_get_aConstraint_usable_KWNAMES);
    ARG_MANDATORY ARG_int(which);
    ARG_EPILOG(NULL, IndexInfo_get_aConstraint_usable_USAGE, );
  }
  CHECK_RANGE(nConstraint);

  if (self->index_info->aConstraint[which].usable)
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

/** .. method:: get_aConstraint_collation(which: int) -> str

 Returns collation name for *aConstraint[which]*

 -* sqlite3_vtab_collation

*/
static PyObject *
SqliteIndexInfo_get_aConstraint_collation(SqliteIndexInfo *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int which;

  CHECK_INDEX(NULL);

  {
    IndexInfo_get_aConstraint_collation_CHECK;
    ARG_PROLOG(1, IndexInfo_get_aConstraint_collation_KWNAMES);
    ARG_MANDATORY ARG_int(which);
    ARG_EPILOG(NULL, IndexInfo_get_aConstraint_collation_USAGE, );
  }
  CHECK_RANGE(nConstraint);

  return convertutf8string(sqlite3_vtab_collation(self->index_info, which));
}

/** .. method:: get_aConstraint_rhs(which: int) -> SQLiteValue

 Returns right hand side value if known, else None.

 -* sqlite3_vtab_rhs_value

*/
static PyObject *
SqliteIndexInfo_get_aConstraint_rhs(SqliteIndexInfo *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int which, res;
  sqlite3_value *pval = NULL;
  CHECK_INDEX(NULL);

  {
    IndexInfo_get_aConstraint_rhs_CHECK;
    ARG_PROLOG(1, IndexInfo_get_aConstraint_rhs_KWNAMES);
    ARG_MANDATORY ARG_int(which);
    ARG_EPILOG(NULL, IndexInfo_get_aConstraint_rhs_USAGE, );
  }
  CHECK_RANGE(nConstraint);

  res = sqlite3_vtab_rhs_value(self->index_info, which, &pval);
  if (res == SQLITE_NOTFOUND)
    Py_RETURN_NONE;

  if (res != SQLITE_OK)
  {
    SET_EXC(res, NULL);
    return NULL;
  }

  return convert_value_to_pyobject(pval, 0, 0);
}

/** .. method:: get_aOrderBy_iColumn(which: int) -> int

 Returns *iColumn* for *aOrderBy[which]*

*/
static PyObject *
SqliteIndexInfo_get_aOrderBy_iColumn(SqliteIndexInfo *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int which;

  CHECK_INDEX(NULL);

  {
    IndexInfo_get_aOrderBy_iColumn_CHECK;
    ARG_PROLOG(1, IndexInfo_get_aOrderBy_iColumn_KWNAMES);
    ARG_MANDATORY ARG_int(which);
    ARG_EPILOG(NULL, IndexInfo_get_aOrderBy_iColumn_USAGE, );
  }
  CHECK_RANGE(nOrderBy);

  return PyLong_FromLong(self->index_info->aOrderBy[which].iColumn);
}

/** .. method:: get_aOrderBy_desc(which: int) -> bool

 Returns *desc* for *aOrderBy[which]*

*/
static PyObject *
SqliteIndexInfo_get_aOrderBy_desc(SqliteIndexInfo *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int which;

  CHECK_INDEX(NULL);

  {
    IndexInfo_get_aOrderBy_desc_CHECK;
    ARG_PROLOG(1, IndexInfo_get_aOrderBy_desc_KWNAMES);
    ARG_MANDATORY ARG_int(which);
    ARG_EPILOG(NULL, IndexInfo_get_aOrderBy_desc_USAGE, );
  }
  CHECK_RANGE(nOrderBy);

  return Py_NewRef(self->index_info->aOrderBy[which].desc ? Py_True : Py_False);
}

/** .. method:: get_aConstraintUsage_argvIndex(which: int) -> int

 Returns *argvIndex* for *aConstraintUsage[which]*

*/
static PyObject *
SqliteIndexInfo_get_aConstraintUsage_argvIndex(SqliteIndexInfo *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int which;

  CHECK_INDEX(NULL);

  {
    IndexInfo_get_aConstraintUsage_argvIndex_CHECK;
    ARG_PROLOG(1, IndexInfo_get_aConstraintUsage_argvIndex_KWNAMES);
    ARG_MANDATORY ARG_int(which);
    ARG_EPILOG(NULL, IndexInfo_get_aConstraintUsage_argvIndex_USAGE, );
  }
  CHECK_RANGE(nConstraint);

  return PyLong_FromLong(self->index_info->aConstraintUsage[which].argvIndex);
}

/** .. method:: set_aConstraintUsage_argvIndex(which: int, argvIndex: int) -> None

 Sets *argvIndex* for *aConstraintUsage[which]*

*/
static PyObject *
SqliteIndexInfo_set_aConstraintUsage_argvIndex(SqliteIndexInfo *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int which, argvIndex;

  CHECK_INDEX(NULL);

  {
    IndexInfo_set_aConstraintUsage_argvIndex_CHECK;
    ARG_PROLOG(2, IndexInfo_set_aConstraintUsage_argvIndex_KWNAMES);
    ARG_MANDATORY ARG_int(which);
    ARG_MANDATORY ARG_int(argvIndex);
    ARG_EPILOG(NULL, IndexInfo_set_aConstraintUsage_argvIndex_USAGE, );
  }
  CHECK_RANGE(nConstraint);

  self->index_info->aConstraintUsage[which].argvIndex = argvIndex;
  Py_RETURN_NONE;
}

/** .. method:: get_aConstraintUsage_omit(which: int) -> bool

 Returns *omit* for *aConstraintUsage[which]*

*/
static PyObject *
SqliteIndexInfo_get_aConstraintUsage_omit(SqliteIndexInfo *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int which;

  CHECK_INDEX(NULL);

  {
    IndexInfo_get_aConstraintUsage_omit_CHECK;
    ARG_PROLOG(1, IndexInfo_get_aConstraintUsage_omit_KWNAMES);
    ARG_MANDATORY ARG_int(which);
    ARG_EPILOG(NULL, IndexInfo_get_aConstraintUsage_omit_USAGE, );
  }
  CHECK_RANGE(nConstraint);

  if (self->index_info->aConstraintUsage[which].omit)
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

/** .. method:: set_aConstraintUsage_omit(which: int, omit: bool) -> None

 Sets *omit* for *aConstraintUsage[which]*

*/
static PyObject *
SqliteIndexInfo_set_aConstraintUsage_omit(SqliteIndexInfo *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int which, omit;

  CHECK_INDEX(NULL);

  {
    IndexInfo_set_aConstraintUsage_omit_CHECK;
    ARG_PROLOG(2, IndexInfo_set_aConstraintUsage_omit_KWNAMES);
    ARG_MANDATORY ARG_int(which);
    ARG_MANDATORY ARG_bool(omit);
    ARG_EPILOG(NULL, IndexInfo_set_aConstraintUsage_omit_USAGE, );
  }
  CHECK_RANGE(nConstraint);

  self->index_info->aConstraintUsage[which].omit = omit;
  Py_RETURN_NONE;
}

/** .. method:: get_aConstraintUsage_in(which: int) -> bool

 Returns True if the constraint is *in* - eg column in (3, 7, 9)

 -* sqlite3_vtab_in
*/
static PyObject *
SqliteIndexInfo_get_aConstraintUsage_in(SqliteIndexInfo *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int which;

  CHECK_INDEX(NULL);

  {
    IndexInfo_get_aConstraintUsage_in_CHECK;
    ARG_PROLOG(1, IndexInfo_get_aConstraintUsage_in_KWNAMES);
    ARG_MANDATORY ARG_int(which);
    ARG_EPILOG(NULL, IndexInfo_get_aConstraintUsage_in_USAGE, );
  }
  CHECK_RANGE(nConstraint);

  if (sqlite3_vtab_in(self->index_info, which, -1))
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

/** .. method:: set_aConstraintUsage_in(which: int, filter_all: bool) -> None

 If *which* is an *in* constraint, and *filter_all* is True then your :meth:`VTCursor.Filter`
 method will have all of the values at once.

 -* sqlite3_vtab_in
*/
static PyObject *
SqliteIndexInfo_set_aConstraintUsage_in(SqliteIndexInfo *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  int which, filter_all;

  CHECK_INDEX(NULL);

  {
    IndexInfo_set_aConstraintUsage_in_CHECK;
    ARG_PROLOG(2, IndexInfo_set_aConstraintUsage_in_KWNAMES);
    ARG_MANDATORY ARG_int(which);
    ARG_MANDATORY ARG_bool(filter_all);
    ARG_EPILOG(NULL, IndexInfo_set_aConstraintUsage_in_USAGE, );
  }
  CHECK_RANGE(nConstraint);

  if (sqlite3_vtab_in(self->index_info, which, -1))
  {
    sqlite3_vtab_in(self->index_info, which, filter_all);
    Py_RETURN_NONE;
  }
  return PyErr_Format(PyExc_ValueError, "Constraint %d is not an 'in' which can be set", which);
}

/** .. attribute:: idxNum
  :type: int

  Number used to identify the index
*/
static PyObject *
SqliteIndexInfo_get_idxNum(SqliteIndexInfo *self)
{
  CHECK_INDEX(NULL);

  return PyLong_FromLong(self->index_info->idxNum);
}

static int
SqliteIndexInfo_set_idxNum(SqliteIndexInfo *self, PyObject *value)
{
  int v;

  CHECK_INDEX(-1);

  if (!PyLong_Check(value))
  {
    PyErr_Format(PyExc_TypeError, "Expected an int, not %s", Py_TypeName(value));
    return -1;
  }
  v = PyLong_AsInt(value);
  if (PyErr_Occurred())
    return -1;
  self->index_info->idxNum = v;
  return 0;
}

/** .. attribute:: idxStr
  :type: Optional[str]

  Name used to identify the index
*/
static PyObject *
SqliteIndexInfo_get_idxStr(SqliteIndexInfo *self)
{
  CHECK_INDEX(NULL);

  return convertutf8string(self->index_info->idxStr);
}

static int
SqliteIndexInfo_set_idxStr(SqliteIndexInfo *self, PyObject *value)
{
  CHECK_INDEX(-1);

  if (!Py_IsNone(value) && !PyUnicode_Check(value))
  {
    PyErr_Format(PyExc_TypeError, "Expected None or str, not %s", Py_TypeName(value));
    return -1;
  }

  if (self->index_info->idxStr && self->index_info->needToFreeIdxStr)
  {
    sqlite3_free(self->index_info->idxStr);
  }

  self->index_info->idxStr = NULL;
  self->index_info->needToFreeIdxStr = 0;

  if (!Py_IsNone(value))
  {
    const char *svalue = PyUnicode_AsUTF8(value);
    if (!svalue)
      return -1;
    const char *isvalue = sqlite3_mprintf("%s", svalue);
    if (!isvalue)
    {
      PyErr_NoMemory();
      return -1;
    }
    self->index_info->idxStr = (char *)isvalue;
    self->index_info->needToFreeIdxStr = 1;
  }

  return 0;
}

/** .. attribute:: orderByConsumed
  :type: bool

  True if index output is already ordered
*/
static PyObject *
SqliteIndexInfo_get_orderByConsumed(SqliteIndexInfo *self)
{
  CHECK_INDEX(NULL);

  return Py_NewRef(self->index_info->orderByConsumed ? Py_True : Py_False);
}

static int
SqliteIndexInfo_set_OrderByConsumed(SqliteIndexInfo *self, PyObject *value)
{
  CHECK_INDEX(-1);

  self->index_info->orderByConsumed = PyObject_IsTrueStrict(value);
  if (self->index_info->orderByConsumed == -1)
  {
    assert(PyErr_Occurred());
    return -1;
  }

  return 0;
}

/** .. attribute:: estimatedCost
  :type: float

  Estimated cost of using this index
*/
static PyObject *
SqliteIndexInfo_get_estimatedCost(SqliteIndexInfo *self)
{
  CHECK_INDEX(NULL);

  return PyFloat_FromDouble(self->index_info->estimatedCost);
}

static int
SqliteIndexInfo_set_estimatedCost(SqliteIndexInfo *self, PyObject *value)
{
  double v;
  CHECK_INDEX(-1);

  v = PyFloat_AsDouble(value);

  if (PyErr_Occurred())
    return -1;

  self->index_info->estimatedCost = v;

  return 0;
}

/** .. attribute:: estimatedRows
  :type: int

  Estimated number of rows returned
*/
static PyObject *
SqliteIndexInfo_get_estimatedRows(SqliteIndexInfo *self)
{
  CHECK_INDEX(NULL);

  return PyLong_FromLongLong(self->index_info->estimatedRows);
}

static int
SqliteIndexInfo_set_estimatedRows(SqliteIndexInfo *self, PyObject *value)
{
  sqlite3_int64 v;
  CHECK_INDEX(-1);

  if (!PyLong_Check(value))
  {
    PyErr_Format(PyExc_TypeError, "Expected an int, not %s", Py_TypeName(value));
    return -1;
  }

  v = PyLong_AsLongLong(value);

  if (PyErr_Occurred())
    return -1;

  self->index_info->estimatedRows = v;

  return 0;
}

/** .. attribute:: idxFlags
  :type: int

  Mask of :attr:`SQLITE_INDEX_SCAN flags <apsw.mapping_virtual_table_scan_flags>`
*/
static PyObject *
SqliteIndexInfo_get_idxFlags(SqliteIndexInfo *self)
{
  CHECK_INDEX(NULL);

  return PyLong_FromLong(self->index_info->idxFlags);
}

static int
SqliteIndexInfo_set_idxFlags(SqliteIndexInfo *self, PyObject *value)
{
  int v;
  CHECK_INDEX(-1);

  if (!PyLong_Check(value))
  {
    PyErr_Format(PyExc_TypeError, "Expected an int, not %s", Py_TypeName(value));
    return -1;
  }

  v = PyLong_AsInt(value);
  if (PyErr_Occurred())
    return -1;
  self->index_info->idxFlags = v;
  return 0;
}

/** .. attribute:: colUsed
  :type: set[int]

  (Read-only) Columns used by the statement.  Note that a set is returned, not
  the underlying integer.
*/
static PyObject *
SqliteIndexInfo_get_colUsed(SqliteIndexInfo *self)
{
  PyObject *retval = NULL, *tmp = NULL;
  sqlite3_uint64 colUsed, mask;
  int i;
  CHECK_INDEX(NULL);

  colUsed = self->index_info->colUsed;

  retval = PySet_New(NULL);
  if (!retval)
    goto finally;

  for (mask = 1, i = 0; i <= 63; i++, mask <<= 1)
  {
    if (colUsed & mask)
    {
      tmp = PyLong_FromLong(i);
      if (!tmp)
        goto finally;
      if (0 != PySet_Add(retval, tmp))
        goto finally;
      Py_CLEAR(tmp);
    }
  }

finally:
  if (PyErr_Occurred())
  {
    Py_CLEAR(retval);
    Py_CLEAR(tmp);
  }

  return retval;
}

/** .. attribute:: distinct
  :type: int

  (Read-only) How the query planner would like output ordered
  if the query is using group by or distinct.

  -* sqlite3_vtab_distinct
*/
static PyObject *
SqliteIndexInfo_get_distinct(SqliteIndexInfo *self)
{
  CHECK_INDEX(NULL);

  return PyLong_FromLong(sqlite3_vtab_distinct(self->index_info));
}

static PyGetSetDef SqliteIndexInfo_getsetters[] = {
    {"nConstraint", (getter)SqliteIndexInfo_get_nConstraint, NULL, IndexInfo_nConstraint_DOC},
    {"nOrderBy", (getter)SqliteIndexInfo_get_nOrderBy, NULL, IndexInfo_nOrderBy_DOC},
    {"idxNum", (getter)SqliteIndexInfo_get_idxNum, (setter)SqliteIndexInfo_set_idxNum, IndexInfo_idxNum_DOC},
    {"idxStr", (getter)SqliteIndexInfo_get_idxStr, (setter)SqliteIndexInfo_set_idxStr, IndexInfo_idxStr_DOC},
    {"orderByConsumed", (getter)SqliteIndexInfo_get_orderByConsumed, (setter)SqliteIndexInfo_set_OrderByConsumed, IndexInfo_orderByConsumed_DOC},
    {"estimatedCost", (getter)SqliteIndexInfo_get_estimatedCost, (setter)SqliteIndexInfo_set_estimatedCost, IndexInfo_estimatedCost_DOC},
    {"estimatedRows", (getter)SqliteIndexInfo_get_estimatedRows, (setter)SqliteIndexInfo_set_estimatedRows, IndexInfo_estimatedRows_DOC},
    {"idxFlags", (getter)SqliteIndexInfo_get_idxFlags, (setter)SqliteIndexInfo_set_idxFlags, IndexInfo_idxFlags_DOC},
    {"colUsed", (getter)SqliteIndexInfo_get_colUsed, NULL, IndexInfo_colUsed_DOC},
    {"distinct", (getter)SqliteIndexInfo_get_distinct, NULL, IndexInfo_distinct_DOC},
    /* sentinel */
    {NULL, NULL, NULL, NULL}};

static PyMethodDef SqliteIndexInfo_methods[] = {
    {"get_aConstraint_iColumn", (PyCFunction)SqliteIndexInfo_get_aConstraint_iColumn, METH_FASTCALL | METH_KEYWORDS,
     IndexInfo_get_aConstraint_iColumn_DOC},
    {"get_aConstraint_op", (PyCFunction)SqliteIndexInfo_get_aConstraint_op, METH_FASTCALL | METH_KEYWORDS,
     IndexInfo_get_aConstraint_op_DOC},
    {"get_aConstraint_usable", (PyCFunction)SqliteIndexInfo_get_aConstraint_usable, METH_FASTCALL | METH_KEYWORDS,
     IndexInfo_get_aConstraint_usable_DOC},
    {"get_aConstraint_collation", (PyCFunction)SqliteIndexInfo_get_aConstraint_collation, METH_FASTCALL | METH_KEYWORDS,
     IndexInfo_get_aConstraint_collation_DOC},
    {"get_aConstraint_rhs", (PyCFunction)SqliteIndexInfo_get_aConstraint_rhs, METH_FASTCALL | METH_KEYWORDS,
     IndexInfo_get_aConstraint_rhs_DOC},
    {"get_aOrderBy_iColumn", (PyCFunction)SqliteIndexInfo_get_aOrderBy_iColumn, METH_FASTCALL | METH_KEYWORDS,
     IndexInfo_get_aOrderBy_iColumn_DOC},
    {"get_aOrderBy_desc", (PyCFunction)SqliteIndexInfo_get_aOrderBy_desc, METH_FASTCALL | METH_KEYWORDS,
     IndexInfo_get_aOrderBy_desc_DOC},
    {"get_aConstraintUsage_argvIndex", (PyCFunction)SqliteIndexInfo_get_aConstraintUsage_argvIndex, METH_FASTCALL | METH_KEYWORDS,
     IndexInfo_get_aConstraintUsage_argvIndex_DOC},
    {"set_aConstraintUsage_argvIndex", (PyCFunction)SqliteIndexInfo_set_aConstraintUsage_argvIndex, METH_FASTCALL | METH_KEYWORDS,
     IndexInfo_set_aConstraintUsage_argvIndex_DOC},
    {"get_aConstraintUsage_omit", (PyCFunction)SqliteIndexInfo_get_aConstraintUsage_omit, METH_FASTCALL | METH_KEYWORDS,
     IndexInfo_get_aConstraintUsage_omit_DOC},
    {"set_aConstraintUsage_omit", (PyCFunction)SqliteIndexInfo_set_aConstraintUsage_omit, METH_FASTCALL | METH_KEYWORDS,
     IndexInfo_set_aConstraintUsage_omit_DOC},
    {"get_aConstraintUsage_in", (PyCFunction)SqliteIndexInfo_get_aConstraintUsage_in, METH_FASTCALL | METH_KEYWORDS,
     IndexInfo_get_aConstraintUsage_in_DOC},
    {"set_aConstraintUsage_in", (PyCFunction)SqliteIndexInfo_set_aConstraintUsage_in, METH_FASTCALL | METH_KEYWORDS,
     IndexInfo_set_aConstraintUsage_in_DOC},

    /* sentinel */
    {NULL, NULL, 0, NULL}};

static PyTypeObject SqliteIndexInfoType = {
    PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "apsw.IndexInfo",
    .tp_doc = IndexInfo_class_DOC,
    .tp_basicsize = sizeof(SqliteIndexInfo),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_getset = SqliteIndexInfo_getsetters,
    .tp_methods = SqliteIndexInfo_methods,
};

#undef CHECK_INDEX
#undef CHECK_RANGE

/** .. class:: VTModule

.. note::

  There is no actual *VTModule* class - it is shown this way for
  documentation convenience and is present as a `typing protocol
  <https://docs.python.org/3/library/typing.html#typing.Protocol>`__.

A module instance is used to create the virtual tables.  Once you have
a module object, you register it with a connection by calling
:meth:`Connection.create_module`::

  # make an instance
  mymod=MyModuleClass()

  # register the vtable on connection con
  con.create_module("modulename", mymod)

  # tell SQLite about the table
  con.execute("create VIRTUAL table tablename USING modulename('arg1', 2)")

The create step is to tell SQLite about the existence of the table.
Any number of tables referring to the same module can be made this
way.
*/

typedef struct
{
  sqlite3_vtab used_by_sqlite; /* I don't touch this */
  PyObject *vtable;            /* object implementing vtable */
  PyObject *functions;         /* functions returned by vtabFindFunction */
  int bestindex_object;        /* 0: tuples are passed to xBestIndex, 1: object is */
  int use_no_change;           /* 1: we understand no_change updating */
  Connection *connection;
} apsw_vtable;

static int
apswvtabCreateOrConnect(sqlite3 *db,
                        void *pAux,
                        int argc,
                        const char *const *argv,
                        sqlite3_vtab **pVTab,
                        char **errmsg,
                        /* args above are to Create/Connect method */
                        PyObject *methodname,
                        const char *declarevtabtracebackname,
                        const char *pyexceptionname)
{
  PyGILState_STATE gilstate;
  vtableinfo *vti;
  PyObject *pyres = NULL, *schema = NULL, *vtable = NULL;
  apsw_vtable *avi = NULL;
  int res = SQLITE_OK;
  int i;
  PyObject **vargs_cleanup = NULL;

  VLA_PYO(vargs, 3 + argc);

  gilstate = PyGILState_Ensure();

  MakeExistingException();

  vti = (vtableinfo *)pAux;
  assert(db == vti->connection->db);

  Connection *self = vti->connection;
  CALL_ENTER(xConnect);

  if (PyErr_Occurred())
    goto pyexception;

  vargs[0] = NULL;
  vargs[1] = vti->datasource;
  vargs[2] = (PyObject *)self;

  for (i = 0; i < argc; i++)
    vargs[3 + i] = convertutf8string(argv[i]);

  vargs_cleanup = vargs;

  for (i = 0; i < argc; i++)
    if (!vargs[3 + i])
      goto pyexception;

  pyres = PyObject_VectorcallMethod(methodname, vargs + 1, (2 + argc) | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);

  if (!pyres)
    goto pyexception;

  /* pyres should be a tuple of two values - a string of sql describing
     the table and an object implementing it */
  if (!PySequence_Check(pyres) || PySequence_Size(pyres) != 2)
  {
    if (!PyErr_Occurred())
      PyErr_Format(PyExc_TypeError, "Expected two values - a string with the table schema and a vtable object implementing it");
    goto pyexception;
  }

  vtable = PySequence_GetItem(pyres, 1);
  if (!vtable)
    goto pyexception;

  schema = PySequence_GetItem(pyres, 0);
  if (!schema)
    goto pyexception;
  if (!PyUnicode_Check(schema))
  {
    PyErr_Format(PyExc_TypeError, "Expected string for schema");
    goto pyexception;
  }
  {
    const char *utf8schema = PyUnicode_AsUTF8(schema);
    if (!utf8schema)
      goto pyexception;
    _PYSQLITE_CALL_E(db, res = sqlite3_declare_vtab(db, utf8schema));
    if (res != SQLITE_OK)
    {
      SET_EXC(res, db);
      AddTraceBackHere(__FILE__, __LINE__, declarevtabtracebackname, "{s: O}", "schema", OBJ(schema));
      goto finally;
    }
  }

  assert(res == SQLITE_OK);
  avi = PyMem_Calloc(1, sizeof(apsw_vtable));
  if (!avi)
    goto pyexception;
  assert((void *)avi == (void *)&(avi->used_by_sqlite)); /* detect if weird padding happens */
  avi->bestindex_object = vti->bestindex_object;
  avi->use_no_change = vti->use_no_change;
  avi->connection = self;

  *pVTab = (sqlite3_vtab *)avi;
  avi->vtable = Py_NewRef(vtable);
  goto finally;

pyexception: /* we had an exception in python code */
  res = MakeSqliteMsgFromPyException(errmsg);
  AddTraceBackHere(__FILE__, __LINE__, pyexceptionname,
                   "{s: s, s: s, s: s, s: O}", "modulename", argv[0], "database", argv[1], "tablename", argv[2], "schema", OBJ(schema));

finally: /* cleanup */
  if (vargs_cleanup)
    for (i = 0; i < argc; i++)
      Py_XDECREF(vargs_cleanup[3 + i]);
  Py_XDECREF(pyres);
  Py_XDECREF(schema);
  Py_XDECREF(vtable);
  CALL_LEAVE(xConnect);
  PyGILState_Release(gilstate);
  assert((*pVTab != 0 && res == SQLITE_OK) || (*pVTab == 0 && res != SQLITE_OK));
  return res;
}

/** .. method:: Connect(connection: Connection, modulename: str, databasename: str, tablename: str, *args: tuple[SQLiteValue, ...])  -> tuple[str, VTTable]

    The parameters and return are identical to
    :meth:`~VTModule.Create`.  This method is called
    when there are additional references to the table.  :meth:`~VTModule.Create` will be called the first time and
    :meth:`~VTModule.Connect` after that.

    The advise is to create caches, generated data and other
    heavyweight processing on :meth:`~VTModule.Create` calls and then
    find and reuse that on the subsequent :meth:`~VTModule.Connect`
    calls.

    The corresponding call is :meth:`VTTable.Disconnect`.  If you have a simple virtual table implementation, then just
    set :meth:`~VTModule.Connect` to be the same as :meth:`~VTModule.Create`::

      class MyModule:

           def Create(self, connection, modulename, databasename, tablename, *args):
               # do lots of hard work

           Connect=Create

*/

static int
apswvtabCreate(sqlite3 *db,
               void *pAux,
               int argc,
               const char *const *argv,
               sqlite3_vtab **pVTab,
               char **errmsg)
{
  return apswvtabCreateOrConnect(db, pAux, argc, argv, pVTab, errmsg, apst.Create, "VirtualTable.xCreate.sqlite3_declare_vtab",
                                 "VirtualTable.xCreate");
}

/** .. method:: Create(connection: Connection, modulename: str, databasename: str, tablename: str, *args: tuple[SQLiteValue, ...])  -> tuple[str, VTTable]

   Called when a table is first created on a :class:`connection
   <Connection>`.

   :param connection: An instance of :class:`Connection`
   :param modulename: The string name under which the module was :meth:`registered <Connection.create_module>`
   :param databasename: The name of the database.  `main`, `temp`, the name in `ATTACH <https://sqlite.org/lang_attach.html>`__
   :param tablename: Name of the table the user wants to create.
   :param args: Any arguments that were specified in the `create virtual table <https://sqlite.org/lang_createvtab.html>`_ statement.

   :returns: A list of two items.  The first is a SQL `create table <https://sqlite.org/lang_createtable.html>`_ statement.  The
        columns are parsed so that SQLite knows what columns and declared types exist for the table.  The second item
        is an object that implements the :class:`table <VTTable>` methods.

   The corresponding call is :meth:`VTTable.Destroy`.
*/

static int
apswvtabConnect(sqlite3 *db,
                void *pAux,
                int argc,
                const char *const *argv,
                sqlite3_vtab **pVTab,
                char **errmsg)
{
  return apswvtabCreateOrConnect(db, pAux, argc, argv, pVTab, errmsg, apst.Connect, "VirtualTable.xConnect.sqlite3_declare_vtab",
                                 "VirtualTable.xConnect");
}

/** .. method:: ShadowName(table_suffix: str) -> bool

  This method is called to check if
  *table_suffix* is a `shadow name
  <https://www.sqlite.org/vtab.html#the_xshadowname_method>`__

  The default implementation always returns *False*.

  If a virtual table is created using this module
  named :code:`example` and then a  real table is created
  named :code:`example_content`, this would be called with
  a *table_suffix* of :code:`content`
*/

/* actual implementation is later */

/** .. class:: VTTable

  .. note::

    There is no actual *VTTable* class - it is shown this way for
    documentation convenience and is present as a `typing protocol
    <https://docs.python.org/3/library/typing.html#typing.Protocol>`__.

  The :class:`VTTable` object contains knowledge of the indices, makes
  cursors and can perform transactions.

  A virtual table is structured as a series of rows, each of which has
  the same number of columns.  The value in a column must be one of the `5
  supported types <https://sqlite.org/datatype3.html>`_, but the
  type can be different between rows for the same column.  The virtual
  table routines identify the columns by number, starting at zero.

  Each row has a **unique** 64 bit integer `rowid
  <https://sqlite.org/autoinc.html>`_ with the :class:`Cursor
  <VTCursor>` routines operating on this number, as well as some of
  the :class:`Table <VTTable>` routines such as :meth:`UpdateChangeRow
  <VTTable.UpdateChangeRow>`.

  It is possible to `not have a rowid
  <https://www.sqlite.org/vtab.html#_without_rowid_virtual_tables_>`__

*/

static void freeShadowName(sqlite3_module *mod, PyObject *datasource);

static void
apswvtabFree(void *context)
{
  PyGILState_STATE gilstate;
  gilstate = PyGILState_Ensure();

  vtableinfo *vti = (vtableinfo *)context;

  if (vti->sqlite3_module_def && vti->sqlite3_module_def->xShadowName)
    freeShadowName(vti->sqlite3_module_def, vti->datasource);

  Py_XDECREF(vti->datasource);
  PyMem_Free(vti->sqlite3_module_def);
  /* connection was a borrowed reference so no decref needed */
  PyMem_Free(vti);

  PyGILState_Release(gilstate);
}

static int
apswvtabDestroyOrDisconnect(sqlite3_vtab *pVtab, PyObject *methodname, const char *exception_name)
{
  PyObject *vtable, *res = NULL;
  PyGILState_STATE gilstate;
  int sqliteres = SQLITE_OK;

  gilstate = PyGILState_Ensure();
  vtable = ((apsw_vtable *)pVtab)->vtable;

  MakeExistingException();

  CHAIN_EXC_BEGIN
  /* mandatory for Destroy, optional for Disconnect */
  if (methodname == apst.Destroy || PyObject_HasAttr(vtable, methodname))
  {
    PyObject *vargs[] = {NULL, vtable};
    res = PyObject_VectorcallMethod(methodname, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);

    if (!res)
    {
      sqliteres = MakeSqliteMsgFromPyException(NULL);
      AddTraceBackHere(__FILE__, __LINE__, exception_name, "{s: O}", "self", OBJ(vtable));
    }
    else
      Py_DECREF(res);
  }
  CHAIN_EXC_END;

  if (sqliteres == SQLITE_OK || methodname == apst.Disconnect)
  {
    Py_DECREF(vtable);
    Py_XDECREF(((apsw_vtable *)pVtab)->functions);
    PyMem_Free(pVtab);
  }

  if (PyErr_Occurred())
    apsw_write_unraisable(NULL);

  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: Destroy() -> None

  The opposite of :meth:`VTModule.Create`.  This method is called when
  the table is no longer used.  Note that you must always release
  resources even if you intend to return an error, as it will not be
  called again on error.
*/

static int
apswvtabDestroy(sqlite3_vtab *pVTab)
{
  return apswvtabDestroyOrDisconnect(pVTab, apst.Destroy, "VirtualTable.xDestroy");
}

/** .. method:: Disconnect() -> None

  The opposite of :meth:`VTModule.Connect`.  This method is called when
  a reference to a virtual table is no longer used, but :meth:`VTTable.Destroy` will
  be called when the table is no longer used.
*/

static int
apswvtabDisconnect(sqlite3_vtab *pVTab)
{
  return apswvtabDestroyOrDisconnect(pVTab, apst.Disconnect, "VirtualTable.xDisconnect");
}

/** .. method:: BestIndexObject(index_info: IndexInfo) -> bool

  This method is called instead of :meth:`BestIndex` if
  *use_bestindex_object* was *True* in the call to
  :meth:`Connection.create_module`.

  Use the :class:`IndexInfo` to tell SQLite about your indexes, and
  extract other information.

  Return *True* to indicate all is well.  If you return *False* or there is an error,
  then `SQLITE_CONSTRAINT
  <https://www.sqlite.org/vtab.html#return_value>`__ is returned to
  SQLite.
*/
static int
apswvtabBestIndexObject(sqlite3_vtab *pVtab, sqlite3_index_info *in_index_info)
{
  PyGILState_STATE gilstate;
  PyObject *vtable;
  PyObject *res = NULL;
  int sqlite_res = SQLITE_ERROR;
  struct SqliteIndexInfo *index_info = NULL;

  gilstate = PyGILState_Ensure();
  vtable = ((apsw_vtable *)pVtab)->vtable;

  MakeExistingException();

  if (PyErr_Occurred())
    goto finally;

  index_info = (struct SqliteIndexInfo *)_PyObject_New(&SqliteIndexInfoType);
  if (!index_info)
    goto finally;

  index_info->index_info = in_index_info;

  PyObject *vargs[] = {NULL, vtable, (PyObject *)index_info};

  res = PyObject_VectorcallMethod(apst.BestIndexObject, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  if (!res)
    goto finally;

  if (Py_IsTrue(res))
    sqlite_res = SQLITE_OK;
  else if (Py_IsFalse(res))
    sqlite_res = SQLITE_CONSTRAINT;
  else
    PyErr_Format(PyExc_TypeError, "Expected bool result, not %s", Py_TypeName(res));

finally:
  if (PyErr_Occurred())
  {
    sqlite_res = MakeSqliteMsgFromPyException(&(pVtab->zErrMsg));
    AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xBestIndexObject", "{s: O, s: O, s: O}",
                     "self", vtable, "index_info", OBJ((PyObject *)index_info), "res", OBJ(res));
  }
  if (index_info)
    index_info->index_info = NULL;
  Py_XDECREF((PyObject *)index_info);
  Py_XDECREF(res);
  PyGILState_Release(gilstate);

  return sqlite_res;
}

/** .. method:: BestIndex(constraints: Sequence[tuple[int, int]], orderbys: Sequence[tuple[int, int]]) -> Any

  This is a complex method. To get going initially, just return
  *None* and you will be fine. You should also consider using
  :meth:`BestIndexObject` instead.

  Implementing this method reduces the number of rows scanned
  in your table to satisfy queries, but only if you have an
  index or index like mechanism available.

  .. note::

    The implementation of this method differs slightly from the
    `SQLite documentation
    <https://sqlite.org/vtab.html>`__
    for the C API. You are not passed "unusable" constraints. The
    argv/constraintarg positions are not off by one. In the C api, you
    have to return position 1 to get something passed to
    :meth:`VTCursor.Filter` in position 0. With the APSW
    implementation, you return position 0 to get Filter arg 0,
    position 1 to get Filter arg 1 etc.

  The purpose of this method is to ask if you have the ability to
  determine if a row meets certain constraints that doesn't involve
  visiting every row. An example constraint is ``price > 74.99``. In a
  traditional SQL database, queries with constraints can be speeded up
  `with indices <https://sqlite.org/lang_createindex.html>`_. If
  you return None, then SQLite will visit every row in your table and
  evaluate the constraints itself. Your index choice returned from
  BestIndex will also be passed to the :meth:`~VTCursor.Filter` method on your cursor
  object. Note that SQLite may call this method multiple times trying
  to find the most efficient way of answering a complex query.

  **constraints**

  You will be passed the constraints as a sequence of tuples containing two
  items. The first item is the column number and the second item is
  the operation.

     Example query: ``select * from foo where price > 74.99 and
     quantity<=10 and customer='Acme Widgets'``

     If customer is column 0, price column 2 and quantity column 5
     then the constraints will be::

       (2, apsw.SQLITE_INDEX_CONSTRAINT_GT),
       (5, apsw.SQLITE_INDEX_CONSTRAINT_LE),
       (0, apsw.SQLITE_INDEX_CONSTRAINT_EQ)

     Note that you do not get the value of the constraint (ie "Acme
     Widgets", 74.99 and 10 in this example).

  If you do have any suitable indices then you return a sequence the
  same length as constraints with the members mapping to the
  constraints in order. Each can be one of None, an integer or a tuple
  of an integer and a boolean.  Conceptually SQLite is giving you a
  list of constraints and you are returning a list of the same length
  describing how you could satisfy each one.

  Each list item returned corresponding to a constraint is one of:

     None
       This means you have no index for that constraint. SQLite
       will have to iterate over every row for it.

     integer
       This is the argument number for the constraintargs being passed
       into the :meth:`~VTCursor.Filter` function of your
       :class:`cursor <VTCursor>` (the values "Acme Widgets", 74.99
       and 10 in the example).

     (integer, boolean)
       By default SQLite will check what you return. For example if
       you said that you had an index on price and so would only
       return rows greater than 74.99, then SQLite will still
       check that each row you returned is greater than 74.99.
       If the boolean is True then SQLite will not double
       check, while False retains the default double checking.

  Example query: ``select * from foo where price > 74.99 and
  quantity<=10 and customer=='Acme Widgets'``.  customer is column 0,
  price column 2 and quantity column 5.  You can index on customer
  equality and price.

  +----------------------------------------+--------------------------------+
  | Constraints (in)                       | Constraints used (out)         |
  +========================================+================================+
  | ::                                     | ::                             |
  |                                        |                                |
  |  (2, apsw.SQLITE_INDEX_CONSTRAINT_GT), |     1,                         |
  |  (5, apsw.SQLITE_INDEX_CONSTRAINT_LE), |     None,                      |
  |  (0, apsw.SQLITE_INDEX_CONSTRAINT_EQ)  |     0                          |
  |                                        |                                |
  +----------------------------------------+--------------------------------+

  When your :class:`~VTCursor.Filter` method in the cursor is called,
  constraintarg[0] will be "Acme Widgets" (customer constraint value)
  and constraintarg[1] will be 74.99 (price constraint value). You can
  also return an index number (integer) and index string to use
  (SQLite attaches no significance to these values - they are passed
  as is to your :meth:`VTCursor.Filter` method as a way for the
  BestIndex method to let the :meth:`~VTCursor.Filter` method know
  which of your indices or similar mechanism to use.

  **orderbys**


  The second argument to BestIndex is a sequence of orderbys because
  the query requested the results in a certain order. If your data is
  already in that order then SQLite can give the results back as
  is. If not, then SQLite will have to sort the results first.

    Example query: ``select * from foo order by price desc, quantity asc``

    Price is column 2, quantity column 5 so orderbys will be::

      (2, True),  # True means descending, False is ascending
      (5, False)

  **Return**

  You should return up to 5 items. Items not present in the return have a default value.

  0: constraints used (default None)
    This must either be None or a sequence the same length as
    constraints passed in. Each item should be as specified above
    saying if that constraint is used, and if so which constraintarg
    to make the value be in your :meth:`VTCursor.Filter` function.

  1: index number (default zero)
    This value is passed as is to :meth:`VTCursor.Filter`

  2: index string (default None)
    This value is passed as is to :meth:`VTCursor.Filter`

  3: orderby consumed (default False)
    Return True if your output will be in exactly the same order as the orderbys passed in

  4: estimated cost (default a huge number)
    Approximately how many disk operations are needed to provide the
    results. SQLite uses the cost to optimise queries. For example if
    the query includes *A or B* and A has 2,000 operations and B has 100
    then it is best to evaluate B before A.

  **A complete example**

  Query is ``select * from foo where price>74.99 and quantity<=10 and
  customer=="Acme Widgets" order by price desc, quantity asc``.
  Customer is column 0, price column 2 and quantity column 5. You can
  index on customer equality and price.

  ::

    BestIndex(constraints, orderbys)

    constraints= ( (2, apsw.SQLITE_INDEX_CONSTRAINT_GT),
                   (5, apsw.SQLITE_INDEX_CONSTRAINT_LE),
                   (0, apsw.SQLITE_INDEX_CONSTRAINT_EQ)  )

    orderbys= ( (2, True), (5, False) )


    # You return

    ( (1, None, 0),   # constraints used
      27,             # index number
      "idx_pr_cust",  # index name
      False,          # results are not in orderbys order
      1000            # about 1000 disk operations to access index
    )


    # Your Cursor.Filter method will be called with:

    27,              # index number you returned
    "idx_pr_cust",   # index name you returned
    "Acme Widgets",  # constraintarg[0] - customer
    74.99            # constraintarg[1] - price

*/

static int
apswvtabBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *indexinfo)
{
  PyGILState_STATE gilstate;
  PyObject *vtable;
  PyObject *constraints = NULL, *orderbys = NULL;
  PyObject *res = NULL, *indices = NULL;
  int i, j;
  int nconstraints = 0;
  int sqliteres = SQLITE_OK;

  if (((apsw_vtable *)pVtab)->bestindex_object)
    return apswvtabBestIndexObject(pVtab, indexinfo);

  gilstate = PyGILState_Ensure();

  MakeExistingException();

  vtable = ((apsw_vtable *)pVtab)->vtable;

  if (PyErr_Occurred())
    goto pyexception;

  /* count how many usable constraints there are */
  for (i = 0; i < indexinfo->nConstraint; i++)
    if (indexinfo->aConstraint[i].usable)
      nconstraints++;

  constraints = PyTuple_New(nconstraints);
  if (!constraints)
    goto pyexception;

  /* fill them in */
  for (i = 0, j = 0; i < indexinfo->nConstraint; i++)
  {
    PyObject *constraint = NULL;
    if (!indexinfo->aConstraint[i].usable)
      continue;

    constraint = Py_BuildValue("(iB)", indexinfo->aConstraint[i].iColumn, indexinfo->aConstraint[i].op);
    if (!constraint)
      goto pyexception;

    PyTuple_SET_ITEM(constraints, j, constraint);
    j++;
  }

  /* order bys */
  orderbys = PyTuple_New(indexinfo->nOrderBy);
  if (!orderbys)
    goto pyexception;

  /* fill them in */
  for (i = 0; i < indexinfo->nOrderBy; i++)
  {
    PyObject *order = NULL;

    order = Py_BuildValue("(iN)", indexinfo->aOrderBy[i].iColumn, PyBool_FromLong(indexinfo->aOrderBy[i].desc));
    if (!order)
      goto pyexception;

    PyTuple_SET_ITEM(orderbys, i, order);
  }

  /* actually call the function */
  PyObject *vargs[] = {NULL, vtable, constraints, orderbys};
  res = PyObject_VectorcallMethod(apst.BestIndex, vargs + 1, 3 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  if (!res)
    goto pyexception;

  /* do we have useful index information? */
  if (Py_IsNone(res))
    goto finally;

  /* check we have a sequence */
  Py_ssize_t sequence_size; /* size is a dynamic call and could give different answers on each call */
  int ok = PySequence_Check(res) && (sequence_size = PySequence_Size(res)) <= 5;
  if (!ok || PyErr_Occurred())
  {
    if (!PyErr_Occurred())
      PyErr_Format(PyExc_TypeError, "Bad result from BestIndex.  It should be a sequence of up to 5 items");
    AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xBestIndex.result_check", "{s: O, s: O}", "self", vtable, "result", OBJ(res));
    goto pyexception;
  }

  /* dig the argv indices out */
  if (sequence_size == 0)
    goto finally;

  indices = PySequence_GetItem(res, 0);
  if (!indices)
    goto pyexception;

  if (!Py_IsNone(indices))
  {
    if (!PySequence_Check(indices) || PySequence_Size(indices) != nconstraints)
    {
      PyErr_Format(PyExc_TypeError, "Bad constraints (item 0 in BestIndex return).  It should be a sequence the same length as the constraints passed in (%d) items", nconstraints);
      AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xBestIndex.result_indices", "{s: O, s: O, s: O}",
                       "self", vtable, "result", OBJ(res), "indices", OBJ(indices));
      goto pyexception;
    }
    /* iterate through the items - i is the SQLite sequence number and j is the apsw one (usable entries) */
    for (i = 0, j = 0; i < indexinfo->nConstraint; i++)
    {
      PyObject *constraint = NULL, *argvindex = NULL, *omit = NULL;
      int omitv;
      if (!indexinfo->aConstraint[i].usable)
        continue;
      constraint = PySequence_GetItem(indices, j);
      if (PyErr_Occurred() || !constraint)
        goto pyexception;
      j++;
      /* it can be None */
      if (Py_IsNone(constraint))
      {
        Py_DECREF(constraint);
        continue;
      }
      /* or an integer */
      if (PyLong_Check(constraint))
      {
        indexinfo->aConstraintUsage[i].argvIndex = PyLong_AsInt(constraint) + 1;
        Py_DECREF(constraint);
        continue;
      }
      /* or a sequence two items long */
      if (!PySequence_Check(constraint) || PySequence_Size(constraint) != 2)
      {
        PyErr_Format(PyExc_TypeError, "Bad constraint (#%d) - it should be one of None, an integer or a tuple of an integer and a boolean", j);
        AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xBestIndex.result_constraint", "{s: O, s: O, s: O, s: O}",
                         "self", vtable, "result", OBJ(res), "indices", OBJ(indices), "constraint", OBJ(constraint));
        Py_DECREF(constraint);
        goto pyexception;
      }
      argvindex = PySequence_GetItem(constraint, 0);
      if (argvindex)
        omit = PySequence_GetItem(constraint, 1);
      if (!argvindex || !omit)
        goto constraintfail;
      if (!PyLong_Check(argvindex))
      {
        PyErr_Format(PyExc_TypeError, "argvindex for constraint #%d should be an integer", j);
        AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xBestIndex.result_constraint_argvindex", "{s: O, s: O, s: O, s: O, s: O}",
                         "self", vtable, "result", OBJ(res), "indices", OBJ(indices), "constraint", OBJ(constraint), "argvindex", OBJ(argvindex));
        goto constraintfail;
      }
      omitv = PyObject_IsTrueStrict(omit);
      if (omitv == -1)
        goto constraintfail;
      indexinfo->aConstraintUsage[i].argvIndex = PyLong_AsInt(argvindex) + 1;
      indexinfo->aConstraintUsage[i].omit = omitv;
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
  if (sequence_size < 2)
    goto finally;
  {
    PyObject *idxnum = PySequence_GetItem(res, 1);
    if (!idxnum)
      goto pyexception;
    if (!Py_IsNone(idxnum))
    {
      if (!PyLong_Check(idxnum))
      {
        PyErr_Format(PyExc_TypeError, "idxnum must be an integer");
        AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xBestIndex.result_indexnum", "{s: O, s: O, s: O}", "self", vtable, "result", OBJ(res), "indexnum", OBJ(idxnum));
        Py_DECREF(idxnum);
        goto pyexception;
      }
      indexinfo->idxNum = PyLong_AsInt(idxnum);
      if (PyErr_Occurred())
        goto pyexception;
    }
    Py_DECREF(idxnum);
  }

  /* item #2 is idxStr */
  if (sequence_size < 3)
    goto finally;
  {
    PyObject *idxstr = NULL;
    idxstr = PySequence_GetItem(res, 2);
    if (!idxstr)
      goto pyexception;
    if (!Py_IsNone(idxstr))
    {
      if (!PyUnicode_Check(idxstr))
      {
        PyErr_Format(PyExc_TypeError, "Expected a string for idxStr not %s", Py_TypeName(idxstr));
        Py_DECREF(idxstr);
        goto pyexception;
      }
      assert(indexinfo->idxStr == NULL);
      const char *svalue = PyUnicode_AsUTF8(idxstr);
      if (!svalue)
      {
        Py_DECREF(idxstr);
        goto pyexception;
      }
      const char *isvalue = sqlite3_mprintf("%s", svalue);
      if (!isvalue)
      {
        PyErr_NoMemory();
        Py_DECREF(idxstr);
        goto pyexception;
      }
      indexinfo->idxStr = (char *)isvalue;
      indexinfo->needToFreeIdxStr = 1;
    }
  }

  /* item 3 is orderByConsumed */
  if (sequence_size < 4)
    goto finally;
  {
    PyObject *orderbyconsumed = NULL;
    int iorderbyconsumed;
    orderbyconsumed = PySequence_GetItem(res, 3);
    if (!orderbyconsumed)
      goto pyexception;
    if (!Py_IsNone(orderbyconsumed))
    {
      iorderbyconsumed = PyObject_IsTrueStrict(orderbyconsumed);
      if (iorderbyconsumed == -1)
      {
        Py_DECREF(orderbyconsumed);
        goto pyexception;
      }
      indexinfo->orderByConsumed = iorderbyconsumed;
    }
    Py_DECREF(orderbyconsumed);
  }

  /* item 4 (final) is estimated cost */
  if (sequence_size < 5)
    goto finally;
  assert(sequence_size == 5);
  {
    PyObject *estimatedcost = NULL, *festimatedcost = NULL;
    estimatedcost = PySequence_GetItem(res, 4);
    if (!estimatedcost)
      goto pyexception;
    if (!Py_IsNone(estimatedcost))
    {
      festimatedcost = PyNumber_Float(estimatedcost);
      if (!festimatedcost)
      {
        Py_DECREF(estimatedcost);
        goto pyexception;
      }
      indexinfo->estimatedCost = PyFloat_AsDouble(festimatedcost);
    }
    Py_XDECREF(festimatedcost);
    Py_DECREF(estimatedcost);
  }

  goto finally;

pyexception: /* we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres = MakeSqliteMsgFromPyException(&(pVtab->zErrMsg));
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xBestIndex", "{s: O, s: O, s: (OO)}", "self", vtable, "result", OBJ(res), "args", OBJ(constraints), OBJ(orderbys));

finally:
  Py_XDECREF(indices);
  Py_XDECREF(res);
  Py_XDECREF(constraints);
  Py_XDECREF(orderbys);
  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: Begin() -> None

  This function is used as part of transactions.  You do not have to
  provide the method.
*/

/** .. method:: Sync() -> None

  This function is used as part of transactions.  You do not have to
  provide the method.
*/

/** .. method:: Commit() -> None

  This function is used as part of transactions.  You do not have to
  provide the method.
*/

/** .. method:: Rollback() -> None

  This function is used as part of transactions.  You do not have to
  provide the method.
*/

static int
apswvtabTransactionMethod(sqlite3_vtab *pVtab, PyObject *name, const char *exception_name)
{
  PyObject *vtable, *res = NULL;
  PyGILState_STATE gilstate;
  int sqliteres = SQLITE_OK;

  gilstate = PyGILState_Ensure();

  MakeExistingException();

  vtable = ((apsw_vtable *)pVtab)->vtable;
  CHAIN_EXC_BEGIN
  if (PyObject_HasAttr(vtable, name))
  {
    PyObject *vargs[] = {NULL, vtable};
    res = PyObject_VectorcallMethod(name, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
    if (res)
      Py_DECREF(res);
    else
    {
      sqliteres = MakeSqliteMsgFromPyException(&(pVtab->zErrMsg));
      AddTraceBackHere(__FILE__, __LINE__, exception_name, "{s: O}", "self", vtable);
    }
  };
  CHAIN_EXC_END;
  PyGILState_Release(gilstate);
  return sqliteres;
}

static int
apswvtabBegin(sqlite3_vtab *pVtab)
{
  return apswvtabTransactionMethod(pVtab, apst.Begin, "VirtualTable.Begin");
}

static int
apswvtabSync(sqlite3_vtab *pVtab)
{
  return apswvtabTransactionMethod(pVtab, apst.Sync, "VirtualTable.Sync");
}

static int
apswvtabCommit(sqlite3_vtab *pVtab)
{
  return apswvtabTransactionMethod(pVtab, apst.Commit, "VirtualTable.Commit");
}

static int
apswvtabRollback(sqlite3_vtab *pVtab)
{
  return apswvtabTransactionMethod(pVtab, apst.Rollback, "VirtualTable.Rollback");
}

/** .. method:: Open() -> VTCursor

  Returns a :class:`cursor <VTCursor>` object.
*/

typedef struct
{
  sqlite3_vtab_cursor used_by_sqlite; /* I don't touch this */
  PyObject *cursor;                   /* Object implementing cursor */
  int use_no_change;
} apsw_vtable_cursor;

static int
apswvtabOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor)
{
  PyObject *vtable = NULL, *res = NULL;
  PyGILState_STATE gilstate;
  apsw_vtable_cursor *avc = NULL;
  int sqliteres = SQLITE_OK;

  gilstate = PyGILState_Ensure();

  MakeExistingException();

  if (PyErr_Occurred())
    goto pyexception;

  vtable = ((apsw_vtable *)pVtab)->vtable;
  PyObject *vargs[] = {NULL, vtable};
  res = PyObject_VectorcallMethod(apst.Open, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  if (!res)
    goto pyexception;
  avc = PyMem_Calloc(1, sizeof(apsw_vtable_cursor));
  if (!avc)
    goto pyexception;
  assert((void *)avc == (void *)&(avc->used_by_sqlite)); /* detect if weird padding happens */
  avc->cursor = res;
  avc->use_no_change = ((apsw_vtable *)pVtab)->use_no_change;
  res = NULL;
  *ppCursor = (sqlite3_vtab_cursor *)avc;
  goto finally;

pyexception: /* we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres = MakeSqliteMsgFromPyException(&(pVtab->zErrMsg));
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xOpen", "{s: O}", "self", OBJ(vtable));

finally:
  Py_XDECREF(res);
  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: UpdateDeleteRow(rowid: int) -> None

  Delete the row with the specified *rowid*.

  :param rowid: 64 bit integer
*/
/** .. method:: UpdateInsertRow(rowid: Optional[int], fields: tuple[SQLiteValue, ...])  -> Optional[int]

  Insert a row with the specified *rowid*.

  :param rowid: *None* if you should choose the rowid yourself, else a 64 bit integer
  :param fields: A tuple of values the same length and order as columns in your table

  :returns: If *rowid* was *None* then return the id you assigned
    to the row.  If *rowid* was not *None* then the return value
    is ignored.
*/
/** .. method:: UpdateChangeRow(row: int, newrowid: int, fields: tuple[SQLiteValue, ...]) -> None

  Change an existing row.  You may also need to change the rowid - for example if the query was
  ``UPDATE table SET rowid=rowid+100 WHERE ...``

  :param row: The existing 64 bit integer rowid
  :param newrowid: If not the same as *row* then also change the rowid to this.
  :param fields: A tuple of values the same length and order as columns in your table
*/
static int
apswvtabUpdate(sqlite3_vtab *pVtab, int argc, sqlite3_value **argv, sqlite3_int64 *pRowid)
{
  PyObject *vtable, *columns = NULL, *res = NULL;
  PyGILState_STATE gilstate;
  int sqliteres = SQLITE_OK;
  int i;
  const char *methodname = "unknown";

  assert(argc); /* should always be >0 */

  gilstate = PyGILState_Ensure();

  MakeExistingException();

  vtable = ((apsw_vtable *)pVtab)->vtable;
  Connection *self = ((apsw_vtable *)pVtab)->connection;

  CALL_ENTER(xUpdate);

  if (PyErr_Occurred())
  {
    sqliteres = SQLITE_ERROR;
    goto finally;
  }

  /* argc=1 means delete row */
  if (argc == 1)
  {
    PyObject *vargs[] = {NULL, vtable, convert_value_to_pyobject_not_in(argv[0])};
    if (vargs[2])
    {
      res = PyObject_VectorcallMethod(apst.UpdateDeleteRow, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
      Py_DECREF(vargs[2]);
    }
    if (!res)
    {
      methodname = "UpdateDeleteRow";
      goto pyexception;
    }
    goto finally;
  }

  /* new row values */
  columns = PyTuple_New(argc - 2);
  if (!columns)
    goto pyexception;

  for (i = 0; i + 2 < argc; i++)
  {
    PyObject *value;
    value = convert_value_to_pyobject(argv[i + 2], 0, ((apsw_vtable *)pVtab)->use_no_change);
    if (!value)
      goto pyexception;
    PyTuple_SET_ITEM(columns, i, value);
  }

  /* insert a row */
  if (sqlite3_value_type(argv[0]) == SQLITE_NULL)
  {
    PyObject *vargs[] = {NULL, vtable, convert_value_to_pyobject_not_in(argv[1]), columns};
    if (vargs[2])
    {
      res = PyObject_VectorcallMethod(apst.UpdateInsertRow, vargs + 1, 3 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
      Py_DECREF(vargs[2]);
      if (res && sqlite3_value_type(argv[1]) == SQLITE_NULL)
      {
        *pRowid = PyLong_AsLongLong(res);
        if (PyErr_Occurred())
        {
          AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xUpdateInsertRow.ReturnedValue", "{s: O}", "result", res);
          methodname = "UpdateInsertRow";
          goto pyexception;
        }
      }
    }
    if (PyErr_Occurred())
    {
      methodname = "UpdateInsertRow";
      goto pyexception;
    }
  }
  /* otherwise changing a row */
  else
  {
    PyObject *vargs[] = {NULL, vtable, convert_value_to_pyobject(argv[0], 0, 0), convert_value_to_pyobject(argv[1], 0, 0), columns};
    if (vargs[2] && vargs[3])
      res = PyObject_VectorcallMethod(apst.UpdateChangeRow, vargs + 1, 4 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
    Py_XDECREF(vargs[2]);
    Py_XDECREF(vargs[3]);

    if (PyErr_Occurred())
    {
      methodname = "UpdateChangeRow";
      goto pyexception;
    }
  }
  goto finally;

pyexception: /* we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres = MakeSqliteMsgFromPyException(&pVtab->zErrMsg);
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xUpdate", "{s: O, s: i, s: s, s: O}", "self", vtable, "argc", argc, "methodname", methodname, "columns", OBJ(columns));

finally:
  Py_XDECREF(columns);
  Py_XDECREF(res);
  CALL_LEAVE(xUpdate);
  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: FindFunction(name: str, nargs: int) -> None |  Callable | tuple[int, Callable]

  Called to find if the virtual table has its own implementation of a
  particular scalar function. You do not have to provide this method.

  :param name: The function name
  :param nargs: How many arguments the function takes

  Return *None* if you don't have the function.  Zero is then returned to SQLite.

  Return a callable if you have one.  One is then returned to SQLite with the function.

  Return a sequence of int, callable.  The int is returned to SQLite with the function.
  This is useful for *SQLITE_INDEX_CONSTRAINT_FUNCTION* returns.

  It isn't possible to tell SQLite about exceptions in this function, so an
  :ref:`unraisable exception <unraisable>` is used.

  .. seealso::

    * :meth:`Connection.overload_function`
*/

/*
  We have to save everything returned for the lifetime of the table.

  This taps into the existing scalar function code in connection.c
*/
static int
apswvtabFindFunction(sqlite3_vtab *pVtab, int nArg, const char *zName,
                     void (**pxFunc)(sqlite3_context *, int, sqlite3_value **),
                     void **ppArg)
{
  PyGILState_STATE gilstate;
  int sqliteres = 0;
  PyObject *vtable, *res = NULL, *item_0 = NULL, *item_1 = NULL;
  FunctionCBInfo *cbinfo = NULL;
  apsw_vtable *av = (apsw_vtable *)pVtab;

  gilstate = PyGILState_Ensure();
  vtable = av->vtable;

  MakeExistingException();
  if (PyErr_Occurred() || !PyObject_HasAttr(vtable, apst.FindFunction))
    goto finally;

  PyObject *vargs[] = {NULL, vtable, PyUnicode_FromString(zName), PyLong_FromLong(nArg)};
  if (vargs[2] && vargs[3])
    res = PyObject_VectorcallMethod(apst.FindFunction, vargs + 1, 3 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);

  Py_XDECREF(vargs[2]);
  Py_XDECREF(vargs[3]);
  if (!res)
  {
    AddTraceBackHere(__FILE__, __LINE__, "apswvtabFindFunction", "{s: s, s: i}", "zName", zName, "nArg", nArg);
    goto finally;
  }

  if (!Py_IsNone(res))
  {
    if (!av->functions)
      av->functions = PyList_New(0);
    if (!av->functions)
    {
      assert(PyErr_Occurred());
      goto finally;
    }
    cbinfo = allocfunccbinfo(zName);
    if (!cbinfo)
      goto finally;
    if (!PyCallable_Check(res))
    {
      if (!PySequence_Check(res) || PySequence_Size(res) != 2)
      {
        PyErr_Format(PyExc_TypeError, "Expected FindFunction to return None, a Callable, or Sequence[int, Callable]");
        AddTraceBackHere(__FILE__, __LINE__, "apswvtabFindFunction", "{s: s, s: i, s: O}", "zName", zName, "nArg", nArg,
                         "result", res);
        goto finally;
      }

      item_0 = PySequence_GetItem(res, 0);
      if (item_0)
        item_1 = PySequence_GetItem(res, 1);

      if (PyErr_Occurred() || !item_0 || !item_1 || !PyLong_Check(item_0) || !PyCallable_Check(item_1))
      {
        PyErr_Format(PyExc_TypeError, "Expected FindFunction sequence to be [int, Callable]");
        AddTraceBackHere(__FILE__, __LINE__, "apswvtabFindFunction", "{s: s, s: i, s: O, s: O, s: O}", "zName", zName, "nArg", nArg,
                         "result", res, "item_0", OBJ(item_0), "item_1", OBJ(item_1));
        goto finally;
      }
      cbinfo->scalarfunc = item_1;
      item_1 = NULL;
      sqliteres = PyLong_AsInt(item_0);
      if (PyErr_Occurred() || sqliteres < SQLITE_INDEX_CONSTRAINT_FUNCTION || sqliteres > 255)
      {
        PyErr_Format(PyExc_ValueError, "Expected FindFunction sequence [int, Callable] to have int between SQLITE_INDEX_CONSTRAINT_FUNCTION and 255, not %i", sqliteres);
        sqliteres = 0;
        goto finally;
      }
    }
    else
    {
      cbinfo->scalarfunc = res;
      sqliteres = 1;
      res = NULL;
    }
    if (0 == PyList_Append(av->functions, (PyObject *)cbinfo))
    {
      *pxFunc = cbdispatch_func;
      *ppArg = cbinfo;
    }
    else
      sqliteres = 0;
  }
finally:
  Py_XDECREF(item_0);
  Py_XDECREF(item_1);
  Py_XDECREF(res);
  Py_XDECREF(cbinfo);
  if (PyErr_Occurred())
    apsw_write_unraisable(NULL);
  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: Rename(newname: str) -> None

  Notification that the table will be given a new name. If you return
  without raising an exception, then SQLite renames the table (you
  don't have to do anything). If you raise an exception then the
  renaming is prevented.  You do not have to provide this method.

*/
static int
apswvtabRename(sqlite3_vtab *pVtab, const char *zNew)
{
  PyGILState_STATE gilstate;
  PyObject *vtable, *res = NULL;
  int sqliteres = SQLITE_OK;

  gilstate = PyGILState_Ensure();
  vtable = ((apsw_vtable *)pVtab)->vtable;

  MakeExistingException();
  if (!PyErr_Occurred() && PyObject_HasAttr(vtable, apst.Rename))
  {
    PyObject *vargs[] = {NULL, vtable, convertutf8string(zNew)};
    if (vargs[2])
    {
      res = PyObject_VectorcallMethod(apst.Rename, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
      Py_DECREF(vargs[2]);
      Py_XDECREF(res);
    }
    if (!res)
      AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xRename", "{s: O, s: s}", "self", vtable, "newname", zNew);
  }

  if (PyErr_Occurred())
    sqliteres = MakeSqliteMsgFromPyException(NULL);
  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: Savepoint(level: int) -> None

  Set nested transaction to *level*.

  If you do not provide this method then the call succeeds (matching
  SQLite behaviour when no callback is provided).
*/
static int
apswvtabSavepoint(sqlite3_vtab *pVtab, int level)
{
  PyGILState_STATE gilstate;
  PyObject *vtable, *res = NULL;
  int sqliteres = SQLITE_OK;

  gilstate = PyGILState_Ensure();
  vtable = ((apsw_vtable *)pVtab)->vtable;

  MakeExistingException();

  if (!PyErr_Occurred() && PyObject_HasAttr(vtable, apst.Savepoint))
  {
    PyObject *vargs[] = {NULL, vtable, PyLong_FromLong(level)};
    if (vargs[2])
    {
      res = PyObject_VectorcallMethod(apst.Savepoint, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
      Py_DECREF(vargs[2]);
      if (!res)
      {
        sqliteres = MakeSqliteMsgFromPyException(NULL);
        AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xSavepoint", "{s: O, s: i}", "self", vtable, "level", level);
      }
    }
  }
  Py_XDECREF(res);
  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: Release(level: int) -> None

  Release nested transactions back to *level*.

  If you do not provide this method then the call succeeds (matching
  SQLite behaviour when no callback is provided).
*/
static int
apswvtabRelease(sqlite3_vtab *pVtab, int level)
{
  PyGILState_STATE gilstate;
  PyObject *vtable, *res = NULL;
  int sqliteres = SQLITE_OK;

  gilstate = PyGILState_Ensure();
  vtable = ((apsw_vtable *)pVtab)->vtable;

  MakeExistingException();

  if (!PyErr_Occurred() && PyObject_HasAttr(vtable, apst.Release))
  {
    PyObject *vargs[] = {NULL, vtable, PyLong_FromLong(level)};
    if (vargs[2])
    {
      res = PyObject_VectorcallMethod(apst.Release, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
      Py_DECREF(vargs[2]);
      if (!res)
      {
        sqliteres = MakeSqliteMsgFromPyException(NULL);
        AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xRelease", "{s: O, s: i}", "self", vtable, "level", level);
      }
    }
  }

  Py_XDECREF(res);
  PyGILState_Release(gilstate);
  return sqliteres;
}

/* .. method:: RollbackTo(level: int) -> None

  Rollback nested transactions back to *level*.

  If you do not provide this method then the call succeeds (matching
  SQLite behaviour when no callback is provided).
*/
static int
apswvtabRollbackTo(sqlite3_vtab *pVtab, int level)
{
  PyGILState_STATE gilstate;
  PyObject *vtable, *res = NULL;
  int sqliteres = SQLITE_OK;

  gilstate = PyGILState_Ensure();
  vtable = ((apsw_vtable *)pVtab)->vtable;

  MakeExistingException();

  if (!PyErr_Occurred() && PyObject_HasAttr(vtable, apst.RollbackTo))
  {
    PyObject *vargs[] = {NULL, vtable, PyLong_FromLong(level)};
    if (vargs[2])
    {
      res = PyObject_VectorcallMethod(apst.RollbackTo, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
      Py_DECREF(vargs[2]);
    }
    if (!res)
    {
      sqliteres = MakeSqliteMsgFromPyException(NULL);
      AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xRollbackTo", "{s: O, s: i}", "self", vtable, "level", level);
    }
  }
  Py_XDECREF(res);
  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: Integrity(schema: str, name: str, is_quick: int) -> str | None

 If present, check the integrity of the virtual table.

 :param schema: Database name `main`, `temp`, the name in `ATTACH <https://sqlite.org/lang_attach.html>`__
 :param name: Name of the table
 :param is_quick: 0 if `pragma integrity_check <https://sqlite.org/pragma.html#pragma_integrity_check>`__ was used,
    1 if `pragma quick_check <https://sqlite.org/pragma.html#pragma_quick_check>`__ was used, and may contain
    other values in the future.

 :returns: None if there are no problems, else a string to be used as an error message.  The string is returned to the
   pragma as is, so it is recommended that you include the database and table name to clarify what database and
   table the message is referring to.
*/
static int
apswvtabIntegrity(sqlite3_vtab *pVtab, const char *zSchema, const char *zName, int isQuick, char **pzErr)
{
  PyGILState_STATE gilstate;
  PyObject *vtable, *res = NULL;
  int sqliteres = SQLITE_OK;

  gilstate = PyGILState_Ensure();
  vtable = ((apsw_vtable *)pVtab)->vtable;

  MakeExistingException();

  if (!PyErr_Occurred() && PyObject_HasAttr(vtable, apst.Integrity))
  {
    PyObject *vargs[] = {NULL, vtable, PyUnicode_FromString(zSchema), PyUnicode_FromString(zName), PyLong_FromLong(isQuick)};
    if (vargs[2] && vargs[3] && vargs[4])
      res = PyObject_VectorcallMethod(apst.Integrity, vargs + 1, 4 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
    Py_XDECREF(vargs[2]);
    Py_XDECREF(vargs[3]);
    Py_XDECREF(vargs[4]);
    if (res)
    {
      if (Py_IsNone(res))
        ;
      else if (PyUnicode_Check(res))
      {
        const char *s = PyUnicode_AsUTF8(res);
        if (s)
        {
          *pzErr = sqlite3_mprintf("%s", s);
          if (!*pzErr)
            PyErr_NoMemory();
        }
      }
      else
        PyErr_Format(PyExc_TypeError, "Expected None or a str not %s", Py_TypeName(res));
    }
  }

  if (PyErr_Occurred())
  {
    sqliteres = MakeSqliteMsgFromPyException(NULL);
    AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xIntegrity", "{s: O, s: s, s: s, s: i}", "self", vtable, "schema", zSchema, "name", zName, "is_quick", isQuick);
  }

  Py_XDECREF(res);
  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. class:: VTCursor

.. note::

  There is no actual *VTCursor* class - it is shown this way for
  documentation convenience and is present as a `typing protocol
  <https://docs.python.org/3/library/typing.html#typing.Protocol>`__.


The :class:`VTCursor` object is used for iterating over a table.
There may be many cursors simultaneously so each one needs to keep
track of where in the table it is.

*/

/** .. method:: Filter(indexnum: int, indexname: str, constraintargs: Optional[tuple]) -> None

  This method is always called first to initialize an iteration to the
  first row of the table. The arguments come from the
  :meth:`~VTTable.BestIndex` or :meth:`~VTTable.BestIndexObject`
  with constraintargs being a tuple of the constraints you
  requested. If you always return None in BestIndex then indexnum will
  be zero, indexstring will be None and constraintargs will be empty).

  If you had an *in* constraint and set :meth:`IndexInfo.set_aConstraintUsage_in`
  then that value will be a :class:`set`.

  -* sqlite3_vtab_in_first sqlite3_vtab_in_next
*/
static int
apswvtabFilter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr,
               int argc, sqlite3_value **sqliteargv)
{
  PyObject *cursor, *argv = NULL, *res = NULL;
  PyGILState_STATE gilstate;
  int sqliteres = SQLITE_OK;
  int i;

  gilstate = PyGILState_Ensure();
  cursor = ((apsw_vtable_cursor *)pCursor)->cursor;

  MakeExistingException();

  if (PyErr_Occurred())
    goto pyexception;

  argv = PyTuple_New(argc);
  if (!argv)
    goto pyexception;
  for (i = 0; i < argc; i++)
  {
    PyObject *value = convert_value_to_pyobject(sqliteargv[i], 1, 0);
    if (!value)
      goto pyexception;
    PyTuple_SET_ITEM(argv, i, value);
  }

  PyObject *vargs[] = {NULL, cursor, PyLong_FromLong(idxNum), convertutf8string(idxStr), argv};
  if (vargs[2] && vargs[3])
    res = PyObject_VectorcallMethod(apst.Filter, vargs + 1, 4 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  Py_XDECREF(vargs[2]);
  Py_XDECREF(vargs[3]);
  if (res)
    goto finally; /* result is ignored */

pyexception: /* we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres = MakeSqliteMsgFromPyException(&(pCursor->pVtab->zErrMsg)); /* SQLite flaw: errMsg should be on the cursor not the table! */
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xFilter", "{s: O}", "self", cursor);

finally:
  Py_XDECREF(argv);
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: Eof() -> bool

  Called to ask if we are at the end of the table. It is called after each call to Filter and Next.

  :returns: False if the cursor is at a valid row of data, else True

  .. note::

    This method can only return True or False to SQLite.  If you have
    an exception in the method or provide a non-boolean return then
    True (no more data) will be returned to SQLite.
*/

static int
apswvtabEof(sqlite3_vtab_cursor *pCursor)
{
  PyObject *cursor, *res = NULL;
  PyGILState_STATE gilstate;
  int sqliteres = 0; /* nb a true/false value not error code */

  gilstate = PyGILState_Ensure();
  cursor = ((apsw_vtable_cursor *)pCursor)->cursor;

  MakeExistingException();

  /* is there already an error? */
  if (PyErr_Occurred())
    goto pyexception;

  PyObject *vargs[] = {NULL, cursor};
  res = PyObject_VectorcallMethod(apst.Eof, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  if (!res)
    goto pyexception;

  sqliteres = PyObject_IsTrueStrict(res);
  if (sqliteres == 0 || sqliteres == 1)
    goto finally;

pyexception: /* we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres = MakeSqliteMsgFromPyException(&(pCursor->pVtab->zErrMsg)); /* SQLite flaw: errMsg should be on the cursor not the table! */
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xEof", "{s: O}", "self", cursor);

finally:
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: Column(number: int) -> SQLiteValue

  Requests the value of the specified column *number* of the current
  row.  If *number* is -1 then return the rowid.

  :returns: Must be one one of the :ref:`5
    supported types <types>`
*/

/*
  Tt would be ideal for the return to be Union[SQLiteValue, apsw.no_change]
  but that then requires apsw.no_change being documented as a class
  which then confuses the documentation extractor.
*/
/** .. method:: ColumnNoChange(number: int) -> SQLiteValue

  :meth:`VTTable.UpdateChangeRow` is going to be called which includes
  values for all columns.  However this column is not going to be changed
  in that update.

  If you return :attr:`apsw.no_change` then :meth:`VTTable.UpdateChangeRow`
  will have :attr:`apsw.no_change` for this column.  If you return
  anything else then it will have that value - as though :meth:`VTCursor.Column`
  had been called.

  This method will only be called if *use_no_change* was *True* in the
  call to :meth:`Connection.create_module`.

  -* sqlite3_vtab_nochange
*/

/* forward decln */
static int set_context_result(sqlite3_context *context, PyObject *obj);

static int
apswvtabColumn(sqlite3_vtab_cursor *pCursor, sqlite3_context *result, int ncolumn)
{
  PyObject *cursor, *res = NULL;
  PyGILState_STATE gilstate;
  int sqliteres = SQLITE_OK, ok;
  int nc;

  gilstate = PyGILState_Ensure();
  cursor = ((apsw_vtable_cursor *)pCursor)->cursor;
  nc = ((apsw_vtable_cursor *)pCursor)->use_no_change && sqlite3_vtab_nochange(result);

  assert(!PyErr_Occurred());

  PyObject *vargs[] = {NULL, cursor, PyLong_FromLong(ncolumn)};
  if (vargs[2])
  {
    res = PyObject_VectorcallMethod(nc ? apst.ColumnNoChange : apst.Column, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
    Py_DECREF(vargs[2]);
  }

  if (!res)
    goto pyexception;

  if (nc && Py_Is(res, (PyObject *)&apsw_no_change_object))
    ok = 1;
  else
    ok = set_context_result(result, res);
  if (!PyErr_Occurred())
  {
    assert(ok);
    (void)ok;
    goto finally;
  }
pyexception: /* we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres = MakeSqliteMsgFromPyException(&(pCursor->pVtab->zErrMsg)); /* SQLite flaw: errMsg should be on the cursor not the table! */
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xColumn", "{s: O, s: O, s: O}", "self", cursor, "res", OBJ(res), "no_change", nc ? Py_True : Py_False);

finally:
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: Next() -> None

  Move the cursor to the next row.  Do not have an exception if there
  is no next row.  Instead return False when :meth:`~VTCursor.Eof` is
  subsequently called.

  If you said you had indices in your :meth:`VTTable.BestIndex`
  return, and they were selected for use as provided in the parameters
  to :meth:`~VTCursor.Filter` then you should move to the next
  appropriate indexed and constrained row.
*/
static int
apswvtabNext(sqlite3_vtab_cursor *pCursor)
{
  PyObject *cursor, *res = NULL;
  PyGILState_STATE gilstate;
  int sqliteres = SQLITE_OK;

  gilstate = PyGILState_Ensure();

  cursor = ((apsw_vtable_cursor *)pCursor)->cursor;
  PyObject *vargs[] = {NULL, cursor};
  res = PyObject_VectorcallMethod(apst.Next, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  if (res)
    goto finally;

  /* pyexception:  we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres = MakeSqliteMsgFromPyException(&(pCursor->pVtab->zErrMsg)); /* SQLite flaw: errMsg should be on the cursor not the table! */
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xNext", "{s: O}", "self", cursor);

finally:
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: Close() -> None

  This is the destructor for the cursor. Note that you must
  cleanup. The method will not be called again if you raise an
  exception.
*/
static int
apswvtabClose(sqlite3_vtab_cursor *pCursor)
{
  PyObject *cursor, *res = NULL;
  PyGILState_STATE gilstate;
  int sqliteres = SQLITE_OK;

  gilstate = PyGILState_Ensure();

  MakeExistingException();

  cursor = ((apsw_vtable_cursor *)pCursor)->cursor;
  PyObject *vargs[] = {NULL, cursor};
  CHAIN_EXC(
      res = PyObject_VectorcallMethod(apst.Close, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL));
  PyMem_Free(pCursor);
  if (res)
    goto finally;

  /* pyexception: we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres = MakeSqliteMsgFromPyException(NULL); /* SQLite api: we can't report error string */
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xClose", "{s: O}", "self", cursor);

finally:
  Py_DECREF(cursor); /* this is where cursor gets freed */
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: Rowid() -> int

  Return the current rowid.
*/
static int
apswvtabRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid)
{
  PyObject *cursor, *res = NULL, *pyrowid = NULL;
  PyGILState_STATE gilstate;
  int sqliteres = SQLITE_OK;

  gilstate = PyGILState_Ensure();

  MakeExistingException();

  cursor = ((apsw_vtable_cursor *)pCursor)->cursor;

  if (PyErr_Occurred())
    goto pyexception;

  PyObject *vargs[] = {NULL, cursor};
  res = PyObject_VectorcallMethod(apst.Rowid, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
  if (!res)
    goto pyexception;

  /* extract result */
  pyrowid = PyNumber_Long(res);
  if (!pyrowid)
    goto pyexception;
  *pRowid = PyLong_AsLongLong(pyrowid);
  if (!PyErr_Occurred()) /* could be bigger than 64 bits */
    goto finally;

pyexception: /* we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres = MakeSqliteMsgFromPyException(&(pCursor->pVtab->zErrMsg)); /* SQLite flaw: errMsg should be on the cursor not the table! */
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xRowid", "{s: O}", "self", cursor);

finally:
  Py_XDECREF(pyrowid);
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres;
}

/* xShadowName has no context information so we have to make
   make multiple functions (so each has a different address
   and do lots of housekeeping.

  https://sqlite.org/forum/forumpost/d5589fe401
*/

/* our multiple copies of the callback then call this
   methir providing the which parameter */
static int
apswvtabShadowName(int which, const char *table_suffix);

#define SN(n)                                    \
  static int xShadowName_##n(const char *suffix) \
  {                                              \
    return apswvtabShadowName(n, suffix);        \
  }

SN(0)
SN(1)
SN(2)
SN(3)
SN(4)
SN(5)
SN(6)
SN(7)
SN(8)
SN(9)
SN(10)
SN(11)
SN(12)
SN(13)
SN(14)
SN(15)
SN(16)
SN(17)
SN(18)
SN(19)
SN(20)
SN(21)
SN(22)
SN(23)
SN(24)
SN(25)
SN(26)
SN(27)
SN(28)
SN(29)
SN(30)
SN(31)
SN(32)

#undef SN
#define SN(n)             \
  {                       \
    xShadowName_##n, 0, 0 \
  }

static struct
{
  /* sqlite callback */
  int (*apsw_xShadowName)(const char *);
  /* associated python object we call */
  PyObject *source;
  /* this isn't needed but we use it with assertions to catch any errors */
  struct sqlite3_module *module;
} shadowname_allocation[] = {
    SN(0),
    SN(1),
    SN(2),
    SN(3),
    SN(4),
    SN(5),
    SN(6),
    SN(7),
    SN(8),
    SN(9),
    SN(10),
    SN(11),
    SN(12),
    SN(13),
    SN(14),
    SN(15),
    SN(16),
    SN(17),
    SN(18),
    SN(19),
    SN(20),
    SN(21),
    SN(22),
    SN(23),
    SN(24),
    SN(25),
    SN(26),
    SN(27),
    SN(28),
    SN(29),
    SN(30),
    SN(31),
    SN(32)};

#undef SN

/* sanity check of entry x.  */
#define SN_CHECK(x)                                                                                    \
  do                                                                                                   \
  {                                                                                                    \
    assert((shadowname_allocation[x].module == NULL && shadowname_allocation[x].source == NULL) ||     \
           shadowname_allocation[x].apsw_xShadowName == shadowname_allocation[x].module->xShadowName); \
  } while (0)

static void allocShadowName(sqlite3_module *mod, PyObject *datasource)
{
  const int max_sn = sizeof(shadowname_allocation) / sizeof(shadowname_allocation[0]);
  int i;
  for (i = 0; i < max_sn; i++)
  {
    SN_CHECK(i);
    if (shadowname_allocation[i].module)
      continue;

    shadowname_allocation[i].module = mod;
    mod->xShadowName = shadowname_allocation[i].apsw_xShadowName;
    shadowname_allocation[i].source = datasource;
    SN_CHECK(i);
    return;
  }
  PyErr_Format(PyExc_Exception, "No xShadowName slots are available.  There can be at most %d at once across all databases.", max_sn);
}

static void freeShadowName(sqlite3_module *mod, PyObject *datasource)
{
  const int max_sn = sizeof(shadowname_allocation) / sizeof(shadowname_allocation[0]);
  int i;
  int (*apsw_xShadowName)(const char *) = mod->xShadowName;

  for (i = 0; i < max_sn; i++)
  {
    SN_CHECK(i);
    if (shadowname_allocation[i].apsw_xShadowName == apsw_xShadowName)
    {
      assert(shadowname_allocation[i].source == datasource && shadowname_allocation[i].module == mod);
      shadowname_allocation[i].source = NULL;
      shadowname_allocation[i].module = NULL;
      SN_CHECK(i);
      return;
    }
  }
}

static int
apswvtabShadowName(int which, const char *table_suffix)
{
  PyGILState_STATE gilstate;
  PyObject *res = NULL;
  int sqliteres = 0;

  gilstate = PyGILState_Ensure();
  SN_CHECK(which);

  MakeExistingException();

  if (PyObject_HasAttr(shadowname_allocation[which].source, apst.ShadowName))
  {
    PyObject *vargs[] = {NULL, shadowname_allocation[which].source, PyUnicode_FromString(table_suffix)};
    if (vargs[2])
    {
      res = PyObject_VectorcallMethod(apst.ShadowName, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
      Py_DECREF(vargs[2]);
    }
    if (!res)
      sqliteres = 0;
    else if (Py_IsNone(res) || Py_IsFalse(res))
      sqliteres = 0;
    else if (Py_IsTrue(res))
      sqliteres = 1;
    else
      PyErr_Format(PyExc_TypeError, "Expected a bool from ShadowName not %s", Py_TypeName(res));

    if (PyErr_Occurred())
    {
      AddTraceBackHere(__FILE__, __LINE__, "VTModule.ShadowName", "{s: s, s: O}", "table_suffix", table_suffix, "res", OBJ(res));
      apsw_write_unraisable(NULL);
    }
  }
  Py_XDECREF(res);
  PyGILState_Release(gilstate);
  return sqliteres;
}

static sqlite3_module *
apswvtabSetupModuleDef(PyObject *datasource, int iVersion, int eponymous, int eponymous_only, int read_only)
{
  sqlite3_module *mod = NULL;
  assert(!PyErr_Occurred());
  if (iVersion < 1 || iVersion > 4)
  {
    PyErr_Format(PyExc_ValueError, "%d is not a valid iVersion - should be 1, 2, 3, or 4", iVersion);
    return NULL;
  }

  assert(iVersion == 1 || iVersion == 2 || iVersion == 3 || iVersion == 4);
  assert(eponymous == 0 || eponymous == 1);
  assert(eponymous_only == 0 || eponymous_only == 1);
  assert(read_only == 0 || read_only == 1);

  if (eponymous_only)
    eponymous = 1;

  mod = PyMem_Calloc(1, sizeof(*mod));
  if (!mod)
    return NULL;

  mod->iVersion = iVersion;
  if (eponymous_only)
    ;
  else if (eponymous)
    mod->xCreate = apswvtabConnect;
  else
    mod->xCreate = apswvtabCreate;
  mod->xConnect = apswvtabConnect;
  mod->xBestIndex = apswvtabBestIndex;
  mod->xDisconnect = apswvtabDisconnect;
  mod->xDestroy = apswvtabDestroy;
  mod->xOpen = apswvtabOpen;
  mod->xClose = apswvtabClose;
  mod->xFilter = apswvtabFilter;
  mod->xNext = apswvtabNext;
  mod->xEof = apswvtabEof;
  mod->xColumn = apswvtabColumn;
  mod->xRowid = apswvtabRowid;
  if (!read_only)
  {
    mod->xUpdate = apswvtabUpdate;
    mod->xBegin = apswvtabBegin;
    mod->xSync = apswvtabSync;
    mod->xCommit = apswvtabCommit;
    mod->xRollback = apswvtabRollback;
  }
  mod->xFindFunction = apswvtabFindFunction;
  if (!read_only)
  {
    mod->xRename = apswvtabRename;
    mod->xSavepoint = apswvtabSavepoint;
    mod->xRelease = apswvtabRelease;
    mod->xRollbackTo = apswvtabRollbackTo;
  }
  if (iVersion >= 3)
  {
    allocShadowName(mod, datasource);
    if (!mod->xShadowName)
    {
      PyMem_Free(mod);
      return NULL;
    }
  }

  mod->xIntegrity = apswvtabIntegrity;

  return mod;
}

/* end of Virtual table code */
