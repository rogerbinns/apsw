
/* to speed this up gendocstrings can generate something like this
   that uses the string length as a hash

    switch(strlen(kwname))
    {
        case 7:
          if(0==strcmp(kwname, "hkjdshfkjd")) return 4;
          if(0==strcmp(kwname, "sdsdshfkjd")) return 2;
          return -1;

        case 2:
          if(0==strcmp(kwname, "ab")) return 1;
          return -1;

        default: return -1;
    }
*/
static int
ARG_WHICH_KEYWORD(PyObject *item, const char *kwlist[], size_t n_kwlist, const char **kwname)
{
  const char *n = PyUnicode_AsUTF8(item);
  size_t cmp;
  int res = -1;
  if (n)
    for (cmp = 0; cmp < n_kwlist; cmp++)
    {
      if (0 == strcmp(n, kwlist[cmp]))
      {
        res = (int)cmp;
        break;
      }
    }
  *kwname = n;
  return res;
}

#define ARG_PROLOG(maxpos_args, kwname_list)                                                                           \
  static const char *kwlist[] = { kwname_list };                                                                       \
  const char *unknown_keyword = NULL;                                                                                  \
  const Py_ssize_t maxpos_args_ = maxpos_args;                                                                         \
  const int maxargs = Py_ARRAY_LENGTH(kwlist);                                                                         \
  PyObject *myargs[Py_ARRAY_LENGTH(kwlist)];                                                                           \
  PyObject **useargs = (PyObject **)fast_args;                                                                         \
  Py_ssize_t actual_nargs = PyVectorcall_NARGS(fast_nargs);                                                            \
  if (actual_nargs > maxpos_args)                                                                                      \
    goto too_many_args;                                                                                                \
  if (fast_kwnames)                                                                                                    \
  {                                                                                                                    \
    useargs = myargs;                                                                                                  \
    memcpy(useargs, fast_args, sizeof(PyObject *) * actual_nargs);                                                     \
    memset(useargs + actual_nargs, 0, sizeof(PyObject *) * (maxargs - actual_nargs));                                  \
    const Py_ssize_t n_fast_args = actual_nargs;                                                                       \
    for (int i_arg_prolog = 0; i_arg_prolog < PyTuple_GET_SIZE(fast_kwnames); i_arg_prolog++)                          \
    {                                                                                                                  \
      PyObject *item = PyTuple_GET_ITEM(fast_kwnames, i_arg_prolog);                                                   \
      Py_ssize_t which_kw = ARG_WHICH_KEYWORD(item, kwlist, maxargs, &unknown_keyword);                                \
      if (which_kw == -1)                                                                                              \
        goto unknown_keyword_arg;                                                                                      \
      if (useargs[which_kw])                                                                                           \
        goto pos_and_keyword;                                                                                          \
      useargs[which_kw] = fast_args[n_fast_args + i_arg_prolog];                                                       \
      actual_nargs = Py_MAX(actual_nargs, which_kw + 1);                                                               \
    }                                                                                                                  \
  }                                                                                                                    \
  Py_ssize_t argp_optindex = 0;

#define ARG_MANDATORY                                                                                                  \
  if (argp_optindex >= actual_nargs || !useargs[argp_optindex])                                                        \
    goto missing_required;

#define ARG_OPTIONAL                                                                                                   \
  if (argp_optindex == actual_nargs)                                                                                   \
    goto success;                                                                                                      \
  if (!useargs[argp_optindex])                                                                                         \
    argp_optindex++;                                                                                                   \
  else

#define ARG_EPILOG(retval, usage, cleanup)                                                                             \
  assert(argp_optindex == actual_nargs);                                                                               \
  goto success;                                                                                                        \
  /* this wont be hit but is here to stop warnings about unused label */                                               \
  goto missing_required;                                                                                               \
  too_many_args:                                                                                                       \
  if (!PyErr_Occurred())                                                                                               \
    PyErr_Format(PyExc_TypeError, "Too many positional arguments %d (max %d) provided to %s", (int)actual_nargs,       \
                 (int)maxpos_args_, usage);                                                                            \
  goto error_return;                                                                                                   \
  missing_required:                                                                                                    \
  if (!PyErr_Occurred())                                                                                               \
    PyErr_Format(PyExc_TypeError, "Missing required parameter #%d '%s' of %s", (int)argp_optindex + 1,                 \
                 kwlist[argp_optindex], usage);                                                                        \
  goto error_return;                                                                                                   \
  unknown_keyword_arg:                                                                                                 \
  if (!PyErr_Occurred())                                                                                               \
    PyErr_Format(PyExc_TypeError, "'%s' is an invalid keyword argument for %s", unknown_keyword, usage);               \
  goto error_return;                                                                                                   \
  pos_and_keyword:                                                                                                     \
  if (!PyErr_Occurred())                                                                                               \
    PyErr_Format(PyExc_TypeError, "argument '%s' given by name and position for %s", unknown_keyword, usage);          \
  goto error_return;                                                                                                   \
  param_error:                                                                                                         \
  assert(PyErr_Occurred());                                                                                            \
  PyErr_AddExceptionNoteV("Processing parameter #%d '%s' of %s", (int)argp_optindex + 1, kwlist[argp_optindex],        \
                          usage);                                                                                      \
  goto error_return;                                                                                                   \
  error_return:                                                                                                        \
  assert(PyErr_Occurred());                                                                                            \
  cleanup;                                                                                                             \
  return retval;                                                                                                       \
  success:                                                                                                             \
  cleanup;

#define ARG_ifnone(expr)                                                                                               \
  if (Py_IsNone(useargs[argp_optindex]))                                                                               \
  {                                                                                                                    \
    expr;                                                                                                              \
    argp_optindex++;                                                                                                   \
  }                                                                                                                    \
  else

#define ARG_pyobject(varname)                                                                                          \
  do                                                                                                                   \
  {                                                                                                                    \
    if (useargs[argp_optindex])                                                                                        \
    {                                                                                                                  \
      varname = useargs[argp_optindex];                                                                                \
      argp_optindex++;                                                                                                 \
    }                                                                                                                  \
    else /* this won't be hit, and is here to ensure the label is used to avoid a warning */                           \
      goto param_error;                                                                                                \
  } while (0)

#define ARG_pointer(varname)                                                                                           \
  do                                                                                                                   \
  {                                                                                                                    \
    varname = PyLong_AsVoidPtr(useargs[argp_optindex]);                                                                \
    if (PyErr_Occurred())                                                                                              \
      goto param_error;                                                                                                \
    argp_optindex++;                                                                                                   \
  } while (0)

#define ARG_str(varname)                                                                                               \
  do                                                                                                                   \
  {                                                                                                                    \
    Py_ssize_t sz;                                                                                                     \
    varname = PyUnicode_AsUTF8AndSize(useargs[argp_optindex], &sz);                                                    \
    if (!varname)                                                                                                      \
      goto param_error;                                                                                                \
    if ((Py_ssize_t)strlen(varname) != sz)                                                                             \
    {                                                                                                                  \
      PyErr_Format(PyExc_ValueError, "String has embedded null bytes");                                                \
      goto param_error;                                                                                                \
    }                                                                                                                  \
    argp_optindex++;                                                                                                   \
  } while (0)

#define ARG_PyUnicode(varname)                                                                                         \
  do                                                                                                                   \
  {                                                                                                                    \
    if (PyUnicode_Check(useargs[argp_optindex]))                                                                       \
    {                                                                                                                  \
      varname = useargs[argp_optindex];                                                                                \
      argp_optindex++;                                                                                                 \
    }                                                                                                                  \
    else                                                                                                               \
    {                                                                                                                  \
      PyErr_Format(PyExc_TypeError, "Expected a str not %s", Py_TypeName(useargs[argp_optindex]));                     \
      goto param_error;                                                                                                \
    }                                                                                                                  \
  } while (0)

#define ARG_optional_str(varname)                                                                                      \
  do                                                                                                                   \
  {                                                                                                                    \
    if (Py_IsNone(useargs[argp_optindex]))                                                                             \
    {                                                                                                                  \
      varname = NULL;                                                                                                  \
      argp_optindex++;                                                                                                 \
    }                                                                                                                  \
    else                                                                                                               \
      ARG_str(varname);                                                                                                \
  } while (0)

#define ARG_optional_list_str(varname)                                                                                 \
  do                                                                                                                   \
  {                                                                                                                    \
    if (Py_IsNone(useargs[argp_optindex]))                                                                             \
    {                                                                                                                  \
      varname = NULL;                                                                                                  \
      argp_optindex++;                                                                                                 \
    }                                                                                                                  \
    else                                                                                                               \
      ARG_list_str(varname);                                                                                           \
  } while (0)

#define ARG_list_str(varname)                                                                                          \
  do                                                                                                                   \
  {                                                                                                                    \
    if (!PyList_Check(useargs[argp_optindex]))                                                                         \
    {                                                                                                                  \
      PyErr_Format(PyExc_TypeError, "Expected a list");                                                                \
      goto param_error;                                                                                                \
    }                                                                                                                  \
    Py_ssize_t list_item_iter = 0;                                                                                     \
    for (; list_item_iter < PyList_GET_SIZE(useargs[argp_optindex]); list_item_iter++)                                 \
    {                                                                                                                  \
      if (!PyUnicode_Check(PyList_GET_ITEM(useargs[argp_optindex], list_item_iter)))                                   \
      {                                                                                                                \
        PyErr_Format(PyExc_TypeError, "Expected list item %zd to be str, not %s", list_item_iter,                      \
                     Py_TypeName(useargs[argp_optindex]));                                                             \
        goto param_error;                                                                                              \
      }                                                                                                                \
    }                                                                                                                  \
    varname = useargs[argp_optindex];                                                                                  \
    argp_optindex++;                                                                                                   \
  } while (0)

#define ARG_Callable(varname)                                                                                          \
  do                                                                                                                   \
  {                                                                                                                    \
    if (PyCallable_Check(useargs[argp_optindex]))                                                                      \
    {                                                                                                                  \
      varname = useargs[argp_optindex];                                                                                \
      argp_optindex++;                                                                                                 \
    }                                                                                                                  \
    else                                                                                                               \
    {                                                                                                                  \
      PyErr_Format(PyExc_TypeError, "Expected a callable not %s", Py_TypeName(useargs[argp_optindex]));                \
      goto param_error;                                                                                                \
    }                                                                                                                  \
  } while (0)

#define ARG_optional_Callable(varname)                                                                                 \
  do                                                                                                                   \
  {                                                                                                                    \
    if (Py_IsNone(useargs[argp_optindex]))                                                                             \
    {                                                                                                                  \
      varname = NULL;                                                                                                  \
      argp_optindex++;                                                                                                 \
    }                                                                                                                  \
    else                                                                                                               \
      ARG_Callable(varname);                                                                                           \
  } while (0)

#define ARG_bool(varname)                                                                                              \
  do                                                                                                                   \
  {                                                                                                                    \
    varname = PyObject_IsTrueStrict(useargs[argp_optindex]);                                                           \
    if (varname == -1)                                                                                                 \
      goto param_error;                                                                                                \
    argp_optindex++;                                                                                                   \
  } while (0)

#define ARG_int(varname)                                                                                               \
  do                                                                                                                   \
  {                                                                                                                    \
    varname = PyLong_AsInt(useargs[argp_optindex]);                                                                    \
    if (varname == -1 && PyErr_Occurred())                                                                             \
      goto param_error;                                                                                                \
    argp_optindex++;                                                                                                   \
  } while (0)

#define ARG_codepoint(varname)                                                                                         \
  do                                                                                                                   \
  {                                                                                                                    \
    if (PyUnicode_Check(useargs[argp_optindex]) && 1 == PyUnicode_GET_LENGTH(useargs[argp_optindex]))                  \
    {                                                                                                                  \
      varname = PyUnicode_READ_CHAR(useargs[argp_optindex], 0);                                                        \
    }                                                                                                                  \
    else if (PyLong_Check(useargs[argp_optindex]))                                                                     \
    {                                                                                                                  \
      long _tmpcp = PyLong_AsLong(useargs[argp_optindex]);                                                             \
      if (_tmpcp == (long)-1 && PyErr_Occurred())                                                                      \
        goto param_error;                                                                                              \
      if (_tmpcp < 0 || _tmpcp > 0x10ffff)                                                                             \
      {                                                                                                                \
        PyErr_Format(PyExc_ValueError, "Codepoint value %ld outside of range 0 to 0x10ffff", _tmpcp);                  \
        goto param_error;                                                                                              \
      }                                                                                                                \
      varname = (Py_UCS4)_tmpcp;                                                                                       \
    }                                                                                                                  \
    else                                                                                                               \
    {                                                                                                                  \
      PyErr_Format(PyExc_TypeError, "codepoint should be an int or one character str not %s",                          \
                   Py_TypeName(useargs[argp_optindex]));                                                               \
      goto param_error;                                                                                                \
    }                                                                                                                  \
    argp_optindex++;                                                                                                   \
  } while (0)

#define ARG_unsigned_long(varname)                                                                                     \
  do                                                                                                                   \
  {                                                                                                                    \
    varname = PyLong_AsUnsignedLong(useargs[argp_optindex]);                                                           \
    if (varname == (unsigned long)-1 && PyErr_Occurred())                                                              \
      goto param_error;                                                                                                \
    argp_optindex++;                                                                                                   \
  } while (0)

#define ARG_unsigned_long_long(varname)                                                                                \
  do                                                                                                                   \
  {                                                                                                                    \
    varname = PyLong_AsUnsignedLongLong(useargs[argp_optindex]);                                                       \
    if (varname == (unsigned long long)-1 && PyErr_Occurred())                                                         \
      goto param_error;                                                                                                \
    argp_optindex++;                                                                                                   \
  } while (0)

/* note this allows a position one after the last actual character */
#define ARG_PyUnicode_offset(varname, text)                                                                            \
  do                                                                                                                   \
  {                                                                                                                    \
    assert(PyUnicode_Check(text));                                                                                     \
    varname = PyLong_AsSsize_t(useargs[argp_optindex]);                                                                \
    if (varname == -1 && PyErr_Occurred())                                                                             \
      goto param_error;                                                                                                \
    if (varname < 0 || varname > 1 + PyUnicode_GET_LENGTH(text))                                                       \
    {                                                                                                                  \
      PyErr_Format(PyExc_ValueError, "offset %zd out of range 0 through %zd", varname,                                 \
                   1 + PyUnicode_GET_LENGTH(text));                                                                    \
      goto param_error;                                                                                                \
    }                                                                                                                  \
    argp_optindex++;                                                                                                   \
  } while (0)

#define ARG_int64(varname)                                                                                             \
  do                                                                                                                   \
  {                                                                                                                    \
    varname = PyLong_AsLongLong(useargs[argp_optindex]);                                                               \
    if (varname == -1 && PyErr_Occurred())                                                                             \
      goto param_error;                                                                                                \
    argp_optindex++;                                                                                                   \
  } while (0)

#define ARG_Py_ssize_t(varname)                                                                                        \
  do                                                                                                                   \
  {                                                                                                                    \
    varname = PyLong_AsSsize_t(useargs[argp_optindex]);                                                                \
    if (varname == -1 && PyErr_Occurred())                                                                             \
      goto param_error;                                                                                                \
    argp_optindex++;                                                                                                   \
  } while (0)

#define ARG_TYPE_CHECK(varname, type, cast)                                                                            \
  do                                                                                                                   \
  {                                                                                                                    \
    switch (PyObject_IsInstance(useargs[argp_optindex], type))                                                         \
    {                                                                                                                  \
    case 1:                                                                                                            \
      varname = (cast)useargs[argp_optindex];                                                                          \
      argp_optindex++;                                                                                                 \
      break;                                                                                                           \
    case 0:                                                                                                            \
      PyErr_Format(PyExc_TypeError, "Expected %s not %s", Py_TypeName(type), Py_TypeName(useargs[argp_optindex]));     \
      /* fallthru */                                                                                                   \
    case -1:                                                                                                           \
      goto param_error;                                                                                                \
    }                                                                                                                  \
  } while (0)

#define ARG_Connection(varname) ARG_TYPE_CHECK(varname, (PyObject *)&ConnectionType, Connection *)

/* PySequence_Check is too strict and rejects things that are
    accepted by PySequence_Fast like sets and generators,
    so everything is accepted */
#define ARG_optional_Bindings(varname)                                                                                 \
  do                                                                                                                   \
  {                                                                                                                    \
    if (Py_IsNone(useargs[argp_optindex]))                                                                             \
      varname = NULL;                                                                                                  \
    else                                                                                                               \
      varname = useargs[argp_optindex];                                                                                \
    argp_optindex++;                                                                                                   \
  } while (0)

#define ARG_optional_str_URIFilename(varname)                                                                          \
  do                                                                                                                   \
  {                                                                                                                    \
    if (Py_IsNone(useargs[argp_optindex]) || PyUnicode_Check(useargs[argp_optindex])                                   \
        || PyObject_IsInstance(useargs[argp_optindex], (PyObject *)&APSWURIFilenameType))                              \
    {                                                                                                                  \
      varname = useargs[argp_optindex];                                                                                \
      argp_optindex++;                                                                                                 \
    }                                                                                                                  \
    else                                                                                                               \
    {                                                                                                                  \
      PyErr_Format(PyExc_TypeError, "Expected None | str | apsw.URIFilename, not %s",                                  \
                   Py_TypeName(useargs[argp_optindex]));                                                               \
      goto param_error;                                                                                                \
    }                                                                                                                  \
  } while (0)

#define ARG_List_int_int(varname)                                                                                      \
  do                                                                                                                   \
  {                                                                                                                    \
    if (!PyList_Check(useargs[argp_optindex]) || PyList_Size(useargs[argp_optindex]) != 2)                             \
    {                                                                                                                  \
      if (!PyErr_Occurred())                                                                                           \
        PyErr_Format(PyExc_TypeError, "Expected a two item list of int");                                              \
      goto param_error;                                                                                                \
    }                                                                                                                  \
    for (int i = 0; i < 2; i++)                                                                                        \
    {                                                                                                                  \
      PyObject *list_item = PyList_GetItem(useargs[argp_optindex], i);                                                 \
      if (!list_item)                                                                                                  \
        goto param_error;                                                                                              \
      if (!PyLong_Check(list_item))                                                                                    \
      {                                                                                                                \
        if (!PyErr_Occurred())                                                                                         \
          PyErr_Format(PyExc_TypeError, "Function argument list[int,int] expected int for item %d not %s", i,          \
                       Py_TypeName(list_item));                                                                        \
        goto param_error;                                                                                              \
      }                                                                                                                \
    }                                                                                                                  \
    varname = useargs[argp_optindex];                                                                                  \
    argp_optindex++;                                                                                                   \
  } while (0)

#define ARG_optional_set(varname)                                                                                      \
  do                                                                                                                   \
  {                                                                                                                    \
    if (Py_IsNone(useargs[argp_optindex]))                                                                             \
      varname = NULL;                                                                                                  \
    else if (PySet_Check(useargs[argp_optindex]))                                                                      \
      varname = useargs[argp_optindex];                                                                                \
    else                                                                                                               \
    {                                                                                                                  \
      PyErr_Format(PyExc_TypeError, "Expected None or set, not %s", Py_TypeName(useargs[argp_optindex]));              \
      goto param_error;                                                                                                \
    }                                                                                                                  \
    argp_optindex++;                                                                                                   \
  } while (0)

#define ARG_py_buffer(varname)                                                                                         \
  do                                                                                                                   \
  {                                                                                                                    \
    if (!PyObject_CheckBuffer(useargs[argp_optindex]))                                                                 \
    {                                                                                                                  \
      PyErr_Format(PyExc_TypeError, "Expected bytes or similar type that supports buffer protocol, not %s",            \
                   Py_TypeName(useargs[argp_optindex]));                                                               \
      goto param_error;                                                                                                \
    }                                                                                                                  \
    varname = useargs[argp_optindex];                                                                                  \
    argp_optindex++;                                                                                                   \
  } while (0)

#define ARG_optional_UTF8AndSize(varname)                                                                              \
  do                                                                                                                   \
  {                                                                                                                    \
    if (!Py_IsNone(useargs[argp_optindex]) && !PyUnicode_Check(useargs[argp_optindex]))                                \
    {                                                                                                                  \
      PyErr_Format(PyExc_TypeError, "Expected a str or None, not %s", Py_TypeName(useargs[argp_optindex]));            \
      goto param_error;                                                                                                \
    }                                                                                                                  \
    if (!Py_IsNone(useargs[argp_optindex]))                                                                            \
    {                                                                                                                  \
      varname = PyUnicode_AsUTF8AndSize(useargs[argp_optindex], &varname##_size);                                      \
      if (!varname)                                                                                                    \
        goto param_error;                                                                                              \
    }                                                                                                                  \
    argp_optindex++;                                                                                                   \
  } while (0)

/* 1 is added to the size of fast_args to ensure the vla is always at
   least 1 item long.  If it ends up as zero then sanitizers complain. */
#define ARG_CONVERT_VARARGS_TO_FASTCALL                                                                                \
  Py_ssize_t fast_nargs = PyTuple_GET_SIZE(args);                                                                      \
  VLA_PYO(fast_args, 1 + fast_nargs + (kwargs ? PyDict_GET_SIZE(kwargs) : 0));                                         \
  PyObject *fast_kwnames = NULL;                                                                                       \
  Py_ssize_t acvtf_i;                                                                                                  \
  for (acvtf_i = 0; acvtf_i < fast_nargs; acvtf_i++)                                                                   \
    fast_args[acvtf_i] = PyTuple_GET_ITEM(args, acvtf_i);                                                              \
  if (kwargs)                                                                                                          \
  {                                                                                                                    \
    fast_kwnames = PyTuple_New(PyDict_GET_SIZE(kwargs));                                                               \
    if (!fast_kwnames)                                                                                                 \
      return -1;                                                                                                       \
    PyObject *pkey, *pvalue;                                                                                           \
    int fa_pos = (int)fast_nargs;                                                                                      \
    acvtf_i = 0;                                                                                                       \
    while (PyDict_Next(kwargs, &acvtf_i, &pkey, &pvalue))                                                              \
    {                                                                                                                  \
      fast_args[fa_pos] = pvalue; /* borrowing reference */                                                            \
      PyTuple_SET_ITEM(fast_kwnames, fa_pos - fast_nargs, Py_NewRef(pkey));                                            \
      fa_pos++;                                                                                                        \
    }                                                                                                                  \
  }

/* see issue 488 */
#define PREVENT_INIT_MULTIPLE_CALLS                                                                                    \
  if (self->init_was_called)                                                                                           \
  {                                                                                                                    \
    PyErr_Format(PyExc_RuntimeError, "__init__ has already been called, and cannot be called again");                  \
    return -1;                                                                                                         \
  }                                                                                                                    \
  self->init_was_called = 1;
