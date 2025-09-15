/**

JSONB
*****

blah blah blah

tighter checking than SQLite, especially around UTF8

note many single byte is valid JSONB

2GB limit because SQLite

object keys are strings in JSON always.  following stdlib json, str |
None | True | False | int | Float are automatically stringized

*/

/* returns 0 if not jsonb else 1 if it is */
static int
jsonb_detect_internal(const void *data, size_t length)
{
  /* ::TODO:: replace with actual implementation */
  return 1;
}

/* passed as context to the encoding routines */
struct JSONBuffer
{
  /* where the data is being assembled */
  void *data;
  /* current size which is also the offset to where the next data item goes */
  size_t size;
  /* how big the buffer allocation is, so we don't keep doing small reallocations */
  size_t allocated;
  /* callback for unknown types */
  PyObject *default_;
  /* a set if check_circular is true of ids we have seen */
  PyObject *seen;
  /* do we skip non-string dict keys? */
  int skip_keys;
};

/* The JT_ prefix is needed to avoid name clashes */
enum JSONBTag
{
  JT_NULL = 0,
  JT_TRUE = 1,
  JT_FALSE = 2,
  JT_INT = 3,
  JT_INT5 = 4,
  JT_FLOAT = 5,
  JT_FLOAT5 = 6,
  JT_TEXT = 7,
  JT_TEXTJ = 8,
  JT_TEXT5 = 9,
  JT_TEXTRAW = 10,
  JT_ARRAY = 11,
  JT_OBJECT = 12,
  JT_RESERVED_13 = 13,
  JT_RESERVED_14 = 14,
  JT_RESERVED_15 = 15,
};

/* ::TODO:: these json tag manipulation functions need to be added to fault inject*/

/* returns 0 on success, anything else on failure */
static int
jsonb_grow_buffer(struct JSONBuffer *buf, size_t count)
{
  size_t new_size = buf->size + count;
  if (new_size < buf->allocated)
  {
    buf->size = new_size;
    return 0;
  }
  if (new_size >= INT32_MAX)
  {
    SET_EXC(SQLITE_TOOBIG, NULL);
    return -1;
  }
#ifndef APSW_DEBUG
  /* in production builds we add a little extra*/
  size_t alloc_size = (new_size + 8192) & 0xffffff00u;
#else
  /* and in debug just 1 byte to flush out bugs */
  size_t alloc_size = new_size + 1;
#endif
  void *new_data = realloc(buf->data, alloc_size);
  if (!new_data)
  {
    assert(PyErr_Occurred());
    return -1;
  }
  buf->data = new_data;
  buf->size = new_size;
  buf->allocated = alloc_size;
  return 0;
}

/* returns 0 on success, anything else on failure. length is either
the correct length, or the maximum possible length which will be
adjusted later via jsonb_update_tag */
static int
jsonb_add_tag(struct JSONBuffer *buf, enum JSONBTag tag, size_t length)
{
  assert(tag >= JT_NULL && tag <= JT_OBJECT);
  size_t offset = buf->size;

#define WRITE(pos, val)                                                                                                \
  do                                                                                                                   \
  {                                                                                                                    \
    assert((pos) >= offset && (pos) <= buf->size);                                                                     \
    assert((val) >= 0 && (val) <= 255);                                                                                \
    ((unsigned char *)buf->data)[pos] = val;                                                                           \
  } while (0)

  if (length <= 11)
  {
    if (jsonb_grow_buffer(buf, 1))
      return -1;
    WRITE(offset, (length << 4) | tag);
  }
  else if (length <= 0xff)
  {
    if (jsonb_grow_buffer(buf, 2))
      return -1;
    WRITE(offset, (12 << 4) | tag);
    WRITE(offset + 1, length);
  }
  else if (length <= 0xffff)
  {
    if (jsonb_grow_buffer(buf, 3))
      return -1;
    WRITE(offset, (13 << 4) | tag);
    WRITE(offset + 1, (length & 0xff00) >> 8);
    WRITE(offset + 2, (length & 0x00ff) >> 0);
  }
  else if (length <= 0xffffffffu)
  {
    if (jsonb_grow_buffer(buf, 5))
      return -1;
    WRITE(offset, (14 << 4) | tag);
    WRITE(offset + 1, (length & 0xff000000u) >> 24);
    WRITE(offset + 2, (length & 0x00ff0000u) >> 16);
    WRITE(offset + 3, (length & 0x0000ff00u) >> 8);
    WRITE(offset + 4, (length & 0x000000ffu) >> 0);
  }
  else
  {
    SET_EXC(SQLITE_TOOBIG, NULL);
    return -1;
  }

  return 0;
}

/* 0 on success, anything else on failure */
static int
jsonb_update_tag(struct JSONBuffer *buf, enum JSONBTag tag, size_t offset, size_t new_length)
{
  assert(offset < buf->size);
  /* the tag is only used for assertion integrity check */
  assert((((unsigned char *)buf->data)[offset] & 0x0f) == tag);
  /* we only support 4 byte lengths */
  assert((((unsigned char *)buf->data)[offset] & 0xf0) == (14 << 4));

  if (new_length >= INT32_MAX)
  {
    SET_EXC(SQLITE_TOOBIG, NULL);
    return -1;
  }

  WRITE(offset + 1, (new_length & 0xff000000u) >> 24);
  WRITE(offset + 2, (new_length & 0x00ff0000u) >> 16);
  WRITE(offset + 3, (new_length & 0x0000ff00u) >> 8);
  WRITE(offset + 4, (new_length & 0x000000ffu) >> 0);

  return 0;
#undef WRITE
}

/* 0 on success, anything else on failure */
static int
jsonb_append_data(struct JSONBuffer *buf, const void *data, size_t length)
{
  size_t offset = buf->size;
  if (jsonb_grow_buffer(buf, length))
    return -1;
  memcpy(((unsigned char *)buf->data) + offset, data, length);
  return 0;
}

/* 0 on success, anything else on failure */
static int
jsonb_add_tag_and_data(struct JSONBuffer *buf, enum JSONBTag tag, const void *data, size_t length)
{
  int res = jsonb_add_tag(buf, tag, length);
  if (0 == res)
    res = jsonb_append_data(buf, data, length);
  return res;
}

/* 0 on success, anything else on failure */
static int
jsonb_encode_internal(struct JSONBuffer *buf, PyObject *obj)
{
  assert(obj);
  if (Py_IsNone(obj))
    return jsonb_add_tag(buf, JT_NULL, 0);
  if (Py_IsTrue(obj))
    return jsonb_add_tag(buf, JT_TRUE, 0);
  if (Py_IsFalse(obj))
    return jsonb_add_tag(buf, JT_FALSE, 0);
  if (PyLong_Check(obj))
  {
    int res = -1;
    PyObject *s = PyObject_Str(obj);
    if (!s)
      return res;
    Py_ssize_t length;
    const char *utf8 = PyUnicode_AsUTF8AndSize(s, &length);
    if (utf8)
      res = jsonb_add_tag_and_data(buf, JT_INT, utf8, length);
    Py_DECREF(s);
    return res;
  }
  if (PyFloat_Check(obj))
  {
    int res = -1;
    PyObject *tmp_str = NULL;
    const char *utf8 = NULL;
    size_t length;

    double d = PyFloat_AS_DOUBLE(obj);
    if (isnan(d))
    {
      /* utf8 = "NaN" etc */
      return jsonb_add_tag(buf, JT_NULL, 0);
    }
    if (isinf(d))
    {
      /* we want to use Infinity but need SQLite to ok */
      utf8 = (d < 0) ? "-9e999" : "9e999";
      length = strlen(utf8);
    }
    else
    {
      tmp_str = PyObject_Str(obj);
      if (!tmp_str)
        return -1;
      Py_ssize_t py_length;
      utf8 = PyUnicode_AsUTF8AndSize(tmp_str, &py_length);
      length = (size_t)py_length;
    }
    if (utf8)
      res = jsonb_add_tag_and_data(buf, JT_FLOAT, utf8, length);
    Py_XDECREF(tmp_str);
    return res;
  }
  if (PyUnicode_Check(obj))
  {
    Py_ssize_t length;
    const char *utf8 = PyUnicode_AsUTF8AndSize(obj, &length);
    if (!utf8)
      return -1;
    return jsonb_add_tag_and_data(buf, JT_TEXTRAW, utf8, length);
  }

  /* items is dict or list members */
  PyObject *id_of_obj = NULL, *items = NULL;

  /* check for circular references */

  if (buf->seen)
  {
    id_of_obj = PyLong_FromVoidPtr(obj);
    if (!id_of_obj)
      goto error;
    int contains = PySet_Contains(buf->seen, id_of_obj);
    if (contains < 0)
      goto error;
    if (contains == 1)
    {
      PyErr_Format(PyExc_ValueError, "circular reference detected");
      goto error;
    }
    assert(contains == 0);
  }

  /* this works better than pymapping_check */
  int is_dict = PyObject_IsInstance(obj, collections_abc_Mapping);
  if (is_dict < 0)
    goto error;

  if (is_dict)
  {
    Py_ssize_t dict_count = PyMapping_Length(obj);
    if (dict_count < 0)
      goto error;
    size_t tag_offset = buf->size;
    if (jsonb_add_tag(buf, JT_OBJECT, dict_count ? 0xffffffffu : 0))
      goto error;
    if (dict_count == 0)
      goto success;
    if (buf->seen && PySet_Add(buf->seen, id_of_obj))
      goto error;
    items = PyMapping_Items(obj);
    if (!items)
      goto error;
    /* PyMapping_Items guarantees this */
    assert(PyList_CheckExact(items));
    for (Py_ssize_t i = 0; i < PyList_GET_SIZE(items); i++)
    {
      PyObject *tuple = PyList_GET_ITEM(items, i);
      if (!PyTuple_CheckExact(tuple) || PyTuple_GET_SIZE(tuple) != 2)
      {
        PyErr_Format(PyExc_ValueError, "mapping items not 2-tuples");
        goto error;
      }
      PyObject *key = PyTuple_GET_ITEM(tuple, 0), *value = PyTuple_GET_ITEM(tuple, 1);

      /* keys have to be string, we copy stdlib json by stringizing some non-string
         types */
      if (PyUnicode_Check(key))
      {
        if (jsonb_encode_internal(buf, key))
          goto error;
      }
      else if (PyFloat_Check(key) || PyLong_Check(key))
      {
        /* for these we write them out as their own types
           and then alter the tag to be string */
        unsigned char *tag_byte = (unsigned char *)buf->data + buf->size;
        if (jsonb_encode_internal(buf, key))
          goto error;
        *tag_byte = (*tag_byte & 0x0f) | JT_TEXTRAW;
      }
      else if (Py_IsNone(key) || Py_IsTrue(key) || Py_IsFalse(key))
      {
        PyObject *key_subst = NULL;
        if (Py_IsNone(key))
          key_subst = apst.snull;
        else if (Py_IsTrue(key))
          key_subst = apst.strue;
        else
          key_subst = apst.sfalse;
        if (jsonb_encode_internal(buf, key_subst))
          goto error;
      }
      else if (buf->skip_keys)
      {
        continue;
      }
      else
      {
        PyErr_Format(PyExc_TypeError, "Keys must be str, int, float, bool or None. not %s", Py_TypeName(key));
        goto error;
      }
      if (jsonb_encode_internal(buf, value))
        goto error;
    }
    size_t size = buf->size - tag_offset;
    if (jsonb_update_tag(buf, JT_OBJECT, tag_offset, size))
      goto error;
    if (buf->seen)
    {
      int discard = PySet_Discard(buf->seen, id_of_obj);
      if (discard < 0)
        goto error;
      assert(discard);
    }
    goto success;
  }

  int is_sequence = PySequence_Check(obj);
  if (is_sequence < 0)
    goto error;

  if (is_sequence)
  {
    size_t tag_offset = buf->size;
    items = PySequence_Fast(obj, "expected a sequence for array");
    if (!items)
      goto error;
    Py_ssize_t sequence_count = PySequence_Fast_GET_SIZE(items);
    if (jsonb_add_tag(buf, JT_ARRAY, sequence_count ? 0xffffffffu : 0))
      goto error;
    if (sequence_count == 0)
      goto success;

    if (buf->seen && PySet_Add(buf->seen, id_of_obj))
      goto error;
    for (Py_ssize_t i = 0; i < PySequence_Fast_GET_SIZE(items); i++)
    {
      if (jsonb_encode_internal(buf, PySequence_Fast_GET_ITEM(items, i)))
        goto error;
    }
    size_t size = buf->size - tag_offset;
    if (jsonb_update_tag(buf, JT_ARRAY, tag_offset, size))
      goto error;
    if (buf->seen)
    {
      int discard = PySet_Discard(buf->seen, id_of_obj);
      if (discard < 0)
        goto error;
      assert(discard);
    }
    goto success;
  }

  if (buf->default_)
  {
    PyObject *vargs[] = { NULL, obj };
    PyObject *replacement = PyObject_Vectorcall(buf->default_, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
    if (!replacement)
      goto error;
    if (replacement == obj)
    {
      Py_DECREF(replacement);
      PyErr_Format(PyExc_ValueError, "default callback returned the object is was passed and did not encode it");
      goto error;
    }
    if (PyObject_CheckBuffer(replacement))
    {
      Py_buffer replacement_buffer;
      int success = 0;
      if (0 == PyObject_GetBufferContiguous(replacement, &replacement_buffer, PyBUF_SIMPLE))
      {
        if (jsonb_detect_internal(replacement_buffer.buf, replacement_buffer.len))
        {
          size_t start = buf->size;
          if (0 == jsonb_grow_buffer(buf, replacement_buffer.len))
          {
            memcpy((unsigned char *)buf->data + start, replacement_buffer.buf, replacement_buffer.len);
            success = 1;
          }
        }
        else
          PyErr_Format(PyExc_ValueError, "bytes item returned by default callback is not valid JSONB");

        PyBuffer_Release(&replacement_buffer);
      }
      if (!success)
        goto error;
    }
    else
    {
      if (buf->seen && PySet_Add(buf->seen, id_of_obj))
      {
        Py_DECREF(replacement);
        goto error;
      }
      if (jsonb_encode_internal(buf, replacement))
      {
        Py_DECREF(replacement);
        goto error;
      }
      Py_DECREF(replacement);
      if (buf->seen)
      {
        int discard = PySet_Discard(buf->seen, id_of_obj);
        if (discard < 0)
          goto error;
        assert(discard);
      }
    }
    goto success;
  }

  PyErr_Format(PyExc_TypeError, "Unhandled object of type %s", Py_TypeName(obj));
  goto error;

success:
  assert(!PyErr_Occurred());
  Py_XDECREF(items);
  Py_XDECREF(id_of_obj);
  return 0;

error:
  assert(PyErr_Occurred());
  Py_XDECREF(id_of_obj);
  Py_XDECREF(items);
  return -1;
}

/** .. method:: jsonb_encode(obj: Any, *, skipkeys: bool = False, check_circular: bool = True, default: Callable[[Any], JSONBTypes | Buffer] | None = None,) -> bytes:
    Encodes object as JSONB

    :param obj: Object to encode
    :param check_circular: Detects if containers contain themselves
       (even indirectly) and raises :exc:`ValueError`.  If ``False``
       and there is a circular reference, you get
       :exc:`RecursionError` (or worse).
    :param default: Called if an object can't be encoded, and should
       return an object that can be encoded.  If not provided a
       :exc:`TypeError` is raised.

       It can also return binary data in JSONB format.  For example
       numpy.float128 could encode itself as a full precision JSONB
       float.
*/
static PyObject *
JSONB_encode(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *obj;
  int skipkeys = 0;
  int check_circular = 1;
  PyObject *default_ = NULL;
  {
    Apsw_jsonb_encode_CHECK;
    ARG_PROLOG(1, Apsw_jsonb_encode_KWNAMES);
    ARG_MANDATORY ARG_pyobject(obj);
    ARG_OPTIONAL ARG_bool(skipkeys);
    ARG_OPTIONAL ARG_bool(check_circular);
    ARG_OPTIONAL ARG_optional_Callable(default_);
    ARG_EPILOG(NULL, Apsw_jsonb_encode_USAGE, );
  }

  struct JSONBuffer buf = {
    .data = 0,
    .size = 0,
    .allocated = 0,
    .default_ = default_,
    .skip_keys = skipkeys,
    .seen = check_circular ? PySet_New(NULL) : 0,
  };
  if (check_circular && !buf.seen)
    return NULL;

  int res = jsonb_encode_internal(&buf, obj);
  Py_CLEAR(buf.seen);
  PyObject *retval = (0 == res) ? PyBytes_FromStringAndSize(buf.data, buf.size) : NULL;
  free(buf.data);
  return retval;
}

/** .. method:: jsonb_detect(data: Buffer) -> bool

    Returns ``True`` if data is valid JSONB, otherwise ``False``.
*/
static PyObject *
JSONB_detect(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *data;
  {
    Apsw_jsonb_detect_CHECK;
    ARG_PROLOG(1, Apsw_jsonb_detect_KWNAMES);
    ARG_MANDATORY ARG_Buffer(data);
    ARG_EPILOG(NULL, Apsw_jsonb_detect_USAGE, );
  }

  Py_RETURN_NONE;
}

/** .. method:: jsonb_decode(data: Buffer, *,  object_pairs_hook: Callable[[list[tuple[str, JSONBTypes | Any]]], Any] | None = None,  object_hook: Callable[[dict[str, JSONBTypes | Any]], Any] | None = None,    array_hook: Callable[[list[JSONBTypes | Any]], Any] | None = None,    int_hook: Callable[[str], Any] | None = None,    float_hook: Callable[[str], Any] | None = None,) -> Any

    Decodes JSONB binary data into a Python object

    :param data: Binary data to decode
    :param object_pairs_hook: Called after a JSON object has been
        decoded with a list of tuples, each consisting of a
        :class:`str` and corresponding value, and should return a
        replacement value to use instead.
    :param object_hook: Called after a JSON object has been decoded
        into a Python :class:`dict` and should return a replacement
        value to use instead.
    :param array_hook: Called after a JSON array has been decoded into
        a list, and should return a replacement value to use instead.
    :param int_hook: Called with a :class:`str` of the integer, and
        should return a replacement value to use instead.  The default
        is the builtin :class:`int`.
    :param float_hook: Called with a :class:`str` of the float, and
        should return a replacement value to use instead.  The default
        is the builtin :class:`float`.

    Only one of ``object_hook`` or ``object_pairs_hook`` can be
    provided.  ``object_pairs_hook`` is useful when you want something
    other than a dict, care about the order of keys, want to convert
    them (eg case, numbers), want to handle duplicate keys etc.

    The array, int, and float hooks let you use alternate
    implementations.  For example if you are using `numpy
    <https://numpy.org/doc/stable/user/basics.types.html>`__ then you
    could use numpy arrays, or numpy's float128 to get higher
    precision floating numbers with greater exponent range than the
    builtin float type.

    If you use :class:`types.MappingProxyType` as ``object_hook`` and
    :class:`tuple` as ``array_hook`` then the overall returned value
    will be immutable (read only).

*/
static PyObject *
JSONB_decode(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *data;
  PyObject *object_pairs_hook = NULL;
  PyObject *object_hook = NULL;
  PyObject *array_hook = NULL;
  PyObject *int_hook = NULL;
  PyObject *float_hook = NULL;

  {
    Apsw_jsonb_decode_CHECK;
    ARG_PROLOG(1, Apsw_jsonb_decode_KWNAMES);
    ARG_MANDATORY ARG_Buffer(data);
    ARG_OPTIONAL ARG_optional_Callable(object_pairs_hook);
    ARG_OPTIONAL ARG_optional_Callable(object_hook);
    ARG_OPTIONAL ARG_optional_Callable(array_hook);
    ARG_OPTIONAL ARG_optional_Callable(int_hook);
    ARG_OPTIONAL ARG_optional_Callable(float_hook);
    ARG_EPILOG(NULL, Apsw_jsonb_decode_USAGE, );
  }

  Py_RETURN_NONE;
}
