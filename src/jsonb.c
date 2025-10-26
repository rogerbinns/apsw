/*

This code implements encoding, decoding, and detection of SQLite's
binary JSON format.  It also provides the documentation that ends up
on the top level JSON page.

It was originally developed in Python as apsw/jsonb.py but in a way to
make it easy to convert to C.  Then the test suite was developed.  Finally
the translation to C was done.  Looking at git history may be helpful.

There are many inconsistencies between the SQLite code and the JSONB
spec.  I've reported the issues, but was ignored so I'm being strict
here.

https://sqlite.org/forum/forumpost/28e21085f9

*/

/* Not addressing:

https://github.com/python/cpython/issues/69643 - sort_keys when all keys aren't the same type
(we won't stringify and then sort)

https://github.com/python/cpython/issues/131955 - json module always memoizes object keys
so only a single instance is used for the strings.  this isn't intern which does approximately
the same thing.  stdlib uses a memoization dict with PyDict_SetDefaultRef.  PyUnicode_InternInPlace
is probably a better choice because the strings are not immortal - they are un-interned when the
last reference goes away.  CPython does intern attribute names, function names etc, and they
likely overlap with object keys.  However JSONB should be relatively small and focussed.

*/

/**

JSON (Javascript Object Notation)
*********************************

SQLite provides extensive functionality for working with JSON.  APSW
complements that.

What is JSON?
=============

`Javascript Object Notation <https://www.json.org/>`__ (:rfc:`8259`)
is a **TEXT** based format for representing data, encoded in `UTF-8
<https://en.wikipedia.org/wiki/UTF-8>`__.  It is deliberately
constrained in what can be represented, and as a result is very widely
supported across languages and platforms.

Types
-----

.. list-table::
    :header-rows: 1
    :widths: auto

    * - Javascript
      - Python
      - Example
    * - null
      - :class:`None`
      - .. code-block:: json

          null
    * - boolean
      - :class:`bool`
      - .. code-block:: json

          true
    * - string - surrounding double quotes with some characters backslash escaped
        such as double quote and forward slash, while Unicode codepoints can be
        backslash-u escaped allowing for the JSON text to be all ASCII.  Codepoint
        U+2603 is snowman ☃
      - :class:`str`
      - .. code-block:: json

          "hello \" world \/ \u2603"

    * - number
      - :class:`int` and :class:`float`
      - .. code-block:: json

          72
          -3.14E7
    * - array - array contents can be any JSON types
      - :class:`list`
      - .. code-block:: json

          [null, 3.2, "hello"]
    * - object - keys must be strings, but values can be any JSON types
      - :class:`dict`
      - .. code-block:: json

          {
            "description": "Orange",
            "price": 3.07,
            "varieties": ["fall",
                            {"long": "...",
                             "short": 3443}
                         ]
          }

What is not in JSON
-------------------

Versioning and extension mechanisms

  There is no version number, nor is there a way to extend the format
  to add additional functionality within the standard.

Comments

  There is deliberately no syntax for comments.

Whitespace sensitivity

  Although the examples above use indents and spacing for readability,
  JSON ignores all whitespace.  Machine generated JSON usually omits
  whitespace, while JSON intended for human consumption has whitespace
  added.

Dates and times

  There is no native date or time representation.  The most common
  approach is to use `ISO8601
  <https://en.wikipedia.org/wiki/ISO_8601>`__ formatted strings which
  look like ``2025-09-30T09:45:00Z`` for UTC and
  ``2025-09-30T01:15:00-07:00`` with a timezone.

Binary data

  Binary data can't be included in a text format.  The most common approach
  is to use `base 64 <https://en.wikipedia.org/wiki/Base64>`__ strings which
  look like ``bGlnaHQgd29yaw==``

Infinity and NaN

  There is no explicit representation for infinity or for `Not A Number
  <https://en.wikipedia.org/wiki/NaN>`__ which arise from floating point
  calculations.  For example ``1e500`` (1 with 500 zeroes) multiplied by
  itself is too large to be  represented in the most common `64 bit
  floating point <https://en.wikipedia.org/wiki/IEEE_754>`__, while
  subtracting infinity from itself produces ``NaN``.

  Because infinity and NaN can occur in calculations, many JSON libraries
  will produce and accept them despite not being in the JSON standard.
  See below for how SQLite deals with infinity and NaN.

String normalization

  Unicode allows different representations of what will appear as the same
  text.  For example an ``e`` with an accent can be represented as a single
  codepoint, or as two with the ``e`` and the combining accent separately.
  JSON makes no requirements on strings, with all implementations usually
  producing and accepting the strings as is.  You can read about the
  subject `here <https://en.wikipedia.org/wiki/Unicode_equivalence>`__.

Trailing commas

  Trailing commas will not be accepted or produced by JSON libraries.  For example
  a list like ``[1, 2, 3,]`` or an object like ``{"one": 1, "two": 2,}`` are not
  valid.

Explicit limits

  The standard does not say how long strings can be, or how many items
  can be in an array or object.  There is no limit on how many digits
  can be used in numbers nor a minimum or maximum precision.  It is
  common for implementations to have limits especially 64 bits for
  numbers.  String limits may be 1 or 2 billion characters, and arrays /
  objects be limited to a similar number of members.

  Python has a 64  bit limit on floating point numbers when using
  :class:`float` but :mod:`decimal` is unlimited, and has no limit
  on integers, strings, arrays, or objects other than available memory.

  SQLite has an upper limit of 2GB for strings, uses signed 64 bit
  integers, and standard 64 bit floating point.

  If data is large, then other representations are more appropriate.

Object (dict) key order or duplicates

  While arrays (list) are ordered, there is no specification for what
  order object keys are in, or that duplicates are not allowed.  This
  usually doesn't matter, but there are security attacks where one
  component may use the first occurrence of a duplicate key, while
  another component uses the last occurrence.  For example the SQLite
  function extracting values will use the first occurrence of a key,
  while a dict created from the object will use the last occurrence.

  The Python :mod:`json` module, and APSW both let you see objects
  as lists of keys and values so you can do your own validation or
  other processing.

JSON5
=====

`JSON5 <https://json5.org/>`__ is a superset of JSON intended to be
more human readable and writable.  SQLite will accept JSON5 encoded
text, but will never produce it.  While SQLite parses JSON5, you
can't get back JSON5 output from a JSON5 input.

For example JSON5 allows comments, hexadecimal numbers, trailing
commas, infinity and NaN, and omitting some quoting,

Using JSON
==========

Python
------

The standard library :mod:`json` module provides all necessary
functionality, including turning Python objects into JSON text, and
JSON text into Python objects.  You can read and write JSON text, or a
:term:`file object`.

It deviates from the standard:

* ``Infinity`` and ``NaN`` are produced and consumed by default,
  although there is a  keyword argument to turn it off
* When producing JSON objects, keys that are numbers, None, or
  boolean are turned into their corresponding JSON text representation.
  When reading an object back, the reverse transformation is not
  done since there is no way to know if that is intended,
* Various corner cases in Unicode / UTF8 are accepted such as
  unpaired surrogates and UTF8 encoded surrogates.  This was done
  because other implementations at the time could produce this
  kind of encoding.  Attempting to encode the resulting strings as UTF-8
  again will result in exceptions.

You can see a `full list of JSON issues
<https://github.com/orgs/python/projects/6>`__.

SQLite
------

SQLite has over `30 functions <https://sqlite.org/json1.html>`__ for
consuming, extracting, iterating, and producing JSON.  You will need
to ensure that what you get back is what is intended.  You can usually
get back the JSON text representation of values, or the SQLite
value.  For example a SQLite string is the same as a Python
string, while the JSON text representation includes double quotes
around it and various quoting inside. (:ref:`Example
<example_json_functions>`)

You can store JSON text directly in the database, but there is no way
to differentiate it from any other text value.  For example the number
``2`` in JSON is text ``2``.  The `json_valid
<https://sqlite.org/json1.html#jvalid>`__ function may help - for
example as a `CHECK constraint
<https://sqlite.org/lang_createtable.html#check_constraints>`__ on a
column.

Infinity and NaN
----------------

SQLite accepts infinity but represents it as the floating point value
``9e999`` and accepts NaN representing it as ``null`` (None).  Unfortunately
``9e999`` is a valid value for :class:`decimal.Decimal` as well as
``numpy.float128``, so you won't be able to tell if infinity was the
original value

.. _jsonb:

JSONB
=====

SQLite has a binary format for representing JSON - named JSONB.  It is
`specified here <https://sqlite.org/jsonb.html>`__.  It is
significantly faster to use because JSON text requires finding
matching quotes around strings, square brackets around arrays, curly
braces around objects, and ensuring numeric values are valid.  JSONB
has already done all that processing so accessing and extracting
members is a lot quicker.  It also saves some space.

In most cases using SQLite JSON text functions results in SQLite doing an
internal conversion to JSONB (which is cached) and then operating on that.
JSONB internally stores values as a binary tag and length, then the UTF8 text
so producing JSON text again is quick.

You can store JSONB to the database, and again can use the `json_valid
<https://sqlite.org/json1.html#jvalid>`__ function as `CHECK constraint
<https://sqlite.org/lang_createtable.html#check_constraints>`__ with the
value ``8``.

.. note::

  JSONB does not have a version number or any header explicitly
  identifying binary data as JSONB.  There is no checksum or similar
  validation.  As an example a single byte whose value is 0 through 12
  is valid JSONB.

  As byte sequences get longer, the likelihood they are valid JSONB
  decreases.  This table lists what proportion of all byte sequences
  of each length are also valid JSONB.

  .. list-table:: Valid JSONB sequences
    :header-rows: 1
    :widths: auto

    * - Sequence length
      - Proportion valid JSONB
    * - 1 byte
      - 3.52%
    * - 2 bytes
      - 0.71%
    * - 3 bytes
      - 0.35%
    * - 4 bytes
      - 0.18%
    * - 5 bytes
      - 0.10%
    * - 6 bytes
      - 0.05%
    * - 7 bytes
      - 0.03%
    * - 8 bytes
      - 0.02%
    * - 9 bytes
      - 0.01%
    * - 10+ bytes
      - very low

.. _apsw_jsonb:

APSW
----

APSW provides 2 functions for working directly with JSONB, and a
validation function.  This is for performance reasons so that there
is no need for an intermediate step representing objects as JSON text.
The validation function is stricter than SQLite's equivalent to avoid
false positives.

Performance testing was done using SQLite's randomjson code to create
a large object with many nested values- your objects will be
different.

:func:`~apsw.jsonb_encode`

  Converts a Python object directly to JSONB.  The alternative is
  two steps using :mod:`json` to convert to JSON text and then
  SQLite's internal JSON text to JSONB.

  .. list-table:: Test results (CPU time)
    :widths: auto

    * - 0.13 seconds
      - APSW Python object direct to JSONB
    * - 1.20 seconds
      - :mod:`json` same Python object to JSON text
    * - 0.80 seconds
      - SQLite that JSON text to JSONB

  The same parameters as :func:`json.dumps` are available, with more
  providing control over how non-string object keys are converted,
  type matching, and direct conversion to JSONB types can be done.

:func:`~apsw.jsonb_decode`

  Converts JSONB directly back to a Python object.  The alternative
  is two steps using SQLite's internal JSONB to JSON text and then
  :mod:`json` to convert the JSON text to a Python object.

  .. list-table:: Test results (CPU time)
    :widths: auto

    * - 0.48 seconds
      - APSW JSONB direct to Python object
    * - 0.22 seconds
      - SQLite same JSONB to JSON text
    * - 1.35 seconds
      - :mod:`json` that JSON text to Python object

  The same parameters as :func:`json.loads` are available, with an additional
  hook for arrays (lists).

:func:`~apsw.jsonb_detect`

  Returns a boolean if some binary data is valid JSONB.
  If this returns ``True`` then SQLite will always produce valid JSON from the
  JSONB.

  SQLite's `json_valid <https://sqlite.org/json1.html#jvalid>`__  only
  checks the various internal type and length fields are consistent
  and items seem reasonable.  It does not check all corner cases, or
  the UTF8 encoding, and so can produce invalid JSON even if
  json_valid said it was valid JSONB.

Notes
=====

Because SQLite has a 2GB limit on text or blobs (binary data), it
can't work with individual JSON text or JSONB data over that size.

.. _jsontype:

JSON as a SQLite value type
===========================

You can make SQLite automatically support JSON as though it was a
natively supported type.  :ref:`Example code <example_json_quick>`
that does these steps.

:ref:`Store JSONB <jsonb>`

  SQLite's binary JSON representation is stored as a binary blob in the
  database.  This is necessary because JSON text can't easily be
  distinguished from other text, while a blob is far less
  ambiguous.

  SQLite often operates on JSONB internally when using
  the `JSON functions <https://sqlite.org/json1.html>`__, and its
  ``json`` function can turn JSONB into JSON text format if needed.

  SQLite's functions provide full access to all the values inside JSON
  and JSONB for reading, iterating, qnd modifying.

Convert bindings

  The :attr:`~Cursor.convert_binding` callback for SQLite unknown types
  can :func:`encode them <jsonb_encode>` as JSONB.

Convert JSONB

  The :attr:`~Cursor.convert_jsonb` callback is called when a blob would
  be returned and is also valid JSONB.  You can :func:`decode it <jsonb_decode>`
  or return the blob.  The cursor is provided so you can examine
  the :attr:`~Cursor.description` to help decide.

JSONB API
=========

*/

/* returns 0 if not jsonb else 1 if it is */
static int jsonb_detect_internal(const void *data, size_t length);

/* passed as context to the encoding routines */
struct JSONBuffer
{
  /* where the data is being assembled */
  uint8_t *data;
  /* current size which is also the offset to where the next data item goes */
  size_t size;
  /* how big the buffer allocation is, so we don't keep doing small reallocations */
  size_t allocated;
  /* callback for unknown types */
  PyObject *default_;
  /* callback for non string object keys */
  PyObject *default_key;
  /* a set if check_circular is true of ids we have seen */
  PyObject *seen;
  /* do we skip non-string dict keys? */
  int skip_keys;
  /* are dict keys sorted */
  int sort_keys;
  /* are nan/infinity rejected */
  int allow_nan;
  /* do we only look at exact types, or  */
  int exact_types;
};

#undef jsonb_encode_internal
/* returns 0 if encoded successfully,  else -1 */
static int jsonb_encode_internal(struct JSONBuffer *buf, PyObject *obj);

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

/* returns 0 on success, anything else on failure */
#undef jsonb_grow_buffer
static int
jsonb_grow_buffer(struct JSONBuffer *buf, size_t count)
{
#include "faultinject.h"
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
  /* in production builds we go to 1024 if smaller than that, else
     double the size each time which is also what Python internals do.  the
     allocation is short lived as we copy it into a pybytes and free at the
     end  */
  size_t alloc_size = (new_size < 1024) ? 1024 : (2 * new_size);
#else
  /* and in debug alternate between 0 and 7 so we exercise the buffer
     having space and not  */
  static int flip_extra = 0;
  size_t alloc_size = new_size + flip_extra;
  flip_extra = (!flip_extra) ? 7 : 0;
#endif
  assert(alloc_size >= new_size);

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
#undef jsonb_add_tag
static int
jsonb_add_tag(struct JSONBuffer *buf, enum JSONBTag tag, size_t length)
{
#include "faultinject.h"
  assert(tag >= JT_NULL && tag <= JT_OBJECT);
  size_t offset = buf->size;

#define WRITE(pos, val)                                                                                                \
  do                                                                                                                   \
  {                                                                                                                    \
    assert((pos) >= offset && (pos) <= buf->size);                                                                     \
    assert((val) >= 0 && (val) <= 255);                                                                                \
    buf->data[pos] = val;                                                                                              \
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
#undef jsonb_update_tag
static int
jsonb_update_tag(struct JSONBuffer *buf, enum JSONBTag tag, size_t offset, size_t new_length)
{
#include "faultinject.h"
  assert(offset < buf->size);
  /* the tag is only used for assertion integrity check */
  assert((buf->data[offset] & 0x0f) == tag);
  /* we only support 4 byte lengths */
  assert((buf->data[offset] & 0xf0) == (14 << 4));

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
#undef jsonb_append_data
static int
jsonb_append_data(struct JSONBuffer *buf, const void *data, size_t length)
{
#include "faultinject.h"
  size_t offset = buf->size;
  if (jsonb_grow_buffer(buf, length))
    return -1;
  memcpy(((unsigned char *)buf->data) + offset, data, length);
  return 0;
}

/* 0 on success, anything else on failure */
#undef jsonb_add_tag_and_data
static int
jsonb_add_tag_and_data(struct JSONBuffer *buf, enum JSONBTag tag, const void *data, size_t length)
{
#include "faultinject.h"

  int res = jsonb_add_tag(buf, tag, length);
  if (0 == res)
    res = jsonb_append_data(buf, data, length);
  return res;
}

/* 0 on success, anything else on failure. if key is skipped then buf->size won't have changed */
#undef jsonb_encode_object_key
static int
jsonb_encode_object_key(struct JSONBuffer *buf, PyObject *key)
{
#include "faultinject.h"
  /* this is a separate function because we have to stringify
     int/float etc to match stdlib json.dumps behaviour */
  if ((buf->exact_types) ? PyUnicode_CheckExact(key) : PyUnicode_Check(key))
    return jsonb_encode_internal(buf, key);
  if (buf->skip_keys)
    return 0;
  if (buf->default_key)
  {
    PyObject *vargs[] = { NULL, key };
    PyObject *converted = PyObject_Vectorcall(buf->default_key, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
    if (!converted)
      return -1;
    int res = 0;
    if (!PyUnicode_Check(converted))
    {
      PyErr_Format(PyExc_TypeError, "default_key callback needs to return a str, not %s", Py_TypeName(converted));
      res = -1;
    }
    if (res == 0)
      res = jsonb_encode_internal(buf, converted);
    Py_DECREF(converted);
    return res;
  }

  if (Py_IsNone(key) || Py_IsTrue(key) || Py_IsFalse(key))
  {
    PyObject *key_subst = NULL;
    if (Py_IsNone(key))
      key_subst = apst.snull;
    else if (Py_IsTrue(key))
      key_subst = apst.strue;
    else
      key_subst = apst.sfalse;
    return jsonb_encode_internal(buf, key_subst);
  }
  else if ((buf->exact_types) ? (PyFloat_CheckExact(key) || PyLong_CheckExact(key))
                              : (PyFloat_Check(key) || PyLong_Check(key)))
  {
    /* for these we write them out as their own types
           and then alter the tag to be string */
    size_t tag_offset = buf->size;
    if (jsonb_encode_internal(buf, key) == 0)
    {
      unsigned char *as_ptr = (unsigned char *)buf->data;
      as_ptr[tag_offset] = (as_ptr[tag_offset] & 0xf0) | JT_TEXTRAW;
      return 0;
    }
    return -1;
  }
  PyErr_Format(PyExc_TypeError, "Keys must be str, int, float, bool or None. not %s", Py_TypeName(key));
  return -1;
}

static int jsonb_encode_internal_actual(struct JSONBuffer *buf, PyObject *obj);

/* 0 on success, anything else on failure */
#undef jsonb_encode_internal
static int
jsonb_encode_internal(struct JSONBuffer *buf, PyObject *obj)
{
#include "faultinject.h"
  if (Py_EnterRecursiveCall(" encoding JSONB"))
    return -1;
  int res = jsonb_encode_internal_actual(buf, obj);
  Py_LeaveRecursiveCall();
  return res;
}

static int
jsonb_encode_internal_actual(struct JSONBuffer *buf, PyObject *obj)
{
  assert(obj);
  if (Py_IsNone(obj))
    return jsonb_add_tag(buf, JT_NULL, 0);
  if (Py_IsTrue(obj))
    return jsonb_add_tag(buf, JT_TRUE, 0);
  if (Py_IsFalse(obj))
    return jsonb_add_tag(buf, JT_FALSE, 0);
  if ((buf->exact_types) ? PyLong_CheckExact(obj) : PyLong_Check(obj))
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
  if ((buf->exact_types) ? PyFloat_CheckExact(obj) : PyFloat_Check(obj))
  {
    int res = -1;
    PyObject *tmp_str = NULL;
    const char *utf8 = NULL;
    size_t length;

    double d = PyFloat_AS_DOUBLE(obj);
    if (isnan(d))
    {
      if (buf->allow_nan)
        return jsonb_add_tag(buf, JT_NULL, 0);
      PyErr_Format(PyExc_ValueError, "NaN value not allowed by allow_nan parameter");
      return -1;
    }
    if (isinf(d))
    {
      if (buf->allow_nan)
      {
        utf8 = (d < 0) ? "-9e999" : "9e999";
        length = strlen(utf8);
      }
      else
      {
        PyErr_Format(PyExc_ValueError, "Infinity value not allowed by allow_nan parameter");
        return -1;
      }
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
  if ((buf->exact_types) ? PyUnicode_CheckExact(obj) : PyUnicode_Check(obj))
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

  /* I really wanted PySequence_Check but that allows bytes,
     array.array and others,  So we only accept list and tuple, and the
     default converter can handle the other types.  This matches the json
     module. */
  if ((buf->exact_types) ? (PyList_CheckExact(obj) || PyTuple_CheckExact(obj))
                         : (PyList_Check(obj) || PyTuple_Check(obj)))
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

    size_t data_offset = buf->size;

    if (buf->seen && PySet_Add(buf->seen, id_of_obj))
      goto error;
    for (Py_ssize_t i = 0; i < PySequence_Fast_GET_SIZE(items); i++)
    {
      if (jsonb_encode_internal(buf, PySequence_Fast_GET_ITEM(items, i)))
        goto error;
    }
    size_t size = buf->size - data_offset;
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

  /* this works better than pymapping_check */
  int is_dict = PyDict_CheckExact(obj);
  if (!is_dict && !buf->exact_types)
  {
    is_dict = PyObject_IsInstance(obj, collections_abc_Mapping);
    if (is_dict < 0)
      goto error;
  }

  if (is_dict)
  {
    Py_ssize_t dict_count = PyMapping_Size(obj);
    if (dict_count < 0)
      goto error;
    size_t tag_offset = buf->size;
    if (jsonb_add_tag(buf, JT_OBJECT, dict_count ? 0xffffffffu : 0))
      goto error;
    if (dict_count == 0)
      goto success;
    size_t data_offset = buf->size;
    if (buf->seen && PySet_Add(buf->seen, id_of_obj))
      goto error;

    if (!buf->sort_keys && PyDict_CheckExact(obj))
    {
      /* the expected code path */
      Py_ssize_t pos = 0;
      PyObject *key, *value;
      while (PyDict_Next(obj, &pos, &key, &value))
      {
        size_t offset = buf->size;

        if (jsonb_encode_object_key(buf, key))
          goto error;
        if (buf->size != offset && jsonb_encode_internal(buf, value))
          goto error;
      }
    }
    else
    {
      items = PyMapping_Items(obj);
      if (!items)
        goto error;
      /* PyMapping_Items guarantees this */
      assert(PyList_CheckExact(items));
      if (buf->sort_keys && PyList_Sort(items) < 0)
        goto error;
      for (Py_ssize_t i = 0; i < PyList_GET_SIZE(items); i++)
      {
        PyObject *tuple = PyList_GET_ITEM(items, i);
        if (!PyTuple_CheckExact(tuple) || PyTuple_GET_SIZE(tuple) != 2)
        {
          PyErr_Format(PyExc_ValueError, "mapping items not 2-tuples");
          goto error;
        }
        PyObject *key = PyTuple_GET_ITEM(tuple, 0), *value = PyTuple_GET_ITEM(tuple, 1);

        size_t offset = buf->size;

        if (jsonb_encode_object_key(buf, key))
          goto error;
        if (buf->size != offset && jsonb_encode_internal(buf, value))
          goto error;
      }
    }

    size_t size = buf->size - data_offset;
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
      AddTraceBackHere(__FILE__, __LINE__, "jsonb_encode_internal", "{s: O}", "object", obj);
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
        {
          PyErr_Format(PyExc_ValueError, "bytes item returned by default callback is not valid JSONB");
          AddTraceBackHere(__FILE__, __LINE__, "jsonb_encode_internal", "{s: O}", "replacement", replacement, "object", obj);
        }

        PyBuffer_Release(&replacement_buffer);
      }
      Py_DECREF(replacement);
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
  AddTraceBackHere(__FILE__, __LINE__, "jsonb_encode_internal", "{s: O}", "object", obj);
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

/** .. method:: jsonb_encode(obj: Any, *, skipkeys: bool = False, sort_keys: bool = False, check_circular: bool = True, exact_types: bool = False, default: Callable[[Any], JSONBTypes | Buffer] | None = None, default_key: Callable[[Any], str] | None = None, allow_nan:bool = True) -> bytes

    Encodes a Python object as JSONB.  It is like :func:`json.dumps` except it produces
    JSONB bytes instead of JSON text.

    :param obj: Object to encode
    :param skipkeys: If ``True`` and a non-string dict key is
       encountered then it is skipped.  Otherwise :exc:`ValueError`
       is raised.  Default ``False``.  Like :func:`json.dumps` keys
       that are bool, int, float, and None are always converted to
       string.
    :param sort_keys: If ``True`` then objects (dict) will be output
       with the keys sorted.  This produces deterministic output.
       Default ``False``.
    :param check_circular: Detects if containers contain themselves
       (even indirectly) and raises :exc:`ValueError`.  If ``False``
       and there is a circular reference, you eventually get
       :exc:`RecursionError` (or run out of memory or similar).
    :param default: Called if an object can't be encoded, and should
       return an object that can be encoded.  If not provided a
       :exc:`TypeError` is raised.

       It can also return binary data in JSONB format.  For example
       :mod:`decimal` values can be encoded as a full precision JSONB
       float.  :func:`apsw.ext.make_jsonb` can be used.
    :param default_key: Objects (dict) must have string keys.  If a
       non-string key is encountered, it is skipped if ``skipkeys``
       is ``True``.  Otherwise this is called.  If not supplied the
       default matches the standard library :mod:`json` which
       converts None, bool, int and float to their string JSON
       equivalents and uses those.  This callback is useful if
       you want to raise an exception, or use a different way
       of generating the key string.
    :param allow_nan: If ``True`` (default) then following SQLite practise,
        infinity is converted to float ``9e999`` and NaN is converted
        to ``None``.  If ``False`` a :exc:`ValueError` is raised.
    :param exact_types: By default subclasses of int, float, list (including
        tuple), dict (including :class:`collections.abc.Mapping`), and
        :class:`str` are converted the same as the parent class.  This
        is usually what you want.  However sometimes you are using a
        subclass and want them converted by the ``default`` function
        with an example being :class:`enum.IntEnum`.  If this parameter
        is ``True`` then only the exact types are directly converted
        and subclasses will be passed to ``default`` or ``default_key``.

    You will get a :exc:`~apsw.TooBigError` if the resulting JSONB
    will exceed 2GB because SQLite can't handle it.
*/
static PyObject *
JSONB_encode(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *obj;
  int skipkeys = 0;
  int sort_keys = 0;
  int check_circular = 1;
  int allow_nan = 1;
  int exact_types = 0;

  PyObject *default_ = NULL;
  PyObject *default_key = NULL;
  {
    Apsw_jsonb_encode_CHECK;
    ARG_PROLOG(1, Apsw_jsonb_encode_KWNAMES);
    ARG_MANDATORY ARG_pyobject(obj);
    ARG_OPTIONAL ARG_bool(skipkeys);
    ARG_OPTIONAL ARG_bool(sort_keys);
    ARG_OPTIONAL ARG_bool(check_circular);
    ARG_OPTIONAL ARG_bool(exact_types);
    ARG_OPTIONAL ARG_optional_Callable(default_);
    ARG_OPTIONAL ARG_optional_Callable(default_key);
    ARG_OPTIONAL ARG_bool(allow_nan);
    ARG_EPILOG(NULL, Apsw_jsonb_encode_USAGE, );
  }

  if (skipkeys && default_key)
    return PyErr_Format(PyExc_ValueError, "You can't both skipkeys and default_key");

  struct JSONBuffer buf = {
    .data = 0,
    .size = 0,
    .allocated = 0,
    .default_ = Py_XNewRef(default_),
    .default_key = Py_XNewRef(default_key),
    .skip_keys = skipkeys,
    .sort_keys = sort_keys,
    .allow_nan = allow_nan,
    .exact_types = exact_types,
    .seen = check_circular ? PySet_New(NULL) : 0,
  };
  if (check_circular && !buf.seen)
    return NULL;

  int res = jsonb_encode_internal(&buf, obj);
  assert((0 == res && !PyErr_Occurred()) || (res != 0 && PyErr_Occurred()));

  Py_CLEAR(buf.seen);
  Py_CLEAR(buf.default_);
  Py_CLEAR(buf.default_key);
  PyObject *retval = (0 == res) ? PyBytes_FromStringAndSize((const char *)buf.data, buf.size) : NULL;
  free(buf.data);
  return retval;
}

struct JSONBDecodeBuffer
{
  const uint8_t *const buffer; /* what we are decoding */
  size_t offset;               /* current decode position */
  size_t end_offset;           /* offset of last position we can access + 1 (ie the length) */
  /* Optional Callables for hooks, or NULL if not present */
  PyObject *object_pairs_hook;
  PyObject *object_hook;
  PyObject *array_hook;
  PyObject *parse_int;
  PyObject *parse_float;
  int alloc; /* zero if doing a detect (no allocations), non-zero if doing a decode (allocations) */
};

/* these are used in non alloc (detect) mode and are valid PyObject
   pointers but are not reference counted etc */
#define DecodeSuccess ((PyObject *)1)
#define DecodeFailure ((PyObject *)NULL)

static PyObject *
malformed(struct JSONBDecodeBuffer *buf, const char *msg, ...)
{
  if (buf->alloc)
  {
    va_list args;
    va_start(args, msg);
    PyErr_FormatV(PyExc_ValueError, msg, args);
    va_end(args);
    return NULL;
  }
  return DecodeFailure;
}

/* forward declarations of our various checking functions.  They
   return zero if checks fail and non-zero if they succeed.  start is
   position in the buffer, and end is the first offset after the value
*/
static int jsonb_check_int(struct JSONBDecodeBuffer *buf, size_t start, size_t end);
static int jsonb_check_int5hex(struct JSONBDecodeBuffer *buf, size_t start, size_t end);
static int jsonb_check_float(struct JSONBDecodeBuffer *buf, size_t start, size_t end);
static int jsonb_check_float5(struct JSONBDecodeBuffer *buf, size_t start, size_t end);

/* return like above check routines,  decodes into unistr if provided  */
static int jsonb_decode_utf8_string(const uint8_t *buf, size_t end, PyObject *unistr, enum JSONBTag tag,
                                    size_t *pLength, Py_UCS4 *pMax_char);

static PyObject *jsonb_decode_one_actual(struct JSONBDecodeBuffer *buf);

static PyObject *
jsonb_decode_one(struct JSONBDecodeBuffer *buf)
{
  if (Py_EnterRecursiveCall(" decoding JSONB"))
  {
    if (!buf->alloc)
    {
      PyErr_Clear();
      return DecodeFailure;
    }
    return NULL;
  }

  PyObject *res = jsonb_decode_one_actual(buf);
  Py_LeaveRecursiveCall();
  return res;
}

static PyObject *
jsonb_decode_one_actual(struct JSONBDecodeBuffer *buf)
{
  if (buf->offset >= buf->end_offset)
    return malformed(buf, "item goes beyond end of buffer");

  uint8_t tag_and_len = buf->buffer[buf->offset];
  enum JSONBTag tag = tag_and_len & 0x0f;
  size_t tag_len = (tag_and_len & 0xf0) >> 4;
  buf->offset += 1;

  size_t value_offset = buf->offset;

  if (tag_len >= 12)
  {
    size_t var_len = 0;
    switch (tag_len)
    {
    case 12:
      var_len = 1;
      break;
    case 13:
      var_len = 2;
      break;
    case 14:
      var_len = 4;
      break;
    case 15:
      var_len = 8;
      break;
    }

    if (buf->offset + var_len > buf->end_offset)
      return malformed(buf, "insufficient space for length");

    value_offset += var_len;
    tag_len = 0;

    while (var_len)
    {
      tag_len <<= 8;
      tag_len += buf->buffer[buf->offset];
      buf->offset += 1;
      var_len -= 1;
    }
  }

  /* value_offset is now start of value, after tag + length bytes */
  if (value_offset + tag_len > buf->end_offset)
    return malformed(buf, "insufficent space for value");

  /* set offset to start of next value */
  buf->offset = value_offset + tag_len;

  switch (tag)
  {
  case JT_NULL:
    if (tag_and_len != JT_NULL || tag_len != 0)
      return malformed(buf, "NULL has length");
    return (buf->alloc) ? Py_NewRef(Py_None) : DecodeSuccess;

  case JT_TRUE:
    if (tag_and_len != JT_TRUE || tag_len != 0)
      return malformed(buf, "TRUE has length");
    return (buf->alloc) ? Py_NewRef(Py_True) : DecodeSuccess;

  case JT_FALSE:
    if (tag_and_len != JT_FALSE || tag_len != 0)
      return malformed(buf, "FALSE has length");
    return (buf->alloc) ? Py_NewRef(Py_False) : DecodeSuccess;

  case JT_INT:
    if (!jsonb_check_int(buf, value_offset, buf->offset))
      return malformed(buf, "not a valid int");
    if (!buf->alloc)
      return DecodeSuccess;
    {
      /* we cant use PyLong_FromString because the end of the string can't be passed in */
      PyObject *text = PyUnicode_FromKindAndData(PyUnicode_1BYTE_KIND, (const char *)(buf->buffer + value_offset),
                                                 buf->offset - value_offset);
      if (!text)
        return NULL;
      PyObject *result = NULL;
      if (buf->parse_int)
      {
        PyObject *vargs[] = { NULL, text };
        result = PyObject_Vectorcall(buf->parse_int, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
      }
      else
        result = PyLong_FromUnicodeObject(text, 10);
      Py_DECREF(text);
      return result;
    }

  case JT_INT5:
    /* JSON5 allows leading +, regular numbers (JT_INT), and hex.  SQLite
       only allows hex .*/
    if (!jsonb_check_int5hex(buf, value_offset, buf->offset))
      return malformed(buf, "not a valid int5");
    if (!buf->alloc)
      return DecodeSuccess;
    {
      /* we cant use PyLong_FromString because the end of the string can't be passed in */
      PyObject *text = PyUnicode_FromKindAndData(PyUnicode_1BYTE_KIND, (const char *)(buf->buffer + value_offset),
                                                 buf->offset - value_offset);
      if (!text)
        return NULL;
      PyObject *result = NULL;
      if (buf->parse_int)
      {
        /* we need to pass zero as the base so leading sign and 0x are processed as expected */
        PyObject *vargs[] = { NULL, text, PyLong_FromLong(0) };
        if (vargs[2])
          result = PyObject_Vectorcall(buf->parse_int, vargs + 1, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
        Py_XDECREF(vargs[2]);
      }
      else
        result = PyLong_FromUnicodeObject(text, 0);
      Py_DECREF(text);
      return result;
    }

  case JT_FLOAT:
  case JT_FLOAT5:
    if ((tag == JT_FLOAT) ? !jsonb_check_float(buf, value_offset, buf->offset)
                          : !jsonb_check_float5(buf, value_offset, buf->offset))
      return malformed(buf, (tag == JT_FLOAT) ? "not a valid float" : "not a valid float5");
    if (!buf->alloc)
      return DecodeSuccess;
    {
      PyObject *text = PyUnicode_FromKindAndData(PyUnicode_1BYTE_KIND, (const char *)(buf->buffer + value_offset),
                                                 buf->offset - value_offset);
      if (!text)
        return NULL;
      PyObject *result = NULL;
      if (buf->parse_float)
      {
        PyObject *vargs[] = { NULL, text };
        result = PyObject_Vectorcall(buf->parse_float, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
      }
      else
        result = PyFloat_FromString(text);
      Py_DECREF(text);
      return result;
    }

  case JT_TEXT:
  case JT_TEXTJ:
  case JT_TEXT5:
  case JT_TEXTRAW:
    /* zero length? */
    if (value_offset == buf->offset)
      return (buf->alloc) ? PyUnicode_New(0, 0) : DecodeSuccess;
    /* this is the length in codepoints */
    size_t length = 0;
    Py_UCS4 max_char = 0;
    if (!jsonb_decode_utf8_string(buf->buffer + value_offset, buf->offset - value_offset, NULL, tag, &length,
                                  &max_char))
      return malformed(buf, "not a valid string");
    assert(max_char > 0);
    if (!buf->alloc)
      return DecodeSuccess;
    PyObject *retval = NULL;
    if (tag == JT_TEXT || tag == JT_TEXTRAW)
    {
      /* these two have no escapes to decode */
      retval
          = (max_char == 127)
                ? PyUnicode_FromKindAndData(PyUnicode_1BYTE_KIND, (const char *)(buf->buffer + value_offset),
                                            buf->offset - value_offset)
                : PyUnicode_FromStringAndSize((const char *)(buf->buffer + value_offset), buf->offset - value_offset);
      if (retval)
        assert((size_t)PyUnicode_GET_LENGTH(retval) == length);
      return retval;
    }
    retval = PyUnicode_New(length, max_char);
    if (!retval)
      return retval;
    int success
        = jsonb_decode_utf8_string(buf->buffer + value_offset, buf->offset - value_offset, retval, tag, NULL, NULL);
    (void)success;
    assert(success);
    return retval;

  case JT_ARRAY: {
    PyObject *res = buf->alloc ? PyList_New(0) : NULL;
    if (buf->alloc && !res)
      return NULL;
    size_t saved_end_offset = buf->end_offset;
    buf->end_offset = buf->offset;
    buf->offset = value_offset;
    while (buf->offset < buf->end_offset)
    {
      PyObject *item = jsonb_decode_one(buf);
      if (!item)
      {
        Py_XDECREF(res);
        return item;
      }
      if (buf->alloc)
      {
        if (PyList_Append(res, item) < 0)
        {
          Py_DECREF(res);
          Py_DECREF(item);
          return NULL;
        }
        Py_DECREF(item);
      }
    }
    assert(buf->offset == buf->end_offset);
    buf->end_offset = saved_end_offset;
    if (!buf->alloc)
      return DecodeSuccess;
    if (!buf->array_hook)
      return res;
    PyObject *vargs[] = { NULL, res };
    PyObject *new_res = PyObject_Vectorcall(buf->array_hook, vargs + 1, 1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
    Py_DECREF(res);
    return new_res;
  }

  case JT_OBJECT: {
    PyObject *builder = NULL;
    if (buf->alloc)
    {
      builder = (buf->object_pairs_hook) ? PyList_New(0) : PyDict_New();
      if (!builder)
        return NULL;
    }

    size_t saved_end_offset = buf->end_offset;
    buf->end_offset = buf->offset;
    buf->offset = value_offset;

    while (buf->offset < buf->end_offset)
    {
      enum JSONBTag key_tag = buf->buffer[buf->offset] & 0x0f;
      if (key_tag != JT_TEXT && key_tag != JT_TEXTJ && key_tag != JT_TEXT5 && key_tag != JT_TEXTRAW)
        return malformed(buf, "object key is not a string");
      PyObject *key = jsonb_decode_one(buf);
      if (!key)
        return key;
      if (buf->offset >= buf->end_offset)
      {
        if (buf->alloc)
          Py_DECREF(key);
        return malformed(buf, "no value for key");
      }
      PyObject *value = jsonb_decode_one(buf);
      if (!value)
      {
        if (buf->alloc)
          Py_DECREF(key);
        return value;
      }
      if (builder)
      {
        int added = -1;
        if (buf->object_pairs_hook)
        {
          PyObject *tuple = PyTuple_Pack(2, key, value);
          if (tuple)
            added = PyList_Append(builder, tuple);
          Py_XDECREF(tuple);
        }
        else
          added = PyDict_SetItem(builder, key, value);
        Py_DECREF(key);
        Py_DECREF(value);
        if (added < 0)
          return NULL;
      }
      else
        assert(key == DecodeSuccess && value == DecodeSuccess);
    }
    assert(buf->offset == buf->end_offset);
    buf->end_offset = saved_end_offset;
    if (!buf->alloc)
      return DecodeSuccess;
    if (!buf->object_hook && !buf->object_pairs_hook)
      return builder;
    PyObject *vargs[] = { NULL, builder };
    PyObject *new_res = PyObject_Vectorcall(buf->object_hook ? buf->object_hook : buf->object_pairs_hook, vargs + 1,
                                            1 | PY_VECTORCALL_ARGUMENTS_OFFSET, NULL);
    Py_DECREF(builder);
    return new_res;
  }
  case JT_RESERVED_13:
  case JT_RESERVED_14:
  case JT_RESERVED_15:
    return malformed(buf, "unknown tag");
  }
  Py_UNREACHABLE();
}

static int
jsonb_check_int(struct JSONBDecodeBuffer *buf, size_t start, size_t end)
{
  /*
    optional minus
    at least one digit
    no leading zeroes
  */

  if (end == start)
    return 0;

  int seen_sign = 0, seen_digit = 0, seen_first_is_zero = 0;

  for (size_t offset = start; offset < end; offset++)
  {
    uint8_t t = buf->buffer[offset];

    /* must be in ascii normal range of utf8 */
    if (t < 32 || t > 127)
      return 0;

    switch (t)
    {
    case '-':
      if (seen_sign)
        return 0;
      if (seen_digit)
        return 0;
      seen_sign = 1;
      break;
    case '0':
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
    case '8':
    case '9':
      /* leading zero is not allowed unless the whole number is zero */
      if (seen_digit && seen_first_is_zero)
        return 0;
      if (!seen_digit && t == '0')
        seen_first_is_zero = 1;
      seen_digit = 1;
      break;
    default:
      return 0;
    }
  }
  return seen_digit;
}

static int
jsonb_check_int5hex(struct JSONBDecodeBuffer *buf, size_t start, size_t end)
{
  /*
    optional minus
    zero
    x or X
    at least one hex digit
  */
  if (end - start < 3)
    return 0;
  int seen_sign = 0, seen_x = 0, seen_leading_zero = 0, seen_digit = 0;

  for (size_t offset = start; offset < end; offset++)
  {
    uint8_t t = buf->buffer[offset];

    /* must be in ascii normal range of utf8 */
    if (t < 32 || t > 127)
      return 0;

    switch (t)
    {
    case '-':
      if (seen_sign)
        return 0;
      /* can't be after x / leading zero / digits */
      if (seen_x || seen_leading_zero || seen_digit)
        return 0;
      seen_sign = 1;
      break;
    case '0':
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
    case '8':
    case '9':
    case 'a':
    case 'A':
    case 'b':
    case 'B':
    case 'c':
    case 'C':
    case 'd':
    case 'D':
    case 'e':
    case 'E':
    case 'f':
    case 'F':
      if (t == '0')
      {
        if (!seen_x && !seen_leading_zero)
        {
          seen_leading_zero = 1;
          continue;
        }
      }
      if (!seen_x)
        return 0;
      seen_digit = 1;
      break;
    case 'x':
    case 'X':
      if (seen_x)
        return 0;
      if (!seen_leading_zero)
        return 0;
      seen_x = 1;
      break;
    default:
      return 0;
    }
  }
  return seen_digit;
}

static int
jsonb_check_float(struct JSONBDecodeBuffer *buf, size_t start, size_t end)
{
  /*
    optional minus
    at least one digit
    dot
    at least one digit
    optional E
        optional sign
          at least one digit
  */

  if (end - start < 3)
    return 0;

  int seen_sign = 0, seen_dot = 0, seen_digit = 0, seen_e = 0, seen_first_is_zero = 0;

  for (size_t offset = start; offset < end; offset++)
  {
    uint8_t t = buf->buffer[offset];
    /* must be in ascii normal range of utf8 */
    if (t < 32 || t > 127)
      return 0;

    switch (t)
    {
    case '+':
    case '-':
      /* + only allowed after E */
      if (t == '+' && !seen_e)
        return 0;
      /* can't have more than one */
      if (seen_sign)
        return 0;
      /* can't be after digits */
      if (seen_digit)
        return 0;
      /* can't be after dot */
      if (seen_dot)
        return 0;
      seen_sign = 1;
      break;
    case '.':
      /* can't be after E */
      if (seen_e)
        return 0;
      /* can't have more than one */
      if (seen_dot)
        return 0;
      /* must be after at least one digit */
      if (!seen_digit)
        return 0;
      /* a digit will be required after this */
      seen_dot = 1;
      seen_digit = 0;
      break;

    case '0':
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
    case '8':
    case '9':
      if (seen_e || seen_dot)
      {
        /* all digits allowed after E or dot */
        seen_digit = 1;
        continue;
      }
      /* leading zero not allowed */
      if (seen_digit && seen_first_is_zero)
        return 0;
      /* leading zero but could 0.123 */
      if (!seen_digit && t == '0')
        seen_first_is_zero = 1;
      seen_digit = 1;
      break;
    case 'e':
    case 'E':
      /*  must be at least one digit */
      if (!seen_digit)
        return 0;
      /* can't have more than one E */
      if (seen_e)
        return 0;
      /* reset state to post E */
      seen_e = 1;
      seen_digit = 0;
      seen_sign = 0;
      seen_dot = 0;
      break;
    default:
      return 0;
    }
  }
  return seen_digit;
}

static int
jsonb_check_float5(struct JSONBDecodeBuffer *buf, size_t start, size_t end)
{
  /*
    optional minus
    at least one digit with at most one dot anywhere including
       before or after any digits.  This is the big JSON5 difference
    optional E
       optional sign
         at least one digit
  */

  /* If SQLite allows NaN/Infinity it would be handled here */

  if (end - start < 2)
    return 0;

  int seen_sign = 0, seen_dot = 0, seen_digit = 0, seen_e = 0, seen_first_is_zero = 0;

  for (size_t offset = start; offset < end; offset++)
  {
    uint8_t t = buf->buffer[offset];
    /* must be in ascii normal range of utf8 */
    if (t < 32 || t > 127)
      return 0;

    switch (t)
    {
    case '+':
    case '-':
      /* + only allowed after E (JSON5 does allow leading but SQLite does not) */
      if (t == '+' && !seen_e)
        return 0;
      /* can't have more than one */
      if (seen_sign)
        return 0;
      /* can't be after digits */
      if (seen_digit)
        return 0;
      /* can't be after dot */
      if (seen_dot)
        return 0;
      seen_sign = 1;
      break;

    case '.':
      /* can't be after E */
      if (seen_e)
        return 0;
      /* can't have more than one */
      if (seen_dot)
        return 0;
      seen_dot = 1;
      break;

    case '0':
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
    case '8':
    case '9':
      if (seen_e || seen_dot)
      {
        /* all digits allowed after E or dot */
        seen_digit = 1;
        continue;
      }
      /* leading zero not allowed */
      if (seen_digit && seen_first_is_zero)
        return 0;
      /* leading zero but could 0.123 */
      if (!seen_digit && t == '0')
        seen_first_is_zero = 1;
      seen_digit = 1;
      break;
    case 'e':
    case 'E':
      /*  must be at least one digit */
      if (!seen_digit)
        return 0;
      /* can't have more than one E */
      if (seen_e)
        return 0;
      /* reset state to post E */
      seen_e = 1;
      seen_digit = 0;
      seen_sign = 0;
      seen_dot = 0;
      break;
    default:
      return 0;
    }
  }
  return seen_digit;
}

/* returns 0 if not acceptable, 1 if it is */
static int
acceptable_codepoint(Py_UCS4 codepoint)
{
  /*
    0 is allowed, surrogate pair ranges are not valid
    the builtin json decoder will allow a standalone surrogate
    but python won't allow a surrogate when constructing a string.
    python accepting lone surrogates https://bugs.python.org/issue11489

    "abc\ud83edef"  -- python rejects
    json.loads(r'"abc \ud83e def"')  -- json.loads accepts but
    it is really invalid and we reject
  */

  /* no surrogates */
  if ((codepoint >= 0xD800 && codepoint <= 0xDBFF) || (codepoint >= 0xDC00 && codepoint <= 0xDFFF))
    return 0;

  /* out of range */
  if ((codepoint < 0) || (codepoint > 0x10FFFF))
    return 0;

  return 1;
}

/* returns negative number on error.  checking the number of digits
   are available must be done by caller.  */
static int
get_hex(const uint8_t *buf, int num_digits)
{
  int value = 0;

  while (num_digits)
  {
    uint8_t c = *buf;
    if (c >= '0' && c <= '9')
      c = c - '0';
    else if (c >= 'A' && c <= 'F')
      c = 10 + c - 'A';
    else if (c >= 'a' && c <= 'f')
      c = 10 + c - 'a';
    else
      return -1;
    num_digits -= 1;
    buf++;
    value = (value << 4) + c;
  }
  return value;
}

static int
jsonb_decode_utf8_string_complex(const uint8_t *buf, size_t end, PyObject *unistr, enum JSONBTag tag, size_t *pLength,
                                 Py_UCS4 *pMax_char)
{
  /*
    unistr is PyUnicode to fill in, or NULL
    tag is JT_TEXTJ for json escapes, TEXT5 for json5 escapes, else no
    escapes
    pLength is returning length in codepoints
    pMax_char is returning maximum codepoint value

    int return is zero for not valid utf8 + plus escapes, else non-zero
    for success
  */

  /* zero length should not be passed in */
  assert(end > 0);
  /* only valid values.  TEXTRAW should be passed as TEXT  */
  assert(tag == JT_TEXT || tag == JT_TEXTJ || tag == JT_TEXT5 || tag == JT_TEXTRAW);

  Py_UCS4 max_char = 127;

  /* next byte to read */
  size_t sin_index = 0;
  /* next output codepoint to write, used when unistr supplied or to calculate length */
  Py_ssize_t sout_index = 0;

  void *unistr_DATA = unistr ? PyUnicode_DATA(unistr) : 0;
  int unistr_KIND = unistr ? PyUnicode_KIND(unistr) : 0;

  while (sin_index < end)
  {
    Py_UCS4 b = buf[sin_index];
    sin_index++;

    if ((b & 0x80) == 0) /* 0b1000_0000 */
    {
      if (tag != JT_TEXTRAW)
      {
        /* handle various banned chars */
        if (b < 0x20 && (tag == JT_TEXT || tag == JT_TEXTJ))
          return 0;

        if (b == '"' && (tag == JT_TEXT || tag == JT_TEXTJ))
          return 0;

        if (b == '\\')
        {
          if (tag == JT_TEXT)
            return 0;
          /* there must be at least one more byte */
          if (sin_index == end)
            return 0;

          b = buf[sin_index];
          sin_index++;

          /* process JSON escapes */
          if (b == '\\' || b == '"' || b == '/')
          {
            /* do nothing - left as is */
          }
          else if (b == 'b' || b == 'f' || b == 'n' || b == 'r' || b == 't' || b == 'v')
          {
            switch (b)
            {
            case 'b':
              b = '\b';
              break;

            case 'f':
              b = '\f';
              break;

            case 'n':
              b = '\n';
              break;

            case 'r':
              b = '\r';
              break;

            case 't':
              b = '\t';
              break;

            case 'v':
              b = '\v';
              if (tag == JT_TEXTJ)
                return 0;
              break;
            }
          }
          else if (tag == JT_TEXT5 && b == '0')
          {
            b = 0;
            /* but it must be followed by a non-digit or end of string */
            if (sin_index < end)
            {
              if (buf[sin_index] >= '0' && buf[sin_index] <= '9')
                return 0;
            }
          }
          else if (tag == JT_TEXT5 && (b == 'x' || b == 'X'))
          {
            if (sin_index + 2 <= end)
            {
              int v = get_hex(buf + sin_index, 2);
              if (v < 0)
                return 0;
              b = v;
              sin_index += 2;
            }
            else
              return 0;
          }
          else if (tag == JT_TEXT5 && b == '\'')
          {
            /* do nothing - json5 can backslash escape single quote */
          }
          else if (b == 'u')
          {
            if (sin_index + 4 <= end)
            {
              int v = get_hex(buf + sin_index, 4);
              if (v < 0)
                return 0;
              b = v;
              sin_index += 4;
            }
            else
              return 0;
            /* surrogate pair? */
            if (b >= 0xd800 && b <= 0xdbff)
            {
              if (sin_index + 6 <= end && buf[sin_index] == '\\' && buf[sin_index + 1] == 'u')
              {
                /* skip \u */
                sin_index += 2;
                int second = get_hex(buf + sin_index, 4);
                sin_index += 4;
                /* need to be in second part range */
                if (second < 0 || second < 0xdc00 || second > 0xdfff)
                  return 0;
                b = ((b - 0xD800) << 10) + (second - 0xDC00) + 0x10000;
              }
              else
                return 0;
              /* surrogate pairs can't express unacceptable codepoints */
              assert(acceptable_codepoint(b));
            }
          }
          else if (tag == JT_TEXT5)
          {
            /* json5 swallows backslash LineTerminatorSequence */
            if (b == '\n')
              continue;
            /* detect U+2028 or U+2029 as utf8 bytes */
            if (b == 0xe2 && sin_index + 1 < end && buf[sin_index] == 0x80
                && (buf[sin_index + 1] == 0xa8 || buf[sin_index + 1] == 0xa9))
            {
              sin_index += 2;
              continue;
            }
            if (b == '\r')
            {
              /* if \r\n then swallow both, else just the \r */
              if (sin_index < end && buf[sin_index] == '\n')
                sin_index++;
              continue;
            }
            else
              /* not a valid backslash escape */
              return 0;
          }
          else
            /* not an acceptable escape */
            return 0;

          /* all unacceptable codepoints are >= 0x80 which requires a
           multibyte-sequence to express. */
          assert(acceptable_codepoint(b));
        }
        max_char = Py_MAX(b, max_char);
      }
      if (unistr)
        PyUnicode_WRITE(unistr_KIND, unistr_DATA, sout_index, b);
      sout_index++;
      continue;
    } /* end of single byte codepoint */

    /* utf8 multi-byte sequences */
    Py_UCS4 codepoint = 0;
    int remaining = 0;
    if ((b & 0xf8 /* 0b1111_1000 */) == 0xf0 /* 0b1111_0000*/)
    {
      codepoint = b & 0x07; /* 0b0000_0111 */
      remaining = 3;
    }
    else if ((b & 0xf0 /* 0b1111_0000*/) == 0xe0 /* 0b1110_0000 */)
    {
      codepoint = b & 0x0f; /* 0b0000_1111 */
      remaining = 2;
    }
    else if ((b & 0xe0 /* 0b1110_0000 */) == 0xc0 /* 0b1100_0000 */)
    {
      codepoint = b & 0x1f; /* 0b0001_1111 */
      remaining = 1;
    }
    else
      /* not a valid utf8 encoding */
      return 0;

    int encoding_len = 1 + remaining;
    if (sin_index + remaining > end)
      /* not enough continuation bytes */
      return 0;

    while (remaining)
    {
      codepoint <<= 6;
      b = buf[sin_index];
      sin_index++;
      if ((b & 0xc0 /*0b1100_0000*/) != 0x80 /* 0b1000_0000 */)
        /* invalid continuation byte */
        return 0;
      codepoint += b & 0x3f; /* 0b0011_1111 */
      remaining -= 1;
    }

    if (!acceptable_codepoint(codepoint))
      return 0;

    /* check for overlong encoding */
    if (codepoint < 0x80 || ((codepoint >= 0x80 && codepoint <= 0x7FF) && encoding_len != 2)
        || ((codepoint >= 0x800 && codepoint <= 0xFFFF) && encoding_len != 3))
      return 0;

    max_char = Py_MAX(codepoint, max_char);
    if (unistr)
      PyUnicode_WRITE(unistr_KIND, unistr_DATA, sout_index, codepoint);
    sout_index++;
    continue;
  }
  if (pLength)
    *pLength = sout_index;
  if (pMax_char)
    *pMax_char = max_char;
  return 1;
}

/* the most common case is dealing with ascii range and no
   backslashes, quotes etc.  SQLite never generates TEXTRAW but it does
   generate the other 3 text types so fast path */
static int
jsonb_decode_utf8_string(const uint8_t *buf, size_t end, PyObject *unistr, enum JSONBTag tag, size_t *pLength,
                         Py_UCS4 *pMax_char)
{
  if (unistr || tag == JT_TEXT5 || tag == JT_TEXTJ)
    return jsonb_decode_utf8_string_complex(buf, end, unistr, tag, pLength, pMax_char);

  assert(tag == JT_TEXTRAW || tag == JT_TEXT);

  switch (tag)
  {
  case JT_TEXTRAW: {
    for (size_t pos = 0; pos < end; pos++)
      if (buf[pos] & 0x80)
        return jsonb_decode_utf8_string_complex(buf, end, unistr, tag, pLength, pMax_char);
    break;
  }
  case JT_TEXT: {
    for (size_t pos = 0; pos < end; pos++)
    {
      uint8_t b = buf[pos];
      /* jsonIsOk table in sqlite source and JSONB_TEXT case in jsonbValidityCheck.
         bizarrely single quote is allowed even though it needs to be escaped in
         sql contrary to the spec */
      if (b < 0x20 || b == 0x22 || b == 0x5c)
        return 0;
      if (b & 0x80)
        return jsonb_decode_utf8_string_complex(buf, end, unistr, tag, pLength, pMax_char);
    }
    break;
  }
  default:
    Py_UNREACHABLE();
  }
  *pMax_char = 127;
  *pLength = end;
  return 1;
}

/** .. method:: jsonb_detect(data: Buffer) -> bool

    Returns ``True`` if data is valid JSONB, otherwise ``False``.  If this returns
    ``True`` then SQLite will produce valid JSON from it.

    SQLite's json_valid only checks the various internal type and length fields are consistent
    and items seem reasonable.  It does not check all corner cases, or the UTF8
    encoding, and so can produce invalid JSON even if json_valid said it was valid JSONB.

    .. note::

      :func:`~apsw.jsonb_decode` always validates the data as it decodes, so there is no
      need to call this function separately.  This function is useful for determining if
      some data is valid, and not some other binary format such as an image.
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

  Py_buffer data_buffer;

  if (PyObject_GetBufferContiguous(data, &data_buffer, PyBUF_SIMPLE) < 0)
    return NULL;

  struct JSONBDecodeBuffer buf = {
    .buffer = data_buffer.buf,
    .end_offset = data_buffer.len,
    .alloc = 0,
  };

  assert(!PyErr_Occurred());
  PyObject *res = jsonb_decode_one(&buf);
  assert(!PyErr_Occurred() && (res == DecodeFailure || res == DecodeSuccess));

  if (res == DecodeSuccess && buf.offset != buf.end_offset)
    res = DecodeFailure;

  PyBuffer_Release(&data_buffer);

  if (res == DecodeFailure)
    Py_RETURN_FALSE;
  Py_RETURN_TRUE;
}

/* returns 0 if not jsonb else 1 if it is */
static int
jsonb_detect_internal(const void *data, size_t length)
{
  struct JSONBDecodeBuffer buf = {
    .buffer = data,
    .end_offset = length,
    .alloc = 0,
  };

  PyObject *res = jsonb_decode_one(&buf);
  assert(!PyErr_Occurred() && (res == DecodeFailure || res == DecodeSuccess));
  return (res == DecodeSuccess && buf.offset == length) ? 1 : 0;
}

/** .. method:: jsonb_decode(data: Buffer, *,  object_pairs_hook: Callable[[list[tuple[str, JSONBTypes | Any]]], Any] | None = None,  object_hook: Callable[[dict[str, JSONBTypes | Any]], Any] | None = None,    array_hook: Callable[[list[JSONBTypes | Any]], Any] | None = None,    parse_int: Callable[[str], Any] | None = None,    parse_float: Callable[[str], Any] | None = None,) -> Any

    Decodes JSONB binary data into a Python object.  It is like :func:`json.loads`
    but operating on JSONB binary source instead of a JSON text source.

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
    :param parse_int: Called with a :class:`str` of the integer, and
        should return a value to use.  The default is :class:`int`.
        If the integer is hexadecimal then it will be called with a
        second parameter of 16.
    :param parse_float: Called with a :class:`str` of the float, and
        should return a value to use.  The default is :class:`float`.

    Only one of ``object_hook`` or ``object_pairs_hook`` can be
    provided.  ``object_pairs_hook`` is useful when you want something
    other than a dict, care about the order of keys, want to convert
    them first (eg case, numbers, normalization), want to handle duplicate
    keys etc.

    The array, int, and float hooks let you use alternate implementations.
    For example if you are using `numpy
    <https://numpy.org/doc/stable/user/basics.types.html>`__ then you
    could use numpy arrays instead of lists, or numpy's float128 to get
    higher precision floating numbers with greater exponent range than the
    builtin float type.

    If you use :class:`types.MappingProxyType` as ``object_hook`` and
    :class:`tuple` as ``array_hook`` then the overall returned value
    will be immutable (read only).

    .. note::

      The data is always validated during decode.  There is no need to
      separately call :func:`~apsw.jsonb_detect`.
*/
static PyObject *
JSONB_decode(PyObject *self_, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *data;
  PyObject *object_pairs_hook = NULL;
  PyObject *object_hook = NULL;
  PyObject *array_hook = NULL;
  PyObject *parse_int = NULL;
  PyObject *parse_float = NULL;

  {
    Apsw_jsonb_decode_CHECK;
    ARG_PROLOG(1, Apsw_jsonb_decode_KWNAMES);
    ARG_MANDATORY ARG_Buffer(data);
    ARG_OPTIONAL ARG_optional_Callable(object_pairs_hook);
    ARG_OPTIONAL ARG_optional_Callable(object_hook);
    ARG_OPTIONAL ARG_optional_Callable(array_hook);
    ARG_OPTIONAL ARG_optional_Callable(parse_int);
    ARG_OPTIONAL ARG_optional_Callable(parse_float);
    ARG_EPILOG(NULL, Apsw_jsonb_decode_USAGE, );
  }

  if (object_pairs_hook && object_hook)
    return PyErr_Format(PyExc_ValueError, "You can't provide both object_hook and object_pairs_hook");

  Py_buffer data_buffer;

  if (PyObject_GetBufferContiguous(data, &data_buffer, PyBUF_SIMPLE) < 0)
    return NULL;

  struct JSONBDecodeBuffer buf = {
    .buffer = data_buffer.buf,
    .end_offset = data_buffer.len,
    .object_pairs_hook = object_pairs_hook,
    .object_hook = object_hook,
    .array_hook = array_hook,
    .parse_int = parse_int,
    .parse_float = parse_float,
    .alloc = 1,
  };

  PyObject *res = jsonb_decode_one(&buf);
  PyBuffer_Release(&data_buffer);

  assert((PyErr_Occurred() && !res) || (!PyErr_Occurred() && res));
  if (res && buf.offset != buf.end_offset)
  {
    Py_CLEAR(res);
    PyErr_Format(PyExc_ValueError, "not a valid jsonb value");
  }

  return res;
}
