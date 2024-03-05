/*

Implements the Unicode Technical Report #29 break algorithms

This code is performance sensitive.  It is run against every character
of every string that gets indexed, against every query string, and
often on query matches.  Characters are processed multiple times eg to
find word segments, then a second time to determine if characters
within are letters/numbers or not.  Lookaheads may have to backout.

The code was originally developed in Python - see the git history of
file apsw/_tr29py.py for development process.  This code is then a
translation of the Python into C.

The TextIterator comes from that Python code.  In C++ it would be
templated taking the category function as a template parameter, but
in C I am limited to static inline functions, aka macros.

It is ugly, but it works.

*/

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "_tr29db.c"

/* if pyutil.c is included then the compiler whines about all the
   static definitions that aren't used, so we do these instead */
#define PyErr_AddExceptionNoteV(...)
#define Py_TypeName(o) (Py_TYPE(o)->tp_name)

#include "argparse.c"

/* the break routines take the same 2 arguments */
#define break_KWNAMES "text", "offset"

/*
TextIterator keeps track of the current character being examined, the
next character (lookahead), and the position.

The character/lookahead are the category flags, not the codepoint
value, obtained by calling cat_func.  They will always have at least
one bit set, except for the final lookahead one position beyond the
last actual character which is set to zero.  Tests are then performed
using binary and.

The position value is one beyond the current position.  This is how
FTS5 offsets work, how TR29 defines positions, and how Python works -
eg range(10) doesn't include 10 itself.

When more than one character lookahead needs to be done, the current
state is stored in the saved structure.

The methods are implemented as macros.

it_advance

Accepts the current character, and moves to the next

it_absorb(match, extend)

Many of the rules are to take zero or more of a category, which this
does. There are also extend rules where category X followed by zero or
more extends is treated as though it was just X.  This keeps advancing
while those criteria are met.  Crucially curchar retains its original
value during the advancing.

it_begin

Saves the current state.

it_rollback

Restores prior saved state.

it_commit

Saved state is not needed.

*/

typedef struct
{
  Py_ssize_t pos;
  unsigned curchar;
  unsigned lookahead;

#ifndef NDEBUG
  /* This field is used to catch attempts at nested transactions which
     are a programming error */
  int in_transaction;
#endif

  struct
  {
    Py_ssize_t pos;
    unsigned curchar;
    unsigned lookahead;
  } saved;

} TextIterator;

#define TEXT_INIT                                                                                                      \
  {                                                                                                                    \
    .pos = offset, .curchar = 0,                                                                                       \
    .lookahead = (offset == text_end) ? 0 : cat_func(PyUnicode_READ(text_kind, text_data, offset)),                    \
  }

#define it_advance()                                                                                                   \
  do                                                                                                                   \
  {                                                                                                                    \
    assert(it.pos < text_end);                                                                                         \
    it.curchar = it.lookahead;                                                                                         \
    it.pos++;                                                                                                          \
    it.lookahead = (it.pos == text_end) ? 0 : cat_func(PyUnicode_READ(text_kind, text_data, it.pos));                  \
  } while (0)

#define it_absorb(match, extend)                                                                                       \
  do                                                                                                                   \
  {                                                                                                                    \
    if (it.lookahead & (match))                                                                                        \
    {                                                                                                                  \
      unsigned savechar = it.curchar;                                                                                  \
      while (it.lookahead & (match))                                                                                   \
      {                                                                                                                \
        it_advance();                                                                                                  \
        while (it.lookahead & (extend))                                                                                \
          it_advance();                                                                                                \
      }                                                                                                                \
      it.curchar = savechar;                                                                                           \
    }                                                                                                                  \
  } while (0)

#define it_begin_base()                                                                                                \
  do                                                                                                                   \
  {                                                                                                                    \
    it.saved.pos = it.pos;                                                                                             \
    it.saved.curchar = it.curchar;                                                                                     \
    it.saved.lookahead = it.lookahead;                                                                                 \
  } while (0)

#define it_rollback_base()                                                                                             \
  do                                                                                                                   \
  {                                                                                                                    \
    it.pos = it.saved.pos;                                                                                             \
    it.curchar = it.saved.curchar;                                                                                     \
    it.lookahead = it.saved.lookahead;                                                                                 \
  } while (0)

#ifndef NDEBUG
#define it_begin()                                                                                                     \
  do                                                                                                                   \
  {                                                                                                                    \
    assert(!it.in_transaction);                                                                                        \
    it_begin_base();                                                                                                   \
    it.in_transaction = 1;                                                                                             \
  } while (0)

#define it_commit()                                                                                                    \
  do                                                                                                                   \
  {                                                                                                                    \
    assert(it.in_transaction);                                                                                         \
    it.in_transaction = 0;                                                                                             \
  } while (0)

#define it_rollback()                                                                                                  \
  do                                                                                                                   \
  {                                                                                                                    \
    assert(it.in_transaction);                                                                                         \
    it_rollback_base();                                                                                                \
    it.in_transaction = 0;                                                                                             \
  } while (0)

#else
#define it_begin()                                                                                                     \
  do                                                                                                                   \
  {                                                                                                                    \
    it_begin_base();                                                                                                   \
  } while (0)

#define it_commit()                                                                                                    \
  do                                                                                                                   \
  {                                                                                                                    \
  } while (0)

#define it_rollback()                                                                                                  \
  do                                                                                                                   \
  {                                                                                                                    \
    it_rollback_base();                                                                                                \
  } while (0)
#endif

static PyObject *
sentence_next_break(PyObject *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *text = NULL;
  Py_ssize_t offset;

  ARG_PROLOG(2, break_KWNAMES);
  ARG_MANDATORY ARG_PyUnicode(text);
  ARG_MANDATORY ARG_PyUnicode_offset(offset, text);
  ARG_EPILOG(NULL, "sentence_next_break(text: str, offset: int)", );

  /*  From spec */
#define ParaSep (SC_Sep | SC_CR | SC_LF)
#define SATerm (SC_STerm | SC_ATerm)

  void *text_data = PyUnicode_DATA(text);
  int text_kind = PyUnicode_KIND(text);
  Py_ssize_t text_end = PyUnicode_GET_LENGTH(text);

#define cat_func sentence_category
  TextIterator it = TEXT_INIT;

  /* SB1 implicit */

  /* SB2 */
  while (it.pos < text_end)
  {
    it_advance();

    /* SB3 */
    if (it.curchar & SC_CR && it.lookahead & SC_LF)
    {
      it_advance();
      break;
    }

    /* SB4 */
    if (it.curchar & ParaSep)
      break;

    /* SB5 */
    it_absorb(SC_Format | SC_Extend, 0);
  }

  return PyLong_FromLong(it.pos);
}

#define category_name_KWNAMES "which", "codepoint"
static PyObject *
category_name(PyObject *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  const char *which = NULL;
  Py_UCS4 codepoint;

  ARG_PROLOG(2, category_name_KWNAMES);
  ARG_MANDATORY ARG_str(which);
  ARG_MANDATORY ARG_codepoint(codepoint);
  ARG_EPILOG(NULL, "category_name(which: str, codepoint: int)", );

  PyObject *res = NULL;

  /* the majority of codepoints only have one associated value, so
     we resize during the rare occasions when there are more than one */

#define X(v)                                                                                                           \
  do                                                                                                                   \
  {                                                                                                                    \
    if ((val & v) == v)                                                                                                \
    {                                                                                                                  \
      PyObject *tmpstring = PyUnicode_FromString(#v);                                                                  \
      if (!tmpstring)                                                                                                  \
        goto error;                                                                                                    \
      if (!res)                                                                                                        \
      {                                                                                                                \
        res = PyTuple_New(1);                                                                                          \
        if (!res)                                                                                                      \
        {                                                                                                              \
          Py_CLEAR(tmpstring);                                                                                         \
          goto error;                                                                                                  \
        }                                                                                                              \
        PyTuple_SET_ITEM(res, 0, tmpstring);                                                                           \
      }                                                                                                                \
      else                                                                                                             \
      {                                                                                                                \
        if (0 != _PyTuple_Resize(&res, 1 + PyTuple_GET_SIZE(res)))                                                     \
        {                                                                                                              \
          Py_CLEAR(tmpstring);                                                                                         \
          goto error;                                                                                                  \
        }                                                                                                              \
        PyTuple_SET_ITEM(res, PyTuple_GET_SIZE(res) - 1, tmpstring);                                                   \
      }                                                                                                                \
    }                                                                                                                  \
  } while (0);

  if (0 == strcmp(which, "grapheme"))
  {
    unsigned val = grapheme_category(codepoint);
    ALL_GC_VALUES;
  }
  else if (0 == strcmp(which, "word"))
  {
    unsigned int val = word_category(codepoint);
    ALL_WC_VALUES;
  }
  else if (0 == strcmp(which, "sentence"))
  {
    unsigned int val = sentence_category(codepoint);
    ALL_SC_VALUES;
  }
  else
  {
    PyErr_Format(PyExc_ValueError, "Unknown which parameter \"%s\" - should be one of grapheme, word, sentence", which);
    Py_CLEAR(res);
  }

  return res;
error:
  Py_CLEAR(res);
  return NULL;
}

static PyObject *
get_category_category(PyObject *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  Py_UCS4 codepoint;

  ARG_PROLOG(1, "codepoint");
  ARG_MANDATORY ARG_codepoint(codepoint);
  ARG_EPILOG(NULL, "category_category(codepoint: int)", );

  return PyLong_FromUnsignedLong(category_category(codepoint));
}

static PyMethodDef methods[] = {
  { "category_name", (PyCFunction)category_name, METH_FASTCALL | METH_KEYWORDS,
    "Returns category names codepoint corresponds to" },
  { "category_category", (PyCFunction)get_category_category, METH_FASTCALL | METH_KEYWORDS,
    "Returns Unicode category" },
  { "sentence_next_break", (PyCFunction)sentence_next_break, METH_FASTCALL | METH_KEYWORDS,
    "Returns next sentence break offset" },
  { NULL, NULL, 0, NULL },
};

static PyModuleDef module_def = {
  .m_base = PyModuleDef_HEAD_INIT,
  .m_name = "apsw._tr29c",
  .m_doc = "C implementation of Unicode tr29 methods and lookups",
  .m_methods = methods,
};

PyObject *
PyInit__tr29c(void)
{
  PyObject *module = PyModule_Create(&module_def);
  if (module)
  {
    PyObject *ver_str = PyUnicode_FromString(unicode_version);
    if (!ver_str)
      Py_CLEAR(module);
    if (PyModule_AddObject(module, "unicode_version", ver_str) < 0)
    {
      Py_CLEAR(module);
      Py_CLEAR(ver_str);
    }
  }
  return module;
}