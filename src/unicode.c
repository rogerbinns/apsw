/*
This code provides Unicode relevant functions:

* Break locations for grapheme clusters, words, sentences, and lines
* String operations operating on grapheme cluster boundaries
* Lookups mapping codepoints to category, version added,
* Codepoint conversion case folding, accent/combining removal,
  compatibility codepoints
* Codepoint name lookup
* Text widths on terminals

There are two helpers for code that needs to map between UTF8 byte
offsets and codepoint index, which are significantly more performant
than the original Python implementations.

None of this code is publicly documented - it is wrapped by unicode.py
which provides the documentation and API.
*/

#include <stddef.h>

#ifdef _MSC_VER
#include <malloc.h>
#endif

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#if defined(APSW_TESTFIXTURES) && PY_VERSION_HEX >= 0x030c0000
#include "faultinject.h"
#endif

/* back compat - we can't use pyutil because the compilers whine about
   unused static defs */

/* Various routines added in python 3.10 */
#if PY_VERSION_HEX < 0x030a0000
static PyObject *
Py_NewRef(PyObject *o)
{
  Py_INCREF(o);
  return o;
}

static int
Py_Is(const PyObject *left, const PyObject *right)
{
  return left == right;
}

static int
Py_IsNone(const PyObject *val)
{
  return Py_Is(val, Py_None);
}

#define Py_TPFLAGS_IMMUTABLETYPE 0

#endif

/* end of back compat */

#define EOT 0
#include "_unicodedb.c"

/* if pyutil.c is included then the compiler whines about all the
   static definitions that aren't used, so we do these instead */
#define PyErr_AddExceptionNoteV(...)
#define Py_TypeName(o) (Py_TYPE(o)->tp_name)

/* msvc doesn't support vla, so do it the hard way */
#if defined(_MSC_VER) || defined(__STDC_NO_VLA__)
#define VLA(name, size, type) type *name = alloca(sizeof(type) * (size))
#else
#define VLA(name, size, type) type name[size]
#endif

#define VLA_PYO(name, size) VLA(name, size, PyObject *)

#include "argparse.c"

typedef struct
{
  /* used in ObjectMapper separate */
  PyObject *separator;
} module_state;

/* the break routines take the same 2 arguments */
#define break_KWNAMES "text", "offset"

/*

The majority of the following code implements Unicode TR29 and TR14
algorithms for finding the location between grapheme clusters, words,
sentences, and line breaks.

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

it_begin()

Saves the current state.

it_rollback()

Restores prior saved state.

it_commit()

Saved state is not needed.

it_has_accepted - variable

True if at least one character has been accepted (ie not at start of
text)

Converting rules to code
========================

Each break algorithm is a while loop over the text being processed.
It starts by advancing the position so it.curchar is being examined.
A `break` statement will return a break before it.curchar.  `continue`
will accept it.curchar and move the loop to the next character.

Here are some rule patterns and how to convert them to code, where A,
B, C etc are categories.

x is the do break marker.  If you must not break, then continue to
advance.

x A

  break

A x B

  it_advance() -- moves to the gap
  break

A B x

  it_advance() -- moves between them
  it_advance() -- moves after B
  break

*/

typedef struct
{
  Py_ssize_t pos;
  unsigned long long curchar;
  unsigned long long lookahead;

#ifndef NDEBUG
  /* This field is used to catch attempts at nested transactions which
     are a programming error */
  int in_transaction;
#endif

  struct
  {
    Py_ssize_t pos;
    unsigned long long curchar;
    unsigned long long lookahead;
  } saved;

} TextIterator;

#define TEXT_INIT                                                                                                      \
  {                                                                                                                    \
    .pos = offset,                                                                                                     \
    .curchar = -1,                                                                                                     \
    .lookahead = (offset == text_end) ? EOT : cat_func(PyUnicode_READ(text_kind, text_data, offset)),                  \
  }

#define it_advance()                                                                                                   \
  do                                                                                                                   \
  {                                                                                                                    \
    assert(it.curchar != EOT);                                                                                         \
    it.curchar = it.lookahead;                                                                                         \
    if (it.curchar != EOT)                                                                                             \
    {                                                                                                                  \
      it.pos++;                                                                                                        \
      it.lookahead = (it.pos == text_end) ? EOT : cat_func(PyUnicode_READ(text_kind, text_data, it.pos));              \
    }                                                                                                                  \
  } while (0)

#define it_absorb(match, extend)                                                                                       \
  do                                                                                                                   \
  {                                                                                                                    \
    if (it.lookahead & (match))                                                                                        \
    {                                                                                                                  \
      unsigned long long savechar = it.curchar;                                                                        \
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

static Py_ssize_t
grapheme_next_break(PyObject *text, Py_ssize_t offset)
{
  assert(PyUnicode_Check(text));
  assert(offset >= 0);

  void *text_data = PyUnicode_DATA(text);
  int text_kind = PyUnicode_KIND(text);
  Py_ssize_t text_end = PyUnicode_GET_LENGTH(text);

#define cat_func grapheme_category
  TextIterator it = TEXT_INIT;
  int it_has_accepted = 0;

  /* GB1 implicit */

  /* GB2 */
  while (it.pos < text_end)
  {
    it_has_accepted = it.pos > offset;
    it_advance();

    /* GB3 */
    if (it.curchar & GC_CR && it.lookahead & GC_LF)
    {
      it.pos++;
      break;
    }

    /* GB4 */
    if (it.curchar & (GC_Control | GC_CR | GC_LF))
    {
      /* GB5: break before if any chars are accepted */
      if (it_has_accepted)
        it.pos--;
      break;
    }

    /* GB6 */
    if (it.curchar & GC_L && it.lookahead & (GC_L | GC_V | GC_LV | GC_LVT))
      continue;

    /* GB7 */
    if (it.curchar & (GC_LV | GC_V) && it.lookahead & (GC_V | GC_T))
      continue;

    /* GB8 */
    if (it.curchar & (GC_LVT | GC_T) && it.lookahead & GC_T)
      continue;

    /* GB9a */
    if (it.lookahead & GC_SpacingMark)
      continue;

    /* GB9b */
    if (it.curchar & GC_Prepend)
      continue;

    /* GB9c */
    if (it.curchar & GC_InCB_Consonant && it.lookahead & (GC_InCB_Extend | GC_InCB_Linker))
    {
      it_begin();
      int seen_linker = it.lookahead & GC_InCB_Linker;
      it_advance();
      while (it.lookahead & (GC_InCB_Extend | GC_InCB_Linker))
      {
        seen_linker = seen_linker || it.lookahead & GC_InCB_Linker;
        it_advance();
      }
      if (seen_linker && it.lookahead & GC_InCB_Consonant)
      {
        it_commit();
        continue;
      }
      it_rollback();
    }

    /* GB11 */
    if (it.curchar & GC_Extended_Pictographic && it.lookahead & (GC_Extend | GC_ZWJ))
    {
      it_begin();
      while (it.lookahead & GC_Extend)
        it_advance();
      if (it.lookahead & GC_ZWJ)
      {
        it_advance();
        if (it.lookahead & GC_Extended_Pictographic)
        {
          it_commit();
          continue;
        }
      }
      it_rollback();
    }

    /* GB9 - has to be after GB9c and GB11 because all InCB_Linker and
       InCB_Extend are also extend */
    if (it.lookahead & (GC_Extend | GC_ZWJ))
      continue;

    /* GB12 */
    if (it.curchar & GC_Regional_Indicator && it.lookahead & GC_Regional_Indicator)
    {
      it_advance();
      /* reapply GB9 */
      if (it.lookahead & (GC_Extend | GC_ZWJ | GC_InCB_Extend))
        continue;
      break;
    }

    /* GB999 */
    break;
  }

  return it.pos;
}

static PyObject *
grapheme_next_break_api(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs,
                        PyObject *fast_kwnames)
{
  PyObject *text = NULL;
  Py_ssize_t offset;

  ARG_PROLOG(2, break_KWNAMES);
  ARG_MANDATORY ARG_PyUnicode(text);
  ARG_MANDATORY ARG_PyUnicode_offset(offset, text);
  ARG_EPILOG(NULL, "grapheme_next_break(text: str, offset: int)", );

  return PyLong_FromSsize_t(grapheme_next_break(text, offset));
}

static PyObject *
word_next_break(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *text = NULL;
  Py_ssize_t offset;

  ARG_PROLOG(2, break_KWNAMES);
  ARG_MANDATORY ARG_PyUnicode(text);
  ARG_MANDATORY ARG_PyUnicode_offset(offset, text);
  ARG_EPILOG(NULL, "word_next_break(text: str, offset: int)", );

  void *text_data = PyUnicode_DATA(text);
  int text_kind = PyUnicode_KIND(text);
  Py_ssize_t text_end = PyUnicode_GET_LENGTH(text);

#undef cat_func
#define cat_func word_category
  TextIterator it = TEXT_INIT;
  int it_has_accepted = 0;

  /* From spec */
#define AHLetter (WC_ALetter | WC_Hebrew_Letter)
#define MidNumLetQ (WC_MidNumLet | WC_Single_Quote)

  /* WB1 implicit */

  /* WB2 */
  while (it.pos < text_end)
  {
  loop_top:
    it_has_accepted = it.pos > offset;
    it_advance();

    /* WB3 */
    if (it.curchar & WC_CR && it.lookahead & WC_LF)
    {
      it.pos++;
      break;
    }

    /* WB3a/b */
    if (it.curchar & (WC_Newline | WC_CR | WC_LF))
    {
      /* break before if any chars are accepted */
      if (it_has_accepted)
      {
        it.pos--;
        break;
      }
      /* else break after */
      break;
    }

    /* WB3c */
    if (it.curchar & WC_ZWJ && it.lookahead & WC_Extended_Pictographic)
      continue;

    if (it.lookahead & WC_ZWJ)
    {
      it_begin();
      it_advance();
      if (it.lookahead & WC_Extended_Pictographic)
      {
        it_advance();
        it_commit();
        continue;
      }
      it_rollback();
    }

    /* WB3d */
    if (it.curchar & WC_WSegSpace && it.lookahead & WC_WSegSpace)
      continue;

    /* WB4 */
    if (it.lookahead & (WC_Extend | WC_ZWJ | WC_Format))
    {
      unsigned long long saved_char = it.curchar;
      while (it.lookahead & (WC_Extend | WC_ZWJ | WC_Format))
      {
        if (it.lookahead & WC_ZWJ)
        {
          /* Re-apply wb3c */
          it_advance();
          if (it.lookahead & WC_Extended_Pictographic)
            goto loop_top;
        }
        else
          it_advance();
      }
      /* ignore the extending chars */
      it.curchar = saved_char;
    }

    /* WB5 */
    if (it.curchar & AHLetter && it.lookahead & AHLetter)
      continue;

    /* WB6/7 */
    if (it.curchar & AHLetter && it.lookahead & (WC_MidLetter | MidNumLetQ))
    {
      it_begin();
      it_advance();
      it_absorb(WC_Extend | WC_Format | WC_ZWJ, 0);
      if (it.lookahead & AHLetter)
      {
        it_commit();
        continue;
      }
      it_rollback();
    }

    /* WB7a */
    if (it.curchar & WC_Hebrew_Letter && it.lookahead & WC_Single_Quote)
      continue;

    /* WB7b/c */
    if (it.curchar & WC_Hebrew_Letter && it.lookahead & WC_Double_Quote)
    {
      it_begin();
      it_advance();
      if (it.lookahead & WC_Hebrew_Letter)
      {
        it_commit();
        continue;
      }
      it_rollback();
    }

    /* WB8 */
    if (it.curchar & WC_Numeric && it.lookahead & WC_Numeric)
      continue;

    /* WB9 */
    if (it.curchar & AHLetter && it.lookahead & WC_Numeric)
      continue;

    /* WB10 */
    if (it.curchar & WC_Numeric && it.lookahead & AHLetter)
      continue;

    /* WB11/12 */
    if (it.curchar & WC_Numeric && it.lookahead & (WC_MidNum | MidNumLetQ))
    {
      it_begin();
      it_advance();
      it_absorb(WC_Extend | WC_Format | WC_ZWJ, 0);
      if (it.lookahead & WC_Numeric)
      {
        it_commit();
        continue;
      }
      it_rollback();
    }

    /* WB13 */
    if (it.curchar & WC_Katakana && it.lookahead & WC_Katakana)
      continue;

    /* WB13a */
    if (it.curchar & (AHLetter | WC_Numeric | WC_Katakana | WC_ExtendNumLet) && it.lookahead & WC_ExtendNumLet)
      continue;

    /* WB13b */
    if (it.curchar & WC_ExtendNumLet && it.lookahead & (AHLetter | WC_Numeric | WC_Katakana))
      continue;

    /* WB15/16 */
    if (it.curchar & WC_Regional_Indicator && it.lookahead & WC_Regional_Indicator)
    {
      it_advance();
      it_absorb(WC_Extend | WC_ZWJ | WC_Format, 0);
      break;
    }

    /* WB999 */
    break;
  }
  return PyLong_FromSsize_t(it.pos);
}

static PyObject *
sentence_next_break(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs,
                    PyObject *fast_kwnames)
{
  PyObject *text = NULL;
  Py_ssize_t offset;

  ARG_PROLOG(2, break_KWNAMES);
  ARG_MANDATORY ARG_PyUnicode(text);
  ARG_MANDATORY ARG_PyUnicode_offset(offset, text);
  ARG_EPILOG(NULL, "sentence_next_break(text: str, offset: int)", );

  void *text_data = PyUnicode_DATA(text);
  int text_kind = PyUnicode_KIND(text);
  Py_ssize_t text_end = PyUnicode_GET_LENGTH(text);

#undef cat_func
#define cat_func sentence_category
  TextIterator it = TEXT_INIT;

  /*  From spec */
#define ParaSep (SC_Sep | SC_CR | SC_LF)
#define SATerm (SC_STerm | SC_ATerm)

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

    /* SB6 */
    if (it.curchar & SC_ATerm && it.lookahead & SC_Numeric)
      continue;

    /* SB7 */
    if (it.curchar & (SC_Upper | SC_Lower) && it.lookahead & SC_ATerm)
    {
      it_begin();
      it_advance();
      it_absorb(SC_Format | SC_Extend, 0);
      if (it.lookahead & SC_Upper)
      {
        it_commit();
        continue;
      }
      it_rollback();
    }

    /*  SB8 */
    if (it.curchar & SC_ATerm)
    {
      it_begin();
      it_absorb(SC_Close, SC_Format | SC_Extend);
      it_absorb(SC_Sp, SC_Format | SC_Extend);
      it_absorb(0xFFFFFFFFu ^ SC_OLetter ^ SC_Upper ^ SC_Lower ^ ParaSep ^ SATerm, 0);
      it_absorb(SC_Format | SC_Extend, 0);
      if (it.lookahead & SC_Lower)
      {
        it_absorb(SC_Format | SC_Extend, 0);
        it_commit();
        continue;
      }
      it_rollback();
    }

    /* SB8a */
    if (it.curchar & SATerm)
    {
      it_begin();
      it_absorb(SC_Close, SC_Format | SC_Extend);
      it_absorb(SC_Sp, SC_Format | SC_Extend);
      if (it.lookahead & (SC_SContinue | SATerm))
      {
        it_advance();
        it_absorb(SC_Format | SC_Extend, 0);
        it_commit();
        continue;
      }
      it_rollback();
    }

    /* SB9 / SB10 / SB11 */
    if (it.curchar & SATerm)
    {
      /* This will result in a break with the rules to absorb
             zero or more close then space, and one optional ParaSep */
      it_absorb(SC_Close, SC_Format | SC_Extend);
      it_absorb(SC_Sp, SC_Format | SC_Extend);
      if (it.lookahead & ParaSep)
      {
        /* Process parasep in SB3/4 above */
        continue;
      }
      break;
    }

    /* SB999 */
    continue;
  }

  return PyLong_FromSsize_t(it.pos);
}

static PyObject *
line_next_hard_break(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs,
                     PyObject *fast_kwnames)
{
  PyObject *text = NULL;
  Py_ssize_t offset;

  ARG_PROLOG(2, break_KWNAMES);
  ARG_MANDATORY ARG_PyUnicode(text);
  ARG_MANDATORY ARG_PyUnicode_offset(offset, text);
  ARG_EPILOG(NULL, "line_next_hard_break(text: str, offset: int)", );

  void *text_data = PyUnicode_DATA(text);
  int text_kind = PyUnicode_KIND(text);
  Py_ssize_t text_end = PyUnicode_GET_LENGTH(text);

#undef cat_func
#define cat_func line_category
  TextIterator it = TEXT_INIT;

  while (it.pos < text_end)
  {
    it_advance();

    if (it.curchar & LB_BK)
      break;

    if (it.curchar & LB_CR && it.lookahead & LB_LF)
    {
      it_advance();
      break;
    }

    if (it.curchar & (LB_CR | LB_LF | LB_NL))
      break;
  }

  return PyLong_FromSsize_t(it.pos);
}

static PyObject *
line_next_break(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *text = NULL;
  Py_ssize_t offset;

  ARG_PROLOG(2, break_KWNAMES);
  ARG_MANDATORY ARG_PyUnicode(text);
  ARG_MANDATORY ARG_PyUnicode_offset(offset, text);
  ARG_EPILOG(NULL, "line_next_break(text: str, offset: int)", );

  void *text_data = PyUnicode_DATA(text);
  int text_kind = PyUnicode_KIND(text);
  Py_ssize_t text_end = PyUnicode_GET_LENGTH(text);

#undef cat_func
#define cat_func line_category
  TextIterator it = TEXT_INIT;
  int it_has_accepted = 0;

  /* LB1 is already applied in the line_category data */

  /* LB2 implicit */

  /* LB3 */
  while (it.pos < text_end)
  {
    it_has_accepted = it.pos > offset;
    it_advance();
  top_of_loop:

    /* LB4 */
    if (it.curchar & LB_BK)
      break;

    /* LB5 */
    if (it.curchar & LB_CR && it.lookahead & LB_LF)
    {
      it_advance();
      break;
    }

    if (it.curchar & (LB_CR | LB_LF | LB_NL))
      break;

    /* LB6 */
    if (it.lookahead & (LB_BK | LB_CR | LB_LF | LB_NL))
      continue;

    /* LB7 can't be implemented here because rules lower down match
       longer sequences including lookahead space.  Originally I tried to
       incorporate those rules here which got unmanageable.  So now we check
       for LB7 applying in lower locations that would otherwise break using
       this macro */

#define LB7_APPLIES (it.lookahead & (LB_SP | LB_ZW))

    /* LB8 */
    if (it.curchar & LB_ZW)
    {
      it_absorb(LB_SP | LB_ZW, 0);
      if (it.lookahead & (LB_BK | LB_CR | LB_LF | LB_NL))
        continue;
      break;
    }

    /* LB8a */
    if (it.curchar & LB_ZWJ)
      continue;

    /* LB9 */
    if (!(it.curchar & (LB_BK | LB_CR | LB_LF | LB_NL | LB_SP | LB_ZW)) && it.lookahead & (LB_CM | LB_ZWJ))
    {
      unsigned long long savedchar = it.curchar;
      while (it.lookahead & (LB_CM | LB_ZWJ))
      {
        /* We need to remember if ZWJ was present so LB8a can be
           applied.  It would also be possible to outwit this with silly
           combinations of CM and ZWJ.  THe 10k item test suite passed without
           this! */
        savedchar |= it.lookahead & LB_ZWJ;
        it_advance();
      }
      it.curchar = savedchar;
      /* We already advanced and have to rerun the earlier rules again */
      goto top_of_loop;
    }

    /* LB10 */
    if (it.curchar & (LB_CM | LB_ZWJ))
      it.curchar = LB_AL;

    /* LB11 */
    if (it.curchar & LB_WJ)
      continue;
    if (it.lookahead & LB_WJ)
    {
      it_advance();
      continue;
    }

  /* LB25 here out of sequence.  The LB12 thru LB24 rules can match part
    of what LB25 matches which confuses things.  It isn't explicitly
    stated but is tested that if you have two consecutive spans that match
    LB25 then you can't break between them. So we implement the LB25 regex
    here. */
  lb25_again:
    if (it.curchar & (LB_PR | LB_PO | LB_OP | LB_HY | LB_IS | LB_NU))
    {
      it_begin();
      Py_ssize_t saved_pos = it.pos;
      if (it.curchar & (LB_PR | LB_PO))
      {
        it_advance();
        while (it.curchar & (LB_CM | LB_ZWJ))
          it_advance();
      }
      if (it.curchar & (LB_OP | LB_HY))
      {
        it_advance();
        while (it.curchar & (LB_CM | LB_ZWJ))
          it_advance();
      }
      if (it.curchar & LB_IS)
      {
        it_advance();
        while (it.curchar & (LB_CM | LB_ZWJ))
          it_advance();
      }
      if (it.curchar & LB_NU)
      {
        while (it.lookahead & (LB_NU | LB_SY | LB_IS))
        {
          it_advance();
          while (it.lookahead & (LB_CM | LB_ZWJ))
            it_advance();
        }
        if (it.lookahead & (LB_CL | LB_CP))
        {
          it_advance();
          while (it.lookahead & (LB_CM | LB_ZWJ))
            it_advance();
        }
        if (it.lookahead & (LB_PR | LB_PO))
        {
          it_advance();
          while (it.lookahead & (LB_CM | LB_ZWJ))
            it_advance();
        }
        it_commit();
        if (it.pos != saved_pos)
          goto lb25_again;
        goto lb25_end;
      }
      it_rollback();
    }
  lb25_end:

    /* LB12 */
    if (it.curchar & LB_GL)
      continue;

    /* LB12a */
    if (it.lookahead & LB_GL)
    {
      if (it.curchar & (LB_SP | LB_BA | LB_HY))
        break;
      it_advance();
      continue;
    }

    /* LB13 */
    if (it.lookahead & (LB_CL | LB_CP | LB_EX | LB_SY))
    {
      /* LB25 matches longer sequence */
      if (!(it.curchar & LB_NU && it.lookahead & (LB_IS | LB_SY)))
        continue;
      it_advance();
      continue;
    }

    /* LB15a */
    /* LB_SP was in the curchar mask, but LB18 forces a break after
       space so we can't use it here */
    if ((it.lookahead & LB_QU && it.lookahead & LB_Punctuation_Initial_Quote
         && it.curchar & (LB_BK | LB_CR | LB_NL | LB_OP | LB_QU | LB_GL | LB_ZW))
        ||
        /* handle SOT case */
        (!it_has_accepted && it.curchar & LB_QU && it.curchar & LB_Punctuation_Initial_Quote))
    {
      it_begin();
      if (!(!it_has_accepted && it.curchar & LB_QU && it.curchar & LB_Punctuation_Initial_Quote))
        it_advance();
      assert(it.curchar & LB_QU);
      it_absorb(LB_SP, LB_CM);
      it_commit();
      continue;
    }

    /* LB15b */
    if (it.lookahead & LB_QU && it.lookahead & LB_Punctuation_Final_Quote)
    {
      it_begin();
      it_advance();
      if (it.lookahead == EOT
          || it.lookahead
                 & (LB_SP | LB_GL | LB_WJ | LB_CL | LB_QU | LB_CP | LB_EX | LB_IS | LB_SY | LB_BK | LB_CR | LB_LF
                    | LB_NL | LB_ZW))
      {
        it_commit();
        continue;
      }
      it_rollback();
    }

    /* LB 15c */
    if (it.curchar & LB_SP && it.lookahead & LB_IS)
    {
      it_begin();
      it_advance();
      if (it.lookahead & LB_NU)
      {
        it_rollback();
        break;
      }
      it_rollback();
    }

    /* LB 15d */
    if (it.lookahead & LB_IS)
    {
      continue;
    }

    /* LB14 - has to be after LB15 because LB15 looks for curchar & LB_OP */
    if (it.curchar & LB_OP)
    {
      /* LB20a prevents a break in SP HY AL but if we skip past the SP
         here then LB20a never sees the SP and so doesn't fire.  So we
         implement that here too. */
      if (it.lookahead & LB_SP)
      {
        it_begin();
        it_advance();
        if (it.lookahead & (LB_HY | LB_HYPHEN))
        {
          it_advance();
          if (it.lookahead & LB_AL)
          {
            it_commit();
            continue;
          }
        }
        it_rollback();
      }
      it_absorb(LB_SP, LB_CM);
      continue;
    }

    /* LB16 */
    if (it.curchar & (LB_CL | LB_CP) && it.lookahead & (LB_SP | LB_NS))
    {
      it_begin();
      it_absorb(LB_SP, LB_CM);
      if (it.lookahead & LB_NS)
      {
        it_advance();
        it_commit();
      }
      else
        it_rollback();
    }

    /* LB17 */
    if (it.curchar & LB_B2 && it.lookahead & (LB_SP | LB_B2))
    {
      it_begin();
      it_absorb(LB_SP, LB_CM);
      if (it.lookahead & LB_B2)
      {
        it_advance();
        it_commit();
        continue;
      }
      it_rollback();
    }

    /* LB18 - but LB7 is higher priority */
    if (it.curchar & LB_SP && !LB7_APPLIES)
      break;

    /* LB19a has to be before LB19 because the rules are more general */
    if ((it.curchar & LB_EastAsianWidth_FWH) == 0 && it.lookahead & LB_QU)
    {
      it_advance();
      continue;
    }
    if (it.lookahead & LB_QU)
    {
      it_begin();
      it_advance();
      if (it.lookahead == 0 || (it.lookahead & LB_EastAsianWidth_FWH) == 0)
      {
        it_commit();
        continue;
      }
      it_rollback();
    }
    if (it.curchar & LB_QU && (it.lookahead & LB_EastAsianWidth_FWH) == 0)
      continue;
    if (!it_has_accepted && it.curchar & LB_QU)
    {
      it_advance();
      continue;
    }

    /* LB19 */
    if ((it.curchar & (LB_QU | LB_Punctuation_Final_Quote)) == LB_QU)
      continue;
    if ((it.lookahead & (LB_QU | LB_Punctuation_Initial_Quote)) == LB_QU)
      continue;

    /* LB20 */
    if (it.curchar & LB_CB && !LB7_APPLIES)
      break;
    if (it.lookahead & LB_CB)
      break;

    /* LB 20a */
    if (!it_has_accepted && it.curchar & (LB_HY | LB_HYPHEN) && it.lookahead & LB_AL)
    {
      it_advance();
      continue;
    }
    if (it.curchar & (LB_BK | LB_CR | LB_LF | LB_NL | LB_SP | LB_ZW | LB_CB | LB_GL)
        && it.lookahead & (LB_HY | LB_HYPHEN))
    {
      it_begin();
      it_advance();
      if (it.lookahead & LB_AL)
      {
        it_advance();
        it_commit();
        continue;
      }
      it_rollback();
    }

    /* LB21a - has to be before LB21 because both lookahead & LB_HY */
    if (it.curchar & LB_HL && it.lookahead & (LB_HY | LB_BA) && !(it.lookahead & LB_EastAsianWidth_FWH))
    {
      it_begin();
      it_advance();
      if ((it.lookahead & LB_HL) == 0)
      {
        it_commit();
        continue;
      }
      it_rollback();
    }

    /* LB21 */
    if (it.lookahead & (LB_BA | LB_HY | LB_NS))
      continue;
    if (it.curchar & LB_BB)
      continue;

    /* LB21b */
    if (it.curchar & LB_SY && it.lookahead & LB_HL)
      continue;

    /* LB22 */
    if (it.lookahead & LB_IN)
      continue;

    /* LB23 */
    if (it.curchar & (LB_AL | LB_HL) && it.lookahead & LB_NU)
      continue;
    if (it.curchar & LB_NU && it.lookahead & (LB_AL | LB_HL))
      continue;

    /* LB23a */
    if (it.curchar & LB_PR && it.lookahead & (LB_ID | LB_EB | LB_EM))
      continue;
    if (it.curchar & (LB_ID | LB_EB | LB_EM) && it.lookahead & LB_PO)
      continue;

    /* LB24 */
    if (it.curchar & (LB_PR | LB_PO) && it.lookahead & (LB_AL | LB_HL))
      continue;
    if (it.curchar & (LB_AL | LB_HL) && it.lookahead & (LB_PR | LB_PO))
      continue;

    /* LB26 */
    if (it.curchar & LB_JL && it.lookahead & (LB_JL | LB_JV | LB_H2 | LB_H3))
      continue;
    if (it.curchar & (LB_JV | LB_H2) && it.lookahead & (LB_JV | LB_JT))
      continue;
    if (it.curchar & (LB_JT | LB_H3) && it.lookahead & LB_JT)
      continue;

    /* LB27 */
    if (it.curchar & (LB_JL | LB_JV | LB_JT | LB_H2 | LB_H3) && it.lookahead & LB_PO)
      continue;
    if (it.curchar & LB_PR && it.lookahead & (LB_JL | LB_JV | LB_JT | LB_H2 | LB_H3))
      continue;

    /* LB28 */
    if (it.curchar & (LB_AL | LB_HL) && it.lookahead & (LB_AL | LB_HL))
      continue;

    /* LB28A  */
    if (it.curchar & LB_AP && it.lookahead & (LB_AK | LB_DOTTED_CIRCLE | LB_AS))
      continue;
    /* 3rd rule before 2nd because it matches more text */
    if (it.curchar & (LB_AK | LB_DOTTED_CIRCLE | LB_AS) && it.lookahead & LB_VI)
    {
      it_begin();
      it_advance();
      assert(it.curchar & LB_VI);
      it_absorb(LB_CM, 0);
      if (it.lookahead & (LB_AK | LB_DOTTED_CIRCLE))
      {
        it_commit();
        continue;
      }
      it_rollback();
    }
    /* 2nd rule */
    if (it.curchar & (LB_AK | LB_DOTTED_CIRCLE | LB_AS) && it.lookahead & (LB_VF | LB_VI))
      continue;
    if (it.curchar & (LB_AK | LB_DOTTED_CIRCLE | LB_AS) && it.lookahead & (LB_AK | LB_DOTTED_CIRCLE | LB_AS))
    {
      it_begin();
      it_advance();
      if (it.lookahead & LB_VF)
      {
        it_commit();
        continue;
      }
      it_rollback();
    }

    /* LB29 */
    if (it.curchar & LB_IS && it.lookahead & (LB_AL | LB_HL))
      continue;

    /* LB30 */
    if (it.curchar & (LB_AL | LB_HL | LB_NU) && (it.lookahead & (LB_OP | LB_EastAsianWidth_FWH)) == LB_OP)
      continue;

    if ((it.curchar & (LB_CP | LB_EastAsianWidth_FWH)) == LB_CP && it.lookahead & (LB_AL | LB_HL | LB_NU))
      continue;

    /* LB30a */
    if (it.curchar & LB_RI && it.lookahead & LB_RI)
    {
      it_advance();
      it_absorb(LB_CM, 0);
      if (!LB7_APPLIES)
        break;
    }

    /* LB30b */
    if (it.curchar & LB_EB && it.lookahead & LB_EM)
      continue;
    if ((it.curchar & (LB_Extended_Pictographic | LB_Other_NotAssigned))
            == (LB_Extended_Pictographic | LB_Other_NotAssigned)
        && it.lookahead & LB_EM)
      continue;

    if (LB7_APPLIES)
      continue;

    /* LB999 */
    break;
  }

  return PyLong_FromSsize_t(it.pos);
}

static void
add_string_to_tuple(PyObject **tuple, const char *string)
{
  if (!*tuple)
  {
    *tuple = PyTuple_New(0);
    if (!*tuple)
      goto error;
  }
  PyObject *tmpstring = PyUnicode_FromString(string);
  if (!tmpstring)
    goto error;

  if (0 != _PyTuple_Resize(tuple, 1 + PyTuple_GET_SIZE(*tuple)))
  {
    Py_CLEAR(tmpstring);
    goto error;
  }
  PyTuple_SET_ITEM(*tuple, PyTuple_GET_SIZE(*tuple) - 1, tmpstring);
  return;

error:
  if (*tuple)
    Py_CLEAR(*tuple);
}

#define category_name_KWNAMES "which", "codepoint"
static PyObject *
category_name(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
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
  if ((val & v) == v)                                                                                                  \
  {                                                                                                                    \
    add_string_to_tuple(&res, #v);                                                                                     \
    if (!res)                                                                                                          \
      return NULL;                                                                                                     \
  }

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
  else if (0 == strcmp(which, "line_break"))
  {
    unsigned long long val = line_category(codepoint);
    ALL_LB_VALUES;
  }
  else if (0 == strcmp(which, "category"))
  {
    unsigned long long val = category_category(codepoint);
    ALL_CATEGORY_VALUES;
  }
  else
  {
    PyErr_Format(PyExc_ValueError,
                 "Unknown which parameter \"%s\" - should be one of grapheme, word, sentence, line_break, category",
                 which);
    Py_CLEAR(res);
  }

  return res;
}

static PyObject *
get_category_category(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs,
                      PyObject *fast_kwnames)
{
  Py_UCS4 codepoint;

  ARG_PROLOG(1, "codepoint");
  ARG_MANDATORY ARG_codepoint(codepoint);
  ARG_EPILOG(NULL, "category_category(codepoint: int)", );

  return PyLong_FromUnsignedLongLong(category_category(codepoint));
}

static PyObject *
has_category(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *text;
  Py_ssize_t start, end;
  unsigned long long mask;

#define has_category_KWARGS "text", "start", "end", "mask"
  ARG_PROLOG(4, has_category_KWARGS);
  ARG_MANDATORY ARG_PyUnicode(text);
  ARG_MANDATORY ARG_PyUnicode_offset(start, text);
  ARG_MANDATORY ARG_PyUnicode_offset(end, text);
  ARG_MANDATORY ARG_unsigned_long_long(mask);
  ARG_EPILOG(NULL, "has_category(text: str, start:int, end: int, mask: int)", );

  int kind = PyUnicode_KIND(text);
  void *data = PyUnicode_DATA(text);

  while (start < end)
  {
    if (category_category(PyUnicode_READ(kind, data, start)) & mask)
      Py_RETURN_TRUE;
    start++;
  }
  Py_RETURN_FALSE;
}

static PyObject *
casefold_ascii(PyObject *text)
{
  Py_ssize_t source_length = PyUnicode_GET_LENGTH(text);
  int source_kind = PyUnicode_KIND(text);
  void *source_data = PyUnicode_DATA(text);
  Py_ssize_t source_pos;

  for (source_pos = 0; source_pos < source_length; source_pos++)
  {
    Py_UCS4 source_char = PyUnicode_READ(source_kind, source_data, source_pos);
    /* ascii detect */
    if (source_char >= 'A' && source_char <= 'Z')
      break;
  }
  /* no changes */
  if (source_pos == source_length)
    return Py_NewRef(text);

  PyObject *dest = PyUnicode_New(source_length, 127);
  if (!dest)
    return NULL;
  assert(source_kind == PyUnicode_KIND(dest));
  void *dest_data = PyUnicode_DATA(dest);

  for (source_pos = 0; source_pos < source_length; source_pos++)
  {
    Py_UCS4 source_char = PyUnicode_READ(source_kind, source_data, source_pos);
    /* ascii detect */
    if (source_char >= 'A' && source_char <= 'Z')
      source_char += 32;
    PyUnicode_WRITE(source_kind, dest_data, source_pos, source_char);
  }

  return dest;
}

static PyObject *
casefold(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *text;

#define casefold_KWARGS "text"
  ARG_PROLOG(1, casefold_KWARGS);
  ARG_MANDATORY ARG_PyUnicode(text);
  ARG_EPILOG(NULL, "casefold(text: str)", );

  if (PyUnicode_MAX_CHAR_VALUE(text) <= 127)
    return casefold_ascii(text);

  Py_ssize_t source_length = PyUnicode_GET_LENGTH(text);
  int source_kind = PyUnicode_KIND(text);
  void *source_data = PyUnicode_DATA(text);

  /* We do two phases - the first looking for how much the result string
     is expanded because some codepoints expand to more than one folded
     codepoint.  During this phase we also detect if any changes would be
     made.  If not the original string can be returned.  We also need to
     work out what the resulting maxchar value will be because debug
     Python builds assertion fail if too generous.

     The second phase then does the folding.
*/
  int changed = 0;
  Py_ssize_t expansion = 0;

/* these are bitfield, and 127 is a miniumum so it needs no value */
#define CASEFOLD_MAXCHAR_127 0
#define CASEFOLD_MAXCHAR_255 1
#define CASEFOLD_MAXCHAR_65535 2
#define CASEFOLD_MAXCHAR_1114111 4

  int maxchar = CASEFOLD_MAXCHAR_127;

  Py_ssize_t source_pos;
  for (source_pos = 0; source_pos < source_length; source_pos++)
  {
    Py_UCS4 source_char = PyUnicode_READ(source_kind, source_data, source_pos);
    /* ascii shortcut */
    if (source_char >= 'A' && source_char <= 'Z')
    {
      changed = 1;
      continue;
    }
    if (source_char <= 127)
      continue;
    switch (source_char)
    {
      /* generated, present in _unicodedb.c */
      CASEFOLD_EXPANSION
    default:
      /* unchanged codepoint */
      if (source_char >= 128 && source_char <= 255)
        maxchar |= CASEFOLD_MAXCHAR_255;
      else if (source_char >= 256 && source_char <= 65535)
        maxchar |= CASEFOLD_MAXCHAR_65535;
      else
        maxchar |= CASEFOLD_MAXCHAR_1114111;
    }
  }

  if (!changed)
    return Py_NewRef(text);

  Py_UCS4 max_char_value = 127;
  if (maxchar & CASEFOLD_MAXCHAR_1114111)
    max_char_value = 1114111;
  else if (maxchar & CASEFOLD_MAXCHAR_65535)
    max_char_value = 65535;
  else if (maxchar & CASEFOLD_MAXCHAR_255)
    max_char_value = 255;

  PyObject *dest = PyUnicode_New(source_length + expansion, max_char_value);
  if (!dest)
    return NULL;

  int dest_kind = PyUnicode_KIND(dest);
  void *dest_data = PyUnicode_DATA(dest);

  Py_ssize_t dest_pos;

  for (source_pos = dest_pos = 0; source_pos < source_length; source_pos++)
  {
    /* each source corresponds to one or more dest chars.  The CASEFOLD_WRITE
       macro provides just the replacement for one, and writes all except
       the last when it is more than one.  That is why this macro needs to
       be available. */

#define WRITE_DEST(c)                                                                                                  \
  do                                                                                                                   \
  {                                                                                                                    \
    PyUnicode_WRITE(dest_kind, dest_data, dest_pos, (c));                                                              \
    dest_pos++;                                                                                                        \
  } while (0)

    Py_UCS4 dest_char = PyUnicode_READ(source_kind, source_data, source_pos);
    /* ascii shortcut */
    if (dest_char >= 'A' && dest_char <= 'Z')
      dest_char += 32;
    else
      switch (dest_char)
      {
        /* generated, present in _unicodedb.c */
        CASEFOLD_WRITE
      }
    WRITE_DEST(dest_char);
  }
  return dest;
}

static PyObject *
strip(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
#define strip_KWNAMES "text"
  PyObject *text = NULL;

  ARG_PROLOG(1, strip_KWNAMES);
  ARG_MANDATORY ARG_PyUnicode(text);
  ARG_EPILOG(NULL, "strip(text: str)", );

  Py_ssize_t source_length = PyUnicode_GET_LENGTH(text);
  int source_kind = PyUnicode_KIND(text);
  void *source_data = PyUnicode_DATA(text);

  /* We pack replacement codepoints into 21 bit chunks in the 64 bit value */
#define BITS21_MASK ((1 << 21) - 1)
#define CP0(x) ((x) & BITS21_MASK)
#define CP1(x) (((x) >> 21) & BITS21_MASK)

  /* Pass 1:
      * Figure out if there is any change
      * Figure out if max char value changes
      * Figure out length of replacement
  */
  /* would the result be any different */
  int is_changed = 0;
  /* new MAX_CHAR_VALUE  */
  unsigned long long maxchar_flags = 0;
  /* number of codepoints in result */
  Py_ssize_t result_length = 0;

  Py_ssize_t source_pos;
  for (source_pos = 0; source_pos < source_length; source_pos++)
  {
    Py_UCS4 source_char = PyUnicode_READ(source_kind, source_data, source_pos);
    unsigned long long conv = strip_category(source_char);
    /* it doesn't matter if we have the other bits */
    maxchar_flags |= conv;

    conv &= ~STRIP_MAXCHAR_MASK;
    if (conv == 1)
    {
      result_length += 1;
      continue;
    }
    is_changed = 1;
    if (conv == 0)
      continue;

    if (conv < 30)
    {
      result_length += conv;
      continue;
    }
    result_length += 1;
    if (CP1(conv))
      result_length += 1;
  }

  if (!is_changed)
  {
    /* Sanity check */
    assert(result_length == source_length);
    return Py_NewRef(text);
  }

  /* Pass 2:
    Create and populate result string
  */
  Py_UCS4 maxchar = 0;
  if (maxchar_flags & STRIP_MAXCHAR_1114111)
    maxchar = 1114111;
  else if (maxchar_flags & STRIP_MAXCHAR_65535)
    maxchar = 65535;
  else if (maxchar_flags & STRIP_MAXCHAR_255)
    maxchar = 255;
  else if (maxchar_flags & STRIP_MAXCHAR_127)
    maxchar = 127;

  PyObject *dest = PyUnicode_New(result_length, maxchar);
  if (!dest)
    return NULL;

  int dest_kind = PyUnicode_KIND(dest);
  void *dest_data = PyUnicode_DATA(dest);

  Py_ssize_t dest_pos;

  for (source_pos = dest_pos = 0; source_pos < source_length; source_pos++)
  {
    /* WRITE_DEST comes from casefold above */
    Py_UCS4 source_char = PyUnicode_READ(source_kind, source_data, source_pos);
    unsigned long long conv = strip_category(source_char) & ~STRIP_MAXCHAR_MASK;
    if (conv == 0)
      continue;
    if (conv == 1)
    {
      WRITE_DEST(source_char);
      continue;
    }
    if (conv >= 30)
    {
      Py_UCS4 c = CP0(conv);
      WRITE_DEST(c);
      c = CP1(conv);
      if (c)
        WRITE_DEST(c);
      continue;
    }
    switch (source_char)
    {
      /* generated, present in _unicodedb.c */
      STRIP_WRITE
    default:
      Py_UNREACHABLE();
    }
  }

  assert(dest_pos == result_length);

  return dest;
}

static PyObject *
text_width(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *text = NULL;
  Py_ssize_t offset;
  ARG_PROLOG(2, break_KWNAMES);
  ARG_MANDATORY ARG_PyUnicode(text);
  ARG_MANDATORY ARG_PyUnicode_offset(offset, text);
  ARG_EPILOG(NULL, "text_width(text: str, offset: int)", );

  void *text_data = PyUnicode_DATA(text);
  int text_kind = PyUnicode_KIND(text);
  Py_ssize_t text_end = PyUnicode_GET_LENGTH(text);

  Py_ssize_t width = 0;

  int last_was_zwj = 0;

  for (; offset < text_end; offset++)
  {
    Py_UCS4 codepoint = PyUnicode_READ(text_kind, text_data, offset);
    unsigned long long cat = category_category(codepoint);
    if (cat & Category_WIDTH_INVALID)
    {
      width = -1;
      break;
    }
    if (last_was_zwj && (cat & Category_Extended_Pictographic))
    {
      /* ZWJ followed by Extended Pictographic is zero even though the
         Extended Pictographic will be marked as two wide */
      ; /* do nothing */
    }
    else if (cat & Category_WIDTH_TWO)
    {
      width += 2;
    }
    else if (cat & Category_WIDTH_ZERO)
    {
      ; /* do nothing */
    }
    else
    {
      width += 1;
    }
    last_was_zwj = (codepoint == 0x200D);
  }

  return PyLong_FromSsize_t(width);
}

static PyObject *
grapheme_find(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
#define grapheme_find_KWNAMES "text", "substring", "start", "end"

  PyObject *text;
  PyObject *substring;
  Py_ssize_t start, end;

  ARG_PROLOG(4, grapheme_find_KWNAMES);
  ARG_MANDATORY ARG_PyUnicode(text);
  ARG_MANDATORY ARG_PyUnicode(substring);
  ARG_MANDATORY ARG_Py_ssize_t(start);
  ARG_MANDATORY ARG_Py_ssize_t(end);
  ARG_EPILOG(NULL, "grapheme_find(text: str, substring: str, start: int, end: int)", );

  void *text_data = PyUnicode_DATA(text);
  int text_kind = PyUnicode_KIND(text);
  Py_ssize_t text_end = PyUnicode_GET_LENGTH(text);

  void *substring_data = PyUnicode_DATA(substring);
  int substring_kind = PyUnicode_KIND(substring);
  Py_ssize_t substring_end = PyUnicode_GET_LENGTH(substring);

  /* fixup offsets */
  if (start < 0)
    start = Py_MAX(0, text_end + start);
  if (end < 0)
    end = text_end + end;
  end = Py_MIN(end, text_end) - substring_end + 1;

  /* zero length is always found if start is 0 even if end is before start! */
  if (substring_end == 0 && start == 0)
    return PyLong_FromLong(0);
  /* early out */
  if (substring_end > text_end || start >= end)
    goto notfound;

  Py_ssize_t offset = start;

  while (offset < end)
  {
    /* we only allow the empty substring to be found at grapheme boundaries */
    if (substring_end == 0
        || PyUnicode_READ(text_kind, text_data, offset) == PyUnicode_READ(substring_kind, substring_data, 0))
    {
      int matched = 1;
      for (Py_ssize_t check = 1; check < substring_end; check++)
      {
        if (PyUnicode_READ(text_kind, text_data, offset + check)
            != PyUnicode_READ(substring_kind, substring_data, check))
        {
          matched = 0;
          break;
        }
      }
      if (matched)
      {

        Py_ssize_t expected = offset + substring_end;
        Py_ssize_t boundary = offset;
        while (boundary < expected)
        {
          boundary = grapheme_next_break(text, boundary);
        }
        if (boundary == expected)
        {
          return PyLong_FromSsize_t(offset);
        }
      }
    }
    offset = grapheme_next_break(text, offset);
  }

notfound:
  return PyLong_FromSsize_t(-1);
}

static PyObject *
grapheme_length(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *text = NULL;
  Py_ssize_t offset;

  ARG_PROLOG(2, break_KWNAMES);
  ARG_MANDATORY ARG_PyUnicode(text);
  ARG_MANDATORY ARG_PyUnicode_offset(offset, text);
  ARG_EPILOG(NULL, "grapheme_length(text: str, offset: int)", );

  Py_ssize_t text_length = PyUnicode_GET_LENGTH(text);
  size_t count = 0;

  while (offset < text_length)
  {
    offset = grapheme_next_break(text, offset);
    count++;
  }

  return PyLong_FromSize_t(count);
}

static PyObject *
grapheme_substr(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *text = NULL;
  Py_ssize_t start, stop;

#define grapheme_substr_KWNAMES "text", "start", "stop"
  ARG_PROLOG(3, grapheme_substr_KWNAMES);
  ARG_MANDATORY ARG_PyUnicode(text);
  Py_ssize_t len_text = PyUnicode_GET_LENGTH(text);
  ARG_MANDATORY ARG_ifnone(start = 0) ARG_Py_ssize_t(start);
  ARG_MANDATORY ARG_ifnone(stop = len_text) ARG_Py_ssize_t(stop);
  ARG_EPILOG(NULL, "grapheme_substr(text: str, start: int, stop: int)", );

  if (start > len_text || start == stop || stop == 0 || (start > 0 && stop >= 0 && start >= stop))
    return PyUnicode_New(0, 0);

  PyObject *offsets = NULL;

  if (start < 0 || stop < 0)
  {
    /* we are doing addressing relative to the end of the string so we
       have to track offsets of the whole string and then index */
    offsets = PyList_New(1);
    if (!offsets)
      goto error;
    PyObject *zero = PyLong_FromLong(0);
    if (!zero)
      goto error;
    PyList_SET_ITEM(offsets, 0, zero);
  }

  Py_ssize_t count = 0;
  Py_ssize_t text_offset = 0;

  Py_ssize_t start_offset = (start == 0) ? 0 : len_text;
  Py_ssize_t stop_offset = len_text;

  while (text_offset < len_text)
  {
    text_offset = grapheme_next_break(text, text_offset);
    count++;
    if (offsets)
    {
      PyObject *o = PyLong_FromSsize_t(text_offset);
      if (!o)
        goto error;
      if (PyList_Append(offsets, o) != 0)
      {
        Py_DECREF(o);
        goto error;
      }
    }
    if (start == count)
      start_offset = text_offset;
    if (stop == count)
    {
      stop_offset = text_offset;
      if (!offsets)
        break;
    }
  }

  if (!offsets)
  {
    if (stop_offset == start_offset)
      return PyUnicode_New(0, 0);
    assert(stop_offset > start_offset);
    return PyUnicode_Substring(text, start_offset, stop_offset);
  }

  Py_ssize_t offsets_len = PyList_GET_SIZE(offsets) - 1;

  Py_ssize_t nchars = PySlice_AdjustIndices(offsets_len, &start, &stop, 1);
  if (nchars)
  {
    start_offset = PyLong_AsSsize_t(PyList_GET_ITEM(offsets, start));
    stop_offset = PyLong_AsSsize_t(PyList_GET_ITEM(offsets, stop));
    Py_CLEAR(offsets);
    return PyUnicode_Substring(text, start_offset, stop_offset);
  }

  Py_CLEAR(offsets);
  return PyUnicode_New(0, 0);
error:
  Py_CLEAR(offsets);
  return NULL;
}

static PyObject *
version_added(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  Py_UCS4 codepoint;

  ARG_PROLOG(1, "codepoint");
  ARG_MANDATORY ARG_codepoint(codepoint);
  ARG_EPILOG(NULL, "version_added(codepoint: int)", );

  const char *age = age_category(codepoint);
  if (!age)
    Py_RETURN_NONE;
  return PyUnicode_FromString(age);
}

static PyObject *
hangul_syllable(Py_UCS4 codepoint)
{
  /* Chapter 3 of the unicode standard gives example Java code for how
     to do this.  Annoyingly it is only as a PDF so not easily linked. */

  /* common constants (unused omitted) */
  int SBase = 0xAC00, VCount = 21, TCount = 28, NCount = VCount * TCount;

  /* tables */
  static const char *JAMO_L_TABLE[]
      = { "G", "GG", "N", "D", "DD", "R", "M", "B", "BB", "S", "SS", "", "J", "JJ", "C", "K", "T", "P", "H" };
  static const char *JAMO_V_TABLE[] = { "A",  "AE", "YA", "YAE", "EO", "E",  "YEO", "YE", "O",  "WA", "WAE",
                                        "OE", "YO", "U",  "WEO", "WE", "WI", "YU",  "EU", "YI", "I" };
  static const char *JAMO_T_TABLE[]
      = { "",   "G",  "GG", "GS", "N",  "NJ", "NH", "D",  "L", "LG", "LM", "LB", "LS", "LT",
          "LP", "LH", "M",  "B",  "BS", "S",  "SS", "NG", "J", "C",  "K",  "T",  "P",  "H" };

  unsigned SIndex = codepoint - SBase;
  assert(codepoint >= 0xAC00 && codepoint <= 0xD7A3);
  unsigned LIndex = SIndex / NCount;
  unsigned VIndex = (SIndex % NCount) / TCount;
  unsigned TIndex = SIndex % TCount;

  static const char *PREFIX = "HANGUL SYLLABLE ";

  unsigned size
      = strlen(PREFIX) + strlen(JAMO_L_TABLE[LIndex]) + strlen(JAMO_V_TABLE[VIndex]) + strlen(JAMO_T_TABLE[TIndex]);

  PyObject *result = PyUnicode_New(size, 127);
  if (!result)
    return NULL;

  Py_ssize_t index = 0;
  const char *src = PREFIX;

#define COPY_STR(source)                                                                                               \
  do                                                                                                                   \
  {                                                                                                                    \
    src = source;                                                                                                      \
    while (*src)                                                                                                       \
    {                                                                                                                  \
      PyUnicode_WriteChar(result, index, *src);                                                                        \
      index++;                                                                                                         \
      src++;                                                                                                           \
    }                                                                                                                  \
  } while (0);

  COPY_STR(PREFIX);
  COPY_STR(JAMO_L_TABLE[LIndex]);
  COPY_STR(JAMO_V_TABLE[VIndex]);
  COPY_STR(JAMO_T_TABLE[TIndex]);

  return result;
}

static PyObject *
name_expand(const unsigned char *name, unsigned skip)
{
  unsigned compressed_length = name[0];
  while (skip)
  {
    name += compressed_length + 1;
    compressed_length = name[0];
    skip--;
  }

  if (compressed_length == 0)
    Py_RETURN_NONE;

  /* first pass to get length */
  unsigned expanded_length = 0, pos;
  for (pos = 0; pos < compressed_length; pos++)
    expanded_length += name_subs[name[1 + pos]][0];

  /* now construct the string */
  PyObject *result = PyUnicode_New(expanded_length, 127);
  if (!result)
    return NULL;

  /* and copy each segment */
  Py_ssize_t result_offset = 0;
  for (pos = 0; pos < compressed_length; pos++)
  {
    unsigned segment_size = name_subs[name[1 + pos]][0];
    unsigned segment_offset = 0;
    for (; segment_offset < segment_size; result_offset++, segment_offset++)
      PyUnicode_WriteChar(result, result_offset, name_subs[name[1 + pos]][1 + segment_offset]);
  }

  return result;
}

static PyObject *
name_with_hex_suffix(const char *prefix, Py_UCS4 codepoint)
{
#if Py_VERSION_HEX >= 0x030c0000
  return PyUnicode_FromFormat("%s%04X", prefix, codepoint);
#else
  /* Python < 3.12 doesn't have upper case X */
  char buffer[16];

  sprintf(buffer, "%04X", codepoint);
  return PyUnicode_FromFormat("%s%s", prefix, buffer);
#endif
}

static PyObject *
codepoint_name(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  Py_UCS4 codepoint;

  ARG_PROLOG(1, "codepoint");
  ARG_MANDATORY ARG_codepoint(codepoint);
  ARG_EPILOG(NULL, "codepoint_name(codepoint: int)", );

  NAME_RANGES(codepoint);

  if (codepoint >= TAG_RANGE_START && codepoint <= TAG_RANGE_END)
    return name_expand(tag_range_names, codepoint - TAG_RANGE_START);

  if (codepoint >= 0xAC00 && codepoint <= 0xD7A3)
    return hangul_syllable(codepoint);

  return regular_codepoint_to_name(codepoint);
}

/* Given a str offset provide the corresponding UTF8 bytes offset */

typedef struct
{
  PyObject_HEAD
  vectorcallfunc vectorcall;
  Py_ssize_t bytes_len;
  Py_ssize_t str_offset;
  Py_ssize_t bytes_offset;
  Py_buffer buffer;
  /* we often go backwards as spans are iterated so remember previous */
  Py_ssize_t last_str_offset;
  Py_ssize_t last_bytes_offset;
  PyObject *str;
} ToUtf8PositionMapper;

static void
ToUtf8PositionMapper_finalize(ToUtf8PositionMapper *self)
{
  /* this is intentionally implemented to be safe to call multiple times */
  if (self->buffer.obj)
  {

    PyBuffer_Release(&self->buffer);
    self->buffer.obj = NULL;
  }
  Py_CLEAR(self->str);
}

static PyObject *
ToUtf8PositionMapper_call(ToUtf8PositionMapper *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                          PyObject *fast_kwnames)
{
  Py_ssize_t pos;
  ARG_PROLOG(1, "pos");
  ARG_MANDATORY ARG_Py_ssize_t(pos);
  ARG_EPILOG(NULL, "to_utf8_position_mapper.__call__(pos: int)", );

  if (pos < 0)
    return PyErr_Format(PyExc_ValueError, "position needs to be zero or positive");

  if (pos < self->str_offset)
  {
    /* went backwards */
    if (pos >= self->last_str_offset)
    {
      self->str_offset = self->last_str_offset;
      self->bytes_offset = self->last_bytes_offset;
    }
    else
    { /* restart */
      self->str_offset = self->bytes_offset = 0;
    }
  }
  else
  {
    self->last_bytes_offset = self->bytes_offset;
    self->last_str_offset = self->str_offset;
  }

  while (self->str_offset < pos)
  {
    if (self->bytes_offset >= self->buffer.len)
      return PyErr_Format(PyExc_IndexError, "position is beyond end of string");

    unsigned b = ((unsigned char *)self->buffer.buf)[self->bytes_offset];

    if ((b & 0x80 /* 0b1000_0000 */) == 0)
      self->bytes_offset += 1;
    else if ((b & 0xf8 /* 0b1111_1000 */) == 0xf0 /* 0b1111_0000 */)
      self->bytes_offset += 4;
    else if ((b & 0xf0 /* 0b1111_0000 */) == 0xe0 /* 0b1110_0000 */)
      self->bytes_offset += 3;
    else
    {
      assert((b & 0xe0 /* 0b1110_0000 */) == 0xc0 /* 0b1100_0000 */);
      self->bytes_offset += 2;
    }
    self->str_offset++;
  }
  return PyLong_FromSsize_t(self->bytes_offset);
}

static int
ToUtf8PositionMapper_init(ToUtf8PositionMapper *self, PyObject *args, PyObject *kwargs)
{

  ARG_CONVERT_VARARGS_TO_FASTCALL

  PyObject *utf8 = NULL;
  ARG_PROLOG(1, "utf8");
  ARG_MANDATORY ARG_py_buffer(utf8);
  ARG_EPILOG(-1, "to_utf8_position_mapper.__init__(utf8: bytes)", );

  int res = PyObject_GetBuffer(utf8, &self->buffer, PyBUF_SIMPLE);
  if (res != 0)
    return -1;

  self->str = PyUnicode_DecodeUTF8(self->buffer.buf, self->buffer.len, "strict");
  if (!self->str)
  {
    ToUtf8PositionMapper_finalize(self);
    return -1;
  }

  self->vectorcall = (vectorcallfunc)ToUtf8PositionMapper_call;
  return 0;
}

#if PY_VERSION_HEX < 0x030c0000
#include "structmember.h"
#define Py_T_PYSSIZET T_PYSSIZET
#define Py_T_OBJECT_EX T_OBJECT_EX
#define Py_READONLY READONLY
#endif

static PyMemberDef ToUtf8PositionMapper_memberdef[] = {
  { "__vectorcalloffset__", Py_T_PYSSIZET, offsetof(ToUtf8PositionMapper, vectorcall), Py_READONLY },
  { "str", Py_T_OBJECT_EX, offsetof(ToUtf8PositionMapper, str), Py_READONLY },
  { NULL },
};

static PyType_Slot ToUtf8PositionMapper_slots[] = {
  { Py_tp_finalize, ToUtf8PositionMapper_finalize },
  { Py_tp_init, ToUtf8PositionMapper_init },
  { Py_tp_call, PyVectorcall_Call },
  { Py_tp_members, ToUtf8PositionMapper_memberdef },
  { 0, NULL },
};

/* this object is not publicly documented or exposed so we are light
   on details */
static PyType_Spec ToUtf8PositionMapper_spec = {
  .name = "apsw._unicode.to_utf8_position_mapper",
  .basicsize = sizeof(ToUtf8PositionMapper),
  .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE | Py_TPFLAGS_HAVE_VECTORCALL,
  .slots = ToUtf8PositionMapper_slots,
};

/* Given a UTF-8 offset provide the corresponding str offset */

typedef struct
{
  PyObject_HEAD
  vectorcallfunc vectorcall;
  Py_ssize_t bytes_len;
  Py_ssize_t str_offset;
  Py_ssize_t bytes_offset;
  /* we often go backwards as spans are iterated so remember previous */
  Py_ssize_t last_str_offset;
  Py_ssize_t last_bytes_offset;
  const char *bytes;
  PyObject *bytes_object;
} FromUtf8PositionMapper;

static void
FromUtf8PositionMapper_finalize(FromUtf8PositionMapper *self)
{
  /* this is intentionally implemented to be safe to call multiple times */
  Py_CLEAR(self->bytes_object);
  self->bytes = NULL;
}

static PyObject *
FromUtf8PositionMapper_call(FromUtf8PositionMapper *self, PyObject *const *fast_args, Py_ssize_t fast_nargs,
                            PyObject *fast_kwnames)
{
  Py_ssize_t pos;
  ARG_PROLOG(1, "pos");
  ARG_MANDATORY ARG_Py_ssize_t(pos);
  ARG_EPILOG(NULL, "from_utf8_position_mapper.__call__(pos: int)", );

  if (pos < 0 || pos > self->bytes_len)
    return PyErr_Format((pos < 0) ? PyExc_ValueError : PyExc_IndexError, "position needs to be zero to length of utf8");

  /* Verify it is valid offset */
  if (pos != self->bytes_len)
  {
    unsigned b = self->bytes[pos];

    if ((b & 0x80 /* 0b1000_0000 */) == 0 || (b & 0xf8 /* 0b1111_1000 */) == 0xf0 /* 0b1111_0000 */
        || (b & 0xf0 /* 0b1111_0000 */) == 0xe0                                   /* 0b1110_0000 */
        || (b & 0xe0 /* 0b1110_0000 */) == 0xc0 /* 0b1100_0000 */)
      ; /* all good */
    else
      return PyErr_Format(PyExc_ValueError, "position %zd is an invalid offset in the utf8", pos);
  }

  if (pos < self->bytes_offset)
  {
    /* went backwards */
    if (pos >= self->last_bytes_offset)
    {
      self->str_offset = self->last_str_offset;
      self->bytes_offset = self->last_bytes_offset;
    }
    else
    { /* restart */
      self->str_offset = self->bytes_offset = 0;
    }
  }
  else
  {
    self->last_bytes_offset = self->bytes_offset;
    self->last_str_offset = self->str_offset;
  }

  while (self->bytes_offset < pos)
  {
    /* ::TODO:: is this test reachable? */
    if (self->bytes_offset >= self->bytes_len)
      break;

    unsigned b = self->bytes[self->bytes_offset];

    if ((b & 0x80 /* 0b1000_0000 */) == 0)
      self->bytes_offset += 1;
    else if ((b & 0xf8 /* 0b1111_1000 */) == 0xf0 /* 0b1111_0000 */)
      self->bytes_offset += 4;
    else if ((b & 0xf0 /* 0b1111_0000 */) == 0xe0 /* 0b1110_0000 */)
      self->bytes_offset += 3;
    else
    {
      assert((b & 0xe0 /* 0b1110_0000 */) == 0xc0 /* 0b1100_0000 */);
      self->bytes_offset += 2;
    }
    self->str_offset++;
  }
  return PyLong_FromSsize_t(self->str_offset);
}

static int
FromUtf8PositionMapper_init(FromUtf8PositionMapper *self, PyObject *args, PyObject *kwargs)
{

  ARG_CONVERT_VARARGS_TO_FASTCALL

  PyObject *string = NULL;
  ARG_PROLOG(1, "string");
  ARG_MANDATORY ARG_PyUnicode(string);
  ARG_EPILOG(-1, "from_utf8_position_mapper.__init__(string: str)", );

  self->bytes_object = PyUnicode_AsUTF8String(string);
  if (!self->bytes_object)
    return -1;

  self->bytes_len = PyBytes_GET_SIZE(self->bytes_object);
  self->bytes = PyBytes_AS_STRING(self->bytes_object);

  self->vectorcall = (vectorcallfunc)FromUtf8PositionMapper_call;
  return 0;
}

static PyMemberDef FromUtf8PositionMapper_memberdef[] = {
  { "__vectorcalloffset__", Py_T_PYSSIZET, offsetof(FromUtf8PositionMapper, vectorcall), Py_READONLY },
  { "bytes", Py_T_OBJECT_EX, offsetof(FromUtf8PositionMapper, bytes_object), Py_READONLY },
  { NULL },
};

static PyType_Slot FromUtf8PositionMapper_slots[] = {
  { Py_tp_finalize, FromUtf8PositionMapper_finalize },
  { Py_tp_init, FromUtf8PositionMapper_init },
  { Py_tp_call, PyVectorcall_Call },
  { Py_tp_members, FromUtf8PositionMapper_memberdef },
  { 0, NULL },
};

/* this object is not publicly documented or exposed so we are light
   on details */
static PyType_Spec FromUtf8PositionMapper_spec = {
  .name = "apsw._unicode.from_utf8_position_mapper",
  .basicsize = sizeof(FromUtf8PositionMapper),
  .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE | Py_TPFLAGS_HAVE_VECTORCALL,
  .slots = FromUtf8PositionMapper_slots,
};

/* maps between str offsets and str offsets

  Used by HTML and JSON tokenizers as they need to map the resulting
  text offsets back to the original source offsets
*/

struct MapperEntry
{
  Py_ssize_t location;
  Py_ssize_t offset;
};

typedef struct
{
  PyObject_HEAD
  vectorcallfunc vectorcall;
  PyObject *accumulate;
  PyObject *text;
  struct MapperEntry *offset_map;
  Py_ssize_t num_offsets;
  Py_ssize_t last_location;
  Py_ssize_t last_offset;
  /* size of accumulated segments so far */
  Py_ssize_t length;
  /* used when we materialize the text */
  Py_UCS4 max_char_value;
  /* track if last addition was a separator because we don't add
     multiple separators in a row */
  int last_is_separator;
} OffsetMapper;

static PyObject *
OffsetMapper_add(OffsetMapper *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  if (!self->accumulate)
    return PyErr_Format(PyExc_Exception, "Text has been materialized - you cannot add more segments");

  PyObject *text;
  Py_ssize_t source_start, source_end;

#define OffsetMapper_add_KWNAMES "text", "source_start", "source_end"
  ARG_PROLOG(3, OffsetMapper_add_KWNAMES);
  ARG_MANDATORY ARG_PyUnicode(text);
  ARG_MANDATORY ARG_Py_ssize_t(source_start);
  ARG_MANDATORY ARG_Py_ssize_t(source_end);
  ARG_EPILOG(NULL, "OffsetMapper.add()text: str, source_start: int, source_end: int", );

  /* reject going backwards */
  if (source_end < source_start)
    return PyErr_Format(PyExc_ValueError, "Source end %zd is before source start %zd", source_end, source_start);
  if (source_start < self->offset_map[self->num_offsets - 1].offset)
    return PyErr_Format(PyExc_ValueError, "Source start %zd is before previous end %zd", source_start,
                        self->offset_map[self->num_offsets - 1].offset);

  struct MapperEntry *oldmap = self->offset_map;
  PyMem_Resize(self->offset_map, struct MapperEntry, self->num_offsets + 2);
  if (!self->offset_map)
  {
    self->offset_map = oldmap;
    return NULL;
  }

  if (0 != PyList_Append(self->accumulate, text))
    return NULL;

  self->offset_map[self->num_offsets].location = self->length;
  self->offset_map[self->num_offsets].offset = source_start;
  self->length += PyUnicode_GET_LENGTH(text);
  self->max_char_value = Py_MAX(self->max_char_value, PyUnicode_MAX_CHAR_VALUE(text));
  self->offset_map[self->num_offsets + 1].location = self->length;
  self->offset_map[self->num_offsets + 1].offset = source_end;

  self->num_offsets += 2;
  self->last_is_separator = 0;

  Py_RETURN_NONE;
}

static PyObject *
OffsetMapper_separate(OffsetMapper *self, PyTypeObject *defining_class, PyObject *const *args, Py_ssize_t nargs,
                      PyObject *kwnames)
{
  if (nargs || kwnames)
    return PyErr_Format(PyExc_TypeError, "OffsetMapper.separate takes no arguments");
  if (!self->accumulate)
    return PyErr_Format(PyExc_Exception, "Text has been materialized - you cannot add more segments");
  if (self->last_is_separator)
    Py_RETURN_NONE;
  module_state *state = PyType_GetModuleState(defining_class);

  if (0 != PyList_Append(self->accumulate, state->separator))
    return NULL;
  self->length += PyUnicode_GET_LENGTH(state->separator);
  self->last_is_separator = 1;
  Py_RETURN_NONE;
}

static PyObject *
OffsetMapper_text(OffsetMapper *self, void *Py_UNUSED(closure))
{
  if (self->text)
    return Py_NewRef(self->text);

  self->text = PyUnicode_New(self->length, self->max_char_value);
  if (!self->text)
    return NULL;

  Py_ssize_t offset = 0;
  for (Py_ssize_t i = 0; i < PyList_GET_SIZE(self->accumulate); i++)
  {
    PyObject *segment = PyList_GET_ITEM(self->accumulate, i);
    PyUnicode_CopyCharacters(self->text, offset, segment, 0, PyUnicode_GET_LENGTH(segment));
    offset += PyUnicode_GET_LENGTH(segment);
  }

  Py_CLEAR(self->accumulate);
  return Py_NewRef(self->text);
}

static PyObject *
OffsetMapper_call(OffsetMapper *self, PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  if (!self->text)
    return PyErr_Format(PyExc_Exception, "Text has not been materialized - you cannot get offsets until getting text");

  Py_ssize_t location;

  ARG_PROLOG(1, "location");
  ARG_MANDATORY ARG_Py_ssize_t(location);
  ARG_EPILOG(NULL, "OffsetMapper.__call__(offset: int", );

  if (location < self->last_location)
  {
    self->last_location = self->last_offset = 0;
  }
  for (Py_ssize_t i = self->last_offset; i < self->num_offsets - 1; i++)
  {
    if (location >= self->offset_map[i].location && location < self->offset_map[i + 1].location)
    {
      self->last_location = self->offset_map[i].location;
      self->last_offset = i;
      return PyLong_FromSsize_t(self->offset_map[i].offset + (location - self->last_location));
    }
  }
  if (location == self->offset_map[self->num_offsets - 1].location)
    return PyLong_FromSsize_t(self->offset_map[self->num_offsets - 1].offset);

  return PyErr_Format(PyExc_IndexError, "location is out of range");
}

static void
OffsetMapper_finalize(OffsetMapper *self)
{
  Py_CLEAR(self->accumulate);
  Py_CLEAR(self->text);
  PyMem_Free(self->offset_map);
  self->offset_map = 0;
}

static int
OffsetMapper_init(OffsetMapper *self, PyObject *args, PyObject *kwargs)
{
  if (PyTuple_GET_SIZE(args) || kwargs)
  {
    PyErr_Format(PyExc_TypeError, "OffsetMapper.__init__ takes no arguments");
    return -1;
  }
  self->vectorcall = (vectorcallfunc)OffsetMapper_call;
  /* cleanup in case init is called multiple times */
  OffsetMapper_finalize(self);
  self->accumulate = PyList_New(0);
  self->offset_map = PyMem_Calloc(1, sizeof(struct MapperEntry));
  self->num_offsets = 1;
  self->last_location = 0;
  self->last_offset = 0;
  self->max_char_value = 0;
  self->last_is_separator = 0;
  if (!self->accumulate || !self->offset_map)
  {
    OffsetMapper_finalize(self);
    return -1;
  }
  return 0;
}

static PyMemberDef OffsetMapper_memberdef[] = {
  { "__vectorcalloffset__", Py_T_PYSSIZET, offsetof(OffsetMapper, vectorcall), Py_READONLY },
  { NULL },
};

static PyMethodDef OffsetMapper_methods[] = {
  { "add", (PyCFunction)OffsetMapper_add, METH_FASTCALL | METH_KEYWORDS },
  { "separate", (PyCFunction)OffsetMapper_separate, METH_METHOD | METH_FASTCALL | METH_KEYWORDS },
  { NULL },
};

static PyGetSetDef OffsetMapper_getset[] = {
  { "text", (getter)OffsetMapper_text, NULL, "resulting text" },
  { NULL },
};

static PyType_Slot OffsetMapper_slots[] = {
  { Py_tp_finalize, OffsetMapper_finalize },
  { Py_tp_init, OffsetMapper_init },
  { Py_tp_call, PyVectorcall_Call },
  { Py_tp_members, OffsetMapper_memberdef },
  { Py_tp_methods, OffsetMapper_methods },
  { Py_tp_getset, OffsetMapper_getset },
  { 0, NULL },
};

/* this object is not publicly documented or exposed so we are light
   on details */
static PyType_Spec OffsetMapper_spec = {
  .name = "apsw._unicode.OffsetMapper",
  .basicsize = sizeof(OffsetMapper),
  .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE | Py_TPFLAGS_HAVE_VECTORCALL,
  .slots = OffsetMapper_slots,
};

static int
unicode_traverse(PyObject *module, visitproc visit, void *arg)
{
  module_state *state = PyModule_GetState(module);
  Py_VISIT(state->separator);
  return 0;
}

static int
unicode_clear(PyObject *module)
{
  module_state *state = PyModule_GetState(module);
  Py_CLEAR(state->separator);
  return 0;
}

static int
unicode_exec(PyObject *module)
{
  int rc;
  PyObject *hard_breaks = NULL, *tmp = NULL, *upm = NULL;

  module_state *state = PyModule_GetState(module);
  state->separator = PyUnicode_FromStringAndSize("\n", 1);

  if (!state->separator)
    goto error;

  upm = PyType_FromModuleAndSpec(module, &ToUtf8PositionMapper_spec, NULL);
  if (!upm)
    goto error;
  rc = PyModule_AddObject(module, "to_utf8_position_mapper", upm);
  if (rc != 0)
    goto error;
  upm = NULL; /* ref was stolen in addobject */

  upm = PyType_FromModuleAndSpec(module, &FromUtf8PositionMapper_spec, NULL);
  if (!upm)
    goto error;
  rc = PyModule_AddObject(module, "from_utf8_position_mapper", upm);
  if (rc != 0)
    goto error;
  upm = NULL; /* ref was stolen in addobject */

  upm = PyType_FromModuleAndSpec(module, &OffsetMapper_spec, NULL);
  if (!upm)
    goto error;
  rc = PyModule_AddObject(module, "OffsetMapper", upm);
  if (rc != 0)
    goto error;
  upm = NULL; /* ref was stolen in addobject */

  rc = PyModule_AddStringConstant(module, "unicode_version", unicode_version);
  if (rc != 0)
    goto error;
  hard_breaks = PyFrozenSet_New(NULL);
  if (!hard_breaks)
    goto error;
#undef X
#define X(v)                                                                                                           \
  {                                                                                                                    \
    tmp = PyLong_FromLong(v);                                                                                          \
    if (!tmp)                                                                                                          \
      goto error;                                                                                                      \
    rc = PySet_Add(hard_breaks, tmp);                                                                                  \
    if (rc != 0)                                                                                                       \
      goto error;                                                                                                      \
    Py_CLEAR(tmp);                                                                                                     \
  }

  ALL_LINE_HARD_BREAKS

  rc = PyModule_AddObject(module, "hard_breaks", hard_breaks);
  if (rc != 0)
    goto error;
  hard_breaks = NULL; /* ref was stolen in addobject */

  return 0;
error:
  Py_XDECREF(tmp);
  Py_XDECREF(hard_breaks);
  Py_XDECREF(upm);
  unicode_clear(module);
  return -1;
}

static PyMethodDef methods[] = {
  { "category_name", (PyCFunction)category_name, METH_FASTCALL | METH_KEYWORDS,
    "Returns category names codepoint corresponds to" },
  { "category_category", (PyCFunction)get_category_category, METH_FASTCALL | METH_KEYWORDS,
    "Returns Unicode category" },
  { "sentence_next_break", (PyCFunction)sentence_next_break, METH_FASTCALL | METH_KEYWORDS,
    "Returns next sentence break offset" },
  { "grapheme_next_break", (PyCFunction)grapheme_next_break_api, METH_FASTCALL | METH_KEYWORDS,
    "Returns next grapheme break offset" },
  { "word_next_break", (PyCFunction)word_next_break, METH_FASTCALL | METH_KEYWORDS, "Returns next word break offset" },
  { "line_next_break", (PyCFunction)line_next_break, METH_FASTCALL | METH_KEYWORDS, "Returns next line break offset" },
  { "line_next_hard_break", (PyCFunction)line_next_hard_break, METH_FASTCALL | METH_KEYWORDS,
    "Returns next line hard break offset" },
  { "has_category", (PyCFunction)has_category, METH_FASTCALL | METH_KEYWORDS,
    "Returns True if any codepoints are covered by the mask" },
  { "casefold", (PyCFunction)casefold, METH_FASTCALL | METH_KEYWORDS, "Does case folding for comparison" },
  { "strip", (PyCFunction)strip, METH_FASTCALL | METH_KEYWORDS,
    "Returns new string omitting accents, punctuation etc" },
  { "grapheme_length", (PyCFunction)grapheme_length, METH_FASTCALL | METH_KEYWORDS,
    "Length of string in grapheme clusters" },
  { "grapheme_substr", (PyCFunction)grapheme_substr, METH_FASTCALL | METH_KEYWORDS, "Substring in grapheme clusters" },
  { "text_width", (PyCFunction)text_width, METH_FASTCALL | METH_KEYWORDS, "Columns width of text" },
  { "grapheme_find", (PyCFunction)grapheme_find, METH_FASTCALL | METH_KEYWORDS, "Find substring in text" },
  { "version_added", (PyCFunction)version_added, METH_FASTCALL | METH_KEYWORDS,
    "Version of unicode a codepoint was added" },
  { "codepoint_name", (PyCFunction)codepoint_name, METH_FASTCALL | METH_KEYWORDS, "codepoint name" },
  { NULL, NULL, 0, NULL },
};

static PyModuleDef_Slot module_slots[] = {
  { Py_mod_exec, unicode_exec },
#if PY_VERSION_HEX >= 0x030c0000
  { Py_mod_multiple_interpreters, Py_MOD_PER_INTERPRETER_GIL_SUPPORTED },
#endif
  { 0, NULL },
};

static PyModuleDef module_def = {
  .m_base = PyModuleDef_HEAD_INIT,
  .m_name = "apsw._unicode",
  .m_doc = "C implementation of Unicode methods and lookups",
  .m_methods = methods,
  .m_size = sizeof(module_state),
  .m_slots = module_slots,
  .m_traverse = unicode_traverse,
  .m_clear = unicode_clear,
};

PyObject *
PyInit__unicode(void)
{
  return PyModuleDef_Init(&module_def);
}

#if defined(APSW_TESTFIXTURES) && PY_VERSION_HEX >= 0x030c0000
/* we can't include pyutil.c because then there are warnings about all
the unused static functions.  We also aren't going to bother with pre
Python 3.12 exception stuff */

#define PY_ERR_FETCH(name) PyObject *name = PyErr_GetRaisedException()
#define PY_ERR_CLEAR(name) Py_CLEAR(name)
#define PY_ERR_NOT_NULL(name) (name)
#define PY_ERR_RESTORE(name) PyErr_SetRaisedException(name)
#define PY_ERR_NORMALIZE(name)                                                                                         \
  {                                                                                                                    \
  }

static PyObject *
OBJ(PyObject *v)
{
  return v ? v : Py_None;
}

#include "faultinject.c"
#endif
