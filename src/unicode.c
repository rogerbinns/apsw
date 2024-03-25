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

#define EOT 0
#include "_unicodedb.c"

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

it_has_accepted

True if at least one character has been accepted.

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
    .lookahead = (offset == text_end) ? EOT : cat_func(PyUnicode_READ(text_kind, text_data, offset)),                  \
  }

#define it_advance()                                                                                                   \
  do                                                                                                                   \
  {                                                                                                                    \
    assert(it.pos < text_end);                                                                                         \
    it.curchar = it.lookahead;                                                                                         \
    it.pos++;                                                                                                          \
    it.lookahead = (it.pos == text_end) ? EOT : cat_func(PyUnicode_READ(text_kind, text_data, it.pos));                \
  } while (0)

/* note it.pos currently points to lookahead and subtract one for curchar. */
#define it_lookahead_char() ((it.pos == text_end) ? EOT : PyUnicode_READ(text_kind, text_data, it.pos))
#define it_curchar() (PyUnicode_READ(text_kind, text_data, it.pos - 1))

#define it_lookahead_category(value) ((category_category(it_lookahead_char()) & (value)) == (value))

#define it_curchar_category(value) ((category_category(it_curchar()) & (value)) == (value))

#define it_lookahead_ischar(c) ((c) == it_lookahead_char())

#define it_curchar_ischar(c) ((c) == it_curchar())

/* the first advance sets pos == offset + 1 but nothing is accepted
   yet, hence +1.  this also wrong if the first char we are processing is
   followed by combiners, so technically nothing has actually been
   accepted yet  */
#define it_has_accepted() (it.pos > offset + 1)

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

  /* GB1 implicit */

  /* GB2 */
  while (it.pos < text_end)
  {
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
      if (it_has_accepted())
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

  /* From spec */
#define AHLetter (WC_ALetter | WC_Hebrew_Letter)
#define MidNumLetQ (WC_MidNumLet | WC_Single_Quote)

  /* WB1 implicit */

  /* WB2 */
  while (it.pos < text_end)
  {
  loop_top:
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
      if (it_has_accepted())
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
      Py_UCS4 saved_char = it.curchar;
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

    if (it.curchar == LB_BK)
      break;

    if (it.curchar == LB_CR && it.lookahead == LB_LF)
    {
      it_advance();
      break;
    }

    if (it.curchar == LB_CR || it.curchar == LB_LF || it.curchar == LB_NL)
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

  /*
    Important note:  We have to use equality checking NOT bitwise and
    because there were too many categories to use a bitset.
  */

  /* LB2 implicit */

  /* LB3 */
  while (it.pos < text_end)
  {
    it_advance();
  top_of_loop:

    /* LB4 */
    if (it.curchar == LB_BK)
      break;

    /* LB5 */
    if (it.curchar == LB_CR && it.lookahead == LB_LF)
    {
      it_advance();
      break;
    }

    if (it.curchar == LB_CR || it.curchar == LB_LF || it.curchar == LB_NL)
      break;

    /* LB6 */
    if (it.lookahead == LB_BK || it.lookahead == LB_CR || it.lookahead == LB_LF || it.lookahead == LB_NL)
      continue;

    /* LB17 - LB7 lookahead==SP, so we have to be evaluated first */
    if (it.curchar == LB_B2 && (it.lookahead == LB_SP || it.lookahead == LB_B2))
    {
      it_begin();
      while (it.lookahead == LB_SP)
        it_advance();
      if (it.lookahead == LB_B2)
      {
        it_advance();
        it_commit();
        continue;
      }
      it_rollback();
    }

    /* LB7 */
    if (it.lookahead == LB_SP || it.lookahead == LB_ZW)
      continue;

    /* LB8 */
    if (it.curchar == LB_ZW)
    {
      while (it.lookahead == LB_SP)
        it_advance();
      break;
    }

    /* LB8a */
    if (it.curchar == LB_ZWJ)
      continue;

    /* LB9 */
    if ((it.curchar != LB_BK && it.curchar != LB_CR && it.curchar != LB_LF && it.curchar != LB_NL && it.curchar != LB_SP
         && it.curchar != LB_ZW)
        && (it.lookahead == LB_CM || it.lookahead == LB_ZWJ))
    {
      unsigned savechar = it.curchar;
      while (it.lookahead == LB_CM || it.lookahead == LB_ZWJ)
        it_advance();
      /* ::TODO:: this won't help with it_curchar* macros because they reread the character
          and will have the wrong position. maybe it won't matter? */
      it.curchar = savechar;
      /* We already advanced so re-evaluate above rules again */
      goto top_of_loop;
    }

    /* LB10 */
    if (it.curchar == LB_CM || it.curchar == LB_ZWJ)
      it.curchar = LB_AL; /* See ::TODO:: above, the ~3 rules mentioning AL could just be applied here */

    /* LB11 */
    if (it.curchar == LB_WJ)
      continue;
    if (it.lookahead == LB_WJ)
    {
      it_advance();
      continue;
    }

    /* LB12 */
    if (it.curchar == LB_GL)
      continue;

    /* LB12a */
    if (it.lookahead == LB_GL)
    {
      if (it.curchar == LB_SP || it.curchar == LB_BA || it.curchar == LB_HY)
        break;
      it_advance();
      continue;
    }

    /* LB13 */
    if (it.lookahead == LB_CL || it.lookahead == LB_CP || it.lookahead == LB_EX || it.lookahead == LB_IS
        || it.lookahead == LB_SY)
      continue;

    /* LB14 */
    if (it.curchar == LB_OP)
    {
      while (it.lookahead == LB_SP)
        it_advance();
      continue;
    }

    /* LB15a */
    if (it.lookahead == LB_QU && it_lookahead_category(Category_Punctuation_InitialQuote)
        && (it.curchar == LB_BK || it.curchar == LB_CR || it.curchar == LB_NL || it.curchar == LB_OP
            || it.curchar == LB_QU || it.curchar == LB_GL || it.curchar == LB_SP || it.curchar == LB_ZW))
    {
      it_begin();
      it_advance();
      assert(it.curchar == LB_QU);
      while (it.lookahead == LB_SP)
        it_advance();
      if (it.lookahead)
      {
        it_commit();
        continue;
      }
      it_rollback();
    }

    /* LB15b */
    if (it.lookahead == LB_QU && it_lookahead_category(Category_Punctuation_FinalQuote))
    {
      it_begin();
      it_advance();
      if (it.lookahead == LB_SP || it.lookahead == LB_GL || it.lookahead == LB_WJ || it.lookahead == LB_CL
          || it.lookahead == LB_QU || it.lookahead == LB_CP || it.lookahead == LB_EX || it.lookahead == LB_IS
          || it.lookahead == LB_SY || it.lookahead == LB_BK || it.lookahead == LB_CR || it.lookahead == LB_LF
          || it.lookahead == LB_NL || it.lookahead == LB_ZW || it.lookahead == EOT)
      {
        it_commit();
        continue;
      }
      it_rollback();
    }

    /* LB16 */
    if ((it.curchar == LB_CL || it.curchar == LB_CP) && (it.lookahead == LB_SP || it.lookahead == LB_NS))
    {
      it_begin();
      while (it.lookahead == LB_SP)
        it_advance();
      if (it.lookahead == LB_NS)
      {
        it_advance();
        it_commit();
        continue;
      }
      it_rollback();
    }

    /* LB17 again */
    if (it.curchar == LB_B2 && (it.lookahead == LB_SP || it.lookahead == LB_B2))
    {
      it_begin();
      while (it.lookahead == LB_SP)
        it_advance();
      if (it.lookahead == LB_B2)
      {
        it_advance();
        it_commit();
        continue;
      }
      it_rollback();
    }

    /* LB18 */
    if (it.curchar == LB_SP)
      break;

    /* LB19 */
    if (it.curchar == LB_QU)
      continue;
    if (it.lookahead == LB_QU)
    {
      it_advance();
      continue;
    }

    /* LB20 */
    if (it.curchar == LB_CB && it_has_accepted())
    {
      it.pos--;
      break;
    }
    if (it.lookahead == LB_CB)
      break;

    /* LB21 */
    if (it.lookahead == LB_BA || it.lookahead == LB_HY || it.lookahead == LB_NS)
    {
      it_advance();
      continue;
    }
    if (it.curchar == LB_BB)
      continue;

    /* LB21a */
    if (it.curchar == LB_HL && (it.lookahead == LB_HY || it.lookahead == LB_BA))
    {
      it_advance();
      continue;
    }

    /* LB21b */
    if (it.curchar == LB_SY && it.lookahead == LB_HL)
      continue;

    /* LB22 */
    if (it.lookahead == LB_IN)
      continue;

    /* LB23 */
    if ((it.curchar == LB_AL || it.curchar == LB_HL) && it.lookahead == LB_NU)
      continue;
    if (it.curchar == LB_NU && (it.lookahead == LB_AL || it.lookahead == LB_HL))
      continue;

    /* LB23a */
    if (it.curchar == LB_PR && (it.lookahead == LB_ID || it.lookahead == LB_EB || it.lookahead == LB_EM))
      continue;
    if ((it.curchar == LB_ID || it.curchar == LB_EB || it.curchar == LB_EM) && it.lookahead == LB_PO)
      continue;

    /* LB24 */
    if ((it.curchar == LB_PR || it.curchar == LB_PO) && (it.lookahead == LB_AL || it.lookahead == LB_HL))
      continue;
    if ((it.curchar == LB_AL || it.curchar == LB_HL) && (it.lookahead == LB_PR || it.lookahead == LB_PO))
      continue;

#define PAIR(x, y) (it.curchar == LB_##x && it.lookahead == LB_##y)

    /* LB25 */
    if (PAIR(CL, PO) || PAIR(CP, PO) || PAIR(CL, PR) || PAIR(CP, PR) || PAIR(NU, PO) || PAIR(NU, PR) || PAIR(PO, OP)
        || PAIR(PO, NU) || PAIR(PR, OP) || PAIR(PR, NU) || PAIR(HY, NU) || PAIR(IS, NU) || PAIR(NU, NU) || PAIR(SY, NU))
      continue;

#undef PAIR

    /* LB26 */
    if (it.curchar == LB_JL
        && (it.lookahead == LB_JL || it.lookahead == LB_JV || it.lookahead == LB_H2 || it.lookahead == LB_H3))
      continue;
    if ((it.curchar == LB_JV || it.curchar == LB_H2) && (it.lookahead == LB_JV || it.lookahead == LB_JT))
      continue;
    if ((it.curchar == LB_JT || it.curchar == LB_H3) && it.lookahead == LB_JT)
      continue;

    /* LB27 */
    if ((it.curchar == LB_JL || it.curchar == LB_JV || it.curchar == LB_JT || it.curchar == LB_H2
         || it.curchar == LB_H3)
        && it.lookahead == LB_PO)
      continue;
    if (it.curchar == LB_PR
        && (it.lookahead == LB_JL || it.lookahead == LB_JV || it.lookahead == LB_JT || it.lookahead == LB_H2
            || it.lookahead == LB_H3))
      continue;

    /* LB28 */
    if ((it.curchar == LB_AL || it.curchar == LB_HL) && (it.lookahead == LB_AL || it.lookahead == LB_HL))
      continue;

#define DOTCIRCLE 0x25CC /* ◌ */

    /* LB28A */
    if (it.curchar == LB_AP && (it.lookahead == LB_AK || it_lookahead_ischar(DOTCIRCLE) || it.lookahead == LB_AS))
      continue;
    if ((it.curchar == LB_AK || it_curchar_ischar(DOTCIRCLE) || it.curchar == LB_AS)
        && (it.lookahead == LB_VF || it.lookahead == LB_VI))
      continue;
    if ((it.curchar == LB_AK || it_curchar_ischar(DOTCIRCLE) || it.curchar == LB_AS) && it.lookahead == LB_VI)
    {
      it_begin();
      it_advance();
      assert(it.curchar == LB_VI);
      if (it.lookahead == LB_AK || it_lookahead_ischar(DOTCIRCLE))
      {
        it_commit();
        continue;
      }
      it_rollback();
    }
    if ((it.curchar == LB_AK || it_curchar_ischar(DOTCIRCLE) || it.curchar == LB_AS)
        && (it.lookahead == LB_AK || it_lookahead_ischar(DOTCIRCLE) || it.lookahead == LB_AS))
    {
      it_begin();
      it_advance();
      if (it.lookahead == LB_VF)
      {
        it_commit();
        continue;
      }
      it_rollback();
    }

    /* LB29 */
    if (it.curchar == LB_IS && (it.lookahead == LB_AL || it.lookahead == LB_HL))
      continue;

    /* LB30 */
    if ((it.curchar == LB_AL || it.curchar == LB_HL || it.curchar == LB_NU) && it.lookahead == LB_OP)
    {
      switch (it_lookahead_char())
      {
        /* at the time of writing, there were 95 OP of which 65 were not FWH */
#define X(v) case v:
        ALL_LB30_OP_not_FWH
#undef X
            continue;
      default:
        break;
      }
    }

    if (it.curchar == LB_CP && (it.lookahead == LB_AL || it.lookahead == LB_HL || it.lookahead == LB_NU))
    {
      switch (it_curchar())
      {
        /* at the time of writing, there were 2 CP of which 2 were not FWH */
#define X(v) case v:
        ALL_LB30_CP_not_FWH
#undef X
            continue;
      default:
        break;
      }
    }

    /* LB30a */
    if (it.curchar == LB_RI && it.lookahead == LB_RI)
    {
      it_advance();
      break;
    }

    /* LB30b */
    if (it.curchar == LB_EB && it.lookahead == LB_EM)
      continue;
    if ((it_curchar_category(Category_Extended_Pictographic) || it_curchar_category(Category_Other_NotAssigned))
        && it.lookahead == LB_EM)
      continue;

    /* LB31 */
    //if(it_has_accepted())
    //  it.pos--;
    break;
  }

  return PyLong_FromSsize_t(it.pos);
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
  else if (0 == strcmp(which, "line_break"))
  {
    /* these are a traditional enum, not a bitmask */
    unsigned int val = line_category(codepoint);
#undef X
#define X(v)                                                                                                           \
  case v:                                                                                                              \
    return Py_BuildValue("(s)", #v);
    switch (val)
    {
      ALL_LB_VALUES
    default:
      return Py_BuildValue("(s)", "NOT_DEFINED_LB_VALUE");
    }
  }
  else
  {
    PyErr_Format(PyExc_ValueError,
                 "Unknown which parameter \"%s\" - should be one of grapheme, word, sentence, line_break", which);
    Py_CLEAR(res);
  }

  return res;
error:
  Py_CLEAR(res);
  return NULL;
}

static PyObject *
get_category_category(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs,
                      PyObject *fast_kwnames)
{
  Py_UCS4 codepoint;

  ARG_PROLOG(1, "codepoint");
  ARG_MANDATORY ARG_codepoint(codepoint);
  ARG_EPILOG(NULL, "category_category(codepoint: int)", );

  return PyLong_FromUnsignedLong(category_category(codepoint));
}

static PyObject *
has_category(PyObject *Py_UNUSED(self), PyObject *const *fast_args, Py_ssize_t fast_nargs, PyObject *fast_kwnames)
{
  PyObject *text;
  Py_ssize_t start, end;
  unsigned long mask;

#define has_category_KWARGS "text", "start", "end", "mask"
  ARG_PROLOG(4, has_category_KWARGS);
  ARG_MANDATORY ARG_PyUnicode(text);
  ARG_MANDATORY ARG_PyUnicode_offset(start, text);
  ARG_MANDATORY ARG_PyUnicode_offset(end, text);
  ARG_MANDATORY ARG_unsigned_long(mask);
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
     made.  If not the original string can be returned.

     The second phase then does the folding.

     The only codepoint that could change the max char value is U+00B5
     MICRO SIGN which expands to U+03BC GREEK SMALL LETTER MU which is
     verified in ucdprops2code.py
*/
  int changed = 0;
  int UB5_seen = 0;
  Py_ssize_t expansion = 0;

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
    if (source_char == 0xB5)
      UB5_seen = 1;
    switch (source_char)
    {
      /* generated, present in _unicodedb.c */
      CASEFOLD_EXPANSION
    }
  }

  if (!changed)
    return Py_NewRef(text);

  Py_UCS4 dest_max = Py_MAX(PyUnicode_MAX_CHAR_VALUE(text), UB5_seen ? 65535 : 0);

  PyObject *dest = PyUnicode_New(source_length + expansion, dest_max);
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

static int
unicode_exec(PyObject *module)
{
  int rc;
  PyObject *hard_breaks = NULL, *tmp = NULL;
  rc = PyModule_AddStringConstant(module, "unicode_version", unicode_version);
  if (rc != 0)
    goto error;
  hard_breaks = PyFrozenSet_New(NULL);
#undef X
#define X(v)                                                                                                           \
  {                                                                                                                    \
    tmp = PyLong_FromLong(v);                                                                                          \
    if (!v)                                                                                                            \
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
  { "grapheme_length", (PyCFunction)grapheme_length, METH_FASTCALL | METH_KEYWORDS,
    "Length of string in grapheme clusters" },
  { "grapheme_substr", (PyCFunction)grapheme_substr, METH_FASTCALL | METH_KEYWORDS, "Substring in grapheme clusterss" },
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
  .m_size = 0,
  .m_slots = module_slots,
};

PyObject *
PyInit__unicode(void)
{
  return PyModuleDef_Init(&module_def);
}