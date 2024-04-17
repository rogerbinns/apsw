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

it_begin()

Saves the current state.

it_rollback()

Restores prior saved state.

it_commit()

Saved state is not needed.

it_has_accepted - variable

True if at least one character has been accepted.

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
    .pos = offset, .curchar = -1,                                                                                      \
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
    if (it.lookahead & (LB_CL | LB_CP | LB_EX | LB_IS | LB_SY))
    {
      /* LB25 matches longer sequence */
      if (!(it.curchar & LB_NU && it.lookahead & (LB_IS | LB_SY)))
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

    /* LB14 - has to be after LB15 because LB15 looks for curchar & LB_OP */
    if (it.curchar & LB_OP)
    {
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

    /* LB19 */
    if (it.curchar & LB_QU)
      continue;
    if (it.lookahead & LB_QU)
    {
      it_advance();
      continue;
    }
#define LB19_APPLIES (it.lookahead & LB_QU)

    /* LB20 */
    if (it.curchar & LB_CB && !LB7_APPLIES)
      break;
    if (it.lookahead & LB_CB)
      break;

    /* LB21a - has to be before LB21 */
    if (it.curchar & LB_HL && it.lookahead & (LB_HY | LB_BA))
    {
      it_advance();
      continue;
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

    /* LB25 */
    /* This was originally implemented as the pairwise don't break
       list.  However that then fails many tests because they do expect a
       break between CL and PO as the first example (without the stuff
       preceding that) and also because CM from LB9/10 isn't handled.  So
       here we implement the full regex equivalent.
    */
  lb25_again:
    if (it.curchar & (LB_PR | LB_PO | LB_OP | LB_HY | LB_NU))
    {
      int is_lb25 = 1;
      Py_ssize_t initial_pos = it.pos;
      do
      {
        it_begin();
        if (it.curchar & (LB_PR | LB_PO))
        {
          it_advance();
          it_absorb(LB_CM, 0);
        }
        if (it.curchar & (LB_OP | LB_HY))
        {
          it_advance();
          it_absorb(LB_CM, 0);
        }
        /* There has to be at least one LB_NU */
        if (0 == (it.curchar & LB_NU))
        {
          is_lb25 = 0;
          break;
        }
        it_absorb(LB_CM, 0);
        it_absorb(LB_NU | LB_SY | LB_IS, LB_CM);

        if (it.lookahead & (LB_CL | LB_CP))
        {
          it_advance();
          it_absorb(LB_CM, 0);
        }
        if (it.lookahead & (LB_PR | LB_PO))
        {
          it_advance();
          it_absorb(LB_CM, 0);
        }
      } while (0);
      if (is_lb25)
      {
        it_commit();
        /* directly adjacent LB25 can't be broken, so repeat if we absorbed chars */
        if (it.pos != initial_pos)
          goto lb25_again;
      }
      else
        it_rollback();
    }

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
      if (!LB7_APPLIES && !LB19_APPLIES)
        break;
    }

    /* LB30b */
    if (it.curchar & LB_EB && it.lookahead & LB_EM)
      continue;
    if ((it.curchar & (LB_Extended_Pictographic | LB_Other_NotAssigned))
            == (LB_Extended_Pictographic | LB_Other_NotAssigned)
        && it.lookahead & LB_EM)
      continue;

    if (LB7_APPLIES || LB19_APPLIES)
      continue;

    /* LB999 */
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
    unsigned long long val = line_category(codepoint);
    ALL_LB_VALUES;
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
    unsigned cat = category_category(codepoint);
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
  { "strip", (PyCFunction)strip, METH_FASTCALL | METH_KEYWORDS,
    "Returns new string omitting accents, punctuation etc" },
  { "grapheme_length", (PyCFunction)grapheme_length, METH_FASTCALL | METH_KEYWORDS,
    "Length of string in grapheme clusters" },
  { "grapheme_substr", (PyCFunction)grapheme_substr, METH_FASTCALL | METH_KEYWORDS, "Substring in grapheme clusters" },
  { "text_width", (PyCFunction)text_width, METH_FASTCALL | METH_KEYWORDS, "Columns width of text" },
  { "grapheme_find", (PyCFunction)grapheme_find, METH_FASTCALL | METH_KEYWORDS, "Find substring in text" },
  { "version_added", (PyCFunction)version_added, METH_FASTCALL | METH_KEYWORDS,
    "Version of unicode a codepoint was added" },
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