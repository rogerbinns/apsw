Full text search
****************

.. currentmodule:: apsw.fts

APSW provides complete access to SQLite's full text search functionality.
SQLite provides the `FTS5 extension <https://www.sqlite.org/fts5.html>`__
as the implementation.  FTS5 is enabled by default in :ref:`PyPI <pypi>`
installs.

The :mod:`apsw.fts <apsw.fts>` module makes it easy to to customise
and enhance usage of FTS5.  See the :ref:`recommendations
<fts_recommendations>`.


Key Concepts
============

Searching

  SQL is based around the entire contents of a value.  You can test
  for equality, you can do greater or less then, you can build indices
  to improve performance, you can do joins between tables on the
  values, and more.

  But you can't (practically) do that on a subset of a value,
  especially text.  You can't ask which rows/columns/values contain
  certain words, and you can't do searches for content like you can in
  a web browser.  This is the functionality that full text search
  provides.

  FTS5 has `query syntax
  <https://www.sqlite.org/fts5.html#full_text_query_syntax>`__ so you
  can form complex queries including NOT, OR, NEAR, prefixes, phrases
  etc.


Tokens

  Values first need to broken down into discrete units, called tokens.
  In English these would correspond to words, but they don't have to
  be.  Tokens are the unit that full text searches work with, with
  content considered to be a sequence of tokens.  The tokens don't
  have to occur in the content - they are used from your search to
  find content that includes them.

  FTS5 tokenizers are `specified when creating a table
  <https://www.sqlite.org/fts5.html#tokenizers>`__ and `provides an
  API <https://www.sqlite.org/fts5.html#custom_tokenizers>`__ for
  implementing your own.  Use :meth:`apsw.Connection.fts5_tokenizer` to get
  an existing tokenizer, and register your own with
  :meth:`apsw.Connection.register_fts5_tokenizer`.

  :ref:`List of available tokenizers <all_tokenizers>`,

Full Text Index

  FTS5 builds an index where a token can be looked up, and which rows
  and columns containing it are returned including their position in
  that column value.  So if you search for "hello world" and "hello"
  is at position 17 in a particular row/column, and "world" is at
  position 18 you now have a match.   Building the index can be quite
  time consuming, and it can take quite a lot of storage space.  But
  it is fast to use.

  :class:`FTS5Table` encapsulates an index.

Stop words

  Some words can be very frequent in text such as "the" in English,
  which is present in almost all content.  A common optimization is
  to exclude them from the index and queries, to reduce storage
  space and increase performance.  The downside is it becomes
  impossible to search for stop words.

  :class:`StopWordsTokenizer` provides a base for implementation.

Stemming

  It is often useful to use the `stem <https://en.wikipedia.org/wiki/Word_stem>`__
  of a word as a token, so that all words of similar meaning map onto the
  same token.  For example you could stem ``run``, ``ran``, ``runs``, ``running``,
  and ``runners`` to the same token.

  FTS5 includes the `porter stemmer <https://tartarus.org/martin/PorterStemmer/>`__
  which works on English, while the `Snowball stemmer <https://snowballstem.org/>`__
  is more recent, supports more languages, and has a `Python module
  <https://github.com/snowballstem/pystemmer>`__.

  :class:`TransformTokenizer` provides a base for
  implementation.

Ranking

  Once matches are found, you want the most relevant ones first.
  A ranking function is used to assign each match a numerical score, so
  that can value can be used for sorting.  Ranking functions try
  to take into account how rare the tokens are, and other factors
  like if the tokens are in headings, and how many tokens are
  in the content it was found in.

  ::TODO:: add in ranking stuff here once implemented


Unicode

Unicode Codepoints

UTF8

Unicode Categories


Normalization



Tokenizers
==========



* Convert bytes into a sequence of tokens
* colocated
* generator vs wrapper
* taking arguments

.. _all_tokenizers:

All tokenizers
--------------

.. list-table::
  :header-rows: 1
  :widths: auto

  * - Name
    - Purpose
  * - ``unicode61``
    - `SQLite builtin
      <https://www.sqlite.org/fts5.html#unicode61_tokenizer>`__ using
      Unicode categories to generate tokens
  * - ``ascii``
    - `SQLite builtin
      <https://www.sqlite.org/fts5.html#ascii_tokenizer>`__ using
      ASCII to generate tokens
  * - ``porter``
    - `SQLite builtin
      <https://www.sqlite.org/fts5.html#porter_tokenizer>`__ wrapper
      applying the `porter stemming algorithm <https://tartarus.org/martin/PorterStemmer/>`__
      to supplied tokens
  * - ``trigram``
    - `SQLite builtin
      <https://www.sqlite.org/fts5.html#the_trigram_tokenizer>`__ that
      turns the entire text into trigrams (token generator).  Note it
      does not turn tokens into trigrams, but everything.
  * - :class:`PyUnicodeTokenizer`
    - Uses Python's more recent :mod:`Unicode database <unicodedata>`
      to generate tokens
  * - :class:`RegexTokenizer`
    - Use :mod:`regular expressions <re>` to generate tokens
  * - :class:`HTMLTokenizer`
    - Wrapper that converts HTML to plan text for a further tokenizer to generate
      tokens
  * - :class:`SimplifyTokenizer`
    - Wrapper that transforms the token stream such as converting case, removing
      diacritics, and Unicode normalization.
  * - :class:`SynonymTokenizer`
    - Wrapper that provides additional tokens for existing ones such as ``first``
      for ``1st``
  * - :class:`StopWordsTokenizer`
    - Wrapper that removes tokens from the token stream that occur too often to be useful, such as
      ``the`` in English text
  * - :class:`StringTokenizer`
    - A decorator for your own tokenizers so that they operate on strings, performing the
      mapping to UTF8 bytes for you.

.. _byte_offsets:

UTF8 byte offsets
-----------------

* into original utf8 (ie not changed/normalized)
* start is first byte
* end is first byte **after** token like python does with :class:`range`
* end minus start is length of token in bytes
* must be complete utf8, errors if middle of a utf8 byte making up codepoint

.. _fts_recommendations:

Recommendations
===============

Tokenizer sequence
  For general text, use ``simplify case lower normalize NFKD
  remove_categories 'M* *m Sk'`` ``pyunicode single_token_categories
  'So Lo'``

  :class:`simplify <SimplifyTokenizer>`:

    * Lower cases the tokens
    * Uses compatibility codepoints, and removes combining marks and diacritics
    * Removes marks and diacritics

  :class:`pyunicode <PyUnicodeTokenizer>`:

    * Makes emoji (So symbols other) be individually searchable
    * Makes codepoints (Lo letters other) individually searchable
      which is useful if you have some content in languages that do
      not use spaces to separate words (often from Asia).

      Those codepoints can correspond to letters, syllables, or words,
      and will result in a large index if you have a lot of such
      content, while functions like `snippet
      <https://www.sqlite.org/fts5.html#the_snippet_function>`__ won't
      work well. Correctly determining those  words `requires
      additional code and tables
      <https://www.unicode.org/reports/tr29/>`__ not included with
      Python.


Use external content table
  Means can have many FTS tables referencing it, subset of fields,
  different tokenizers, autocomplete table

Have db be only content table and fts indices and attach
  Best for non-trivial amount of content

Normalize the Unicode
  Insert as NFC as adding to content table - too hard to correct later

API
===

.. include:: fts.rst

apsw.fts module
===============

Provided by the :mod:`apsw.fts` module.

.. automodule:: apsw.fts
    :synopsis: Helpers for working with full text search
    :members:
    :undoc-members: