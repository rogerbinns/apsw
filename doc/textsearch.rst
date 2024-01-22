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
      does not turn tokens into trigrams, but the entire text including
      all spaces and punctuation.
  * - :func:`PyUnicodeTokenizer`
    - Uses Python's more recent :mod:`Unicode database <unicodedata>`
      to generate tokens
  * - :func:`RegexTokenizer`
    - Use :mod:`regular expressions <re>` to generate tokens
  * - :func:`NGramTokenizer`
    - Generates ngrams from the text, where you can specify the sizes and
      unicode categories.  Useful for doing autocomplete as you type.
  * - :func:`HTMLTokenizer`
    - Wrapper that converts HTML to plan text for a further tokenizer to generate
      tokens
  * - :func:`SimplifyTokenizer`
    - Wrapper that transforms the token stream such as converting case, removing
      diacritics, and Unicode normalization.
  * - :func:`SynonymTokenizer`
    - Wrapper that provides additional tokens for existing ones such as ``first``
      for ``1st``
  * - :func:`StopWordsTokenizer`
    - Wrapper that removes tokens from the token stream that occur too often to be useful, such as
      ``the`` in English text
  * - :func:`TransformTokenizer`
    - Wrapper to transform tokens, such as when stemming.
  * - :func:`NGramTokenTokenizer`
    - Wrapper that Generates ngrams from the token stream, where you can specify the sizes and
      unicode categories.  Useful for doing autocomplete as you type.
  * - :func:`StringTokenizer`
    - A decorator for your own tokenizers so that they operate on strings, performing the
      mapping to UTF8 bytes for you.

.. _byte_offsets:

UTF8 byte offsets
-----------------

The underlying FTS5 apis work on UTF8 and expect to be given the start
and end offset into the UTF8 for each token.  However this information
is never stored, and is only used by auxiliary functions like
`snippet()
<https://www.sqlite.org/fts5.html#the_snippet_function>`__ which
uses the offsets to work out where to put the markers.

The ``end`` is the first byte **after** the token, like Python does
with :class:`range`.  ``end`` minus ``start`` is the token length in
bytes.

If you do not care about the offsets, or they make no sense for your content,
then you can return zero for the ``start`` and ``end``.  You can omit the
offsets in your tokenizer and APSW automatically substitures zero.

.. _fts_recommendations:

Recommendations
===============

Tokenizer sequence
  For general text, use the following as one string, broken out
  into multiple lines here for clarity::

    simplify
      normalize_pre NFKD
      case casefold
      remove_categories 'M* *m Sk'
      normalize_post NFC
    pyunicode
      single_token_categories 'So Lo'``

  :class:`simplify <SimplifyTokenizer>`:

    * :meth:`Case folds <str.casefold>` the tokens
    * Uses compatibility codepoints
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

Third party libraries
=====================

There are several libraries available on PyPI that can be ``pip`` installed.

NLTK (nltk)
-----------

`Natural Language Toolkit <https://www.nltk.org/>`__ has several
useful methods to help with search.  You can use it do do stemming in
many different languages, and `different algorithms
<https://www.nltk.org/api/nltk.stem.html>`__::

  stemmer = apsw.fts.TransformTokenizer(
    nltk.stem.snowball.EnglishStemmer().stem
  )
  connection.register_fts5_tokenizer("english_stemmer", english_stemmer)

You can use `wordnet <https://www.nltk.org/howto/wordnet.html>`__ to get
synonyms::

  from nltk.corpus import wordnet

  def synonyms(word):
    return [syn.name() for syn in wordnet.synsets(word)]

  wrapper = apsw.fts.SynonymTokenizer(synonyms)
  connection.register_fts5_tokenizer("english_synonyms", wrapper)


Snowball Stemmer (snowballstemmer)
----------------------------------

`Snowball <https://snowballstem.org/>`__ is a successor to the Porter
stemming algorithm (`included in FTS5
<https://www.sqlite.org/fts5.html#porter_tokenizer>`__), and
supports many more languages.  It is also included as part of nltk.::

  stemmer = apsw.fts.TransformTokenizer(
    snowballstemmer.stemmer("english").stemWord
  )
  connection.register_fts5_tokenizer("english_stemmer", english_stemmer)

Unidecode (unidecode)
---------------------

The `algorithm <https://interglacial.com/tpj/22/>`__ turns Unicode
text into ascii text that sounds approximately similar::

  transform = apsw.fts.TransformTokenizer(
    unidecode.unidecode
  )

  connection.register_fts5_tokenizer("unidecode", transform)

.. include:: fts.rst

Full Text Search module
=======================

Provided by the :mod:`apsw.fts` module.

.. automodule:: apsw.fts
    :synopsis: Helpers for working with full text search
    :members:
    :undoc-members: