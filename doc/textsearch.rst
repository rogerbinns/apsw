Full text search
****************

.. currentmodule:: apsw.fts5

APSW provides complete access to SQLite's `full text search extension
5 <https://www.sqlite.org/fts5.html>`__, and extensive related
functionality.  The :doc:`tour <example-fts>` demonstrates some of
what is available.  Highlights include:

* Access to all the FTS5 C APIs to :meth:`register a tokenizer
  <apsw.Connection.register_fts5_tokenizer>`, :meth:`retrieve a
  tokenizer <apsw.Connection.fts5_tokenizer>`, :meth:`call a tokenizer
  <apsw.FTS5Tokenizer.__call__>`, :meth:`register an auxiliary
  function <apsw.Connection.register_fts5_function>`, and all of the
  :class:`extension API <apsw.FTS5ExtensionApi>`.  This includes the
  locale option added in `SQLite 3.47
  <https://sqlite.org/releaselog/3_47_0.html>`__.
* The :ref:`ftsq <shell-cmd-ftsq>` shell command to do a FTS5 query
* :class:`apsw.fts5.Table` for a Pythonic interface to a FTS5 table,
  including getting the :attr:`~apsw.fts5.Table.structure`
* A :meth:`~apsw.fts5.Table.create` method to create the table that
  handles the SQL quoting, triggers, and all FTS5 options.
* :meth:`~apsw.fts5.Table.upsert` to insert or update content, and
  :meth:`~apsw.fts5.Table.delete` to delete, that both understand
  `external content tables
  <https://www.sqlite.org/fts5.html#external_content_tables>`__
* :meth:`~apsw.fts5.Table.query_suggest` that improves a query by
  correcting spelling, and suggesting more popular search terms
* :meth:`~apsw.fts5.Table.key_tokens` for statistically significant
  content in a row
* :meth:`~apsw.fts5.Table.more_like` to provide statistically similar
  rows
* :func:`Unicode Word tokenizer <apsw.fts5.UnicodeWordsTokenizer>`
  that better determines word boundaries across all codepoints,
  punctuation, and conventions.
* Tokenizers that work with :func:`regular expressions
  <apsw.fts5.RegexTokenizer>`, :func:`HTML <apsw.fts5.HTMLTokenizer>`,
  and :func:`JSON <apsw.fts5.JSONTokenizer>`
* Helpers for writing your own tokenizers including :func:`argument
  parsing <apsw.fts5.parse_tokenizer_args>`, and :func:`handling
  conversion <apsw.fts5.StringTokenizer>` between the UTF8 offsets
  used by FTS5 and :class:`str` offsets used in Python.
* :func:`~apsw.fts5.SimplifyTokenizer` can handle case folding and
  accent removal on behalf of other tokenizers, using the latest
  Unicode standard.
* :mod:`apsw.fts5` module for working with FTS5
* :mod:`apsw.fts5query` module for generating, parsing, and modifying
  FTS5 queries
* :mod:`apsw.fts5aux` module with auxiliary functions and helpers
* :mod:`apsw.unicode` module supporting the latest version of Unicode:

  * Splitting text into user perceived characters (grapheme clusters),
    words, sentences, and line breaks.
  * Methods to work on strings in grapheme cluster units rather than
    Python's individual codepoints
  * Case folding
  * Removing accents, combining marks, and using
    compatibility codepoints
  * Codepoint names, regional indicator, extended pictographic
  * Helpers for outputting text to terminals that understand grapheme
    cluster boundaries, how wide the text will be, and using line
    breaking to choose the best location to split lines

.. contents:: Contents
  :local:

Key Concepts
============

How it works

  A series of tokens typically corresponding to words is produced for
  each text value.  For each token, FTS5 indexes:

  * The rowid the token occurred in
  * The column the token was in
  * Which token number it was

  A query is turned into tokens, and the FTS5 index consulted to find
  the rows and columns those exact same query tokens occur in.
  Phrases can be found by looking for consecutive token numbers.

Ranking

  Once matches are found, you want the most relevant ones first.  A
  ranking function is used to assign each match a numerical score
  typically taking into account how rare the tokens are, and how
  densely they appear in that row.  You can usually weight each column
  so for example matches in a title column count for more.

  You can change the ranking function on a `per query basis
  <https://www.sqlite.org/fts5.html#sorting_by_auxiliary_function_results>`__
  or via :meth:`~apsw.fts5.Table.config_rank` for all queries.

Tokens

  While tokens typically correspond to words, there is no requirement
  that they do so.  **Tokens are not shown to the user.**  Generating
  appropriate tokens for your text is the key to making searches
  effective.  FTS5 has a `tokendata
  <https://www.sqlite.org/fts5.html#the_tokendata_option>`__ to
  store extra information with each token.  You should consider:

    * Extracting meaningful tokens first.  An :ref:`example
      <example_fts_apsw_regexpre>` shows extracting product ids and
      then treating what remains as regular text.
    * Mapping equivalent text to the same token by using the
      techniques described below (stemming, synonyms)
    * Consider alternate approaches.  For example the `unidecode
      algorithm <https://interglacial.com/tpj/22/>`__ turns Unicode
      text into ascii text that sounds approximately similar
    * Processing content to normalize it.  For example unifying
      spelling so ``colour`` and ``color`` become the same token.  You
      can use dictionaries to ensure content is consistent.

Stemming

  Queries only work with exact matches on the tokens.  It is often
  desirable to make related words produce the same token.  `Stemming
  <https://en.wikipedia.org/wiki/Stemming>`__ is doing this such as
  removing singular vs plural so ``dog`` and ``dogs`` become the same
  token, and determining the base of a word so ``likes``, ``liked``,
  ``likely``, and ``liking`` become the same token.
  :ref:`fts_third_party` provide this for various languages.

Synonyms

  Synonyms are words that mean the same thing.  FTS5 calls them
  colocated tokens.  In a search you may want ``first`` to find that
  as well as ``1st``, or ``dog`` to also find ``canine``, ``k9``, and
  ``puppy``.  While you can provide additional tokens when content is
  being tokenized for the index, a better place is when a query is
  being tokenized.  The :func:`~apsw.fts5.SynonymTokenizer` provides
  an implementation.

Stop words

  Search terms are only useful if they narrow down which rows are
  potential matches.  Something occurring in almost every row
  increases the size of the index, and the ranking function has to be
  run on more rows for each search.  See the :ref:`example
  <example_fts_tokens>` for determining how many rows tokens occur in,
  and :func:`~apsw.fts5.StopWordsTokenizer` for removing them.

Locale

  SQlite 3.47 added support for `locale
  <https://www.sqlite.org/fts5.html#the_locale_option>`__ - an
  arbitrary string that can be used to mark text.  It is typically
  used to denote a language and region - for example Portuguese in
  Portugal has some differences than Portuguese in Brazil, and
  American English has differences from British English.  You can use
  the locale for other purposes - for example if your text includes
  code then the locale could be used to mark what programming language
  it is.

Tokenizer order and parameters
==============================

The `tokenize <https://www.sqlite.org/fts5.html#tokenizers>`__ option
specifies how tokenization happens.  The string specifies a list of
items.  You can use :meth:`apsw.fts5.Table.create` to provide the list
and have all quoting done correctly, and use
:attr:`apsw.fts5.Table.structure` to see what an existing table
specifies.

Tokenizers often take parameters, which are provided as a separate
name and value::

  tokenizer_name param1 value1 param2 value2 param3 value3

Some tokenizers work in conjunction with others.  For example the
:func:`~apsw.fts5.HTMLTokenizer` passes on the text, excluding HTML
tags, and the :func:`~apsw.fts5.StopWordsTokenizer` removes tokens
coming back from another tokenizer.  When they see a parameter name
they do not understand, they treat that as the name of the next
tokenizer, and following items as parameters to that tokenizer.

The overall flow is that the text to be tokenized flows from left to
right amongst the named tokenizers.  The resulting token stream then
flows from right to left.

This means it matters what order the tokenizers are given, and you
should ensure the order is what is expected.

Recommendations
===============

Unicode normalization
---------------------

For backwards compatibility Unicode allows multiple different ways of
specifying what will be drawn as the same character.  For example Ç can be

* One codepoint U+00C7 LATIN CAPITAL LETTER C WITH CEDILLA
* Two codepoints U+0043 LATIN CAPITAL LETTER C, and U+0327 COMBINING
  CEDILLA

There are more complex examples and description at `Unicode TR15
<https://unicode.org/reports/tr15/#Canon_Compat_Equivalence>`__, which
describes the solution of normalization.

If you have text from multiple sources it is possible that it is in
multiple normalization forms.  You should use
:func:`unicodedata.normalize` to ensure your text is all in the same
form for indexing, and also ensure query text is in that same form.  If
you do not do this, then searches will be confusing and not match when
it visually looks like they should.

Form ``NFC`` is recommended.  If you use
:func:`~apsw.fts5.SimplifyTokenizer` with ``strip`` enabled then it
won't matter as that removes combing marks and uses compatibility
codepoints.

Tokenizer order
---------------

For general text, use the following:

  simplify casefold true strip true unicodewords

:class:`simplify <SimplifyTokenizer>`:

  * Uses compatibility codepoints
  * Removes marks and diacritics
  * Neutralizes case distinctions

:class:`unicodewords <UnicodeWordsTokenizer>`:

  * Finds words using the Unicode algorithm
  * Makes emoji be individually searchable
  * Makes regional indicators be individually searchable

External content tables
-----------------------

Using `external content tables
<https://www.sqlite.org/fts5.html#external_content_tables>`__ is well
handled by :class:`apsw.fts5.Table`.  The
:meth:`~apsw.fts5.Table.create` method has a parameter to generate
triggers that keep the FTS5 table up to date with the content table.

The major advantage of using external content tables is that you can
have multiple FTS5 tables sharing the same content table.  For example
for the same content table you could have FTS5 tables for different
purposes:

* ngram for doing autocomplete
* case folded, accent stripped, stop words, and synonyms for broad
  searching
* full fidelity index preserving case, accents, stop words, and no
  synonyms for doing exact match searches.

If you do not use an external content table then FTS5 by default makes
one of its own.  The content is used for auxiliary functions such as
highlighting or snippets from matches.

Your external content table can have more columns, triggers, and other
SQLite functionality that the FTS5 internal content table does not
support.  It is also possible to have `no content table
<https://www.sqlite.org/fts5.html#contentless_tables>`__.

Ranking functions
-----------------

You can fine tune how matches are scored by applying column weights to
existing ranking functions, or by writing your own ranking functions.
See :mod:`apsw.fts5aux` for some examples.

Facets
------

It is often desirable to group results together.  For example if
searching media then grouping by books, music, and movies.  Searching
a book could group by chapter.  Dated content could be grouped by
items from the last month, the last year, and older than that.  This
is known as `faceting
<https://en.wikipedia.org/wiki/Faceted_search>`__.

This is easy to do in SQLite, and an example of when you would use
`unindexed columns
<https://www.sqlite.org/fts5.html#the_unindexed_column_option>`__.
You can use ``GROUP BY`` to group by a facet and ``LIMIT`` to limit
how many results are available in each.  In our media example where an
unindexed column named ``media`` containing values like ``book``,
``music``, and ``movie`` exists you could do:

.. code-block:: sql

     SELECT title, release_date, media AS facet
        FROM search(?)
        GROUP BY facet
        ORDER BY rank
        LIMIT 5;

If you were using date facets, then you can write an auxiliary
function that returns the facet (eg ``0`` for last month, ``1`` for
last year, and ``2`` for older than that).

.. code-block:: sql

     SELECT title, date_facet(search, release_date) AS facet
        FROM search(?)
        GROUP BY facet
        ORDER BY rank
        LIMIT 5;

Multiple GROUP BY work, so you could facet by media type and date.

.. code-block:: sql

     SELECT title, media AS media_facet,
                   date_facet(search, release_date) AS date_facet
        FROM search(?)
        GROUP BY media_facet, date_facet
        ORDER BY rank
        LIMIT 5;

You do not need to store the facet information in the FTS5 table - it
can be in an external content or any other table, using JOIN on the
rowid of the FTS5 table.

Performance
-----------

Search queries are processed in two logical steps:

  * Find all the rows matching the relevant query tokens
  * Run the ranking function on each row to sort the best matches
    first

FTS5 performs very well.  If you need to improve performance then
closely analyse the find all rows step.  The fewer rows query tokens
match the fewer ranking function calls happen, and less overall work
has to be done.

A typical cause of too many matching rows is having too few different
tokens.  If tokens are case folded, accent stripped, and stemmed then
there may not be that many different tokens.

Initial indexing of your content will take a while as it involves a
lot of text processing.  Profiling will show bottlenecks.

Outgrowing FTS5
---------------

FTS5 depends on exact matches between query tokens and content indexed
tokens.  This constrains search queries to exact matching after
optional steps like stemming and similar processing.

If you have a large amount of text and want to do similarity searching
then you will need to use a solution outside of FTS5.

The approach used is to convert words and sentences into a fixed
length list of floating point values - a vector.   To find matches,
the closest vectors have to be found to the query which approximately
means comparing the query vector to **all** of the content vectors
finding the `smallest overall difference
<https://en.wikipedia.org/wiki/Euclidean_distance#Two_dimensions>`__.
This is highly parallel with implementations using hardware/GPU
functionality.

Producing the vectors requires access to a multi-gigabyte model,
either locally or via a networked service.  In general the bigger the
model, the better vectors it can provide.  For example a model will
have been trained so that the vectors for ``runner`` and ``jogger``
are close to each other, while ``orange`` is further away.

This is all well outside the scope of SQLite and FTS5.

The process of producing vectors is known as `word embedding
<https://en.wikipedia.org/wiki/Word_embedding>`__ and `sentence
embedding <https://en.wikipedia.org/wiki/Sentence_embedding>`__.
`Gensim <https://pypi.org/project/gensim/>`__ is a good package to
start with, with its `tutorial
<https://radimrehurek.com/gensim/auto_examples/core/run_core_concepts.html>`__
giving a good overview of what you have to do.

.. _all_tokenizers:

Available tokenizers
====================

SQLite includes 4 builtin tokenizers while APSW provides several more.

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
      turns the entire text into trigrams.  Note it does not turn
      tokens into trigrams, but the entire text including all spaces
      and punctuation.
  * - :func:`UnicodeWordsTokenizer`
    - Use Unicode algorithm for determining word segments.
  * - :func:`SimplifyTokenizer`
    - Wrapper that transforms the token stream by neutralizing case,
      and removing diacritics and similar marks
  * - :func:`RegexTokenizer`
    - Use :mod:`regular expressions <re>` to generate tokens
  * - :func:`RegexPreTokenizer`
    - Use :mod:`regular expressions <re>` to find tokens (eg
      identifiers) and use a different tokenizer for the text between
      the regular expressions
  * - :func:`NGramTokenizer`
    - Generates ngrams from the text, where you can specify the sizes
      and unicode categories.  Useful for doing autocomplete as you
      type, and substring searches.  Unlike``trigram`` this works on
      units of Unicode grapheme clusters  not individual codepoints.
  * - :func:`HTMLTokenizer`
    - Wrapper that converts HTML to plan text for a further tokenizer
      to generate tokens
  * - :func:`JSONTokenizer`
    - Wrapper that converts JSON to plain text for a further tokenizer
      to generate tokens
  * - :func:`SynonymTokenizer`
    - Wrapper that provides additional tokens for existing ones such
      as ``first`` for ``1st``
  * - :func:`StopWordsTokenizer`
    - Wrapper that removes tokens from the token stream that occur too
      often to be useful, such as ``the`` in English text
  * - :func:`TransformTokenizer`
    - Wrapper to transform tokens, such as when stemming.
  * - :func:`QueryTokensTokenizer`
    - Wrapper that recognises :class:`apsw.fts5query.QueryTokens`
      allowing queries using tokens directly.  This is useful if you
      want to add tokens directly to a query without having to find
      the text to produce the token.
  * - :func:`StringTokenizer`
    - A decorator for your own tokenizers so that they operate on
      :class:`str`, with the decorator performing the mapping to UTF8
      byte offsets for you.

      If you have a string and want to call another tokenizer, use
      :func:`string_tokenize`.

.. _fts_third_party:

Third party libraries
=====================

There are several libraries available on PyPI that can be ``pip``
installed (pip name in parentheses).  You can use them with the
tokenizers APSW provides.

NLTK (nltk)
-----------

`Natural Language Toolkit <https://www.nltk.org/>`__ has several
useful methods to help with search.  You can use it do stemming in
many different languages, and `different algorithms
<https://www.nltk.org/api/nltk.stem.html>`__::

  stemmer = apsw.fts5.TransformTokenizer(
    nltk.stem.snowball.EnglishStemmer().stem
  )
  connection.register_fts5_tokenizer("english_stemmer", english_stemmer)

You can use `wordnet <https://www.nltk.org/howto/wordnet.html>`__ to get
synonyms::

  from nltk.corpus import wordnet

  def synonyms(word):
    return [syn.name() for syn in wordnet.synsets(word)]

  wrapper = apsw.fts5.SynonymTokenizer(synonyms)
  connection.register_fts5_tokenizer("english_synonyms", wrapper)

Snowball Stemmer (snowballstemmer)
----------------------------------

`Snowball <https://snowballstem.org/>`__ is a successor to the Porter
stemming algorithm (`included in FTS5
<https://www.sqlite.org/fts5.html#porter_tokenizer>`__), and
supports many more languages.  It is also included as part of nltk::

  stemmer = apsw.fts5.TransformTokenizer(
    snowballstemmer.stemmer("english").stemWord
  )
  connection.register_fts5_tokenizer("english_stemmer", english_stemmer)

Unidecode (unidecode)
---------------------

The `algorithm <https://interglacial.com/tpj/22/>`__ turns Unicode
text into ascii text that sounds approximately similar::

  transform = apsw.fts5.TransformTokenizer(
    unidecode.unidecode
  )

  connection.register_fts5_tokenizer("unidecode", transform)

Available auxiliary functions
=============================

SQLite includes X builtin auxiliary functions, with APSW providing
some more.

.. list-table::
  :header-rows: 1
  :widths: auto

  * - Name
    - Purpose
  * - ``bm25``
    - `SQLite builtin <https://www.sqlite.org/fts5.html#the_bm25_function>`__
      standard algorithm for ranking matches.  It balances how rare
      the search tokens are with how densely they occur.
  * - ``highlight``
    - `SQLite builtin <https://www.sqlite.org/fts5.html#the_highlight_function>`__
      that returns the whole text value with the search terms
      highlighted
  * - ``snippet``
    - `SQLite builtin <https://www.sqlite.org/fts5.html#the_snippet_function>`__
      that returns a small portion of the text containing the
      highlighted search terms
  * - :func:`~apsw.fts5aux.bm25`
    - A Python implementation of bm25.  This is useful as an example
      of how to write your own ranking function
  * - :func:`~apsw.fts5aux.position_rank`
    - Uses bm25 as a base, increasing rank the earlier in the content
      the search terms occur
  * - :func:`~apsw.fts5aux.subsequence`
    - Uses bm25 as a base, increasing rank when the search phrases
      occur in the same order and closer to each other.  A regular
      bm25 rank for the query ``hello world`` gives the same rank for
      the content having those words in that order, in the opposite
      order, and with any number of other words in between.

Command line tools
==================

FTS5 Tokenization viewer
------------------------

Use :code:`python3 -m apsw.fts5 --help` to see detailed help
information.  This tool produces a HTML file showing how a tokenizer
performs on text you supply, or builtin test text.  This is useful if
you are developing your own tokenizer, or want to work out the best
tokenizer and parameters for you.  (Note the tips in the bottom right
of the HTML.)

The builtin test text includes lots of complicated text from across
all of Unicode including all forms of spaces, numbers, multiple
codepoint sequences, homoglyphs, various popular languages, and hard
to tokenize text.

It is useful to compare the default ``unicode61`` tokenizer against
the recommended ``simplify casefold 1 strip 1 unicodewords``.

Unicode
-------

Use :code:`python3 -m apsw.unicode --help` to see detailed help
information.  Of interest are the ``codepoint`` subcommand to see
exactly which codepoints make up some text and ``textwrap`` to line
wrap text for terminal width.

FTS5 module
===========

.. automodule:: apsw.fts5
    :synopsis: Helpers for working with full text search
    :members:
    :undoc-members:

FTS5 Query module
==================================

.. automodule:: apsw.fts5query
    :synopsis: Helpers for working with `FTS5 queries <https://www.sqlite.org/fts5.html#full_text_query_syntax>`__
    :members:
    :undoc-members:

FTS5 Auxiliary functions module
===============================

.. automodule:: apsw.fts5aux
    :synopsis: Auxiliary functions for ranking and match extraction
    :members:
    :undoc-members:

.. include:: fts.rst

Unicode Text Handling
=====================

.. automodule:: apsw.unicode
    :members:
    :undoc-members:
    :member-order: bysource
