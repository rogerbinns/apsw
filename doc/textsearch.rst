Full text search
****************

.. currentmodule:: apsw

APSW provides complete access to SQLite's full text search functionality.
SQLite provides the `FTS5 extension <https://www.sqlite.org/fts5.html>`__
as the implementation.  It is enabled by default in :ref:`PyPI <pypi>`
installs.

Reading
=======

https://hsivonen.fi/string-length/

https://www.unicode.org/reports/tr29/

https://www.nltk.org/


Key Concepts
============

Searching

  SQL is based around the entire contents of a value.  You can test
  for equality, you can do greater or less then, you can build indices
  to improve performance, you can do joins between tables on the values,
  and more.

  But you can't (practically) do that on a subset of a value, especially
  text.  You can't ask which rows/columns/values contain certain words,
  and you can't do searches for content like you can in a web browser.
  This is the functionality that full text search provides.

Tokens

  Values first need to broken down into discrete units, called tokens.
  Most commonly these would correspond to words in English, but they don't
  have to be.  Tokens are the unit that full text search work with,
  with content considered to be a sequence of tokens.  The tokens
  don't have to occur in the content - they are used from your
  search to find content that includes them.  For example your
  content could include "yesterday" while the token is "1/2/23".

Full Text Index

  FTS5 builds an index where a token can be looked up, and which rows
  and columns containing it are returned including their position in
  that column value.  So if you search for "hello world" and "hello"
  is at position 17 in a particular row/column, and "world" is at
  position 18 you now have a match.  FTS5 lets you include ``NEAR``
  in queries letting their positions be further apart and still be
  a match.

  Building the index can be quite time consuming, and it can take
  quite a lot of storage space.  But it is fast to use.

Stop words

  Some words can be very frequent in text such as "the" in English,
  which would match almost all content.  A common optimization is
  to exclude them from the index and queries, to reduce storage
  space and increase performance.  The downside is it becomes
  impossible to search for stop words.

Ranking

  Once matches are found, you want the most relevant ones first.
  A ranking function is used to assign each match a numerical score, so
  that can value can be used for sorting.  Ranking functions try
  to take into account how rare the tokens are, and other factors
  like if the tokens are in headings, and how many tokens are
  in the content it was found in.

Stemming

  It is often useful to use the `stem <https://en.wikipedia.org/wiki/Word_stem>`__
  of a word as a token, so that all words of similar meaning map onto the
  same token.  For example you could stem ``run``, ``ran``, ``runs``, ``running``,
  and ``runners`` to the same token.

  FTS5 includes the `porter stemmer <https://tartarus.org/martin/PorterStemmer/>`__
  which works on English, while the `Snowball stemmer <https://snowballstem.org/>`__
  is more recent, supports more languages, and has a `Python module
  <https://github.com/snowballstem/pystemmer>`__.

Unicode

Unicode Codepoints

UTF8

Unicode Categories

Combining and Modifiers

Normalization

Case



Tokenizers
==========

* Convert bytes into a sequence of tokens
* Get existing :meth:`Connection.fts5_tokenizer`
* register your own :meth:`Connection.register_fts5_tokenizer`

* colocated
* chaining together


* Normalization

.. _byte_offsets:

UTF8 byte offsets
-----------------

* into original utf8 (ie not changed/normalized)
* start is first byte
* end is first byte **after** token like python does with :class:`range`
* end minus start is length of token in bytes
* must be complete utf8, errors if middle of a utf8 byte making up codepoint

Recommendations
===============

Use external content table
  Means can have many FTS tables referencing it, subset of fields,
  different tokenizers, autocomple table

Have db be only content table and fts indices and attach
  Best for non-trivial amount of content

Normalize the Unicode
  As adding to content table - too hard to correct later

API
===

.. include:: fts.rst

Helpers for working with full text search
=========================================

Provided by the :mod:`apsw.fts` module.

.. automodule:: apsw.fts
    :synopsis: Helpers for working with full text search
    :members:
    :undoc-members: