Query (Separating SQL from Python code)
***************************************
.. currentmodule:: apsw

The documentation and examples have SQL as Python strings, mixed in
with Python code to make it clear what is happening.  With
:mod:`apsw.query` your SQL is in a separate files that can be syntax
checked and highlighted, with the module providing each SQL as a
Python function.

Overview
--------

Create as many SQL files as needed.  A convention is to use ``.sql``
as the extension. Each file has multiple named sections of SQL.  For
each section:

* There is a name - you call the SQL using the name from Python
* Automatically sync or async depending on the Connection
* SQL comments become docstrings for the Python
* (Optional) Specify Python parameters, their types, and default values.
* You can also say that caller's local variables are used
  similar to how ``f`` and ``t`` strings work.
* (Optional) Return type with many conversions, going beyond cursors
* Can have as many SQL statements separated by ``;`` as you need
* Can be a template rather than pure SQL.  This allows
  Python expressions, sequences, and identifiers (eg column and table names)
  which can't be specified as bindings.
* Bindings are always used for values avoid `SQL injection attacks
  <https://en.wikipedia.org/wiki/SQL_injection>`__
* The Python exposing the SQL is fully typed, giving full integration
  with your development and documentation tools.

You can also have sections that are Python, such as overall docstrings,
:code:`import`, and types.

Example
-------

This example covers a database of cities.  It shows segments from the
SQL file and then the Python using that segment.  The section is
copied into the Python, with any SQL comment of lines :code:`/*` and
:code:`*/` omitted.

.. code-block:: sql

  -- python:
  -- This will end up as a Python comment

  /*
  # Docstring for the file
  "Python access for city queries"

  # You would import your types here
  @dataclass
  class City:
    name: str
    population: int
    country: str

  */

Two queries where the second uses parameters.  We use :code:`:` in the
query to name a parameter, with SQLite also supporting :code:`$` and
:code:`@`.

.. code-block:: sql

  -- name: all_cities
  -- Gets all cities

  SELECT * FROM cities;

  -- name: min_pop
  -- Gets all cities with a minumum population

  SELECT * FROM cities
  WHERE :population >= min_population;

In Python code there are several ways of accessing the object
representing the SQL file, including importing it.  (:ref:`More
<python_access>`)

.. code-block:: python

  city = apsw.query.ns_from_file("city.sql")

  # Regular cursor iteration of the SQL
  for row in city.all_cities(connection):
    print(row)

  # And async
  async for row in city.all_cities(async_connection):
    print(row)

A lot more is available:

* Specifying parameters, types, default values LINK HERE
* The SQL can have Python expressions and specifiers to
  treat them as values (bindings), identifiers (column and table names),
  sequences, evaluation and more LINK HERE
* The result shape (eg list, single value, iterator) LINK HERE
* A result type with automatic conversion (:code:`City` in this
  example) LINK HERE


.. code-block:: sql

  -- name: by_population(millions: int) -> list[City]
  -- This shows passing a parameter, getting a
  -- a list of results using the dataclass from
  -- earlier.

  -- The column names must match those in the dataclass.
  -- They do not have to be in the same order.

  SELECT name, population, country FROM cities WHERE
    population >= {millions * 1_000_000:eval};

.. code-block:: python

  big_cities = city.by_population(10)


.. _python_access:

Python access
-------------

  * file path
  * importlib.resources (``__name__``, relative file path)
  * regular import that looks for .sql extension instead of .py
  * Can get Python at runtime, better namespace
  * Build time using CLI


Tips
----

* Contextvar for executor
* SQLAlchemy for ORM / multi-database support
* sqlglot for multi-sql support

Python 3.14+ tstrings
---------------------

Must mingle SQL with Python, executing SQL with variables
same as described in following, more detail at end of this
doc

.. _sql_templates:

SQL preprocessing
-----------------

SQL can have segments like :code:`{expression:spec}` in them, just
like `fstrings
<https://docs.python.org/3/reference/lexical_analysis.html#f-strings>`__.
The expression is a Python level expression, and is not seen by the
executed SQL. The ``spec`` says how to treat the expression.

.. list-table:: Supported specs
  :header-rows: 1
  :width: auto

  * - Spec Example
    - Description

  * - :code:`{name}`
    - No spec means the name's value is used as a binding.

  * - :code:`{name:id}`
    - Uses the name's value as a SQL identifier, such as table and
      column names.  You can't use bindings for identifiers.
      Identifiers end up double quoted in the SQL, and any double
      quotes inside are doubled up.

  * - :code:`{product["sku"]:eval}`
    - :code:`eval` Evaluates the expression.  This example would
      use the resulting value as a binding.

  * - :code:`{columns[3]:eval:id}`
      You can have additional specs after :code:`eval` - this uses
      the evaluation result as an :code:`id`.

  * - :code:`{name:seq}`
    - Treats name's value as a sequence.  This is useful for
      :code:`IN`.  eg in the SQL use :code:`WHERE colour IN ({colours:seq})`
      and the resulting SQL will have a comma separated list of
      binding based on the values in :code:`colours`.

      You can also use :code:`:seq:id` to instead have the SQL be a
      comma separated list of identifiers, such as :code:`SELECT
      {columns:seq:id} FROM table`.

  * - :code:`{name:literal}`
    - The value is copied exactly as is into the SQL.  **Beware** this
      can easily result in SQL injection attacks.  An example would be
      where you want to specify :code:`DESC` or :code:`ASC` for an
      :code:`ORDER BY`.  That cannot be done via bindings or id, only
      as a literal.

.. _query_returns:

Return typing
-------------

The return type is specified after :code:`->` in the name.  If nothing
is specified then a :class:`Cursor` is returned executing the SQL.

Conversions are done.  If a row has one column then the return type is
invoked with that column's value as its :code:`__init__`.

If there is more than one column then the row is converted to a
:class:`dict` where each key is the :meth:`column name
<Cursor.description>` - use :code:`AS` to name columns in your SQL.
The type is then invoked with the dict.  This works really well with
:mod:`dataclasses`.

.. code-block:: sql

    SELECT prods.name AS name,
       prices.price AS price,
       SUM(...) AS quantity
    FROM prods, prices. ..,
    WHERE ...;

.. code-block:: python

    @dataclass
    class Product:
        name: str
        price: float
        quantity: int

.. note:: Advanced

    You can use :func:`dataclasses.__post_init__` to do additional
    processing on your dataclass initialization such as converting
    datestamps.

    `pydantic <https://pydantic.dev/>`__ provides even more dataclass
    like functionality including type validation.

.. list-table::  Type Annotations
    :header-rows: 1
    :widths: auto

    *   - Return type
        - Explanation

    *   - :code:`None`
        - The SQL is executed to completion, ignoring all rows that
          may have resulted.

    *   - :code:`changes`
        - The number of rows added, deleted, or changed.  Cursors
          are not isolated from each other so this will counts all
          database wide changes.  It is an :class:`int`

    *   - :code:`a_type | None`
        - If exactly one row was returned then conversion to
          :code:`a_type` will happen.  If no rows were then
          :code:`None` is returned.  If more than one then an
          exception (TODO which?) is raised.

          You can also use :code:`a_type | value` and
          :code:`value` is returned on no rows instead of
          :code:`None`.

    *   - :code:`a_type`
        - Exactly one row should return and will be converted to
          :code:`a_type`.  If zero or more than one rows were returned
          then an exception (TODO which?) is raised.

    *   - :code:`[a_type]`
        - Each row is converted to :code:`a_type` and is iterable
          (sync or async depending on the underlying
          :class:`Connection`)


apsw.query module
-----------------

TODO mention invoking at runtime vs AOT, ``python3 -m apsw.query`` etc

.. automodule:: apsw.query
    :members:
    :undoc-members:
    :member-order: bysource

Testing
-------

.. automodule:: example2
    :members:
    :undoc-members: