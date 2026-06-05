Query (Separating SQL from Python code)
***************************************
.. currentmodule:: apsw

The documentation and examples have SQL as Python strings, mixed in
with Python code to make it clear what is happening. :mod:`apsw.query`
separates your SQL into their own files.

* The SQL files can be can be syntax checked and highlighted as SQL
* You import the SQL files as though they were native Python code
* Each SQL section becomes a Python function
* Always executes to completion (explain why)
* Automatically sync and async based on :class:`Connection` used

Overview
--------

Create as many SQL files as needed in your Python source tree.  Use
``.sql`` as the extension. Each file has multiple named sections of
SQL.  For each section:

* There is a name - you call the SQL using that name from Python
  providing the :class:`Connection` or :class:`Cursor` to use
* Optionally specify Python parameters, their types, and default
  values that are available as SQL bindings.
* You can also say that caller's local variables are available
  similar to how ``f`` and ``t`` strings work.
* Optional return type with many conversions, going beyond cursors
* Can have as many SQL statements separated by ``;`` as you need
* Can be a template rather than pure SQL.  This allows
  Python expressions, sequences, and identifiers (eg column and table names)
  which can't be specified as bindings.
* Bindings are always used for values avoiding `SQL injection attacks
  <https://en.wikipedia.org/wiki/SQL_injection>`__
* The Python exposing the SQL is fully typed, giving full integration
  with your development and documentation tools.

You can also have sections that are Python, which can provide a top
level docstring, :code:`import`, and types.

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
  # Becomes docstring for the file
  "City SQL queries"

  # You would import your types here.  We'll define the
  # result of a query inline here for demonstration

  import datetime
  import dataclasses

  @dataclasses.dataclass
  class CityInfo:
    name: str
    population: int
    country: str
    rank: int
    founded: datetime.date

    def __post_init__(self):
      # convert from SQLite YYYY-MM-DD representation back to Python
      self.founded = datetime.date.fromisoformat(self.founded)
  */

Two queries where the second uses parameters.  We use :code:`:` in the
query to name a parameter, with SQLite also supporting :code:`$` and
:code:`@`.

.. code-block:: sql
  :force:

  -- name: all_cities
  -- Gets all cities

  SELECT * FROM cities;

  -- name: city_info(founded_after:datetime.date) -> list[CityInfo]
  -- Gets all cities founded after a date including their
  -- population rank within the same country

  -- You should name the result columns using AS

  SELECT
    name          AS name,
    pop           AS population,
    country       AS country,
    RANK() OVER (
      PARTITION BY country
      ORDER BY pop DESC
    )
                  AS rank
  FROM city
  -- Converts Python date to SQLite representation
  WHERE founded >= {founded_after.isoformat():eval};

In Python code import the SQL file at runtime.  (You can also generate
the Python ahead of time - :ref:`More <python_access>`).

.. code-block:: python

  # Use for runtime import
  apsw.query.import_hook()

  # if the file was city_queries.sql then use this
  import city_queries

  # Inside a package you can use a relative import
  from . import city_queries

  # Regular cursor iteration of the SQL
  for row in city_queries.all_cities(connection):
    print(row)

  # And async
  async for row in city_queries.all_cities(async_connection):
    print(row)

  # It is a regular Python function with named parameters
  ranked_cities = city_queries.city_info(founded_after=datetime.date(1973, 6, 30))
  print(len(ranked_cities))

A lot more is available:

* The SQL can have Python expressions and specifiers to treat them as
  values (bindings), identifiers (column and table names), sequences,
  and evaluation. (:ref:`More <sql_templates>`)
* Specifying parameters, types, default values. (:ref:`More
  <query_params>`)
* A result shape (eg list, single value, iterator) and type with
  automatic conversion - :code:`City` in this example. (:ref:`More
  <query_returns>`)

.. _python_access:

Python access
-------------

Call :class:`apsw.query.import_hook` which registers a runtime import hook.
(You can call it multiple times - only one hook will be registered.)
Then :code:`import` your :code:`.sql` file as though it was a Python
file.

There is also a command line interface.  Use :code:`python3 -m
apsw.query --help` to see the options.  This is useful if you want
Python generated code as part of a build process.

.. _sql_templates:

SQL Templates
--------------

SQL can have segments like :code:`{expression:spec}` in them, just
like `fstrings
<https://docs.python.org/3/reference/lexical_analysis.html#f-strings>`__.
The expression is a Python level expression, and is not seen by the
executed SQL. The ``spec`` says how to treat the expression.  The spec
may contain multiple components separated by :code:`|`.

.. list-table:: Supported specs
  :header-rows: 1
  :widths: auto

  * - Spec Example
    - Description

  * - :code:`{name}`
    - No spec means the name's value is used as a binding.

  * - :code:`{name:id}`
    - You can't use bindings for identifiers like table and column
      names, so this uses the name's value as a SQL identifier.
      Identifiers end up double quoted in the SQL, and any double
      quotes inside are doubled up.

  * - :code:`{product["sku"]:eval}`
    - :code:`eval` Evaluates the expression.  This example would
      use the resulting value as a binding.

  * - :code:`{columns[3]:eval|id}`
    - You can have additional specs after :code:`eval` - this uses
      the evaluation result as an :code:`id`.

  * - :code:`{name:seq}`
    - Treats name's value as a sequence.  This is useful for
      :code:`IN`.  eg in the SQL use :code:`WHERE colour IN ({colours:seq})`
      and the resulting SQL will have a comma separated list of
      bindings based on the values in :code:`colours`.

      You can also use :code:`:seq|id` to instead have the SQL be a
      comma separated list of identifiers, such as :code:`SELECT
      {columns:seq|id} FROM ...`.

  * - :code:`{name:literal}`
    - The value is copied exactly as is into the SQL.  **Beware** this
      can easily result in SQL injection attacks.  An example would be
      where you want to specify :code:`DESC` or :code:`ASC` for an
      :code:`ORDER BY`.  That cannot be done via bindings or id, only
      as a literal.

.. _query_params:

Parameters
----------

If no parameters are provided then the variables in the calling
function are available in the SQL.  Use normal Python parameters
including type annotations, defaults, keyword arguments, and keyword
only arguments.

.. code-block:: sql

  -- name: demo(arg, *, kw_only: bool, another: bytes = b'\xaa\xbb')

You cannot specify :code:`*args` to indicate remaining non-keyword
arguments.  :code:`**locals` can be used to indicate that caller
variables are also made available in addition to other arguments.

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
    date stamps.

    `pydantic <https://pydantic.dev/>`__ provides even more dataclass
    like functionality including type validation.

.. list-table::  Type Annotations
    :header-rows: 1
    :widths: auto

    *   - Return type
        - Explanation

    *   - :code:`None`
        - The SQL is executed to completion, ignoring any rows that
          may have resulted.

    *   - :code:`changes`
        - The number of rows added, deleted, or changed.  Cursors
          are not isolated from each other so this will counts all
          database wide changes.  It is an :class:`int`

    *   - :code:`a_type | None`
        - If exactly one row was returned then conversion to
          :code:`a_type` will happen.  If no rows were then
          :code:`None` is returned.  If more than one then
          :exc:`apsw.query.TooManyRows` is raised.

          You can also use :code:`a_type | Literal[value]` and
          :code:`value` is returned on no rows instead of
          :code:`None`.

    *   - :code:`a_type`
        - Exactly one row should return and will be converted to
          :code:`a_type`.  If not then :exc:`apsw.query.RowExpected`
          is raised.

    *   - :code:`Iterator[a_type]`
        - Each row is converted to :code:`a_type` and is iterable
          (sync or async depending on the underlying
          :class:`Connection`)

    *   - :code:`list[a_type]`
        - Each row is converted to :code:`a_type`, as a :class:`list`.

    *   - :code:`Any`
        - In the above cases, using a *type* results in conversion.
          If you use :code:`Any` as the type then no conversion
          happens and the value (if only one column result) or row is
          returned as is.

Tips
----

* Contextvar for executor
* peewee for lightweight ORM
* SQLAlchemy for ORM / multi-database support
* sqlglot for multi-sql support
* :code:`import apsw.sphinx` in :code:`conf.py` to keep
  sphinx happy

Python 3.14+ tstrings
---------------------

Similar functionality as above except:

* SQL is in your Python code as the tstring
* :code:`eval` is not available, and is done by the tstring mechanism
* Bindings can also be provided but they must be
  :class:`collections.abc.Mapping` - the SQL cannot use :code:`?`
  style numbered bindings.

apsw.query module
-----------------

TODO mention invoking at runtime vs AOT, ``python3 -m apsw.query`` etc

.. automodule:: apsw.query
    :members:
    :undoc-members:
    :member-order: bysource

Example
-------

* corresponds to :source:`apsw/_fts5q.sql`
* explain :code:`**locals` usage
* explain :code:`eval` for self stuff
* explain clicking source to see SQL

.. automodule:: apsw._fts5q
    :members:
    :undoc-members:
