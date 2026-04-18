Query
*****
.. currentmodule:: apsw

* SQLAlchemy for ORM / multi-database support


Instead of mingling your SQL within your Python code, you can keep the
SQL separate but easily access it from Python.

.. note:: Python 3.14+ tstrings

    Must mingle SQL with Python, executing SQL with variables
    same as described in following, more detail at end of this
    doc

Each SQL file has multiple sections of SQL.  Each section:

* Has a name - you call this name from Python
* Automatically sync or async depending on Connection
* (Optional) parameters, their types, and default values.
  You can also say that caller's local variables are used
  similar to how ``f`` and ``t`` strings work.
* (Optional) return type with automatic conversion.
  (:ref:`query_returns`)
* Can have as many SQL statements separated by ; as you need
* Can be a template rather than pure SQL.  This allows
  Python expressions, sequences, and identifiers (eg column and table names)
  which can't be specified as bindings.  (:ref:`query_templates`)

When executing the SQL, variables can come from:

* (Optional) parameters specified
* (Optional) locals of caller

Variables used for:

* Bindings (default)
* Identifiers (automatically correctly quoted) such as column and
  table names which can't use bindings
* Expressions like in fstrings
* Sequences eg for ``WHERE column IN (`` `sequence` ``)``

* SQL can be found via:

  * file path
  * importlib.resources (``__name__``, relative file path)
  * regular import that looks for .sql extension instead of .py
  * Can get Python at runtime or build time


.. query_templates:

Template processing
-------------------

Templates that have segments :code:`{name:spec}` in them.
Some examples:

:code:`{name}`

   Uses parameter :code:`name` as a binding value

:code:`{product["sku"]:eval}`

    Evaluates :code:`product["sku"]` and uses as a binding.  This
    executes arbitrary code and is dangerous unless all values are
    under your control.

:code:`{name:id}`

    Gets the value of :code:`name` and uses it as a SQL identifier
    (**not** binding).  This is necessary for column and table names
    which can't be provided as bindings.

You can specify multiple ``spec`` by colon separating them - eg :code:`name+ext:eval:id` will
evaluate :code:`name+ext` and then treat as an id.

.. list-table::  Complete list
    :header-rows: 1
    :widths: auto

    * - Spec
      - Explanation
    * - :code:`name` (no spec)
      - :code:`name` must be a parameter name, or a local variable if
        enabled.  Its value will be used.
    * - :code:`:eval`
      - The :code:`name` portion will be evaluated as an expression.  This
        can be very dangerous because there are no restrictions - eg it could
        be :code:`shutil.rmtree("/")` which will delete all files!
    * - :code:`:id`
      - Rather then the default of using as a SQL value binding, it will be
        treated as an identifier (table or column name etc)
    * - :code:`:seq`
      - Treat as a sequence of values.  By default treat as bindings
        so :code:`SELECT ... WHERE colour IN ({colours:seq)` will make
        a comma separated list of bindings for each member of
        :code:`colours`.  If :code:`:seq:id` is used then it will
        become a comma separated sequence of identifiers like column
        or table names instead.

.. query_returns:

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
    processing on your dataclass such as converting datestamps.

    `pydantic <https://pydantic.dev/>`__ provides even more dataclass
    like functionality including type validation.

.. list-table::  Type Annotations
    :header-rows: 1
    :widths: auto

    *   - :code:`None`
        - The SQL is executed to completion, ignoring all rows that
          may have resulted.

    *   - :code:`changes`
        - The number of rows added, deleted, or changed.  Cursors
          are not isolated from each other so this will counts all
          database wide changes.  It is an :class:`int`

    *   - :code:`Optional[a_type]`
        - If exactly one row was returned then conversion to
          :code:`a_type` will happen.  If no rows were then
          :code:`None` is returned.  If more than one then an
          exception (TODO which?) is raised.

          You can also use :code:`Optional[a_type, value]` and
          :code:`value` is returned on no rows instead of
          :code:`None`.

    *   - :code:`a_type`
        - Exactly one row should return and will be converted to
          :code:`a_type`.  If zero or more than one rows were returned
          then an exception (TODO which?) is raised.

    *   - :code:`[a_type]`
        - Each row is converted to :code:`a_type` and the function is
          an iterable (sync or async depending on the
          :class:`Connection`)


apsw.query module
-----------------

TODO mention invoking at runtime vs AOT, ``python3 -m apsw.query`` etc

.. automodule:: apsw.query