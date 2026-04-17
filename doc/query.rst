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
* (Optional) parameters and their types
* (Optional) return type with automatic conversion
* Can have as many SQL statements separated by ; as you need

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

