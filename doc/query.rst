Query
*****
.. currentmodule:: apsw

Instead of mingling your SQL within your Python code, you can keep the
SQL separate but easily access it from Python.

* SQLAlchemy for ORM / multi-database support

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