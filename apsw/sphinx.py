#
# Sphinx autodoc doesn't look at type stubs and gets upset about
# things not being on the module.  Importing this will add stubs to
# the module and keep autodoc happy.

import typing

import apsw


SQLiteValue : typing.TypeAlias = "apsw.SQLiteValue"
Binding: typing.TypeAlias = "apsw.Binding"
AsyncCursor : typing.TypeAlias = "apsw.AsyncCursor"
AsyncConnection : typing.TypeAlias = "apsw.AsyncConnection"

apsw.AsyncCursor = AsyncCursor
apsw.AsyncConnection = AsyncConnection
apsw.SQLiteValue = SQLiteValue
apsw.Binding = Binding
