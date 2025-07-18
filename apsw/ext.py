# Provides various useful routines

from __future__ import annotations

import abc
import collections
import collections.abc
import contextvars
import dataclasses
import enum
import functools
import html
import inspect
import keyword
import logging
import math
import os
import re
import string
import sys
import time
import traceback
import types
from dataclasses import dataclass, is_dataclass, make_dataclass
from fractions import Fraction
from typing import Any, Callable, Generator, Iterable, Iterator, Literal, Sequence, TextIO, Union

import apsw
import apsw.unicode

NoneType = types.NoneType if sys.version_info > (3, 10) else type(None)


def result_string(code: int) -> str:
    """Turns a result or extended result code into a string.
    The appropriate mapping based on the value is used."""
    if code < 256:
        return apsw.mapping_result_codes.get(code, str(code))  # type: ignore
    return apsw.mapping_extended_result_codes.get(code, str(code))  # type: ignore


class DataClassRowFactory:
    """Returns each row as a :mod:`dataclass <dataclasses>`, accessible by column name.

    To use set an instance as :attr:`Connection.row_trace
    <apsw.Connection.row_trace>` to affect all :class:`cursors
    <apsw.Cursor>`, or on a specific cursor::

        connection.row_trace = apsw.ext.DataClassRowFactory()
        for row in connection.execute("SELECT title, sum(orders) AS total, ..."):
            # You can now access by name
            print (row.title, row.total)
            # you can get the underlying description
            print (row.__description__)

    You can use as many instances of this class as you want, each across as many
    :class:`connections <apsw.Connection>` as you want.

    :param rename:     Column names could be duplicated, or not
        valid in Python (eg a column named `continue`).
        If `rename` is True, then invalid/duplicate names are replaced
        with `_` and their position starting at zero.  For example `title,
        total, title, continue` would become `title, total, _2, _3`.  If
        `rename` is False then problem column names will result in
        :exc:`TypeError` raised by :func:`dataclasses.make_dataclass`

    :param dataclass_kwargs: Additional parameters when creating the dataclass
       as described in :func:`dataclasses.dataclass`.  For example you may
       want `frozen = True` to make the dataclass read-only, or `slots = True`
       to reduce memory consumption.

    """

    def __init__(self, *, rename: bool = True, dataclass_kwargs: dict[str, Any] | None = None):
        self.dataclass_kwargs = dataclass_kwargs or {}
        self.rename = rename

    @functools.lru_cache(maxsize=16)
    def get_dataclass(self, description: tuple[tuple[str, str], ...]) -> tuple[Any, tuple[str, ...]]:
        """Returns dataclass and tuple of (potentially renamed) column names

        The dataclass is what is returned for each row with that
        :meth:`description <apsw.Cursor.get_description>`

        This method caches its results.
        """
        names = [d[0] for d in description]
        if self.rename:
            new_names: list[str] = []
            for i, n in enumerate(names):
                if n.isidentifier() and not keyword.iskeyword(n) and n not in new_names:
                    new_names.append(n)
                else:
                    new_names.append(f"_{i}")
            names = new_names
        types = [self.get_type(d[1]) for d in description]

        kwargs = self.dataclass_kwargs.copy()
        if "namespace" not in kwargs:
            kwargs["namespace"] = {}
        kwargs["namespace"]["__description__"] = description

        # some magic to make the reported classnames different
        suffix = (".%06X" % hash(repr(description)))[:7]

        return make_dataclass(f"{self.__class__.__name__}{suffix}", zip(names, types), **kwargs), tuple(names)

    def get_type(self, t: str | None) -> Any:
        """Returns the :mod:`type hint <typing>` to use in the dataclass based on the type in the :meth:`description <apsw.Cursor.get_description>`

        `SQLite's affinity rules  <https://www.sqlite.org/datatype3.html#affname>`__ are followed.

        The values have no effect on how your program runs, but can be used by tools like
        mypy.  Column information like whether `null` is allowed is not present, so
        this is just a hint.
        """
        if not t:
            return Any
        # From 3.1 https://www.sqlite.org/datatype3.html
        t = t.upper()
        if "INT" in t:
            return int
        if "CHAR" in t or "CLOB" in t or "TEXT" in t:
            return str
        if "BLOB" in t:
            return bytes
        if "REAL" in t or "FLOA" in t or "DOUB" in t:
            return float
        return Union[float, int]

    def __call__(self, cursor: apsw.Cursor, row: apsw.SQLiteValues) -> Any:
        """What the row tracer calls

        This :meth:`looks up <get_dataclass>` the dataclass and column
        names, and then returns an instance of the dataclass.
        """
        dc, column_names = self.get_dataclass(cursor.get_description())
        return dc(**dict(zip(column_names, row)))


class SQLiteTypeAdapter(abc.ABC):
    """A metaclass to indicate conversion to SQLite types is supported

    This is one way to indicate your type supports conversion to a
    value supported by SQLite.  You can either inherit from this class,
    or call the register method::

       apsw.ext.SQLiteTypeAdapter.register(YourClassHere)

    Doing either is entirely sufficient and there is no need to
    register with :class:`TypesConverterCursorFactory`
    """

    @abc.abstractmethod
    def to_sqlite_value(self) -> apsw.SQLiteValue:
        "Return a SQLite compatible value for this object"
        raise NotImplementedError


class TypesConverterCursorFactory:
    """Provides cursors that can convert objects into one of the types supported by SQLite,
    or back from SQLite

    :param abstract_base_class: Which metaclass to consider as conversion capable
    """

    def __init__(self, abstract_base_class: abc.ABCMeta = SQLiteTypeAdapter):
        self.abstract_base_class = abstract_base_class
        # to sqlite value
        self.adapters: dict[type, Callable[[Any], apsw.SQLiteValue]] = {}
        # from sqlite value
        self.converters: dict[str, Callable[[apsw.SQLiteValue], Any]] = {}

    def register_adapter(self, klass: type, callable: Callable[[Any], apsw.SQLiteValue]) -> None:
        """Registers a callable that converts from `klass` to one of the supported SQLite types"""
        self.adapters[klass] = callable

    def register_converter(self, name: str, callable: Callable[[apsw.SQLiteValue], Any]) -> None:
        """Registers a callable that converts from a SQLite value"""
        self.converters[name] = callable

    def __call__(self, connection: apsw.Connection) -> TypeConverterCursor:
        "Returns a new convertor :class:`cursor <apsw.Cursor>` for the `connection`"
        return TypesConverterCursorFactory.TypeConverterCursor(connection, self)

    def adapt_value(self, value: Any) -> apsw.SQLiteValue:
        "Returns SQLite representation of `value`"
        if value is None or isinstance(value, (int, bytes, str, float)):
            return value
        if isinstance(value, self.abstract_base_class):
            return value.to_sqlite_value()
        adapter = self.adapters.get(type(value))
        if not adapter:
            raise TypeError(f"No adapter registered for type {type(value)}")
        return adapter(value)

    def convert_value(self, schematype: str, value: apsw.SQLiteValue) -> Any:
        "Returns Python object from schema type and SQLite value"
        converter = self.converters.get(schematype)
        if not converter:
            return value
        return converter(value)

    def wrap_bindings(self, bindings: apsw.Bindings | None) -> apsw.Bindings | None:
        "Wraps bindings that are supplied to underlying execute"
        if bindings is None:
            return None
        if isinstance(bindings, (dict, collections.abc.Mapping)):
            return TypesConverterCursorFactory.DictAdapter(self, bindings)  # type: ignore[arg-type]
        return tuple(self.adapt_value(v) for v in bindings)

    def wrap_sequence_bindings(
        self, sequenceofbindings: Iterable[apsw.Bindings]
    ) -> Generator[apsw.Bindings, None, None]:
        "Wraps a sequence of bindings that are supplied to the underlying executemany"
        for binding in sequenceofbindings:
            yield self.wrap_bindings(binding)  # type: ignore[misc]

    class DictAdapter(collections.abc.Mapping):
        "Used to wrap dictionaries supplied as bindings"

        def __init__(self, factory: TypesConverterCursorFactory, data: collections.abc.Mapping[str, apsw.SQLiteValue]):
            self.data = data
            self.factory = factory

        def __getitem__(self, key: str) -> apsw.SQLiteValue:
            return self.factory.adapt_value(self.data[key])

        def __iter__(self):
            "Required by mapping, but not used"
            raise NotImplementedError

        def __len__(self):
            "Required by mapping, but not used"
            raise NotImplementedError

    class TypeConverterCursor(apsw.Cursor):
        "Cursor used to do conversions"

        def __init__(self, connection: apsw.Connection, factory: TypesConverterCursorFactory):
            super().__init__(connection)
            self.factory = factory
            self.row_trace = self._rowtracer

        def _rowtracer(self, cursor: apsw.Cursor, values: apsw.SQLiteValues) -> tuple[Any, ...]:
            return tuple(self.factory.convert_value(d[1], v) for d, v in zip(cursor.get_description(), values))

        def execute(
            self,
            statements: str,
            bindings: apsw.Bindings | None = None,
            *,
            can_cache: bool = True,
            prepare_flags: int = 0,
            explain: int = -1,
        ) -> apsw.Cursor:
            """Executes the statements doing conversions on supplied and returned values

            See :meth:`apsw.Cursor.execute` for parameter details"""
            return super().execute(
                statements,
                self.factory.wrap_bindings(bindings),
                can_cache=can_cache,
                prepare_flags=prepare_flags,
                explain=explain,
            )

        def executemany(
            self,
            statements: str,
            sequenceofbindings: Iterable[apsw.Bindings],
            *,
            can_cache: bool = True,
            prepare_flags: int = 0,
            explain: int = -1,
        ) -> apsw.Cursor:
            """Executes the statements against each item in sequenceofbindings, doing conversions on supplied and returned values

            See :meth:`apsw.Cursor.executemany` for parameter details"""
            return super().executemany(
                statements,
                self.factory.wrap_sequence_bindings(sequenceofbindings),  # type: ignore[arg-type]
                can_cache=can_cache,
                prepare_flags=prepare_flags,
                explain=explain,
            )


def log_sqlite(*, level: int = logging.ERROR, logger: logging.Logger | None = None) -> None:
    """Send SQLite `log messages <https://www.sqlite.org/errlog.html>`__ to :mod:`logging`

    :param level: highest `level <https://docs.python.org/3/library/logging.html#levels>`__ to log at
    :param logger: Use the specific logger
    """

    def handler(errcode: int, message: str) -> None:
        nonlocal level
        err_str = result_string(errcode)
        extra = {"sqlite_code": errcode, "sqlite_code_name": err_str, "sqlite_message": message}
        # Level defaults to ERROR but some messages aren't as important
        if errcode & 0xFF == apsw.SQLITE_WARNING:
            level = min(level, logging.WARNING)
        elif errcode & 0xFF == apsw.SQLITE_NOTICE:
            # these are really half way between INFO and WARNING and
            # current instances are recovering journals/WAL etc which
            # happens if the previous process exited abruptly.
            level = min(level, logging.WARNING)
        elif errcode == apsw.SQLITE_SCHEMA:
            # these happen automatically without developer control,
            # especially when using FTS5.  DEBUG is almost more
            # appropriate!
            level = min(level, logging.INFO)

        (logger or logging).log(level, "SQLITE_LOG: %s (%d) %s", message, errcode, err_str, extra=extra)

    apsw.config(apsw.SQLITE_CONFIG_LOG, handler)


def print_augmented_traceback(
    exc_type: type[BaseException],
    exc_value: BaseException,
    exc_traceback: types.TracebackType,
    *,
    file: TextIO | None = None,
) -> None:
    """Prints a standard exception, but also includes the value of variables in each stack frame
    which APSW :ref:`adds <augmentedstacktraces>` to help diagnostics and debugging.

    :param exc_type: The exception type
    :param exc_value: The exception value
    :param exc_traceback: Traceback for the exception
    :param file: (default ``sys.stderr``) Where the print goes

    .. code-block::

        try:
            ....
        except Exception as exc:
            apsw.ext.print_augmented_traceback(*sys.exc_info())
    """

    file = file or sys.stderr

    tbe = traceback.TracebackException(exc_type, exc_value, exc_traceback, capture_locals=True, compact=True)
    for line in tbe.format():
        print(line, file=file)


def index_info_to_dict(
    o: apsw.IndexInfo, *, column_names: list[str] | None = None, rowid_name: str = "__ROWID__"
) -> dict[str, Any]:
    """
    Returns a :class:`apsw.IndexInfo` as a dictionary.

    If *column_names* is supplied then additional keys with column
    names are present, using *rowid_name* for the rowid.

    Here is an example response::

        query = '''
            SELECT orderid, note FROM sales WHERE
                    price > 74.99
                    AND quantity<=?
                    AND customer in ('Acme Widgets', 'Acme Industries')
            ORDER BY date desc
            LIMIT 10'''

        {'aConstraint': [{'collation': 'BINARY',
                        'iColumn': 5,
                        'iColumn_name': 'price',
                        'op': 4,
                        'op_str': 'SQLITE_INDEX_CONSTRAINT_GT',
                        'rhs': 74.99,
                        'usable': True},
                        {'collation': 'BINARY',
                        'iColumn': 7,
                        'iColumn_name': 'quantity',
                        'op': 8,
                        'op_str': 'SQLITE_INDEX_CONSTRAINT_LE',
                        'rhs': None,
                        'usable': True},
                        {'collation': 'BINARY',
                        'iColumn': 8,
                        'iColumn_name': 'customer',
                        'op': 2,
                        'op_str': 'SQLITE_INDEX_CONSTRAINT_EQ',
                        'rhs': None,
                        'usable': True},
                        {'collation': 'BINARY',
                        'op': 73,
                        'op_str': 'SQLITE_INDEX_CONSTRAINT_LIMIT',
                        'rhs': 10,
                        'usable': True}],
        'aConstraintUsage': [{'argvIndex': 0, 'in': False, 'omit': False},
                            {'argvIndex': 0, 'in': False, 'omit': False},
                            {'argvIndex': 0, 'in': True, 'omit': False},
                            {'argvIndex': 0, 'in': False, 'omit': False}],
        'aOrderBy': [{'desc': True, 'iColumn': 9, 'iColumn_name': 'date'}],
        'colUsed': {0, 3, 5, 7, 8, 9},
        'colUsed_names': {'date', 'note', 'customer', 'quantity', 'orderid', 'price'},
        'distinct': 0,
        'estimatedCost': 5e+98,
        'estimatedRows': 25,
        'idxFlags': 0,
        'idxFlags_set': set(),
        'idxNum': 0,
        'idxStr': None,
        'nConstraint': 4,
        'nOrderBy': 1,
        'orderByConsumed': False}
    """

    res = {
        "nConstraint": o.nConstraint,
        "aConstraint": [
            {
                "iColumn": o.get_aConstraint_iColumn(n),
                "op": o.get_aConstraint_op(n),
                "op_str": apsw.mapping_bestindex_constraints.get(o.get_aConstraint_op(n)),
                "usable": o.get_aConstraint_usable(n),
                "collation": o.get_aConstraint_collation(n),
                "rhs": o.get_aConstraint_rhs(n),
            }
            for n in range(o.nConstraint)
        ],
        "nOrderBy": o.nOrderBy,
        "aOrderBy": [
            {
                "iColumn": o.get_aOrderBy_iColumn(n),
                "desc": o.get_aOrderBy_desc(n),
            }
            for n in range(o.nOrderBy)
        ],
        "aConstraintUsage": [
            {
                "argvIndex": o.get_aConstraintUsage_argvIndex(n),
                "omit": o.get_aConstraintUsage_omit(n),
                "in": o.get_aConstraintUsage_in(n),
            }
            for n in range(o.nConstraint)
        ],
        "idxNum": o.idxNum,
        "idxStr": o.idxStr,
        "orderByConsumed": o.orderByConsumed,
        "estimatedCost": o.estimatedCost,
        "estimatedRows": o.estimatedRows,
        "idxFlags": o.idxFlags,
        "idxFlags_set": set(
            v for k, v in apsw.mapping_virtual_table_scan_flags.items() if isinstance(k, int) and o.idxFlags & k
        ),
        "colUsed": o.colUsed,
        "distinct": o.distinct,
    }

    for aConstraint in res["aConstraint"]:  # type: ignore[attr-defined]
        if aConstraint["op"] in (apsw.SQLITE_INDEX_CONSTRAINT_OFFSET, apsw.SQLITE_INDEX_CONSTRAINT_LIMIT):
            del aConstraint["iColumn"]
        if aConstraint["op"] >= apsw.SQLITE_INDEX_CONSTRAINT_FUNCTION and aConstraint["op"] <= 255:
            aConstraint["op_str"] = (
                f"SQLITE_INDEX_CONSTRAINT_FUNCTION+{aConstraint['op'] - apsw.SQLITE_INDEX_CONSTRAINT_FUNCTION}"
            )

    if column_names:
        for aconstraint in res["aConstraint"]:  # type: ignore[attr-defined]
            if "iColumn" in aconstraint:
                aconstraint["iColumn_name"] = (
                    rowid_name if aconstraint["iColumn"] == -1 else column_names[aconstraint["iColumn"]]
                )
        for aorderby in res["aOrderBy"]:  # type: ignore[attr-defined]
            aorderby["iColumn_name"] = rowid_name if aorderby["iColumn"] == -1 else column_names[aorderby["iColumn"]]
        # colUsed has all bits set when SQLite just wants the whole row
        # eg when doing an update
        res["colUsed_names"] = set(column_names[i] for i in o.colUsed if i < len(column_names))
        if 63 in o.colUsed:  # could be one or more of the rest - we add all
            res["colUsed_names"].update(column_names[63:])  # type: ignore[attr-defined]

    return res


def dbinfo(
    db: apsw.Connection, schema: str = "main"
) -> tuple[DatabaseFileInfo | None, JournalFileInfo | WALFileInfo | None]:
    """Extracts fields from the database, journal, and wal files

    Based on the `file format description <https://www.sqlite.org/fileformat2.html>`__.  The
    headers are read using :meth:`apsw.Connection.read` so you see inside encrypted, compressed,
    zip etc formats, not necessarily the actual on disk file.

    Memory databases return `None` for both.
    """

    dbinfo: DatabaseFileInfo | None = None
    journalinfo: JournalFileInfo | WALFileInfo | None = None

    try:
        ok, header_page = db.read(schema, 0, 0, 128)
    except apsw.SQLError:
        ok = False

    be_int = functools.partial(int.from_bytes, byteorder="big", signed=False)

    be_sint = functools.partial(int.from_bytes, byteorder="big", signed=True)

    def be_bool(b: bytes) -> bool:
        return bool(be_int(b))

    def be_page_size(b: bytes) -> int:
        v = be_int(b)
        if v == 1:
            v = 65536
        return v

    def text_encoding(b: bytes) -> str:
        v = be_int(b)
        return {0: "(pending)", 1: "UTF-8", 2: "UTF-16le", 3: "UTF-16be"}.get(v, f"<< INVALID VALUE {v} >>")

    if ok:
        kw: dict[str, Any] = {"filename": db.filename}
        for name, offset, size, converter in (
            ("header", 0, 16, bytes),
            ("page_size", 16, 2, be_page_size),
            ("write_format", 18, 1, be_int),
            ("read_format", 19, 1, be_int),
            ("reserved_bytes", 20, 1, be_int),
            ("file_change_counter", 24, 4, be_int),
            ("page_count", 28, 4, be_int),
            ("freelist_pages", 36, 4, be_int),
            ("schema_cookie", 40, 4, be_int),
            ("schema_format", 44, 4, be_int),
            ("default_cache_size", 48, 4, be_int),
            ("autovacuum_top_root", 52, 4, be_int),
            ("text_encoding", 56, 4, text_encoding),
            ("user_version", 60, 4, be_int),
            ("incremental_vacuum", 64, 4, be_bool),
            ("application_id", 68, 4, be_int),
            ("version_valid_for", 92, 4, be_int),
            ("sqlite_version", 96, 4, be_int),
        ):
            b = header_page[offset : offset + size]
            kw[name] = converter(b)  # type: ignore [operator]
        dbinfo = DatabaseFileInfo(**kw)

    try:
        ok, journal_page = db.read(schema, 1, 0, 32)
    except apsw.SQLError:
        ok = False

    if ok:
        kw: dict[str, Any] = {}  # type: ignore [no-redef]
        if db.pragma("journal_mode") == "wal":
            kw["filename"] = db.filename_wal
            for name, offset, size, converter in (
                ("magic_number", 0, 4, be_int),
                ("format_version", 4, 4, be_int),
                ("page_size", 8, 4, be_page_size),
                ("checkpoint_sequence_number", 12, 4, be_int),
                ("salt_1", 16, 4, be_int),
                ("salt_2", 20, 4, be_int),
                ("checksum_1", 24, 4, be_int),
                ("checksum_2", 28, 4, be_int),
            ):
                b = journal_page[offset : offset + size]
                kw[name] = converter(b)  # type: ignore [operator]
            journalinfo = WALFileInfo(**kw)
        else:
            header_valid = lambda b: b == b"\xd9\xd5\x05\xf9\x20\xa1\x63\xd7"
            kw["filename"] = db.filename_journal
            for name, offset, size, converter in (
                ("header", 0, 8, bytes),
                ("header_valid", 0, 8, header_valid),
                ("page_count", 8, 4, be_sint),
                ("random_nonce", 12, 4, be_int),
                ("initial_pages", 16, 4, be_int),
                ("sector_size", 20, 4, be_int),
                ("page_size", 24, 4, be_int),
            ):
                b = journal_page[offset : offset + size]
                kw[name] = converter(b)  # type: ignore [operator]
            journalinfo = JournalFileInfo(**kw)

    return dbinfo, journalinfo


def quote_name(name: str, quote: str = '"') -> str:
    """Quotes name to ensure it is parsed as a name

    :meta private:
    """
    if quote in name or re.search(r"[^\w]", name, re.ASCII):
        return quote + name.replace(quote, quote * 2) + quote
    if name.upper() in apsw.keywords:
        return quote + name + quote
    return name


def find_columns(
    table_name: str, column_count: int, pk_columns: set[int], *, connection: apsw.Connection, schema: str | None = None
) -> tuple[str, ...]:
    """Finds a matching table and returns column names for :func:`changeset_to_sql`.

    Changesets only include column numbers, not column names so this
    method is used to find those names.  Use it like this::

      changeset_to_sql(changeset, get_columns=functools.partial(apsw.ext.find_columns, connection=db))

    The table name has to match (following SQLite's case insensitivity rules), have the correct number of columns,
    and corresponding primary key columns.

    :param table_name: Name of the expected table
    :param column_count: How many columns there are
    :param pk_columns: Which columns make up the primary key
    :param connection: The connection to examine
    :param schema: If ``None`` (default) then the ``main`` and all attached databases are searched,
         else only the named one.

    :raises ValueError: If no corresponding table with matching column count and primary key
         columns can be found.

    """

    rowid_found = []

    for dbname in connection.db_names() if schema is None else [schema]:
        columns: list[str] = []
        pks: set[int] = set()

        for column, pk, hidden in connection.execute(
            "SELECT name, pk, hidden FROM pragma_table_xinfo(?, ?)", (table_name, dbname)
        ):
            if hidden:
                continue
            if pk > 0:
                pks.add(len(columns))
            columns.append(column)

        # if no primary keys then SQLITE_SESSION_OBJCONFIG_ROWID
        # applies but we only try them after exhausting all primary
        # key tables across all searched schemas
        if not pks and pk_columns == {0} and len(columns) + 1 == column_count:
            rowid_found.append(columns)

        if not columns or column_count != len(columns) or pks != pk_columns:
            continue

        return tuple(columns)

    if len(rowid_found) == 1:
        columns = rowid_found[0]
        # OBJCONFIG_ROWID only does _rowid_ and produces wrong changeset
        # operations of that is the name of a regular column.  This code
        # originally tried all the aliases
        return ("_rowid_", *columns)

    raise ValueError(f"Can't find {table_name=} in {connection=} with {column_count=} and {pk_columns=}")


def changeset_to_sql(
    changeset: apsw.ChangesetInput, get_columns: Callable[[str, int, set[int]], tuple[str, ...]]
) -> Iterator[str]:
    """Produces SQL equivalent to the contents of a changeset (or patchset)

    :param changeset: The changeset either as bytes, or a streaming
        callable. It is passed to :meth:`apsw.Changeset.iter`.
    :param get_columns:  Because changesets only have column numbers,
        this is called with a table name, column count, and primary
        keys, and should return the corresponding column names to use
        in the SQL.  See :func:`find_columns`.

    SQL statements are provided one at a time, and will directly
    change one row.  (More than one row can be affected due to foreign
    keys.)  If a recorded row change was because of an indirect change
    then the SQL statement begins with the comment ``/* indirect */``.

    See the :ref:`example <example_changesets>`
    """
    tables: dict[str, tuple[str, ...]] = {}
    for change in apsw.Changeset.iter(changeset):
        if change.name not in tables:
            tables[change.name] = tuple(
                quote_name(c) for c in get_columns(change.name, change.column_count, change.pk_columns)
            )
        columns = tables[change.name]
        sql: list[str] = []
        if change.indirect:
            sql.append("/* indirect */ ")

        if change.opcode == apsw.SQLITE_INSERT:
            sql.append(f"INSERT INTO {quote_name(change.name)}(")
            cols: list[str] = []
            values: list[str] = []
            for i in range(len(change.new)):
                if change.new[i] is not apsw.no_change:
                    cols.append(columns[i])
                    values.append(apsw.format_sql_value(change.new[i]))
            sql.append(", ".join(cols))
            sql.append(") VALUES (")
            sql.append(", ".join(values))
            sql.append(");")
            yield "".join(sql)
            continue

        # package up change.old
        assert change.old is not None
        constraints: list[str] = []

        # always do pk columns first, for cosmetic reasons
        def sort_key(n):
            # False is sorted before True
            return (n not in change.pk_columns, n)

        for i in sorted(range(change.column_count), key=sort_key):
            if change.old[i] is apsw.no_change:
                continue
            if change.old[i] is None:
                constraints.append(f"{columns[i]} IS NULL")
            else:
                constraints.append(f"{columns[i]} = {apsw.format_sql_value(change.old[i])}")

        if change.opcode == apsw.SQLITE_UPDATE:
            sql.append(f"UPDATE {quote_name(change.name)} SET ")
            comma = ""
            for i in range(len(change.new)):
                if change.new[i] is not apsw.no_change:
                    sql.append(comma)
                    comma = ", "
                    sql.append(f"{columns[i]}=")
                    sql.append(apsw.format_sql_value(change.new[i]))
            sql.append(" WHERE ")
            sql.append(" AND ".join(constraints))
            sql.append(";")
            yield "".join(sql)
            continue

        assert change.opcode == apsw.SQLITE_DELETE

        sql.append(f"DELETE FROM {quote_name(change.name)} WHERE ")
        sql.append(" AND ".join(constraints))
        sql.append(";")
        yield "".join(sql)
        continue


class Trace:
    """Use as a context manager to show each SQL statement run inside the block

    Statements from your code as well as from other parts of SQLite are shown.::

        with apsw.ext.Trace(sys.stdout, db):
            method()
            db.execute("SQL")
            etc

    :param file: File to print to.  If `None` then no information is
           gathered or printed
    :param db: :class:`~apsw.Connection` to trace.
    :param trigger: The names of triggers being executed is always
           shown.  If this is `True` then each statement of an
           executing trigger is shown too.
    :param vtable: If `True` then statements executed behind the
           scenes by virtual tables are shown.
    :param updates: If `True` and the :meth:`~apsw.Connection.preupdate_hook`
           is available, then inserted, updated, and deleted rows are shown.
           This is very helpful when you use bindings.
    :param transaction: If `True` then transaction start and commit/rollback
           will be shown, using commit/rollback hooks.
    :param truncate: Truncates SQL text to this many characters
    :param indent: Printed before each line of output

    You are shown each regular statement start with a prefix of ``>``,
    end with a prefix of ``<`` if there were in between statements
    like triggers, ``T`` indicating trigger statements, and ``V``
    indicating virtual table statements.  If ``updates`` is on, then
    ``INS``. ``DEL``, and ``UPD`` are shown followed by the rowid, and
    then the columns. For updates, unchanged columns are shown as ``...```.
    Transaction control is shown with a ``!`` prefix.

    As each statement ends you are shown summary information.

    .. list-table::
        :header-rows: 1
        :widths: auto

        * - Example
          - Description
        * - Time: 1.235
          - Elapsed time since the statement started executing in seconds.
            This is always shown.
        * - Rows: 5
          - How many times SQLite stopped execution providing a row to
            be processed
        * - Changes: 77
          - The difference in the `total change count
            <https://sqlite.org/c3ref/total_changes.html>`__ between
            when the statement started and when it ended.  It will
            include changes made by triggers, virtual table code etc.
        * - FullScanRows: 12,334
          - Number of rows visited doing a full scan of a table.  This
            indicates an opportunity for an index.
        * - Sort: 5
          - The number of times SQLite had to do a sorting operation.
            If you have indexes in the desired order then the sorting
            can be skipped.
        * - AutoIndexRows: 55,988
          - SQLite had to create and add this many rows to an
            `automatic index
            <https://sqlite.org/optoverview.html#automatic_query_time_indexes>`__.
            This indicates an opportunity for an index.
        * - VmStep: 55,102
          - How many `internal steps
            <https://sqlite.org/opcode.html>`__ were needed.
        * - Mem: 84.3KB
          - How much memory was used to hold the statement and working data.


    Tracing is done with :meth:`~apsw.Connection.trace_v2`.

    See :func:`ShowResourceUsage` to get summary information about a
    block as a whole.  You can use this and that at the same time.

    See the :ref:`example <example_Trace>`.
    """

    @dataclasses.dataclass
    class stmt:
        """Tracks a sqlite3_stmt for one statement lifetime

        :meta private:
        """

        sql: str | None = None
        rows: int = 0
        vtable: bool = False
        change_count: int = -1

    def __init__(
        self,
        file: TextIO | None,
        db: apsw.Connection,
        *,
        trigger: bool = False,
        vtable: bool = False,
        updates: bool = False,
        transaction: bool = False,
        truncate: int = 75,
        indent: str = "",
    ):
        self.file = file
        self.db = db
        self.trigger = trigger
        self.vtable = vtable
        self.indent = indent
        self.updates = updates
        self.transaction = transaction
        self.truncate = truncate

    def _truncate(self, text: str) -> str:
        text = text.strip()
        return text[: self.truncate] + "..." if len(text) > self.truncate else text

    def __enter__(self):
        if not self.file:
            return self

        self.statements: collections.defaultdict[int, Trace.stmt] = collections.defaultdict(Trace.stmt)

        # id,sql of the last SQLITE_TRACE_STMT we output to detect
        # interleaving of queries
        self.last_emitted = None

        self.db.trace_v2(
            apsw.SQLITE_TRACE_STMT | apsw.SQLITE_TRACE_ROW | apsw.SQLITE_TRACE_PROFILE, self._sqlite_trace, id=self
        )

        if self.updates:
            if hasattr(self.db, "preupdate_hook"):
                self.db.preupdate_hook(self._preupdate, id=self)
            else:
                self.updates = False

        if self.transaction:
            self.db.set_commit_hook(self._commit, id=self)
            self.db.set_rollback_hook(self._rollback, id=self)
            self.transaction_state: str | None = None

        return self

    def _commit(self):
        self._transaction("COMMIT")
        return False

    def _rollback(self):
        self._transaction("ROLLBACK")

    def _transaction(self, state: str):
        if self.transaction and self.transaction_state != state:
            self.transaction_state = state
            print(self.indent, f" !{state}", file=self.file)

    def _preupdate(self, update: apsw.PreUpdate):
        self._transaction("BEGIN")
        out = f"{update.op[:3]} {update.rowid}{f'>{update.rowid_new}' if update.rowid_new != update.rowid else ''} ("
        for num, column in enumerate(
            update.old if update.op == "DELETE" else update.new if update.op == "INSERT" else update.update
        ):
            if len(out) > self.truncate:
                break
            val = "..." if column is apsw.no_change else apsw.format_sql_value(column)
            if num != 0:
                out += ", "
            out += val
        out += ")"

        print(self.indent, " " + "  " * update.depth, self._truncate(out), file=self.file)

    def _sqlite_trace(self, event: dict):
        if event["code"] == apsw.SQLITE_TRACE_STMT:
            if self.db.in_transaction or not event["readonly"]:
                self._transaction("BEGIN")
            stmt = self.statements[event["id"]]
            if stmt.change_count == -1:
                stmt.change_count = event["total_changes"]

            if event["trigger"]:
                if stmt.sql is None:
                    # its really virtual table
                    stmt.vtable = True
                    stmt.sql = event["sql"]
                    if self.vtable:
                        print(self.indent, "V", self._truncate(event["sql"]), file=self.file)
                        self.last_emitted = event["id"], event["sql"]

                else:
                    if self.trigger or event["sql"].startswith("TRIGGER "):
                        print(self.indent, "T", self._truncate(event["sql"]), file=self.file)
                        self.last_emitted = event["id"], event["sql"]
            else:
                assert stmt.sql is None
                stmt.sql = event["sql"]
                print(self.indent, ">", self._truncate(event["sql"]), file=self.file)
                self.last_emitted = event["id"], event["sql"]

        elif event["code"] == apsw.SQLITE_TRACE_ROW:
            self.statements[event["id"]].rows += 1

        elif event["code"] == apsw.SQLITE_TRACE_PROFILE:
            stmt = self.statements.get(event["id"], None)
            if stmt is None:
                return

            is_trigger = stmt.sql != event["sql"]

            if is_trigger and not self.trigger:
                return

            interleaving = self.last_emitted != (event["id"], event["sql"])

            if not is_trigger:
                self.statements.pop(event["id"])

            if stmt.vtable and not self.vtable:
                return

            if interleaving:
                print(self.indent, "<" if not stmt.vtable else "V", self._truncate(event["sql"]), file=self.file)

            seconds = event["nanoseconds"] / 1_000_000_000

            fields = [f"Time: {seconds:.03f}"]

            if stmt.rows:
                fields.append(f"Rows: {stmt.rows:,}")

            changes = event["total_changes"] - stmt.change_count if stmt.change_count != -1 else 0
            if changes:
                fields.append(f"Changes: {changes:,}")

            for field, desc, threshold in (
                ("SQLITE_STMTSTATUS_FULLSCAN_STEP", "FullScanRows", 1000),
                ("SQLITE_STMTSTATUS_SORT", "Sort", 1),
                ("SQLITE_STMTSTATUS_AUTOINDEX", "AutoIndexRows", 100),
                ("SQLITE_STMTSTATUS_VM_STEP", "VmStep", 100),
                ("SQLITE_STMTSTATUS_MEMUSED", "Mem", 16384),
            ):
                val = event["stmt_status"][field]
                if val >= threshold:
                    if field == "SQLITE_STMTSTATUS_MEMUSED":
                        power = math.floor(math.log(val, 1024))
                        suffix = ["B", "KB", "MB", "GB", "TB"][int(power)]
                        val = val / 1024**power
                        fields.append(f"{desc}: {val:.1f}{suffix}")
                    else:
                        fields.append(f"{desc}: {val:,}")

            print(
                self.indent,
                "   ",
                "  ".join(fields),
                file=self.file,
            )

            self.last_emitted = event["id"], event["sql"]

    def __exit__(self, *_):
        self.db.trace_v2(0, None, id=self)
        if self.updates:
            self.db.preupdate_hook(None, id=self)
        if self.transaction:
            self.db.set_commit_hook(None, id=self)
            self.db.set_rollback_hook(None, id=self)


class ShowResourceUsage:
    """Use as a context manager to show a summary of time, resource, and SQLite usage inside
    the block::

        with apsw.ext.ShowResourceUsage(sys.stdout, db=connection, scope="thread"):
            # do things with the database
            connection.execute("...")
            # and other calculations
            do_work()

    When then context finishes a report is printed to the file.  Only
    non-zero fields are shown - eg if no I/O is done then no I/O
    fields are shown.  See the :ref:`example <example_ShowResourceUsage>`.

    :param file: File to print to.  If `None` then no information is gathered or
           printed
    :param db: :class:`~apsw.Connection` to gather SQLite stats from if not `None`.
           Statistics from each SQL statement executed are added together.
    :param scope: Get :data:`thread <resource.RUSAGE_THREAD>` or
           :data:`process <resource.RUSAGE_SELF>` stats, or `None`.
           Note that MacOS only supports process, and Windows doesn't support
           either.
    :param indent: Printed before each line of output

    Timing information comes from :func:`time.monotonic` and :func:`time.process_time`,
    resource usage from :func:`resource.getrusage` (empty for Windows), and SQLite from
    :meth:`~apsw.Connection.trace_v2`.

    See :func:`Trace` to trace individual statements.  You can use
    this and that at the same time.

    See the :ref:`example <example_ShowResourceUsage>`.
    """

    def __init__(
        self,
        file: TextIO | None,
        *,
        db: apsw.Connection | None = None,
        scope: Literal["thread"] | Literal["process"] | None = None,
        indent: str = "",
    ):
        self.file = file
        self.db = db
        self.indent = indent
        if scope not in {"thread", "process", None}:
            raise ValueError(f"scope {scope} not a valid choice")
        self.scope = file and self._get_resource and scope

    try:
        import resource

        _get_resource = resource.getrusage
        _get_resource_param = {
            "thread": getattr("resource", "RUSAGE_THREAD", resource.RUSAGE_SELF),
            "process": resource.RUSAGE_SELF,
        }

        del resource

    except ImportError:
        _get_resource = None

    def __enter__(self):
        if not self.file:
            return self
        self._times = time.process_time(), time.monotonic()
        if self.scope:
            self._usage = self._get_resource(self._get_resource_param[self.scope])
        if self.db:
            self.db.trace_v2(apsw.SQLITE_TRACE_PROFILE, self._sqlite_trace, id=self)
            self.stmt_status = {}
            self.db_status = self.db_status_get()
        return self

    def _sqlite_trace(self, v):
        for k, val in v["stmt_status"].items():
            self.stmt_status[k] = val + self.stmt_status.get(k, 0)

    def db_status_get(self) -> dict[str, int]:
        ":meta private:"
        return {
            "SQLITE_DBSTATUS_LOOKASIDE_USED": self.db.status(apsw.SQLITE_DBSTATUS_LOOKASIDE_USED)[0],
            "SQLITE_DBSTATUS_LOOKASIDE_HIT": self.db.status(apsw.SQLITE_DBSTATUS_LOOKASIDE_HIT)[1],
            "SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE": self.db.status(apsw.SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE)[1],
            "SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL": self.db.status(apsw.SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL)[1],
            "SQLITE_DBSTATUS_CACHE_USED": self.db.status(apsw.SQLITE_DBSTATUS_CACHE_USED)[0],
            "SQLITE_DBSTATUS_SCHEMA_USED": self.db.status(apsw.SQLITE_DBSTATUS_SCHEMA_USED)[0],
            "SQLITE_DBSTATUS_STMT_USED": self.db.status(apsw.SQLITE_DBSTATUS_STMT_USED)[0],
            "SQLITE_DBSTATUS_CACHE_HIT": self.db.status(apsw.SQLITE_DBSTATUS_CACHE_HIT)[0],
            "SQLITE_DBSTATUS_CACHE_MISS": self.db.status(apsw.SQLITE_DBSTATUS_CACHE_MISS)[0],
            "SQLITE_DBSTATUS_CACHE_WRITE": self.db.status(apsw.SQLITE_DBSTATUS_CACHE_WRITE)[0],
            "SQLITE_DBSTATUS_CACHE_SPILL": self.db.status(apsw.SQLITE_DBSTATUS_CACHE_SPILL)[0],
            "SQLITE_DBSTATUS_DEFERRED_FKS": self.db.status(apsw.SQLITE_DBSTATUS_DEFERRED_FKS)[0],
        }

    def __exit__(self, *_) -> None:
        if not self.file:
            return

        vals: list[tuple[str, int | float]] = []

        times = time.process_time(), time.monotonic()
        if times[0] - self._times[0] >= 0.001:
            vals.append((self._descriptions["process_time"], times[0] - self._times[0]))
        if times[1] - self._times[1] >= 0.001:
            vals.append((self._descriptions["monotonic"], times[1] - self._times[1]))

        if self.scope:
            usage = self._get_resource(self._get_resource_param[self.scope])

            for k in dir(usage):
                if not k.startswith("ru_"):
                    continue
                delta = getattr(usage, k) - getattr(self._usage, k)
                if delta >= 0.001:
                    vals.append((self._descriptions.get(k, k), delta))

        if self.db:
            self.db.trace_v2(0, None, id=self)
            if self.stmt_status:
                self.stmt_status.pop("SQLITE_STMTSTATUS_MEMUSED")
                for k, v in self.stmt_status.items():
                    if v:
                        vals.append((self._descriptions[k], v))
                for k, v in self.db_status_get().items():
                    diff = v - self.db_status[k]
                    if diff:
                        vals.append((self._descriptions[k], diff))

        if not vals:
            # there was no meaningful change, so output nothing
            pass
        else:
            max_width = max(len(k) for k in self._descriptions.values())
            for k, v in vals:
                if isinstance(v, float):
                    v = f"{v:.3f}"
                else:
                    v = f"{v:,}"
                print(self.indent, " " * (max_width - len(k)), k, " ", v, file=self.file, sep="")

    _descriptions = {
        "process_time": "Total CPU consumption",
        "monotonic": "Wall clock",
        "SQLITE_STMTSTATUS_FULLSCAN_STEP": "SQLite full table scan",
        "SQLITE_STMTSTATUS_SORT": "SQLite sort operations",
        "SQLITE_STMTSTATUS_AUTOINDEX": "SQLite auto index rows added",
        "SQLITE_STMTSTATUS_VM_STEP": "SQLite vm operations",
        "SQLITE_STMTSTATUS_REPREPARE": "SQLite statement reprepares",
        "SQLITE_STMTSTATUS_RUN": "SQLite statements completed",
        "SQLITE_STMTSTATUS_FILTER_HIT": "SQLite bloom filter hit",
        "SQLITE_STMTSTATUS_FILTER_MISS": "SQLite bloom filter miss",
        "SQLITE_DBSTATUS_LOOKASIDE_USED": "SQLite lookaside slots used",
        "SQLITE_DBSTATUS_LOOKASIDE_HIT": "SQLite allocations using lookaside",
        "SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE": "SQLite allocations too big for lookaside",
        "SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL": "SQLite allocations lookaside full",
        "SQLITE_DBSTATUS_CACHE_USED": "SQLite pager memory",
        "SQLITE_DBSTATUS_SCHEMA_USED": "SQLite schema memory",
        "SQLITE_DBSTATUS_STMT_USED": "SQLite statement memory",
        "SQLITE_DBSTATUS_CACHE_HIT": "SQLite pager cache hit",
        "SQLITE_DBSTATUS_CACHE_MISS": "SQLite pager cache miss",
        "SQLITE_DBSTATUS_CACHE_WRITE": "SQLite pager cache writes",
        "SQLITE_DBSTATUS_CACHE_SPILL": "SQLite pager cache writes during transaction",
        "SQLITE_DBSTATUS_DEFERRED_FKS": "SQLite unresolved foreign keys",
        "ru_utime": "Time in user mode",
        "ru_stime": "Time in system mode",
        "ru_maxrss": "Maximum resident set size",
        "ru_ixrss": "Shared memory size",
        "ru_idrss": "Unshared memory size",
        "ru_isrss": "Unshared stack size",
        "ru_minflt": "Page faults - no I/O",
        "ru_majflt": "Page faults with I/O",
        "ru_nswap": "Number of swapouts",
        "ru_inblock": "Block input operations",
        "ru_oublock": "Block output operations",
        "ru_msgsnd": "Messages sent",
        "ru_msgrcv": "Messages received",
        "ru_nsignals": "Signals received",
        "ru_nvcsw": "Voluntary context switches",
        "ru_nivcsw": "Involuntary context switches",
    }


@dataclasses.dataclass
class PageUsage:
    """Returned by :func:`analyze_pages`"""

    page_size: int
    "Size of pages in bytes.  All pages in the database are the same size."
    pages_used: int
    "Pages with content"
    sequential_pages: int
    "How many pages were sequential in the database file"
    data_stored: int
    "Bytes of SQL content stored"
    cells: int
    """Cells are what is `stored <https://www.sqlite.org/fileformat.html#b_tree_pages>`__
    including sizing information, pointers to overflow etc"""
    max_payload: int
    "Largest cell size"
    tables: list[str]
    "Names of tables providing these statistics"
    indices: list[str]
    "Names of indices providing these statistics"


@dataclasses.dataclass
class DatabasePageUsage(PageUsage):
    """Returned by :func:`analyze_pages` when asking about the database as a whole"""

    pages_total: int
    "Number of pages in the database"
    pages_freelist: int
    "How many pages are unused, for example if data got deleted"
    max_page_count: int
    "Limit on the `number of pages <https://www.sqlite.org/pragma.html#pragma_max_page_count>`__"


def _analyze_pages_for_name(con: apsw.Connection, schema: str, name: str, usage: PageUsage):
    qschema = '"' + schema.replace('"', '""') + '"'

    for pages_used, ncell, payload, mx_payload in con.execute(
        """SELECT pageno, ncell, payload, mx_payload
                    FROM dbstat(?, 1) WHERE name=?
                """,
        (schema, name),
    ):
        usage.pages_used += pages_used
        usage.data_stored += payload
        usage.cells += ncell
        usage.max_payload = max(usage.max_payload, mx_payload)
        t = con.execute(f"select type from {qschema}.sqlite_schema where name=?", (name,)).get
        if t == "index":
            usage.indices.append(name)
            usage.indices.sort()
        else:
            usage.tables.append(name)
            usage.tables.sort()

        # by definition the first page is sequential but won't match next, so fake it
        sequential = 1
        next = None
        for (pageno,) in con.execute("select pageno from dbstat(?) WHERE name=? ORDER BY path", (schema, name)):
            sequential += pageno == next
            next = pageno + 1
        usage.sequential_pages += sequential


def analyze_pages(con: apsw.Connection, scope: int, schema: str = "main") -> DatabasePageUsage | dict[str, PageUsage]:
    """Summarizes page usage for the database

    The `dbstat <https://www.sqlite.org/dbstat.html>`__ virtual table
    is used to gather statistics.

    See `example output <_static/samples/analyze_pages.txt>`__.

    :param con: Connection to use
    :param scope:
        .. list-table::
            :widths: auto
            :header-rows: 1

            * - Value
              - Scope
              - Returns
            * - ``0``
              - The database as a whole
              - :class:`DatabasePageUsage`
            * - ``1``
              - Tables and their indices are grouped together.  Virtual tables
                like FTS5 have multiple backing tables which are grouped.
              - A :class:`dict` where the key is the name of the
                table, and a corresponding :class:`PageUsage` as the
                value.  The :attr:`PageUsage.tables` and
                :attr:`PageUsage.indices` fields tell you which ones
                were included.
            * - ``2``
              - Each table and index separately.
              - :class:`dict` of each name and a corresponding
                :class:`PageUsage` where one of the
                :attr:`PageUsage.tables` and :attr:`PageUsage.indices`
                fields will have the name.


    .. note::

        dbstat is present in PyPI builds, and many platform SQLite
        distributions.  You can use `pragma module_list
        <https://www.sqlite.org/pragma.html#pragma_module_list>`__ to
        check.  If the table is not present then calling this function
        will give :class:`apsw.SQLError` with message ``no such table:
        dbstat``.
    """

    qschema = '"' + schema.replace('"', '""') + '"'

    if scope == 0:
        total_usage = DatabasePageUsage(
            page_size=con.pragma("page_size", schema=schema),
            pages_total=con.pragma("page_count", schema=schema),
            pages_freelist=con.pragma("freelist_count", schema=schema),
            max_page_count=con.pragma("max_page_count", schema=schema),
            pages_used=0,
            data_stored=0,
            sequential_pages=0,
            tables=[],
            indices=[],
            cells=0,
            max_payload=0,
        )

        _analyze_pages_for_name(con, schema, "sqlite_schema", total_usage)
        for (name,) in con.execute(f"select name from {qschema}.sqlite_schema where rootpage!=0"):
            _analyze_pages_for_name(con, schema, name, total_usage)

        return total_usage

    res = {}

    grouping: dict[str, list[str]] = {}

    if scope == 2:
        grouping["sqlite_schema"] = ["sqlite_schema"]
        for (name,) in con.execute(f"select name from {qschema}.sqlite_schema where rootpage!=0"):
            grouping[name] = [name]
    elif scope == 1:
        grouping["sqlite"] = ["sqlite_schema"]
        is_virtual_table: set[str] = set()
        for type, name, tbl_name, rootpage in con.execute(
            # the order by tbl_name is so we get eg fts base table
            # name before the shadow tables.  type desc is so that 'table'
            # comes before index
            f"""select type, name, tbl_name, rootpage
                from {qschema}.sqlite_schema
                where type in ('table', 'index')
                order by tbl_name, type desc"""
        ):
            if type == "index":
                # indexes always know their table-
                grouping[tbl_name].append(name)
                continue
            if name.startswith("sqlite_"):
                grouping["sqlite"].append(name)
                continue
            if rootpage == 0:
                grouping[name] = []
                is_virtual_table.add(name)
                continue
            # shadow table? we assume an underscore separator searching longest names first
            for n in sorted(grouping, key=lambda x: (len(x), x)):
                if n in is_virtual_table and name.startswith(n + "_"):
                    grouping[n].append(name)
                    break
            else:
                grouping[name] = [name] if rootpage else []
    else:
        raise ValueError(f"Unknown {scope=}")

    for group, names in sorted(grouping.items()):
        usage = PageUsage(
            page_size=con.pragma("page_size", schema=schema),
            pages_used=0,
            data_stored=0,
            sequential_pages=0,
            tables=[],
            indices=[],
            cells=0,
            max_payload=0,
        )

        for name in names:
            _analyze_pages_for_name(con, schema, name, usage)
        res[group] = usage

    return res


def storage(v: int) -> str:
    """Converts number to storage size (KB, MB, GB etc)

    :meta private:
    """
    if not v:
        return "0"
    power = math.floor(math.log(v, 1024))
    suffix = ["B", "KB", "MB", "GB", "TB", "PB", "EB"][int(power)]
    if suffix == "B":
        return f"{v}B"
    return f"{v / 1024**power:.1f}".rstrip(".0") + suffix


def page_usage_to_svg(con: apsw.Connection, out: TextIO, schema: str = "main") -> None:
    """Visualize database space usage as a `SVG <https://en.wikipedia.org/wiki/SVG>`__

    You can hover or click on segments to get more details.  The
    centre circle shows information about the database as a whole, the
    middle ring shows usage grouped by database (combing indices,
    shadow tables for virtual tables), while the outer ring shows each
    index and table separately.

    Uses :func:`analyze_pages` to gather the information.

    :param con: Connection to query
    :param out: Where the svg is written to.  You can use
       :class:`io.StringIO` if you want it as a string.
    :param schema: Which attached database to query

    See `example output <_static/samples/chinook.svg>`__.
    """
    # Angles and distances are used within.  They are in the range 0.0
    # to 1.0 .

    # Coordinates are output as int, so this is the scaling factor
    RADIUS = 1000
    # how much whitespace is outside the circles
    OVERSCAN = 1.05

    def colour_for_angle(angle: float) -> str:
        # we use r g b each offset by a third of the circle
        radians = angle * math.pi
        third = 1 / 3 * math.pi

        red = int(255 * abs(math.cos(radians)))
        green = int(255 * abs(math.cos(third + radians)))
        blue = int(255 * abs(math.cos(third + third + radians)))

        return f"#{red:02x}{green:02x}{blue:02x}"

    def pos_for_angle(angle: float, distance: float) -> tuple[float, float]:
        "give x,y for distance from centre"

        # the minus bit is because trig has east as 0 but we want north as
        # zero
        radians = angle * 2 * math.pi - (1 / 4 * 2 * math.pi)

        return distance * math.cos(radians), distance * math.sin(radians)

    # these two are used in fstrings hence the short names
    def c(v: float | list[float]) -> str:
        # outputs a coordinate scaling by RADIUS
        if isinstance(v, float):
            return str(round(v * RADIUS))
        return " ".join(str(round(x * RADIUS)) for x in v)

    def p(angle: float, distance: float):
        # outputs a coordinate scaling by RADIUS
        return c(pos_for_angle(angle, distance))

    def slice(
        id: str,
        start_angle: float,
        end_angle: float,
        start_distance: float,
        end_distance: float,
        color: str | None = None,
    ):
        # produces one of the circular slices
        large = 1 if (end_angle - start_angle) > 1 / 2 else 0

        d = []
        d.append(f"M {p(start_angle, start_distance)}")
        d.append(f"L {p(start_angle, end_distance)}")
        d.append(f"A {c(end_distance)} {c(end_distance)} 0 {large} 1 {p(end_angle, end_distance)}")
        d.append(f"L {p(end_angle, start_distance)}")
        d.append(f"A {c(start_distance)} {c(start_distance)} 0 {large} 0 {p(start_angle, start_distance)}")

        ds = " ".join(d)

        fill = color or colour_for_angle((start_angle + end_angle) / 2)
        return f"""<a href="#" id="{id}"><path d="{ds}" stroke="black" fill="{fill}" stroke-width="1px"/></a>"""

    def text(pos: tuple[float, float], id: str, name: str, ring: int, usage: DatabasePageUsage | PageUsage) -> str:
        # produces text infobox
        x, y = c(pos[0]), c(pos[1])
        vspace = 'dy="1.1em"'
        e = html.escape
        res: list[str] = []
        res.append(f"""<text id="{id}" x="{x}" y="{y}" class="infobox">""")
        res.append(f"""<tspan x="{x}" dy="-3em" class="name">{e(name)}</tspan>""")
        if ring == -1:
            size = storage((usage.pages_total - usage.pages_used) * usage.page_size)
            res.append(f"""<tspan x="{x}" {vspace}>{size} unused</tspan>""")

        elif ring == 0:
            assert isinstance(usage, apsw.ext.DatabasePageUsage)
            total = storage(usage.pages_total * usage.page_size)
            used = storage(usage.pages_used * usage.page_size)
            page = storage(usage.page_size)
            res.append(f"""<tspan x="{x}" {vspace}>{page} page size</tspan>""")
            res.append(f"""<tspan x="{x}" {vspace}>{used} / {total} used</tspan>""")
            res.append(f"""<tspan x="{x}" {vspace}>{len(usage.tables):,} tables</tspan>""")
            res.append(f"""<tspan x="{x}" {vspace}>{len(usage.indices):,} indices</tspan>""")
        else:
            if ring == 2:
                kind = "table" if usage.tables else "index"
                res.append(f"""<tspan x="{x}" {vspace}>({kind})</tspan>""")
            size = storage(usage.pages_used * usage.page_size)
            res.append(f"""<tspan x="{x}" {vspace}>{size}</tspan>""")
            if ring == 1:
                res.append(f"""<tspan x="{x}" {vspace}>{len(usage.tables):,} tables</tspan>""")
                res.append(f"""<tspan x="{x}" {vspace}>{len(usage.indices):,} indices</tspan>""")
        if ring >= 0:
            res.append(
                f"""<tspan x="{x}" {vspace}>{usage.sequential_pages / max(usage.pages_used, 1):.0%} sequential</tspan>"""
            )
            res.append(f"""<tspan x="{x}" {vspace}>{storage(usage.data_stored)} SQL data</tspan>""")
            res.append(f"""<tspan x="{x}" {vspace}>{storage(usage.max_payload)} max payload</tspan>""")
            res.append(f"""<tspan x="{x}" {vspace}>{usage.cells:,} cells</tspan>""")

        res.append("""</text>""")
        return "\n".join(res)

    # check we can get the information
    root, group, each = (apsw.ext.analyze_pages(con, n, schema) for n in range(3))

    if not root.pages_total:
        raise ValueError(f"database {schema=} is empty")

    # maps which element hovering over causes a response on
    hover_response: dict[str, str] = {}

    # z-order is based on output order so texts go last
    texts: list[str] = []

    id_counter = 0

    def next_ids():
        # return pais of element ids used to map slice to corresponding text
        nonlocal id_counter
        PREFIX = "id"
        hover_response[f"{PREFIX}{id_counter}"] = f"{PREFIX}{id_counter + 1}"
        id_counter += 2
        return f"{PREFIX}{id_counter - 2}", f"{PREFIX}{id_counter - 1}"

    print(
        f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="-{round(RADIUS * OVERSCAN)} {-round(RADIUS * OVERSCAN)} {round(RADIUS * OVERSCAN * 2)} {round(RADIUS * OVERSCAN * 2)}">""",
        file=out,
    )

    # inner summary circle
    id, resp = next_ids()
    print(f"""<a href="#" id="{id}"><circle r="{c(0.3)}" fill="#777"/></a>""", file=out)
    texts.append(text(pos_for_angle(0, 0), resp, os.path.basename(con.db_filename(schema)) or '""', 0, root))

    # inner ring
    start = Fraction()
    for name, usage in group.items():
        ring1_proportion = Fraction(usage.pages_used, root.pages_total)
        id, resp = next_ids()
        print(slice(id, float(start), float(start + ring1_proportion), 1 / 3, 0.6), file=out)
        texts.append(text(pos_for_angle(float(start + ring1_proportion / 2), (1 / 3 + 0.6) / 2), resp, name, 1, usage))
        ring2_start = start
        start += ring1_proportion

        # corresponding outer ring
        for child in sorted(usage.tables + usage.indices):
            usage2 = each[child]
            ring2_proportion = Fraction(usage2.pages_used, root.pages_total)
            id, resp = next_ids()
            print(slice(id, float(ring2_start), float(ring2_start + ring2_proportion), 2 / 3, 1.0), file=out)
            texts.append(
                text(pos_for_angle(float(ring2_start + ring2_proportion / 2), (2 / 3 + 1) / 2), resp, child, 2, usage2)
            )
            ring2_start += ring2_proportion

    if root.pages_used < root.pages_total:
        id, resp = next_ids()
        print(slice(id, float(start), 1.0, 1 / 3, 1.0, "#aaa"), file=out)
        texts.append(text(pos_for_angle(float((1 + start) / 2), 0.5), resp, "Free Space", -1, root))

    for t in texts:
        print(t, file=out)

    print(
        """<style>
        .infobox { text-anchor: middle; dominant-baseline: middle; font-size: 28pt;
            fill: black; stroke: white; stroke-width:4pt; paint-order: stroke;
            font-family: ui-sans-serif, sans-serif; }
        tspan {font-weight: 600;}
        .name {font-weight: 900; text-decoration: underline;}
    """,
        file=out,
    )
    for source, target in hover_response.items():
        print(f"""#{target} {{ display: none;}}""", file=out)
        print(
            f"""#{source}:hover ~ #{target}, #{target}:hover, #{source}:active ~ #{target}, #{target}:active
                {{display:block;}}""",
            file=out,
        )

    print("</style></svg>", file=out)


query_limit_context: contextvars.ContextVar[query_limit.limit] = contextvars.ContextVar("apsw.ext.query_limit_context")
"""Stores the current query limits

:meta private:
"""


class QueryLimitNoException(Exception):
    """Indicates that no exception will be raised when a :class:`query_limit` is exceeded"""

    pass


class query_limit:
    """Use as a context manager to limit execution time and rows processed in the block

    When the total number of rows processed hits the row limit, or
    timeout seconds have elapsed an exception is raised to exit the
    block.

    .. code-block::

        with query_limit(db, row_limit = 1000, timeout = 2.5):
            db.execute("...")
            for row in db.execute("..."):
                ,,,
            db.execute("...")

    :param db: Connection to monitor
    :param row_limit: Maximum number of rows to process, across all
        queries.  :class:`None` (default) means no limit
    :param timeout: Maximum elapsed time in seconds.  :class:`None`
        (default) means no limit
    :param row_exception: Class of exception to raise when row limit
        is hit
    :param timeout_exception: Class of exception to raise when timeout
        is hit
    :param timeout_steps: How often the elapsed time is checked in
        SQLite internal operations (see :meth:`~apsw.Connection.set_progress_handler`)

    If the exception is :class:`QueryLimitNoException` (default) then
    no exception is passed on when the block exits.

    Row limits are implemented by :meth:`~apsw.Connection.trace_v2` to
    monitor rows.  Only rows directly in your queries are counted -
    for example rows used by virual tables like FTS5 in the
    background, or triggers are not counted.

    Time limits are implemented by
    :meth:`~apsw.Connection.set_progress_handler` to monitor the
    elapsed :func:`time <time.monotonic>`.  This means the elapsed
    time is only checked while running SQLite queries.

    :meth:`~apsw.Connection.trace_v2` and
    :meth:`~apsw.Connection.set_progress_handler` implement multiple
    registrations so query_limit will not interfere with any you may
    have registered.

    If you use nested query_limit blocks then only the limits set by
    the closest block apply within that block.

    See the :ref:`example <example_query_limit>`
    """

    @dataclasses.dataclass
    class limit:
        """ "Current limit in effect

        :meta private:"""

        rows_remaining: int | None = None
        "How many more rows can be returned"
        time_expiry: float | None = None
        "time.monotic value when we stop"
        statements: dict[int, bool] = dataclasses.field(default_factory=dict)
        """key is statement id, value is if its rows count towards rows remaining"""

    def __init__(
        self,
        db: apsw.Connection,
        *,
        row_limit: int | None = None,
        timeout: float | None = None,
        row_exception: type[Exception] = QueryLimitNoException,
        timeout_exception: type[Exception] = QueryLimitNoException,
        timeout_steps: int = 100,
    ):
        self.db = db
        self.row_limit = row_limit
        self.timeout = timeout
        self.row_exception = row_exception
        self.timeout_exception = timeout_exception
        self.timeout_steps = timeout_steps

    def __enter__(self) -> None:
        "Context manager entry point"

        limit = self.limit()

        if self.row_limit is not None:
            self.db.trace_v2(
                apsw.SQLITE_TRACE_STMT | apsw.SQLITE_TRACE_ROW | apsw.SQLITE_TRACE_PROFILE, self.trace, id=self
            )
            limit.rows_remaining = self.row_limit

        if self.timeout is not None:
            limit.time_expiry = time.monotonic() + self.timeout
            self.db.set_progress_handler(self.progress, self.timeout_steps, id=self)

        self.context_token = query_limit_context.set(limit)
        self.my_limit = limit
        return None

    def __exit__(self, exc_type, *_):
        "Context manager exit point"

        self.db.trace_v2(0, None, id=self)
        self.db.set_progress_handler(None, id=self)

        limit: query_limit.limit = query_limit_context.get()
        query_limit_context.reset(self.context_token)

        if limit is self.my_limit and exc_type is QueryLimitNoException:
            return True

        # pass on whatever the exception was
        return False

    def trace(self, event: dict):
        """Process statement events to check for row limits

        :meta private:"""
        limit: query_limit.limit = query_limit_context.get()

        if limit is not self.my_limit:
            return

        if event["code"] == apsw.SQLITE_TRACE_PROFILE:
            limit.statements.pop(event["id"], None)

        elif event["code"] == apsw.SQLITE_TRACE_STMT:
            # trigger is True for both triggers and vtables, and we ignore them
            limit.statements[event["id"]] = not event["trigger"]

        elif event["code"] == apsw.SQLITE_TRACE_ROW:
            # we ignore unknown statements
            if limit.statements.get(event["id"], False):
                limit.rows_remaining -= 1
                if limit.rows_remaining < 0:
                    raise self.row_exception("query row limit hit")

    def progress(self):
        """Progress handler to check for time expiry

        :meta private:
        """
        limit: query_limit.limit = query_limit_context.get()

        if limit is self.my_limit and time.monotonic() >= limit.time_expiry:
            raise self.timeout_exception("query time limit hit")

        return False


def format_query_table(
    db: apsw.Connection,
    query: str,
    bindings: apsw.Bindings | None = None,
    *,
    colour: bool = False,
    quote: bool = False,
    string_sanitize: Callable[[str], str] | Literal[0] | Literal[1] | Literal[2] = 0,
    binary: Callable[[bytes], str] = lambda x: f"[ {len(x)} bytes ]",
    null: str = "(null)",
    truncate: int = 4096,
    truncate_val: str = " ...",
    text_width: int = 80,
    use_unicode: bool = True,
) -> str:
    r"""Produces query output in an attractive text table

    See :ref:`the example <example_format_query>`.

    :param db: Connection to run the query on
    :param query: Query to run
    :param bindings: Bindings for query (if needed)
    :param colour: If True then `ANSI colours <https://en.wikipedia.org/wiki/ANSI_escape_code#Colors>`__ are
        used to outline the header, and show the type of each value.
    :param quote: If True then :meth:`apsw.format_sql_value` is used to get a textual representation of a
         value
    :param string_sanitize:  If this is a callable then each string is passed to it for cleaning up.
        Bigger numbers give more sanitization to the string.  Using an example source string of::

            '''hello \\ \t\f\0日本語 world'''

        .. list-table::
            :header-rows: 1
            :widths: auto

            * - param
              - example output
              - description
            * - 0
              - hello \\\\  \0日本語 world
              - Various whitespace (eg tabs, vertical form feed) are replaced. backslashes
                are escaped, embedded nulls become \\0
            * - 1
              - hello \\\\  \\0{CJK UNIFIED IDEOGRAPH-65E5}{CJK UNIFIED IDEOGRAPH-672C}{CJK UNIFIED IDEOGRAPH-8A9E} world
              - After step 0, all non-ascii characters are replaced with their :func:`apsw.unicode.codepoint_name`
            * - 2
              - hello.\\........world
              - All non-ascii characters and whitespace are replaced by a dot

    :param binary: Called to convert bytes to string
    :param null: How to represent the null value
    :param truncate: How many characters to truncate long strings at (after sanitization)
    :param truncate_val: Appended to truncated strings to show it was truncated
    :param text_width: Maximum output width to generate
    :param use_unicode: If True then unicode line drawing characters are used.  If False then +---+ and | are
        used.
    """
    # args we pass on to format_table
    kwargs = {
        "colour": colour,
        "quote": quote,
        "string_sanitize": string_sanitize,
        "binary": binary,
        "null": null,
        "truncate": truncate,
        "truncate_val": truncate_val,
        "text_width": text_width,
        "use_unicode": use_unicode,
    }

    res: list[str] = []

    cursor = db.cursor()
    colnames = None
    rows = []

    def trace(c: apsw.Cursor, query: str, bindings: apsw.Bindings | None) -> bool:
        nonlocal colnames, rows
        if colnames:
            res.append(format_query_table._format_table(colnames, rows, **kwargs))
            rows = []
        colnames = [n for n, _ in c.get_description()]
        return True

    cursor.exec_trace = trace
    # mitigate any existing row tracer
    if db.row_trace:
        cursor.row_trace = lambda x, y: y

    for row in cursor.execute(query, bindings):
        rows.append(list(row))

    if colnames:
        res.append(format_query_table._format_table(colnames, rows, **kwargs))  # type: ignore[attr-defined]

    if len(res) == 1:
        return res[0]
    return "\n".join(res)


def _format_table(
    colnames: list[str],
    rows: list[apsw.SQLiteValues],
    colour: bool,
    quote: bool,
    string_sanitize: Callable[[str], str] | Literal[0] | Literal[1] | Literal[2],
    binary: Callable[[bytes], str],
    null: str,
    truncate: int,
    truncate_val: str,
    text_width: int,
    use_unicode: bool,
) -> str:
    "Internal table formatter"
    if colour:
        c: Callable[[int], str] = lambda v: f"\x1b[{v}m"
        colours = {
            # inverse
            "header_start": c(7) + c(1),
            "header_end": c(27) + c(22),
            # red
            "null_start": c(31),
            "null_end": c(39),
            # yellow
            "string_start": c(33),
            "string_end": c(39),
            # blue
            "blob_start": c(34),
            "blob_end": c(39),
            # magenta
            "number_start": c(35),
            "number_end": c(39),
            # green
            "pyobject_start": c(32),
            "pyobject_end": c(39),
        }

        def colour_wrap(text: str, kind: type | None, header: bool = False) -> str:
            if header:
                return colours["header_start"] + text + colours["header_end"]
            if kind is str:
                tkind = "string"
            elif kind is bytes:
                tkind = "blob"
            elif kind in (int, float):
                tkind = "number"
            elif kind is NoneType:
                tkind = "null"
            else:
                tkind = "pyobject"
            return colours[f"{tkind}_start"] + text + colours[f"{tkind}_end"]

    else:
        colours = {}

        def colour_wrap(text: str, kind: type | None, header: bool = False) -> str:
            return text

    colwidths = [max(len(v) for v in c.splitlines()) for c in colnames]
    coltypes: list[set[type]] = [set() for _ in colnames]

    # type, measure and stringize each cell
    for row in rows:
        for i, cell in enumerate(row):
            coltypes[i].add(type(cell))
            if isinstance(cell, str):
                if callable(string_sanitize):
                    cell = string_sanitize(cell)
                else:
                    if string_sanitize in (0, 1):
                        cell = cell.replace("\\", "\\\\")
                        cell = cell.replace("\r\n", "\n")
                        cell = cell.replace("\r", " ")
                        cell = cell.replace("\t", " ")
                        cell = cell.replace("\f", "")
                        cell = cell.replace("\v", "")
                        cell = cell.replace("\0", "\\0")

                    if string_sanitize == 1:

                        def repl(s):
                            if s[0] in string.printable:
                                return s[0]
                            return "{" + (apsw.unicode.codepoint_name(s[0]) or f"\\x{ord(s[0]):02}") + "}"

                        cell = re.sub(".", repl, cell)

                    if string_sanitize == 2:

                        def repl(s):
                            if s[0] in string.printable and s[0] not in string.whitespace:
                                return s[0]
                            return "."

                        cell = re.sub(".", repl, cell)
            if quote:
                if isinstance(cell, (NoneType, str, float, int, bytes)):
                    val = apsw.format_sql_value(cell)
                else:
                    val = repr(cell)
            else:
                if isinstance(cell, str):
                    val = cell
                elif isinstance(cell, (float, int)):
                    val = str(cell)
                elif isinstance(cell, bytes):
                    val = binary(cell)
                elif cell is None:
                    val = null
                else:
                    val = str(cell)
            assert isinstance(val, str), f"expected str not {val!r}"

            # cleanup lines
            lines: list[str] = []
            for line in apsw.unicode.split_lines(val):
                if apsw.unicode.text_width(line) < 0:
                    line = "".join((c if apsw.unicode.text_width(c) >= 0 else "?") for c in line)
                lines.append(line)
            val = "\n".join(lines)

            if truncate > 0 and apsw.unicode.grapheme_length(val) > truncate:
                val = apsw.unicode.grapheme_substr(val, 0, truncate) + truncate_val
            row[i] = (val, type(cell))  # type: ignore[index]
            colwidths[i] = max(
                colwidths[i], max(apsw.unicode.text_width(line) for line in apsw.unicode.split_lines(val)) if val else 0
            )

    ## work out widths
    # we need a space each side of a cell plus a cell separator hence 3
    # "| cell " and another for the final "|"
    total_width: Callable[[], int] = lambda: sum(w + 3 for w in colwidths) + 1

    # proportionally reduce column widths
    victim = len(colwidths) - 1
    while total_width() > text_width:
        # if all are 1 then we can't go any narrower
        if sum(colwidths) == len(colwidths):
            break

        # this makes wider columns take more of the width blame
        proportions = [w * 1.1 / total_width() for w in colwidths]

        excess = total_width() - text_width

        # start with widest columns first
        for _, i in reversed(sorted((proportions[n], n) for n in range(len(colwidths)))):
            w = colwidths[i]
            w -= int(proportions[i] * excess)
            w = max(1, w)
            colwidths[i] = w
            new_excess = total_width() - text_width
            # narrower than needed?
            if new_excess < 0:
                colwidths[i] -= new_excess
                break

        # if still too wide, then punish victim
        if total_width() > text_width:
            if colwidths[victim] > 1:
                colwidths[victim] -= 1
            victim -= 1
            if victim < 0:
                victim = len(colwidths) - 1

    # can't fit
    if total_width() > text_width:
        raise ValueError(
            f"Results can't be fit in text width {text_width} even with 1 wide columns - at least {total_width()} width is needed"
        )

    # break headers and cells into lines
    def wrap(text: str, width: int) -> list[str]:
        return list(apsw.unicode.text_wrap(text, width))

    colnames = [wrap(colnames[i], colwidths[i]) for i in range(len(colwidths))]  # type: ignore
    for row in rows:
        for i, (text, t) in enumerate(row):  # type: ignore[misc]
            row[i] = (wrap(text, colwidths[i]), t)  # type: ignore

    ## output
    # are any cells more than one line?
    multiline = False
    for row in rows:
        if multiline:
            break
        if any(len(cell[0]) > 1 for cell in row):
            multiline = True
            break

    out_lines: list[str] = []

    def do_bar(chars: str) -> None:
        line = chars[0]
        for i, w in enumerate(colwidths):
            line += chars[1] * (w + 2)
            if i == len(colwidths) - 1:
                line += chars[3]
            else:
                line += chars[2]
        out_lines.append(line)

    def do_row(row, sep: str, *, centre: bool = False, header: bool = False) -> None:
        for n in range(max(len(cell[0]) for cell in row)):
            line = sep
            for i, (cell, t) in enumerate(row):
                text = cell[n] if n < len(cell) else ""
                text = " " + text.rstrip() + " "
                lt = apsw.unicode.text_width(text)
                extra = " " * max(colwidths[i] + 2 - lt, 0)
                if centre:
                    lpad = extra[: len(extra) // 2]
                    rpad = extra[len(extra) // 2 :]
                else:
                    lpad = ""
                    rpad = extra
                if header:
                    text = colour_wrap(lpad + text + rpad, None, header=True)
                else:
                    text = lpad + colour_wrap(text, t) + rpad
                line += text + sep
            out_lines.append(line)

    do_bar("┌─┬┐" if use_unicode else "+-++")
    do_row([(c, None) for c in colnames], "│" if use_unicode else "|", centre=True, header=True)

    # rows
    if rows:
        for row in rows:
            if multiline:
                do_bar("├─┼┤" if use_unicode else "+-++")
            do_row(row, "│" if use_unicode else "|")

    do_bar("└─┴┘" if use_unicode else "+-++")

    return "\n".join(out_lines) + "\n"


format_query_table._format_table = _format_table  # type: ignore[attr-defined]
del _format_table


class VTColumnAccess(enum.Enum):
    "How the column value is accessed from a row, for :meth:`make_virtual_module`"

    By_Index = enum.auto()
    "By number like with tuples and lists - eg :code:`row[3]`"
    By_Name = enum.auto()
    "By name like with dicts - eg :code:`row['quantity']`"
    By_Attr = enum.auto()
    "By attribute like with :mod:`dataclasses` - eg :code:`row.quantity`"


def get_column_names(row: Any) -> tuple[Sequence[str], VTColumnAccess]:
    r"""
    Works out column names and access given an example row

    *row* can be an instance of a row, or the class used to make
    one (eg a :mod:`dataclass <dataclasses>`)

    .. list-table::
        :header-rows: 1
        :widths: auto

        * - Type
          - Access
          - Column names From
        * - :external:func:`dataclasses.is_dataclass`
          - :attr:`VTColumnAccess.By_Attr`
          - :func:`dataclasses.fields`
        * - :func:`isinstance <isinstance>`\(:class:`tuple`) and :func:`hasattr <hasattr>`\(:code:`"_fields"`) - eg :func:`~collections.namedtuple`
          - :attr:`VTColumnAccess.By_Index`
          - :code:`row._fields`
        * - :func:`hasattr <hasattr>`\(:code:`"__match_args__"`)
          - :attr:`VTColumnAccess.By_Attr`
          - :code:`row.__match_args__` (if not empty)
        * - :func:`isinstance <isinstance>`\(:class:`dict`)
          - :attr:`VTColumnAccess.By_Name`
          - :meth:`dict.keys`
        * - :func:`isinstance <isinstance>`\(:class:`tuple`\)
          - :attr:`VTColumnAccess.By_Index`
          - :code:`columnX` where *X* is zero up to :func:`len <len>`\(:code:`row`)


    Example usage:

    .. code-block::

        def method(arg1, arg2):
            yield {"fruit": "orange", "price": 17, "quantity": 2}

        example_row = next(method(0, 10))
        method.columns, method.column_access = apsw.ext.get_column_names(example_row)

    """
    if is_dataclass(row):
        return tuple(field.name for field in dataclasses.fields(row)), VTColumnAccess.By_Attr
    if isinstance(row, tuple) and hasattr(row, "_fields"):
        return row._fields, VTColumnAccess.By_Index
    if getattr(row, "__match_args__", None):
        return row.__match_args__, VTColumnAccess.By_Attr
    if isinstance(row, dict):
        return tuple(row.keys()), VTColumnAccess.By_Name
    if isinstance(row, tuple):
        return tuple(f"column{x}" for x in range(len(row))), VTColumnAccess.By_Index
    raise TypeError(f"Can't figure out columns for {row}")


def make_virtual_module(
    db: apsw.Connection,
    name: str,
    callable: Callable,
    *,
    eponymous: bool = True,
    eponymous_only: bool = False,
    repr_invalid: bool = False,
) -> None:
    """
    Registers a read-only virtual table module with *db* based on
    *callable*.  The *callable* must have an attribute named *columns*
    with a list of column names, and an attribute named *column_access*
    with a :class:`VTColumnAccess` saying how to access columns from a row.
    See :meth:`get_column_names` for easily figuring that out.

    The goal is to make it very easy to turn a Python function into a
    virtual table.  For example the following Python function::

      def gendata(start_id, end_id=1000, include_system=False):
          yield (10, "2020-10-21", "readme.txt)
          yield (11, "2019-05-12", "john.txt)

      gendata.columns = ("user_id", "start_date", "file_name")
      gendata.column_access = VTColumnAccess.By_Index

    Will generate a table declared like this, using `HIDDEN
    <https://sqlite.org/vtab.html#hidden_columns_in_virtual_tables>`__
    for parameters:

    .. code-block:: sql

        CREATE TABLE table_name(user_id,
                                start_date,
                                file_name,
                                start_id HIDDEN,
                                end_id HIDDEN,
                                include_system HIDDEN);

    :func:`inspect.signature` is used to discover parameter names.

    Positional parameters to *callable* come from the table definition.

    .. code-block:: sql

      SELECT * from table_name(1, 100, 1);

    Keyword arguments come from WHERE clauses.

    .. code-block:: sql

      SELECT * from table_name(1) WHERE
            include_system=1;

    :func:`iter` is called on *callable* with each iteration expected
    to return the next row.  That means *callable* can return its data
    all at once (eg a list of rows), or *yield* them one row at a
    time.  The number of columns must always be the same, no matter
    what the parameter values.

    :param eponymous: Lets you use the *name* as a table name without
             having to create a virtual table
    :param eponymous_only: Can only reference as a table name
    :param repr_invalid: If *True* then values that are not valid
       :class:`apsw.SQLiteValue` will be converted to a string using
       :func:`repr`

    See the :ref:`example <example_virtual_tables>`

    Advanced
    ++++++++

    The *callable* may also have an attribute named *primary_key*.
    By default the :func:`id` of each row is used as the primary key.
    If present then it must be a column number to use as the primary
    key.  The contents of that column must be unique for every row.

    If you specify a parameter to the table and in WHERE, or have
    non-equality for WHERE clauses of parameters then the query will
    fail with :class:`apsw.SQLError` and a message from SQLite of
    "no query solution"
    """

    class Module:
        def __init__(
            self,
            callable: Callable,
            columns: tuple[str],
            column_access: VTColumnAccess,
            primary_key: int | None,
            repr_invalid: bool,
        ):
            self.columns = columns
            self.callable: Callable = callable
            if not isinstance(column_access, VTColumnAccess):
                raise ValueError(f"Expected column_access to be {VTColumnAccess} not {column_access!r}")
            self.column_access = column_access
            self.parameters: list[str] = []
            # These are as representable as SQLiteValue and are not used
            # for the actual call.
            self.defaults: list[apsw.SQLiteValue] = []
            for p, v in inspect.signature(callable).parameters.items():
                self.parameters.append(p)
                default = None if v.default is inspect.Parameter.empty else v.default
                try:
                    apsw.format_sql_value(default)
                except TypeError:
                    default = repr(default)
                self.defaults.append(default)

            both = set(self.columns) & set(self.parameters)
            if both:
                raise ValueError(f"Same name in columns and in paramters: {both}")

            self.all_columns: tuple[str] = tuple(self.columns) + tuple(self.parameters)  # type: ignore[assignment]
            self.primary_key = primary_key
            if self.primary_key is not None and not (0 <= self.primary_key < len(self.columns)):
                raise ValueError(f"{self.primary_key!r} should be None or a column number < {len(self.columns)}")
            self.repr_invalid = repr_invalid
            column_defs = ""
            for i, c in enumerate(self.columns):
                if column_defs:
                    column_defs += ", "
                column_defs += f"[{c}]"
                if self.primary_key == i:
                    column_defs += " PRIMARY KEY"
            for p in self.parameters:
                column_defs += f",[{p}] HIDDEN"

            self.schema = f"CREATE TABLE ignored({column_defs})"
            if self.primary_key is not None:
                self.schema += " WITHOUT rowid"

        def Create(self, db, modulename, dbname, tablename, *args: apsw.SQLiteValue) -> tuple[str, apsw.VTTable]:
            if len(args) > len(self.parameters):
                raise ValueError(f"Too many parameters: parameters accepted are {' '.join(self.parameters)}")

            param_values = dict(zip(self.parameters, args))

            return self.schema, self.Table(self, param_values)  # type: ignore[return-value]

        Connect = Create

        class Table:
            def __init__(self, module: Module, param_values: dict[str, apsw.SQLiteValue]):
                self.module = module
                self.param_values = param_values

            def BestIndexObject(self, o: apsw.IndexInfo) -> bool:
                idx_str: list[str] = []
                param_start = len(self.module.columns)
                for c in range(o.nConstraint):
                    if o.get_aConstraint_iColumn(c) >= param_start:
                        if not o.get_aConstraint_usable(c):
                            continue
                        if o.get_aConstraint_op(c) != apsw.SQLITE_INDEX_CONSTRAINT_EQ:
                            return False
                        o.set_aConstraintUsage_argvIndex(c, len(idx_str) + 1)
                        o.set_aConstraintUsage_omit(c, True)
                        n = self.module.all_columns[o.get_aConstraint_iColumn(c)]
                        # a parameter could be a function parameter and where
                        #    generate_series(7) where start=8
                        # the order they appear in IndexInfo is random so we
                        # have to abort the query because a random one would
                        # prevail
                        if n in idx_str:
                            return False
                        idx_str.append(n)

                o.idxStr = ",".join(idx_str)
                # say there are a huge number of rows so the query planner avoids us
                o.estimatedRows = 2147483647
                return True

            def Open(self) -> Module.Cursor:
                return self.module.Cursor(self.module, self.param_values)

            def Disconnect(self) -> None:
                pass

            Destroy = Disconnect

        class Cursor:
            def __init__(self, module: Module, param_values: dict[str, apsw.SQLiteValue]):
                self.module = module
                self.param_values = param_values
                self.iterating: Iterator[apsw.SQLiteValues] | None = None
                self.current_row: Any = None
                self.columns = module.columns
                self.repr_invalid = module.repr_invalid
                self.num_columns = len(self.columns)
                self.access = self.module.column_access
                col_func = f"_Column_{self.access.name}"
                f = getattr(self, col_func, self.Column)
                if self.repr_invalid:
                    setattr(self, "Column", self._Column_repr_invalid)
                    setattr(self, "_Column_get", f)
                else:
                    setattr(self, "Column", f)

            def Filter(self, idx_num: int, idx_str: str, args: tuple[apsw.SQLiteValue]) -> None:
                params: dict[str, apsw.SQLiteValue] = self.param_values.copy()
                params.update(zip(idx_str.split(","), args))
                self.iterating = iter(self.module.callable(**params))
                # proactively advance so we can tell if eof
                self.Next()

                self.hidden_values: list[apsw.SQLiteValue] = self.module.defaults[:]
                for k, v in params.items():
                    self.hidden_values[self.module.parameters.index(k)] = v

            def Eof(self) -> bool:
                return self.iterating is None

            def Close(self) -> None:
                if self.iterating:
                    if hasattr(self.iterating, "close"):
                        self.iterating.close()
                    self.iterating = None

            def Column(self, which: int) -> apsw.SQLiteValue:
                # This is the specification/documentation for the custom
                # versions which should produce exactly the same output
                if which >= self.num_columns:
                    return self.hidden_values[which - self.num_columns]
                if self.access is VTColumnAccess.By_Index:
                    v = self.current_row[which]
                elif self.access is VTColumnAccess.By_Name:
                    v = self.current_row[self.columns[which]]
                elif self.access is VTColumnAccess.By_Attr:
                    v = getattr(self.current_row, self.columns[which])
                if self.repr_invalid and v is not None and not isinstance(v, (int, float, str, bytes)):
                    v = repr(v)
                return v  # type: ignore[no-any-return]

            def _Column_repr_invalid(self, which: int) -> apsw.SQLiteValue:
                v = self._Column_get(which)  # type: ignore[attr-defined]
                return v if v is None or isinstance(v, (int, float, str, bytes)) else repr(v)

            def _Column_By_Attr(self, which: int) -> apsw.SQLiteValue:
                return (
                    getattr(self.current_row, self.columns[which])
                    if which < self.num_columns
                    else self.hidden_values[which - self.num_columns]
                )

            def _Column_By_Name(self, which: int) -> apsw.SQLiteValue:
                return (
                    self.current_row[self.columns[which]]
                    if which < self.num_columns
                    else self.hidden_values[which - self.num_columns]
                )

            def _Column_By_Index(self, which: int) -> apsw.SQLiteValue:
                return (
                    self.current_row[which]
                    if which < self.num_columns
                    else self.hidden_values[which - self.num_columns]
                )

            def Next(self) -> None:
                try:
                    self.current_row = next(self.iterating)  # type: ignore[arg-type]
                except StopIteration:
                    if hasattr(self.iterating, "close"):
                        self.iterating.close()  # type: ignore[union-attr]
                    self.iterating = None

            def Rowid(self):
                if self.module.primary_key is None:
                    return id(self.current_row)
                return self.Column(self.module.primary_key)

    mod = Module(
        callable,
        callable.columns,  # type: ignore[attr-defined]
        callable.column_access,  # type: ignore[attr-defined]
        getattr(callable, "primary_key", None),
        repr_invalid,
    )

    # unregister any existing first
    db.create_module(name, None)
    db.create_module(
        name,
        mod,  # type: ignore[arg-type]
        use_bestindex_object=True,
        eponymous=eponymous,
        eponymous_only=eponymous_only,
        read_only=True,
    )


def generate_series_sqlite(start=None, stop=0xFFFFFFFF, step=1):
    """Behaves like SQLite's `generate_series <https://sqlite.org/series.html>`__

    Only integers are supported.  If *step* is negative
    then values are generated from *stop* to *start*

    To use::

        apsw.ext.make_virtual_module(db,
                                     "generate_series",
                                     apsw.ext.generate_series_sqlite)


        db.execute("SELECT value FROM generate_series(1, 10))

    .. seealso::

        :meth:`generate_series`

    """
    if start is None:
        raise ValueError("You must specify a value for start")
    istart = int(start)
    istop = int(stop)
    istep = int(step)
    if istart != start or istop != stop or istep != step:
        raise TypeError("generate_series_sqlite only works with integers")
    if step == 0:
        step = 1
    if step > 0:
        while start <= stop:
            yield (start,)
            start += step
    elif step < 0:
        while stop >= start:
            yield (stop,)
            stop += step


generate_series_sqlite.columns = ("value",)  # type: ignore[attr-defined]
generate_series_sqlite.column_access = VTColumnAccess.By_Index  # type: ignore[attr-defined]
generate_series_sqlite.primary_key = 0  # type: ignore[attr-defined]


def generate_series(start, stop, step=None):
    """Behaves like Postgres and SQL Server

    `Postgres doc
    <https://www.postgresql.org/docs/current/functions-srf.html>`__
    `SQL server doc
    <https://learn.microsoft.com/en-us/sql/t-sql/functions/generate-series-transact-sql>`__

    Operates on floating point as well as integer.  If step is not
    specified then it is 1 if *stop* is greater than *start* and -1 if
    *stop* is less than *start*.

    To use::

        apsw.ext.make_virtual_module(db,
                                     "generate_series",
                                     apsw.ext.generate_series)

        db.execute("SELECT value FROM generate_series(1, 10))

    .. seealso::

        :meth:`generate_series_sqlite`

    """
    if step is None:
        if stop > start:
            step = 1
        else:
            step = -1

    if step > 0:
        while start <= stop:
            yield (start,)
            start += step
    elif step < 0:
        while start >= stop:
            yield (start,)
            start += step
    else:
        raise ValueError("step of zero is not valid")


generate_series.columns = ("value",)  # type: ignore[attr-defined]
generate_series.column_access = VTColumnAccess.By_Index  # type: ignore[attr-defined]
generate_series.primary_key = 0  # type: ignore[attr-defined]


def query_info(
    db: apsw.Connection,
    query: str,
    bindings: apsw.Bindings | None = None,
    *,
    prepare_flags: int = 0,
    actions: bool = False,
    expanded_sql: bool = False,
    explain: bool = False,
    explain_query_plan: bool = False,
) -> QueryDetails:
    """Returns information about the query, without running it.

    `bindings` can be `None` if you want to find out what the bindings
    for the query are.

    Set the various parameters to `True` if you also want the
    actions, expanded_sql, explain, query_plan etc filled in.

    See the :ref:`example <example_query_details>`.
    """
    res: dict[str, Any] = {"actions": None, "query_plan": None, "explain": None}

    # what we use in queries
    query_bindings = bindings if bindings is not None else apsw._null_bindings

    def tracer(cursor: apsw.Cursor, first_query: str, bindings: apsw.Bindings | None):
        nonlocal res
        res.update(
            {
                "first_query": first_query,
                "query": query,
                "bindings": bindings,
                "bindings_count": cursor.bindings_count,
                "bindings_names": cursor.bindings_names,
                "is_explain": cursor.is_explain,
                "is_readonly": cursor.is_readonly,
                "has_vdbe": cursor.has_vdbe,
                "description": cursor.get_description(),
                "description_full": None,
            }
        )
        if hasattr(cursor, "description_full"):
            res["description_full"] = cursor.description_full

        assert query == first_query or query.startswith(first_query)
        res["query_remaining"] = query[len(first_query) :] if len(query) > len(first_query) else None
        res["expanded_sql"] = cursor.expanded_sql if expanded_sql else None
        return False

    actions_taken = []

    def auther(code, third, fourth, dbname, trigview):
        a = {"action": code, "action_name": apsw.mapping_authorizer_function[code]}
        if dbname:
            a["database_name"] = dbname
        if trigview:
            a["trigger_or_view"] = trigview

        # this block corresponds to the table at https://sqlite.org/c3ref/c_alter_table.html
        for op, thirdname, fourthname in (
            (apsw.SQLITE_CREATE_INDEX, "index_name", "table_name"),
            (apsw.SQLITE_CREATE_TABLE, "table_name", None),
            (apsw.SQLITE_CREATE_TEMP_INDEX, "index_name", "table_name"),
            (apsw.SQLITE_CREATE_TEMP_TABLE, "table_name", None),
            (apsw.SQLITE_CREATE_TEMP_TRIGGER, "trigger_name", "table_name"),
            (apsw.SQLITE_CREATE_TEMP_VIEW, "view_name", None),
            (apsw.SQLITE_CREATE_TRIGGER, "trigger_name", "table_name"),
            (apsw.SQLITE_CREATE_VIEW, "view_name", None),
            (apsw.SQLITE_DELETE, "table_name", None),
            (apsw.SQLITE_DROP_INDEX, "index_name", "table_name"),
            (apsw.SQLITE_DROP_TABLE, "table_name", None),
            (apsw.SQLITE_DROP_TEMP_INDEX, "index_name", "table_name"),
            (apsw.SQLITE_DROP_TEMP_TABLE, "table_name", None),
            (apsw.SQLITE_DROP_TEMP_TRIGGER, "trigger_name", "table_name"),
            (apsw.SQLITE_DROP_TEMP_VIEW, "view_name", None),
            (apsw.SQLITE_DROP_TRIGGER, "trigger_name", "table_name"),
            (apsw.SQLITE_DROP_VIEW, "view_name", None),
            (apsw.SQLITE_INSERT, "table_name", None),
            (apsw.SQLITE_PRAGMA, "pragma_name", "pragma_value"),
            (apsw.SQLITE_READ, "table_name", "column_name"),
            (apsw.SQLITE_SELECT, None, None),
            (apsw.SQLITE_TRANSACTION, "operation", None),
            (apsw.SQLITE_UPDATE, "table_name", "column_name"),
            (apsw.SQLITE_ATTACH, "file_name", None),
            (apsw.SQLITE_DETACH, "database_name", None),
            (apsw.SQLITE_ALTER_TABLE, "database_name", "table_name"),
            (apsw.SQLITE_REINDEX, "index_name", None),
            (apsw.SQLITE_ANALYZE, "table_name", None),
            (apsw.SQLITE_CREATE_VTABLE, "table_name", "module_name"),
            (apsw.SQLITE_DROP_VTABLE, "table_name", "module_name"),
            (apsw.SQLITE_FUNCTION, None, "function_name"),
            (apsw.SQLITE_SAVEPOINT, "operation", None),
            (apsw.SQLITE_RECURSIVE, None, None),
        ):
            if code == op:
                if thirdname is not None:
                    a[thirdname] = third
                if fourthname is not None:
                    a[fourthname] = fourth
                break
        else:
            raise ValueError(f"Unknown authorizer code {code}")
        actions_taken.append(QueryAction(**a))
        return apsw.SQLITE_OK

    cur = db.cursor()
    cur.exec_trace = tracer
    if actions:
        orig_authorizer = db.authorizer
        db.authorizer = auther
    try:
        cur.execute(
            query,
            query_bindings,
            can_cache=False,
            prepare_flags=prepare_flags,
        )
    except apsw.ExecTraceAbort:
        pass
    finally:
        if actions:
            db.authorizer = orig_authorizer
    cur.exec_trace = None
    if actions:
        res["actions"] = actions_taken

    if explain and not res["is_explain"]:
        vdbe: list[VDBEInstruction] = []
        for row in cur.execute(res["first_query"], query_bindings, explain=1):
            vdbe.append(
                VDBEInstruction(**dict((v[0][0], v[1]) for v in zip(cur.get_description(), row) if v[1] is not None))
            )
        res["explain"] = vdbe

    if explain_query_plan and not res["is_explain"]:
        subn = "sub"
        byid: Any = {0: {"detail": "QUERY PLAN"}}

        for row in cur.execute(res["first_query"], query_bindings, explain=2):
            node = dict((v[0][0], v[1]) for v in zip(cur.get_description(), row) if v[0][0] != "notused")
            assert len(node) == 3  # catch changes in returned format
            parent: list[str | dict[str, Any]] = byid[node["parent"]]
            if subn not in parent:
                parent[subn] = [node]  # type: ignore[call-overload]
            else:
                parent[subn].append(node)  # type: ignore[call-overload]
            byid[node["id"]] = node

        def flatten(node: Any) -> dict[str, Any]:
            res = {"detail": node["detail"]}
            if subn in node:
                res[subn] = [QueryPlan(**flatten(child)) for child in node[subn]]
            return res

        res["query_plan"] = QueryPlan(**flatten(byid[0]))

    return QueryDetails(**res)


@dataclass
class QueryDetails:
    "A :mod:`dataclass <dataclasses>` that provides detailed information about a query, returned by :func:`query_info`"

    query: str
    "Original query provided"
    bindings: apsw.Bindings | None
    "Bindings provided"
    first_query: str
    "The first statement present in query"
    query_remaining: str | None
    "Query text after the first one if multiple were in query, else None"
    is_explain: int
    ":attr:`Cursor.is_explain <apsw.Cursor.is_explain>`"
    is_readonly: bool
    ":attr:`Cursor.is_readonly <apsw.Cursor.is_readonly>`"
    has_vdbe: bool
    ":attr:`Cursor.has_vdbe <apsw.Cursor.has_vdbe>`"
    bindings_count: int
    "How many :attr:`bindings <apsw.Cursor.bindings_count>` are in the query"
    bindings_names: tuple[str | None]
    "The :attr:`names <apsw.Cursor.bindings_names>`.  The leading marker (``?:@$``) is omitted"
    description: tuple[tuple[str, str], ...]
    ":meth:`Cursor.get_description <apsw.Cursor.get_description>`"
    description_full: tuple[tuple[str, str, str, str, str], ...] | None
    ":attr:`Cursor.description_full <apsw.Cursor.description_full>`"
    expanded_sql: str | None
    ":attr:`Cursor.expanded_sql <apsw.Cursor.expanded_sql>`"
    actions: list[QueryAction] | None
    """A list of the actions taken by the query, as discovered via
    :attr:`Connection.authorizer <apsw.Connection.authorizer>`"""
    explain: list[VDBEInstruction] | None
    """A list of instructions of the `internal code <https://sqlite.org/opcode.html>`__
    used by SQLite to execute the query"""
    query_plan: QueryPlan | None
    """The steps taken against tables and indices `described here <https://sqlite.org/eqp.html>`__"""


@dataclass
class QueryAction:
    """A :mod:`dataclass <dataclasses>` that provides information about one action taken by a query

    Depending on the action, only a subset of the fields will have non-None values"""

    action: int
    """`Authorizer code <https://sqlite.org/c3ref/c_alter_table.html>`__ (also present
    in :attr:`apsw.mapping_authorizer_function`)"""
    action_name: str
    """The string corresponding to the action.  For example `action` could be `21` in which
    case `action_name` will be `SQLITE_SELECT`"""

    column_name: str | None = None
    database_name: str | None = None
    "`main`, `temp`, the name in `ATTACH <https://sqlite.org/lang_attach.html>`__"
    file_name: str | None = None
    function_name: str | None = None
    module_name: str | None = None
    operation: str | None = None
    pragma_name: str | None = None
    pragma_value: str | None = None
    table_name: str | None = None
    trigger_name: str | None = None
    trigger_or_view: str | None = None
    """This action is happening due to a trigger or view, and not
    directly expressed in the query itself"""
    view_name: str | None = None


@dataclass
class QueryPlan:
    "A :mod:`dataclass <dataclasses>` for one step of a query plan"

    detail: str
    "Description of this step"
    sub: list[QueryPlan] | None = None
    "Steps that run inside this one"


@dataclass
class VDBEInstruction:
    "A :mod:`dataclass <dataclasses>` representing one instruction and its parameters"

    addr: int
    "Address of this opcode.  It will be the target of goto, loops etc"
    opcode: str
    "The instruction"
    comment: str | None = None
    "Additional human readable information"
    p1: int | None = None
    "First opcode parameter"
    p2: int | None = None
    "Second opcode parameter"
    p3: int | None = None
    "Third opcode parameter"
    p4: int | None = None
    "Fourth opcode parameter"
    p5: int | None = None
    "Fifth opcode parameter"


@dataclass
class DatabaseFileInfo:
    """Information about the main database file returned by :meth:`dbinfo`

    See `file format description <https://www.sqlite.org/fileformat.html#the_database_header>`__"""

    filename: str
    "database filena name"
    header: bytes
    "Header string"
    page_size: int
    "The database page size in bytes"
    write_format: int
    "File format write version. 1 for legacy; 2 for WAL"
    read_format: int
    "File format read version. 1 for legacy; 2 for WAL."
    reserved_bytes: int
    'Bytes of unused "reserved" space at the end of each page. Usually 0'
    file_change_counter: int
    "File change counter"
    page_count: int
    "Size of the database file in pages"
    freelist_pages: int
    "Total number of freelist pages"
    schema_cookie: int
    "The schema cookie"
    schema_format: int
    "The schema format number. Supported schema formats are 1, 2, 3, and 4"
    default_cache_size: int
    "The schema format number. Supported schema formats are 1, 2, 3, and 4"
    autovacuum_top_root: int
    "The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise"
    text_encoding: str
    "The database text encoding"
    user_version: int
    'The "user version" as read and set by the user_version pragma.'
    incremental_vacuum: bool
    "True (non-zero) for incremental-vacuum mode. False (zero) otherwise."
    application_id: int
    'The "Application ID" set by PRAGMA application_id'
    version_valid_for: int
    "The version-valid-for number."
    sqlite_version: int
    "SQLite version that lost wrote"


@dataclass
class JournalFileInfo:
    """Information about the rollback journal returned by :meth:`dbinfo`

    See the `file format description <https://www.sqlite.org/fileformat2.html#the_rollback_journal>`__"""

    filename: str
    "journal file name"
    header: bytes
    "Header string"
    header_valid: bool
    "If the header is the expected bytes"
    page_count: int
    'The "Page Count" - The number of pages in the next segment of the journal, or -1 to mean all content to the end of the file'
    random_nonce: int
    "A random nonce for the checksum"
    initial_pages: int
    "Initial size of the database in pages"
    sector_size: int
    "Size of a disk sector assumed by the process that wrote this journal"
    page_size: int
    "Size of pages in this journal"


@dataclass
class WALFileInfo:
    """Information about the rollback journal returned by :meth:`dbinfo`

    See the `file format description <https://www.sqlite.org/fileformat2.html#wal_file_format>`__"""

    filename: str
    "WAL file name"
    magic_number: int
    "Magic number"
    format_version: int
    "File format version. Currently 3007000"
    page_size: int
    "Database page size"
    checkpoint_sequence_number: int
    "Checkpoint sequence number"
    salt_1: int
    "Salt-1: random integer incremented with each checkpoint"
    salt_2: int
    "Salt-2: a different random number for each checkpoint"
    checksum_1: int
    "Checksum-1: First part of a checksum on the first 24 bytes of header"
    checksum_2: int
    "Checksum-2: Second part of the checksum on the first 24 bytes of header"
