# Provides various useful routines

from __future__ import annotations
import collections, collections.abc
import sys
if sys.version_info >= (3, 10):
    from types import NoneType
else:
    NoneType = type(None)

import dataclasses
from dataclasses import dataclass, make_dataclass, is_dataclass

from typing import Optional, Tuple, Union, List, Any, Dict, Callable, Sequence, TextIO
import types

import functools
import abc
import enum
import inspect
import unicodedata
import logging
import traceback
import re
import string
import textwrap
import apsw

try:
    from keyword import iskeyword as _iskeyword
except ImportError:
    # From https://docs.python.org/3/reference/lexical_analysis.html#keywords
    _keywords = set("""
    False      await      else       import     pass
    None       break      except     in         raise
    True       class      finally    is         return
    and        continue   for        lambda     try
    as         def        from       nonlocal   while
    assert     del        global     not        with
    async      elif       if         or         yield
    """.split())

    def _iskeyword(s: str) -> bool:
        return s in _keywords


class DataClassRowFactory:
    """Returns each row as a :mod:`dataclass <dataclasses>`, accessible by column name.

    To use set an instance as :attr:`Connection.rowtrace
    <apsw.Connection.rowtrace>` to affect all :class:`cursors
    <apsw.Cursor>`, or on a specific cursor::

        connection.rowtrace = apsw.ext.DataClassRowFactory()
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

    def __init__(self, *, rename: bool = True, dataclass_kwargs: Optional[Dict[str, Any]] = None):
        self.dataclass_kwargs = dataclass_kwargs or {}
        self.rename = rename

    @functools.lru_cache(maxsize=16)
    def get_dataclass(self, description: Tuple[Tuple[str, str], ...]) -> Tuple[Any, Tuple[str, ...]]:
        """Returns dataclass and tuple of (potentially renamed) column names

        The dataclass is what is returned for each row with that
        :meth:`description <apsw.Cursor.getdescription>`

        This method caches it results.
        """
        names = [d[0] for d in description]
        if self.rename:
            new_names: List[str] = []
            for i, n in enumerate(names):
                if n.isidentifier() and not _iskeyword(n) and n not in new_names:
                    new_names.append(n)
                else:
                    new_names.append(f"_{ i }")
            names = new_names
        types = [self.get_type(d[1]) for d in description]

        kwargs = self.dataclass_kwargs.copy()
        if "namespace" not in kwargs:
            kwargs["namespace"] = {}
        kwargs["namespace"]["__description__"] = description

        # some magic to make the reported classnames different
        suffix = (".%06X" % hash(repr(description)))[:7]

        return make_dataclass(f"{ self.__class__.__name__ }{ suffix }", zip(names, types), **kwargs), tuple(names)

    def get_type(self, t: Optional[str]) -> Any:
        """Returns the `type hint <https://docs.python.org/3/library/typing.html>`__ to use in the dataclass based on the type in the :meth:`description <apsw.Cursor.getdescription>`

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
        dc, column_names = self.get_dataclass(cursor.getdescription())
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
    """Provides cursors that can convert objects into one of the types supported by SQLite. or back from SQLite

    :param metaclass: Which metaclass to consider as conversion capable
    """

    def __init__(self, abstract_base_class: abc.ABCMeta = SQLiteTypeAdapter):
        self.abstract_base_class = abstract_base_class
        # to sqlite value
        self.adapters: Dict[type, Callable[[Any], apsw.SQLiteValue]] = {}
        # from sqlite value
        self.converters: Dict[str, Callable[[apsw.SQLiteValue], Any]] = {}

    def register_adapter(self, klass: type, callable: Callable[[Any], apsw.SQLiteValue]) -> None:
        """Registers a callable that converts from `klass` to one of the supported SQLite types"""
        self.adapters[klass] = callable

    def register_converter(self, name: str, callable: Callable[[apsw.SQLiteValue], Any]) -> None:
        """Registers a callable that converts from a SQLite value"""
        self.converters[name] = callable

    def __call__(self, connection: apsw.Connection) -> TypeConverterCursor:
        "Returns a new :class:`cursor <apsw.Cursor>` for the `connection`"
        return TypesConverterCursorFactory.TypeConverterCursor(connection, self)

    def adapt_value(self, value: Any) -> apsw.SQLiteValue:
        "Returns SQLite representation of `value`"
        if isinstance(value, (int, bytes, str, NoneType, float)):
            return value
        if isinstance(value, self.abstract_base_class):
            return value.to_sqlite_value()
        adapter = self.adapters.get(type(value))
        if not adapter:
            raise TypeError(f"No adapter registered for type { type(value) }")
        return adapter(value)

    def convert_value(self, schematype: str, value: apsw.SQLiteValue) -> Any:
        "Returns Python object from schema type and SQLite value"
        converter = self.converters.get(schematype)
        if not converter:
            return value
        return converter(value)

    def wrap_bindings(self, bindings: Optional[apsw.Bindings]) -> Optional[apsw.Bindings]:
        "Wraps bindings that are supplied to underlying execute"
        if bindings is None:
            return None
        if isinstance(bindings, (dict, collections.abc.Mapping)):
            return TypesConverterCursorFactory.DictAdapter(self, bindings)
        # turn into a list since PySequence_Fast does that anyway
        return [self.adapt_value(v) for v in bindings]

    def wrap_sequence_bindings(self, sequenceofbindings: Sequence[apsw.Bindings]):
        for binding in sequenceofbindings:
            yield self.wrap_bindings(binding)

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
            self.rowtrace = self._rowtracer

        def _rowtracer(self, cursor: apsw.Cursor, values: apsw.SQLiteValues) -> Tuple[Any, ...]:
            return tuple(self.factory.convert_value(d[1], v) for d, v in zip(cursor.getdescription(), values))

        def execute(self,
                    statements: str,
                    bindings: Optional[apsw.Bindings] = None,
                    *,
                    can_cache: bool = True,
                    prepare_flags: int = 0) -> apsw.Cursor:
            """Executes the statements doing conversions on supplied and returned values

            See :meth:`apsw.Cursor.execute` for parameter details"""
            return super().execute(statements,
                                   self.factory.wrap_bindings(bindings),
                                   can_cache=can_cache,
                                   prepare_flags=prepare_flags)

        def executemany(self,
                        statements: str,
                        sequenceofbindings: Sequence[apsw.Bindings],
                        *,
                        can_cache: bool = True,
                        prepare_flags: int = 0) -> apsw.Cursor:
            """Executes the statements against each item in sequenceofbindings, doing conversions on supplied and returned values

            See :meth:`apsw.Cursor.executemany` for parameter details"""
            return super().executemany(statements,
                                       self.factory.wrap_sequence_bindings(sequenceofbindings),
                                       can_cache=can_cache,
                                       prepare_flags=prepare_flags)


def log_sqlite(*, level: int = logging.ERROR) -> None:
    """Send SQLite log messages to :mod:`logging`

    :param level: level to log at (default *logging.ERROR*)

    This must be called before doing any operations with SQLite, otherwise a :exc:`apsw.MisuseError`
    will be raised.  (This is a SQLite limitation, not APSW.)
    """

    def handler(errcode: int, message: str) -> None:
        err_str = apsw.mapping_result_codes[errcode & 255]
        extra = {"sqlite_code": errcode, "sqlite_code_name": err_str, "sqlite_message": message}
        logging.log(level,
                    "SQLITE_LOG: %s (%d) %s %s",
                    message,
                    errcode,
                    err_str,
                    apsw.mapping_extended_result_codes.get(errcode, ""),
                    extra=extra)

    apsw.config(apsw.SQLITE_CONFIG_LOG, handler)


def print_augmented_traceback(exc_type: type[BaseException],
                              exc_value: BaseException,
                              exc_traceback: types.TracebackType,
                              *,
                              file: Optional[TextIO] = None) -> None:
    """Prints a standard exception, but also includes the value of variables in each stack frame

    :param exc_type: The exception type
    :param exc_value: The exception value
    :param exc_traceback: Traceback for the exception
    :param file: (default ``sys.stderr``) Where the print goes
    """

    file = file or sys.stderr

    tbe = traceback.TracebackException(exc_type, exc_value, exc_traceback, capture_locals=True, compact=True)
    for line in tbe.format():
        print(line, file=file)


def index_info_to_dict(o: apsw.IndexInfo,
                       *,
                       column_names: Optional[List[str]] = None,
                       rowid_name: str = "__ROWID__") -> Dict[str, Any]:
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
        "nConstraint":
        o.nConstraint,
        "aConstraint": [{
            "iColumn": o.get_aConstraint_iColumn(n),
            "op": o.get_aConstraint_op(n),
            "op_str": apsw.mapping_bestindex_constraints.get(o.get_aConstraint_op(n)),
            "usable": o.get_aConstraint_usable(n),
            "collation": o.get_aConstraint_collation(n),
            "rhs": o.get_aConstraint_rhs(n),
        } for n in range(o.nConstraint)],
        "nOrderBy":
        o.nOrderBy,
        "aOrderBy": [{
            "iColumn": o.get_aOrderBy_iColumn(n),
            "desc": o.get_aOrderBy_desc(n),
        } for n in range(o.nOrderBy)],
        "aConstraintUsage": [{
            "argvIndex": o.get_aConstraintUsage_argvIndex(n),
            "omit": o.get_aConstraintUsage_omit(n),
            "in": o.get_aConstraintUsage_in(n),
        } for n in range(o.nConstraint)],
        "idxNum":
        o.idxNum,
        "idxStr":
        o.idxStr,
        "orderByConsumed":
        o.orderByConsumed,
        "estimatedCost":
        o.estimatedCost,
        "estimatedRows":
        o.estimatedRows,
        "idxFlags":
        o.idxFlags,
        "idxFlags_set":
        set(v for k, v in apsw.mapping_virtual_table_scan_flags.items() if isinstance(k, int) and o.idxFlags & k),
        "colUsed":
        o.colUsed,
        "distinct":
        o.distinct,
    }

    for aConstraint in res["aConstraint"]:
        if aConstraint["op"] in (apsw.SQLITE_INDEX_CONSTRAINT_OFFSET, apsw.SQLITE_INDEX_CONSTRAINT_LIMIT):
            del aConstraint["iColumn"]
        if aConstraint["op"] >= apsw.SQLITE_INDEX_CONSTRAINT_FUNCTION and aConstraint["op"] <= 255:
            aConstraint[
                "op_str"] = f"SQLITE_INDEX_CONSTRAINT_FUNCTION+{ aConstraint['op'] - apsw.SQLITE_INDEX_CONSTRAINT_FUNCTION }"

    if column_names:
        for aconstraint in res["aConstraint"]:
            if "iColumn" in aconstraint:
                aconstraint["iColumn_name"] = rowid_name if aconstraint["iColumn"] == -1 else column_names[
                    aconstraint["iColumn"]]
        for aorderby in res["aOrderBy"]:
            aorderby["iColumn_name"] = rowid_name if aorderby["iColumn"] == -1 else column_names[aorderby["iColumn"]]
        # colUsed has all bits set when SQLite just wants the whole row
        # eg when doing an update
        res["colUsed_names"] = set(column_names[i] for i in o.colUsed if i < len(column_names))
        if 63 in o.colUsed:  # could be one or more of the rest - we add all
            res["colUsed_names"].update(column_names[63:])

    return res


def format_query_table(db: apsw.Connection,
                       query: str,
                       bindings: Optional[apsw.Bindings] = None,
                       *,
                       colour: bool = False,
                       quote: bool = False,
                       string_sanitize: Union[Callable[[str], str], Union[Literal[0], Literal[1], Literal[2]]] = 1,
                       binary: Callable[[bytes], str] = lambda x: f"[ { len(x) } bytes ]",
                       null: str = "(null)",
                       truncate: int = 4096,
                       truncate_val: str = " ...",
                       text_width: int = 80,
                       use_unicode: bool = True,
                       word_wrap: bool = True) -> str:
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
              - After step 0, all non-ascii characters are replaced with their :func:`unicodedata.name` or \\x and hex value
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
    :param word_wrap: If True then :mod:`textwrap` is used to break wide text into fit column width
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
        "word_wrap": word_wrap
    }

    res = []

    cursor = db.cursor()
    colnames = None
    rows = []

    def trace(c, query, bindings):
        nonlocal colnames, rows
        if colnames:
            res.append(format_query_table._format_table(colnames, rows, **kwargs))
            rows = []
        colnames = [n for n, _ in c.getdescription()]
        return True

    cursor.exectrace = trace
    # mitigate any existing rowtracer
    if db.rowtrace:
        cursor.rowtrace = lambda x, y: y

    for row in cursor.execute(query, bindings):
        rows.append(list(row))

    if colnames:
        res.append(format_query_table._format_table(colnames, rows, **kwargs))

    if len(res) == 1:
        return res[0]
    return "\n".join(res)


def _format_table(colnames: list[str],
                 rows: list[apsw.SQLiteValues],
                 colour: bool,
                 quote: bool,
                 string_sanitize: Union[Callable[[str], str], Union[Literal[0], Literal[1], Literal[2]]],
                 binary: Callable[[bytes], str],
                 null: str,
                 truncate: int,
                 truncate_val: str,
                 text_width: int,
                 use_unicode: bool,
                 word_wrap: bool) -> str:
    "Internal table formatter"
    if colour:
        c = lambda v: f"\x1b[{ v }m"
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
        }

        def colour_wrap(text: str, kind: type, header=False) -> str:
            if header:
                return colours["header_start"] + text + colours["header_end"]
            if kind == str:
                tkind = "string"
            elif kind == bytes:
                tkind = "blob"
            elif kind in (int, float):
                tkind = "number"
            else:
                tkind = "null"
            return colours[tkind + "_start"] + text + colours[tkind + "_end"]

    else:
        colours = {}

        def colour_wrap(text, *args, **kwargs):
            return text

    colwidths = [max(len(v) for v in c.splitlines()) for c in colnames]
    coltypes = [set() for _ in colnames]

    # type, measure and stringize each cell
    for row in rows:
        for i, cell in enumerate(row):
            coltypes[i].add(type(cell))
            if isinstance(cell, str):
                if callable(string_sanitize):
                    cell = string_sanitize(cell)
                else:
                    cell = unicodedata.normalize("NFKC", cell)
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
                            try:
                                return "{" + unicodedata.name(s[0]) + "}"
                            except ValueError:
                                return "\\x" + f"{ord(s[0]):02}"

                        cell = re.sub(".", repl, cell)

                    if string_sanitize == 2:

                        def repl(s):
                            if s[0] in string.printable and s[0] not in string.whitespace:
                                return s[0]
                            return "."

                        cell = re.sub(".", repl, cell)
            if quote:
                val = apsw.format_sql_value(cell)
            else:
                if isinstance(cell, str):
                    val = cell
                elif isinstance(cell, (float, int)):
                    val = str(cell)
                elif isinstance(cell, bytes):
                    val = binary(cell)
                else:
                    val = null
            assert isinstance(val, str), f"expected str not { val!r}"

            val = val.replace("\r\n", "\n")

            if truncate > 0 and len(val) > truncate:
                val = val[:truncate] + truncate_val
            row[i] = (val, type(cell))
            colwidths[i] = max(colwidths[i], max(len(v) for v in val.splitlines()) if val else 0)

    ## work out widths
    # we need a space each side of a cell plus a cell separator hence 3
    # "| cell " and another for the final "|"
    total_width = lambda: sum(w + 3 for w in colwidths) + 1

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
        raise ValueError("Results can't be fitted in text width even with 1 char wide columns")

    # break headers and cells into lines
    if word_wrap:

        def wrap(text, width):
            res = []
            for para in text.splitlines():
                if para:
                    res.extend(textwrap.wrap(para, width=width, drop_whitespace=False))
                else:
                    res.append("")
            return res
    else:

        def wrap(text, width):
            res = []
            for para in text.splitlines():
                if len(para) < width:
                    res.append(para)
                else:
                    res.extend([para[s:s + width] for s in range(0, len(para), width)])
            return res

    colnames = [wrap(colnames[i], colwidths[i]) for i in range(len(colwidths))]
    for row in rows:
        for i, (text, t) in enumerate(row):
            row[i] = (wrap(text, colwidths[i]), t)

    ## output
    # are any cells more than one line?
    multiline = max(len(cell[0]) for cell in row for row in rows) > 1

    out_lines = []

    def do_bar(chars):
        line = chars[0]
        for i, w in enumerate(colwidths):
            line += chars[1] * (w + 2)
            if i == len(colwidths) - 1:
                line += chars[3]
            else:
                line += chars[2]
        out_lines.append(line)

    def do_row(row, sep, *, centre=False, header=False):
        # column names
        for n in range(max(len(cell[0]) for cell in row)):
            line = sep
            for i, (cell, t) in enumerate(row):
                text = cell[n] if n < len(cell) else ""
                text = " " + text + " "
                lt = len(text)
                # fudge things a little with this heuristic which
                # works when there is extra space - the earlier textwrap
                # doesn't know about different char widths
                lt += sum(1 if unicodedata.east_asian_width(c) == "W" else 0 for c in text)
                extra = " " * max(colwidths[i] + 2 - lt, 0)
                if centre:
                    lpad = extra[:len(extra) // 2]
                    rpad = extra[len(extra) // 2:]
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

format_query_table._format_table = _format_table
del _format_table

class VTColumnAccess(enum.Enum):
    "How the column value is accessed from a row, for :meth:`make_virtual_module`"
    By_Index = enum.auto()
    "By number like with tuples and lists - eg :code:`row[3]`"
    By_Name = enum.auto()
    "By name like with dicts - eg :code:`row['quantity']`"
    By_Attr = enum.auto()
    "By attribute like with :mod:`dataclasses` - eg :code:`row.quantity`"


def get_column_names(row: Any) -> Tuple[List[str], VTColumnAccess]:
    r"""
    Works out column names and access given an example row

    *row* can be an instance of a row, or the class used to make
    one (eg a :mod:`dataclass <dataclasses>`)

    .. list-table::
        :header-rows: 1

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

    .. code::

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
        return tuple(f"column{ x }" for x in range(len(row))), VTColumnAccess.By_Index
    raise TypeError(f"Can't figure out columns for { row }")


def make_virtual_module(db: apsw.Connection,
                        name: str,
                        callable: Callable,
                        *,
                        eponymous: bool = True,
                        eponymous_only: bool = False,
                        repr_invalid: bool = False) -> None:
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
    fail with :class:`apsw.SQLError` and a message "no query solution"
    """

    class Module:

        def __init__(self, callable: Callable, columns: tuple[str], column_access: VTColumnAccess,
                     primary_key: Optional[int], repr_invalid: bool):
            self.columns = columns
            self.callable: Callable = callable
            if not isinstance(column_access, VTColumnAccess):
                raise ValueError(f"Expected column_access to be { VTColumnAccess } not {column_access!r}")
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
                raise ValueError(f"Same name in columns and in paramters: { both }")

            self.all_columns: tuple[str] = tuple(self.columns) + tuple(self.parameters)
            self.primary_key = primary_key
            if self.primary_key is not None and not (0 <= self.primary_key < len(self.columns)):
                raise ValueError(f"{self.primary_key!r} should be None or a column number < { len(self.columns) }")
            self.repr_invalid = repr_invalid
            column_defs = ""
            for i, c in enumerate(self.columns):
                if column_defs:
                    column_defs += ", "
                column_defs += f"[{ c }]"
                if self.primary_key == i:
                    column_defs += " PRIMARY KEY"
            for p in self.parameters:
                column_defs += f",[{ p }] HIDDEN"

            self.schema = f"CREATE TABLE ignored({ column_defs })"
            if self.primary_key is not None:
                self.schema += " WITHOUT rowid"

        def Create(self, db, modulename, dbname, tablename, *args: apsw.SQLiteValue) -> tuple[str, apsw.VTTable]:

            if len(args) > len(self.parameters):
                raise ValueError(f"Too many parameters: parameters accepted are { ' '.join(self.parameters) }")

            param_values = dict(zip(self.parameters, args))

            return self.schema, self.Table(self, param_values)

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

            def Open(self):
                return self.module.Cursor(self.module, self.param_values)

            def Disconnect(self):
                pass

            Destroy = Disconnect

        class Cursor:

            def __init__(self, module: Module, param_values: dict[str, apsw.SQLiteValue]):
                self.module = module
                self.param_values = param_values
                self.iterating: Optional[Iterator] = None
                self.current_row: Any = None
                self.columns = module.columns
                self.repr_invalid = module.repr_invalid
                self.num_columns = len(self.columns)
                self.access = self.module.column_access

            def Filter(self, idx_num: int, idx_str: str, args: tuple[apsw.SQLiteValue]) -> None:
                params: dict[str, apsw.SQLiteValue] = self.param_values.copy()
                params.update(zip(idx_str.split(","), args))
                self.iterating = iter(self.module.callable(**params))
                # proactively advance so we can tell if eof
                self.Next()

                self.hidden_values: List[SQLiteValue] = self.module.defaults[:]
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
                if which >= self.num_columns:
                    return self.hidden_values[which - self.num_columns]
                if self.access == VTColumnAccess.By_Index:
                    v = self.current_row[which]
                elif self.access == VTColumnAccess.By_Name:
                    v = self.current_row[self.columns[which]]
                elif self.access == VTColumnAccess.By_Attr:
                    v = getattr(self.current_row, self.columns[which])
                if self.repr_invalid:
                    try:
                        apsw.format_sql_value(v)
                    except TypeError:
                        v = repr(v)
                return v

            def Next(self) -> None:
                try:
                    self.current_row = next(self.iterating)
                except StopIteration:
                    if hasattr(self.iterating, "close"):
                        self.iterating.close()
                    self.iterating = None

            def Rowid(self):
                if self.module.primary_key is None:
                    return id(self.current_row)
                return self.Column(self.module.primary_key)

    mod = Module(callable, callable.columns, callable.column_access, getattr(callable, "primary_key", None),
                 repr_invalid)

    # unregister any existing first
    db.createmodule(name, None)
    db.createmodule(name,
                    mod,
                    use_bestindex_object=True,
                    eponymous=eponymous,
                    eponymous_only=eponymous_only,
                    read_only=True)


def generate_series_sqlite(start=None, stop=0xffffffff, step=1):
    """Behaves like SQLite's generate_series

    `SQLite doc <https://sqlite.org/series.html>`__.

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
            yield (start, )
            start += step
    elif step < 0:
        while stop >= start:
            yield (stop, )
            stop += step


generate_series_sqlite.columns = ("value", )
generate_series_sqlite.column_access = VTColumnAccess.By_Index
generate_series_sqlite.primary_key = 0


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

        :meth:`generate_series`

    """
    if step is None:
        if stop > start:
            step = 1
        else:
            step = -1

    if step > 0:
        while start <= stop:
            yield (start, )
            start += step
    elif step < 0:
        while start >= stop:
            yield (start, )
            start += step
    else:
        raise ValueError("step of zero is not valid")


generate_series.columns = ("value", )
generate_series.column_access = VTColumnAccess.By_Index
generate_series.primary_key = 0


def query_info(db: apsw.Connection,
               query: str,
               bindings: Optional[apsw.Bindings] = None,
               *,
               prepare_flags: int = 0,
               actions: bool = False,
               expanded_sql: bool = False,
               explain: bool = False,
               explain_query_plan: bool = False) -> QueryDetails:
    """Returns information about the query, but does not run it.

    Set the various parameters to `True` if you also want the
    actions, expanded_sql, explain, query_plan etc filled in.
    """
    res: dict[str, Any] = {"actions": None, "query_plan": None, "explain": None}

    def tracer(cursor: apsw.Cursor, first_query: str, bindings: Optional[apsw.Bindings]):
        nonlocal res
        res.update({
            "first_query": first_query,
            "query": query,
            "bindings": bindings,
            "is_explain": cursor.is_explain,
            "is_readonly": cursor.is_readonly,
            "has_vdbe": cursor.has_vdbe,
            "description": cursor.getdescription(),
            "description_full": None,
        })
        if hasattr(cursor, "description_full"):
            res["description_full"] = cursor.description_full

        assert query == first_query or query.startswith(first_query)
        res["query_remaining"] = query[len(first_query):] if len(query) > len(first_query) else None
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
            raise ValueError(f"Unknown authorizer code { code }")
        actions_taken.append(QueryAction(**a))
        return apsw.SQLITE_OK

    cur = db.cursor()
    cur.exectrace = tracer
    if actions:
        orig_authorizer = db.authorizer
        db.authorizer = auther
    try:
        cur.execute(query, bindings, can_cache=False, prepare_flags=prepare_flags)
    except apsw.ExecTraceAbort:
        pass
    finally:
        if actions:
            db.authorizer = orig_authorizer
    cur.exectrace = None
    if actions:
        res["actions"] = actions_taken

    if explain and not res["is_explain"]:
        vdbe = []
        for row in cur.execute("EXPLAIN " + res["first_query"], bindings):
            vdbe.append(
                VDBEInstruction(**dict((v[0][0], v[1]) for v in zip(cur.getdescription(), row) if v[1] is not None)))
        res["explain"] = vdbe

    if explain_query_plan and not res["is_explain"]:
        subn = "sub"
        byid = {0: {"detail": "QUERY PLAN"}}

        for row in cur.execute("EXPLAIN QUERY PLAN " + res["first_query"], bindings):
            node = dict((v[0][0], v[1]) for v in zip(cur.getdescription(), row) if v[0][0] != "notused")
            assert len(node) == 3  # catch changes in returned format
            parent = byid[node["parent"]]
            if subn not in parent:
                parent[subn] = [node]
            else:
                parent[subn].append(node)
            byid[node["id"]] = node

        def flatten(node):
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
    bindings: Optional[apsw.Bindings]
    "Bindings provided"
    first_query: str
    "The first statement present in query"
    query_remaining: Optional[str]
    "Query text after the first one if multiple were in query, else None"
    is_explain: int
    ":attr:`Cursor.is_explain <apsw.Cursor.is_explain>`"
    is_readonly: bool
    ":attr:`Cursor.is_readonly <apsw.Cursor.is_readonly>`"
    has_vdbe: bool
    ":attr:`Cursor.has_vdbe <apsw.Cursor.has_vdbe>`"
    description: Tuple[Tuple[str, str], ...]
    ":meth:`Cursor.getdescription <apsw.Cursor.getdescription>`"
    description_full: Optional[Tuple[Tuple[str, str, str, str, str], ...]]
    ":attr:`Cursor.description_full <apsw.Cursor.description_full>`"
    expanded_sql: Optional[str]
    ":attr:`Cursor.expanded_sql <apsw.Cursor.expanded_sql>`"
    actions: Optional[List[QueryAction]]
    """A list of the actions taken by the query, as discovered via
    :attr:`Connection.authorizer <apsw.Connection.authorizer>`"""
    explain: Optional[List[VDBEInstruction]]
    """A list of instructions of the `internal code <https://sqlite.org/opcode.html>`__
    used by SQLite to execute the query"""
    query_plan: Optional[QueryPlan]
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

    column_name: Optional[str] = None
    database_name: Optional[str] = None
    "eg `main`, `temp`, the name in `ATTACH <https://sqlite.org/lang_attach.html>`__"
    file_name: Optional[str] = None
    function_name: Optional[str] = None
    module_name: Optional[str] = None
    operation: Optional[str] = None
    pragma_name: Optional[str] = None
    pragma_value: Optional[str] = None
    table_name: Optional[str] = None
    trigger_name: Optional[str] = None
    trigger_or_view: Optional[str] = None
    """This action is happening due to a trigger or view, and not
    directly expressed in the query itself"""
    view_name: Optional[str] = None


@dataclass
class QueryPlan:
    "A :mod:`dataclass <dataclasses>` for one step of a query plan"
    detail: str
    "Description of this step"
    sub: Optional[List[QueryPlan]] = None
    "Steps that run within this one"


@dataclass
class VDBEInstruction:
    "A :mod:`dataclass <dataclasses>` representing one instruction and its parameters"
    addr: int
    "Address of this opcode.  It will be the target of goto, loops etc"
    opcode: str
    "The instruction"
    comment: Optional[str] = None
    "Additional human readable information"
    p1: Optional[int] = None
    "First opcode parameter"
    p2: Optional[int] = None
    "Second opcode parameter"
    p3: Optional[int] = None
    "Third opcode parameter"
    p4: Optional[int] = None
    "Fourth opcode parameter"
    p5: Optional[int] = None
    "Fifth opcode parameter"
