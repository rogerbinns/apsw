# Provides various useful routines

from __future__ import annotations
import collections, collections.abc
import sys
if sys.version_info >= (3, 10):
    from types import NoneType
else:
    NoneType = type(None)

from dataclasses import dataclass, make_dataclass

from typing import Optional, Tuple, Union, List, Any, Dict, Callable, Sequence
import functools
import abc

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
