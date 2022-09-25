# Provides various useful routines

from __future__ import annotations

import sys

try:
    from dataclasses import dataclass
except ImportError:
    raise ImportError("You need a Python version that has dataclasses (Python 3.7+")

import apsw

from typing import Optional, Tuple, List

"""
This modules provides various interesting and useful bits of functionality.
"""



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
    actions, expanded_sql, explain, query_plan etc.
    """
    res = None

    def tracer(cursor, firstquery, bindings):
        nonlocal res
        res = {
            "firstquery": firstquery,
            "query": query,
            "bindings": bindings,
            "is_explain": cursor.is_explain,
            "is_readonly": cursor.is_readonly,
            "description": cursor.getdescription()
        }
        if hasattr(cursor, "description_full"):
            res["description_full"] = cursor.description_full

        assert query == firstquery or query.startswith(firstquery)
        res["query_remaining"] = query[len(firstquery):] if len(query) > len(firstquery) else None
        if expanded_sql:
            res["expanded_sql"] = cursor.expanded_sql
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
    cur.setexectrace(tracer)
    if actions:
        db.setauthorizer(auther)
    try:
        cur.execute(query, bindings, can_cache=False, prepare_flags=prepare_flags)
    except apsw.ExecTraceAbort:
        pass
    finally:
        db.setauthorizer(None)
    cur.setexectrace(None)
    if actions:
        res["actions"] = actions_taken

    if explain and not res["is_explain"]:
        vdbe = []
        for row in cur.execute("EXPLAIN " + res["firstquery"], bindings):
            vdbe.append(VDBEInstruction(**dict((v[0][0], v[1]) for v in zip(cur.getdescription(), row) if v[1] is not None)))
        res["explain"] = vdbe

    if explain_query_plan and not res["is_explain"]:
        subn = "sub"
        byid = {0: {"detail": "QUERY PLAN"}}

        for row in cur.execute("EXPLAIN QUERY PLAN " + res["firstquery"], bindings):
            node = dict((v[0][0], v[1]) for v in zip(cur.getdescription(), row) if v[0][0] != "notused")
            assert len(node) == 3 # catch changes in returned format
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

    return res

@dataclass
class QueryDetails:
    "A :mod:`dataclass <dataclasses>` that provides detailed information about a query, returned by :func:`query_info`"
    query: str
    "Original query provided"
    bindings: Optional[apsw.Bindings]
    "Bindings provided"
    firstquery: str
    "The first statement present in query"
    query_remaining: Optional[str]
    "Query text after the first one if multiple were in query, else None"
    is_explain: int
    ":attr:`Cursor.is_explain <apsw.Cursor.is_explain>`"
    is_readonly: bool
    ":attr:`Cursor.is_readonly <apsw.Cursor.is_readonly>`"
    description: Tuple[[str, str], ...]
    ":meth:`Cursor.getdescription <apsw.Cursor.getdescription>`"
    description_full: Optional[Tuple[[str, str, str, str, str], ...]]
    ":attr:`Cursor.description_full <apsw.Cursor.description_full>`"
    expanded_sql: Optional[str]
    ":attr:`Cursor.expanded_sql <apsw.Cursor.expanded_sql>`"
    actions: Optional[List[QueryAction]]
    """A list of the actions taken by the query, as discovered via
    :meth:`Connection.setauthorizer <apsw.Connection.setauthorizer>`"""
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
    "Description of this stage"
    sub: Optional[List[QueryPlan]] = None
    "Stages that run within this one"

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
