# Provides various useful routines

from __future__ import annotations

import apsw

from typing import Optional

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
               explain_query_plan: bool = False) -> dict:
    """Returns information about the query, but does not run it.

    The dictionary returned has the following information:

    .. list-table::
        :header-rows: 1

        * - Key
          - Explanation
        * - query
          - the original query provided
        * - bindings
          - the bindings provided
        * - firstquery
          - the first statement present in `query`
        * - query_remaining
          - query text after the first one if multiple were in `query`, or `None`
        * - is_explain
          - :attr:`Cursor.is_explain`
        * - is_readonly
          - :attr:`Cursor.is_readonly`
        * - description
          - :meth:`Cursor.getdescription`
        * - description_full
          - :attr:`Cursor.description_full`
        * - expanded_sql
          - :attr:`Cursor.expanded_sql`
        * - actions
          - a list of dict of the actions taken by the query, as discovered via the
            :meth:`Connection.setauthorizer`, and is described below.
        * - explain
          - a list of dict of the `internal code <https://sqlite.org/opcode.html>`__
            used by SQLite to execute the query
        * - query_plan
          - the steps taken against tables and indices `described here
            <https://sqlite.org/eqp.html>`__


    Each list item is a dict, and always has the first two keys
    with the rest depending on the action.

    .. list-table:: action dict item
        :header-rows: 1

        * - Key
            - Explanation
        * - action
            - The numeric `authorizer code <https://sqlite.org/c3ref/c_alter_table.html>`__,
            also present in :attr:`apsw.mapping_authorizer_function`
        * - action_name
            - The string of the name - eg `SQLITE_SELECT` will have 21 in `action` and the
            `"SQLITE_SELECT"` in `action_name`
        * - database_name
          - database being operated on (main, temp, the name in `ATTACH <https://sqlite.org/lang_attach.html>`__)
        * - index_name
          -
        * - table_name
          -
        * - trigger_name
          -
        * - view_name
          -
        * - pragma_name
          -
        * - pragma_value
          -

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
            (apsw.SQLITE_ATTACH, "filename", None),
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
        actions_taken.append(a)
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
        for row in cur.execute("EXPLAIN " + res["firstquery"]):
            vdbe.append(dict((v[0][0], v[1]) for v in zip(cur.getdescription(), row) if v[1] is not None))
        res["explain"] = vdbe
    if explain_query_plan and not res["is_explain"]:
        subn = "sub"
        byid = {0: {"detail": "QUERY PLAN"}}


        for row in cur.execute("EXPLAIN QUERY PLAN " + res["firstquery"]):
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
                res[subn] = [flatten(child) for child in node[subn]]
            return res

        res["query_plan"] = flatten(byid[0])

    return res