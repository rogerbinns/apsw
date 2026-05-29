/*

IMPORTANT NOTE

Every mention of the table name self._name must
also include the schema self._schema

*/

-- python:

/*
"""SQL as used by :mod:`apsw.fts5.Table`"""
*/

-- name: structure_record(**locals) -> bytes
-- Gets the structure record which changes on each update to the
-- table. See `forum post <https://sqlite.org/forum/forumpost/2a726411b6974502>`__

SELECT
    block
FROM
    {self._schema:eval|id}.{self._name + "_data":eval|id}
WHERE
    id=10;

-- name: column_names(**locals) -> list[str]
-- Get all column names

SELECT
    name
FROM
    pragma_table_info({self._name:eval|id}, {self._schema:eval|id});

-- name: columns_indexed(**locals) -> list[str]
-- Get column names that are indexed

SELECT
    name
FROM
    pragma_table_info({self._name:eval|id}, {self._schema:eval|id})
WHERE
    name NOT IN ({self.structure.unindexed:eval|seq});

-- name: table_sql(**locals) -> str
-- Gets the SQL for the table

SELECT
    sql
FROM
    {self._schema:eval|id}.sqlite_schema
WHERE
  type = 'table'
    AND
  lower(name) = lower({self._name:eval});

-- name: locale_for_cell(rowid: int, column_number: int, **locals) -> Any

SELECT
    fts5_get_locale({self._name:eval|id}, {column_number})
FROM
    {self._schema:eval|id}.{self._name:eval|id}
WHERE
    rowid = {rowid};