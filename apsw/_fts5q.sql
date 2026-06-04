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

-- name: delete(rowid: int, **locals) -> changes
-- Deletes row from table when NOT using a content table

DELETE
FROM
    {self._schema:eval|id}.{self._name:eval|id}
WHERE
    rowid = {rowid};

-- name: delete_content(rowid: int, **locals) -> changes
-- Deletes row from corresponding content table

DELETE
FROM
    {self._schema:eval|id}.{self.structure.content:eval|id}
WHERE
    {self.structure.content_rowid if self.structure.content_rowid is not None else "rowid":eval|id}
        = {rowid};

-- name: command(cmd, **locals)

INSERT
INTO
    {self._schema:eval|id}.{self._name:eval|id}
        ({self._name:eval|id})
VALUES
    ({cmd});

-- name: command_integrity_check(external_content: bool, **locals)

INSERT
INTO
    {self._schema:eval|id}.{self._name:eval|id}
        ({self._name:eval|id}, 'rank')
VALUES
    ('integrity-check', {external_content});

-- name: command_merge(n: int, **locals) -> changes

INSERT
INTO
    {self._schema:eval|id}.{self._name:eval|id}
        ({self._name:eval|id}, rank)
VALUES
    ('merge', {n});

-- name: config_set(name: str, value: apsw.SQLiteValue, **locals)

INSERT
INTO
    {self._schema:eval|id}.{self._name:eval|id}
        ({self._name:eval|id}, 'rank')
VALUES
    ({name}, {value});

-- name: config_table_set(prefix: str, name: str, value: apsw.SQLiteValue, **locals)

INSERT OR REPLACE
INTO
    {self._schema:eval|id}.{self._name + '_config':eval|id}
    (k, v)
VALUES
    ({prefix + name:eval}, {value});

-- name: config_table_get(prefix: str, name: str, **locals) -> Any | None

SELECT
    v
FROM
    {self._schema:eval|id}.{self._name + '_config':eval|id}
WHERE
    k = {prefix + name:eval};

-- name: ensure_vocab(name: str, type: str, **locals) -> None

CREATE VIRTUAL TABLE
    IF NOT EXISTS
temp.{name:id}
    USING fts5vocab
        (
            {self._schema:eval},
            {self._name:eval},
            {type}
        );

-- name: all_tokens

SELECT
    term, doc
FROM
    temp.{self.fts5vocab_name_new("row"):eval|id};

