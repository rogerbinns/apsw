/*

IMPORTANT NOTE

Every mention of the table name self._name must
also include the schema self._schema

*/

-- python:

/*
"""SQL as used by :mod:`apsw.fts5`"""
*/

-- name: structure_record(**locals) -> bytes
-- Gets the structure record which changes on each update to the
-- database. See `forum post <https://sqlite.org/forum/forumpost/2a726411b6974502>`__

SELECT block
FROM {self._schema:eval|id}.{self._name + "_data":eval|id}
WHERE id=10

-- name: column_names(**locals) -> list[str]

SELECT name
FROM pragma_table_info({self._name:eval|id}, {self._schema:eval|id});

-- name: columns_indexed(**locals) -> list[str]

SELECT name
FROM pragma_table_info({self._name:eval|id}, {self._schema:eval|id})
WHERE name NOT IN ({self.structure.unindexed:eval|seq});