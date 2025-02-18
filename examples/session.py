#!/usr/bin/env python3

# This code uses Python's optional typing annotations.  You can
# ignore them and do not need to use them.  If you do use them
# then you must include this future annotations line first.
from __future__ import annotations

from pprint import pprint
import functools
import pathlib

import apsw
import apsw.ext


### session_check: Is Session available?
# Session must be enabled in SQLite at compile time, and in APSW
# at its compile time.  (PyPI builds always have both enabled)

print("Session in SQLite:", "ENABLE_SESSION" in apsw.compile_options)

print("  Session in APSW:", hasattr(apsw, "Session"))

### setup: Initial database setup
# The database is populated with an items table, a tags table, and a
# link table allowing multiple tags per item.  It has foreign keys and
# a trigger which cause changes beyond the supplied SQL so we can see
# how session handles that.  See the `full SQL
# <_static/samples/session.sql>`__

connection = apsw.Connection("")

connection.execute(pathlib.Path("session.sql").read_text())

### adding_session: Monitoring changes
# You can have multiple :class:`Session` monitoring changes on a
# session.  You need to call :meth:`Session.attach` to say which tables
# to monitor, or use :meth:`Session.table_filter` to get a callback.
# You can pause and resume monitoring with :attr:`Session.enabled`.

session = apsw.Session(connection, "main")
# we now say which tables to monitor - None means all
session.attach(None)

# And now make some changes.  We do every kind of change here -
# INSERT, UPDATE, and DELETE.
connection.execute("""
INSERT INTO items(name) VALUES('kitchen walls');
INSERT INTO item_tag_link(item_id, tag_id)  VALUES(
    (SELECT id FROM items WHERE name='kitchen walls'),
    (SELECT id FROM tags WHERE label='paint')
);
INSERT INTO item_tag_link(item_id, tag_id)  VALUES(
    (SELECT id FROM items WHERE name='kitchen walls'),
    (SELECT id FROM tags WHERE label='cleaning')
);

INSERT INTO items(name) VALUES('microwave');

UPDATE items SET description='high gloss' WHERE name='bathroom ceiling';
UPDATE tags SET cost_centre='95' WHERE label='new';

DELETE FROM tags WHERE label='battery';
""")

### changesets:  Getting and inspecting the changes
# :func:`apsw.ext.changeset_to_sql` is useful to see what
# SQL a changeset is equivalent to.

changeset = session.changeset()
print(f"{len(changeset)=}")

print(
    "\n".join(
        apsw.ext.changeset_to_sql(
            changeset,
            functools.partial(
                apsw.ext.find_columns, connection=connection
            ),
        )
    )
)

### session_end: Cleanup
# We can now close the connections, but it is optional.  APSW automatically
# cleans up sessions etc when their corresponding connections are closed.

if False:
    connection.close()
