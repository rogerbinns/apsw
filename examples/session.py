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
# session.  You need to call :meth:`Session.attach` to say which
# tables to monitor, or use :meth:`Session.table_filter` to get a
# callback.  You can pause and resume monitoring with
# :attr:`Session.enabled`.  For unmonitored changes you can use
# :meth:`Session.diff` to work out the changes between two tables.

session = apsw.Session(connection, "main")
# we now say which tables to monitor - we want all
session.attach()

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
UPDATE tags SET cost_centre=null WHERE label='new';

DELETE FROM tags WHERE label='battery';
""")

### changesets:  Patchsets and Changesets
# Changesets contain all the before and after values for changed rows,
# while patchsets only contain the necessary values to make the
# change.  :func:`apsw.ext.changeset_to_sql` is useful to see what SQL
# a change or patch set is equivalent to.

patchset = session.patchset()
print(f"{len(patchset)=}")

print(
    "\n".join(
        apsw.ext.changeset_to_sql(
            patchset,
            functools.partial(
                apsw.ext.find_columns, connection=connection
            ),
        )
    )
)

# Note how the changeset is larger and contains more information
changeset = session.changeset()
print(f"\n\n{len(changeset)=}")

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


### inverting: Inverting - undo, redo
# We can get the opposite of a changeset which can then form the basis
# of an undo/redo implementation.  One pattern is to have a table
# where you store changesets allowing for a later undo or redo.

# Yes, it is this easy
undo = apsw.Changeset.invert(changeset)

# Compare this to the changeset above, to see how it does the
# opposite.
print(
    "\n".join(
        apsw.ext.changeset_to_sql(
            undo,
            functools.partial(
                apsw.ext.find_columns, connection=connection
            ),
        )
    )
)

### applying: Applying changesets
# We can filter which tables get affected when :meth:`applying a
# changeset <Changeset.apply>` (default all) and can define a conflict
# handler (default abort the transaction).  Conflicts are `described
# here <https://sqlite.org/sessionintro.html#conflicts>`__. We are
# going to undo our earlier changes.

# However it is going to fail ...
try:
    apsw.Changeset.apply(undo, connection)
except apsw.AbortError as exc:
    # It will fail, and the database back in the state when we started
    # the apply.
    print(f"Exception {exc=}")

# The reason it failed is because of the foreign keys automatically
# removing rows in the link table when members of items and tags got
# removed.  Lets do it again, but save the failed changes for
# inspection.

failed = apsw.ChangesetBuilder()


# A conflict handler says what to do
def conflict_handler(reason: int, change: apsw.TableChange) -> int:
    # Print the failure reason
    print(
        "conflict",
        apsw.mapping_session_conflict[reason],
        f"{change.op=}",
    )

    # save the change
    failed.add_change(change)

    # proceed ignoring this failed change
    return apsw.SQLITE_CHANGESET_OMIT


# Undo our earlier changes again
apsw.Changeset.apply(undo, connection, conflict=conflict_handler)

# Now lets see what couldn't apply as SQL
print("\nFailed items")
print(
    "\n".join(
        apsw.ext.changeset_to_sql(
            failed.output(),
            functools.partial(
                apsw.ext.find_columns, connection=connection
            ),
        )
    )
)

### syncing: Synchronising changes made by two users
# Alice and Bob are going to separately work on the same database and
# we are going to synchronise their changes.  Our database is a bit
# more complicated because adding to the items table has a trigger
# that links the ``new`` tag to that item.  We use
# :meth:`apsw.ext.XXX` which handles that sort of thing.

# Start from the same database
alice_connection = apsw.Connection("alice.db")
with alice_connection.backup("main", connection, "main") as backup:
    backup.step()

bob_connection = apsw.Connection("bob.db")
with bob_connection.backup("main", connection, "main") as backup:
    backup.step()

connection.close()

# setup sessions
alice_session = apsw.Session(alice_connection, "main")
alice_session.attach()

bob_session = apsw.Session(bob_connection, "main")
bob_session.attach()

# Each makes changes
alice_connection.execute("""
    UPDATE tags SET label='painting' WHERE label='paint';
    INSERT INTO items(name, description) VALUES('storage closet', 'main storage space');
    INSERT INTO tags(label) VALUES('approval needed');
    -- remove new tag
    DELETE FROM item_tag_link WHERE item_id=(SELECT id FROM items WHERE name='storage closet');
    -- add approval needed
    INSERT INTO item_tag_link(item_id, tag_id)  VALUES(
        (SELECT id FROM items WHERE name='storage closet'),
        (SELECT id FROM tags WHERE label='approval needed')
    );
""")

bob_connection.execute("""
    UPDATE tags SET cost_centre = '150' WHERE label='electrical';
    INSERT INTO items(name) VALUES('storage B');
    -- remove new tag
    DELETE FROM item_tag_link WHERE item_id=(SELECT id FROM items WHERE name='storage B');
    -- add electrical
    INSERT INTO item_tag_link(item_id, tag_id)  VALUES(
        (SELECT id FROM items WHERE name='storage B'),
        (SELECT id FROM tags WHERE label='electrical')
    );
""")

# Get the changesets
alice_changeset = alice_session.changeset()

bob_changeset = bob_session.changeset()

# Apply them to each other's database
apsw.Changeset.apply(alice_changeset, bob_connection)
apsw.Changeset.apply(bob_changeset, alice_connection)


query = """
SELECT items.name AS name, tags.label AS tag, tags.cost_centre AS cost_centre
   FROM tags, items, item_tag_link
   WHERE items.id = item_tag_link.item_id AND tags.id = item_tag_link.tag_id
   ORDER BY items.name, tags.label
"""

print("Alice's database")
print(apsw.ext.format_query_table(alice_connection, query))

print("\nBob's database")
print(apsw.ext.format_query_table(bob_connection, query))

print("\nAlice changeset")
print(
    "\n".join(
        apsw.ext.changeset_to_sql(
            alice_changeset,
            functools.partial(
                apsw.ext.find_columns, connection=alice_connection
            ),
        )
    )
)

print("\nBob changeset")
print(
    "\n".join(
        apsw.ext.changeset_to_sql(
            bob_changeset,
            functools.partial(
                apsw.ext.find_columns, connection=bob_connection
            ),
        )
    )
)


### builder: ChangesetBuilder
# The :class:`ChangesetBuilder` can be used to combine multiple
# changesets and individual :class:`TableChange`.  In this example
# we'll build up all the changes to the ``items`` table.

items = apsw.ChangesetBuilder()

for source in (alice_changeset, bob_changeset):
    for change in apsw.Changeset.iter(source):
        if change.name == "items":
            items.add_change(change)

only_items = items.output()

print(
    "\n".join(
        apsw.ext.changeset_to_sql(
            only_items,
            functools.partial(
                apsw.ext.find_columns, connection=alice_connection
            ),
        )
    )
)

### streaming: Streaming
# The changesets above were all produced as a single

### session_end: Cleanup
# We can now close the connections, but it is optional.  APSW automatically
# cleans up sessions when their corresponding connections are closed.

connection.close()
