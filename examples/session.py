#!/usr/bin/env python3

# This code uses Python's optional typing annotations.  You can
# ignore them and do not need to use them.  If you do use them
# then you must include this future annotations line first.
from __future__ import annotations

import functools
import pathlib
import tempfile

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

# enabled by default.  You can set it to False while making
# changes you do not want recorded. It only stops recording
# changes to rows not already part of the changeset.
print(f"{session.enabled=}")

# We'd like size estimates
session.config(apsw.SQLITE_SESSION_OBJCONFIG_SIZE, True)


# we now say which tables to monitor - no tables are monitored by default.
# The tables must have PRIMARY KEY in their declaration otherwise
# nothing is recorded.
def table_filter(name: str) -> bool:
    print(f"table_filter {name=}")
    # We want them all
    return True


# We could also have done session.attach() to get all tables
# or attach with named tables of interest.
session.table_filter(table_filter)

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

# How much memory is the session using?
print(f"{session.memory_used=}")

### changeset_sql: SQL equivalent of a changeset
# We can iterate the contents of a changeset as SQL statements using
# :func:`apsw.ext.changeset_to_sql`.  It needs to know the column
# names because changesets only use column numbers, so we use
# :meth:`apsw.ext.find_columns` giving it the connection to inspect.


def show_changeset(
    title: str,
    contents: apsw.SessionStreamInput,
    connection: apsw.Connection = connection,
):
    print(title)
    for statement in apsw.ext.changeset_to_sql(
        contents,
        get_columns=functools.partial(
            apsw.ext.find_columns, connection=connection
        ),
    ):
        print(statement)
    print()


### changesets:  Patchsets and Changesets
# Changesets contain all the before and after values for changed rows,
# while patchsets only contain the necessary values to make the
# change.  :func:`apsw.ext.changeset_to_sql` is useful to see what SQL
# a change or patch set is equivalent to.


patchset = session.patchset()
print(f"{len(patchset)=}")

show_changeset("patchset", patchset)

# Note how the changeset is larger and contains more information
changeset = session.changeset()
print(f"{len(changeset)=}")

show_changeset("changeset", changeset)


### inverting: Inverting - undo, redo
# We can get the opposite of a changeset which can then form the basis
# of an undo/redo implementation.  One pattern is to have a table
# where you store changesets allowing for a later undo or redo.

# Yes, it is this easy
undo = apsw.Changeset.invert(changeset)

# Compare this to the changeset above, to see how it does the
# opposite.
show_changeset("undo", undo)

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

# And make some deliberate conflicts
connection.execute("""
    UPDATE items SET description = 'Orange flavour' WHERE name = 'bathroom ceiling';
    -- Refuse deletes to make constraint failure on delete
    CREATE TRIGGER prevent_microwave_deletion
    BEFORE DELETE ON items
        FOR EACH ROW
        WHEN OLD.name = 'microwave'
        BEGIN
            SELECT RAISE(ABORT, 'Cannot delete items with name "microwave"');
        END;
""")


# A conflict handler says what to do
def conflict_handler(reason: int, change: apsw.TableChange) -> int:
    # Print the failure information
    print(
        "conflict",
        apsw.mapping_session_conflict[reason],
        f"{change.op=} {change.opcode=}",
        "\n",
        f"{change.conflict=}",
        "\n",
        f"{change.name=} {change.column_count=}",
        "\n",
        f"{change.fk_conflicts=}",
        f"{change.indirect=}",
        "\n",
        f"{change.old=}\n",
        f"{change.new=}\n",
    )

    # save the change for later
    failed.add_change(change)

    # proceed ignoring this failed change
    return apsw.SQLITE_CHANGESET_OMIT


# Undo our earlier changes again
apsw.Changeset.apply(undo, connection, conflict=conflict_handler)

# Now lets see what couldn't apply as SQL
show_changeset("failed", failed.output())

### syncing: Synchronising changes made by two users
# Alice and Bob are going to separately work on the same database and
# we are going to synchronise their changes.
#
# You will notice that the databases did not end up identical.  This
# is because foreign keys, triggers, and the changesets are all
# fighting each other.  You need to be careful when using all of them
# at the same time.  See :ref:`ChangesetBuilder next
# <example_changesetbuilder>` where you can make your own changesets
# for these more complicated situations.

# Start from the same database
alice_connection = apsw.Connection("alice.db")
with alice_connection.backup("main", connection, "main") as backup:
    backup.step()

bob_connection = apsw.Connection("bob.db")
with bob_connection.backup("main", connection, "main") as backup:
    backup.step()

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

show_changeset("Alice changeset", alice_changeset)

show_changeset("Bob changseset", bob_changeset)


### changesetbuilder: ChangesetBuilder
# The :class:`ChangesetBuilder` can be used to combine multiple
# changesets and individual :class:`TableChange`.  In this example
# we'll build up all the changes to the ``items`` table from
# multiple changesets.  :meth:`ChangesetBuilder.schema` is used
# to ensure the changes map to the expected database table
# structure (names, primary keys, number of columns).

items = apsw.ChangesetBuilder()

items.schema(connection, "main")

for source in (changeset, alice_changeset, bob_changeset):
    for change in apsw.Changeset.iter(source):
        if change.name == "items":
            items.add_change(change)

only_items = items.output()

show_changeset("Only items table changes", only_items)

### streaming: Streaming
# The changesets above were all produced as a single bytes in memory
# all at once.  For larger changesets we can read and write them in
# chunks, such as with blobs, files, or network connections.

# Use a positive number to set that size
chunk_size = apsw.session_config(
    apsw.SQLITE_SESSION_CONFIG_STRMSIZE, -1
)
print("default chunk size", chunk_size)

# Some changes to make the changeset larger.  The size is an estimate.
print(f"Before estimate {session.changeset_size=}")
for i in "abcdefghijklmnopqrstuvwxyz":
    connection.execute(
        "INSERT INTO items(name, description) VALUES(?, ?)",
        (i * 1234, i * 1234),
    )
print(f"After estimate {session.changeset_size=}")

# We'll write to a file
out = tempfile.TemporaryFile("w+b")

num_writes = 0


def write(data: memoryview) -> None:
    global num_writes
    num_writes += 1
    res = out.write(data)
    # The streamer must write all bytes
    assert res == len(data)


session.changeset_stream(write)

print("Output file size is", out.tell())
print("Number of writes", num_writes)

# Lets read from the same file, using the streaming iterator
out.seek(0)
num_reads = 0


def read(amount: int) -> bytes:
    global num_reads
    num_reads += 1
    return out.read(amount)


num_changes = 0
for change in apsw.Changeset.iter(read):
    num_changes += 1

print("Number of reads", num_reads)
print("Number of changes", num_changes)

### rebaser: Rebasing
# You can merge conflict decisions from an earlier changeset into a
# later changeset so that you don't have to separately transport and
# store those conflict decisions.  This can be used to take
# independently made changesets, and turn them into a linear sequence.
# The `rebaser documentation
# <https://www.sqlite.org/session/rebaser.html>`__ includes more
# detail.
#
# To do a rebase, you need to take the conflict resolutions
# from an :meth:`Changeset.apply` to :meth:`Rebaser.configure`, and
# then :meth:`Rebaser.rebase` a following changeset.
#
# We are going to make alice then bob appear to have been done in that
# order without conflicts.

# Reset back to original data with the base changeset applied
connection.execute("""
    DROP TABLE item_tag_link;
    DROP TABLE items;
    DROP TABLE tags;
""")

connection.execute(pathlib.Path("session.sql").read_text())


# The conflict handler we'll use doing latest writer wins - you should
# be more careful.
def conflict_handler(reason: int, change: apsw.TableChange) -> int:
    if reason in (
        apsw.SQLITE_CHANGESET_DATA,
        apsw.SQLITE_CHANGESET_CONFLICT,
    ):
        return apsw.SQLITE_CHANGESET_REPLACE
    return apsw.SQLITE_CHANGESET_OMIT


# apply original changeset that alice was based on
apsw.Changeset.apply(changeset, connection, conflict=conflict_handler)

# Make a rebaser
rebaser = apsw.Rebaser()

# save these conflict resolutions
conflict_resolutions = apsw.Changeset.apply(
    alice_changeset,
    connection,
    conflict=conflict_handler,
    rebase=True,
)

rebaser.configure(conflict_resolutions)

# and apply them to bob's
bob_rebased = rebaser.rebase(bob_changeset)

### session_diff: Table diff
# :meth:`Session.diff` can be used to get the difference between a
# table in another database and this database.  This is useful if the
# other database was updated without a session being recorded.  Note that
# the table must have a ``PRIMARY KEY``, or it will be ignored.

diff_demo = apsw.Connection("diff_demo.db")

diff_demo.execute("""
    -- our session runs on this
    CREATE TABLE example(x PRIMARY KEY, y, z);
    INSERT INTO example VALUES
            (1, 'one', 1.1),
            (2, 'two', 2.2),
            (3, 'three', 3.3);

    -- the other database
    ATTACH 'other.db' AS other;
    -- the table has to have the same name, primary key, and columns
    CREATE TABLE other.example(x PRIMARY KEY, y, z);
    INSERT INTO other.example VALUES
            -- extra row
            (0, 'zero', 0.0),
            -- id 1 deliberately missing
            (2, 'two', 2.2),
            -- different values
            (3, 'trois', 'hello');
""")

session = apsw.Session(diff_demo, "main")

# You must attach (or filter) to include the table
session.attach("example")

session.diff("other", "example")

diff = session.changeset()

show_changeset("Table diff", diff, diff_demo)

### session_end: Cleanup
# We can now close the connections, but it is optional.  APSW automatically
# cleans up sessions when their corresponding connections are closed.

connection.close()
alice_connection.close()
bob_connection.close()
