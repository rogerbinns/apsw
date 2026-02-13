#!/usr/bin/env python3

# This code uses Python's optional typing annotations.  You can
# ignore them and do not need to use them.  If you do use them
# then you must include this future annotations line first.
from __future__ import annotations

import contextlib
import io
import time
import sys

from pprint import pprint

import apsw
import apsw.aio
import apsw.bestpractice
import apsw.ext
import apsw.fts5
import apsw.shell

# all the popular async frameworks are supported
import asyncio
import anyio
import trio

### async_basics: Basics
# Use :meth:`Connection.as_async` to get an async connection, and
# async for to iterate results.  We also apply :doc:`best practice
# <bestpractice>` and use :func:`contextlib.aclosing` to ensure the
# database is closed while the event loop is still running.

apsw.bestpractice.apply(apsw.bestpractice.recommended)


async def basics():
    db = await apsw.Connection.as_async(":memory:")

    # always close database
    async with contextlib.aclosing(db):
        # create some rows
        await db.execute("""
            CREATE TABLE numbers(value, name);
            INSERT INTO numbers VALUES (1, 'one'), (20, 'twenty'),
                         (3, 'three'), (10, 'ten');
        """)

        # query - note we have to await to get the cursor before
        # iterating in the for loop
        async for value, name in await db.execute(
            "SELECT value, name FROM numbers ORDER BY value DESC"
        ):
            print(f"{value=} {name=}")

        # .get is great if you expect only a single value or
        # row.  Lets get the number of registered functions
        count = await (
            await db.execute(
                "SELECT COUNT(DISTINCT(name)) FROM pragma_function_list"
            )
        ).get
        print(f"There are {count} functions")

        # a pragma
        print(f"journal_mode={await db.pragma('journal_mode')}")

        # You should always use a transaction - use async with
        async with db:
            await db.execute("INSERT INTO numbers VALUES(7, 'seven')")
            # nested transactions are supported via savepoints
            async with db:
                await db.execute("DROP TABLE numbers")
                # any exception in the async with block
                # will rollback that block, while successful
                # completion commits the changes


asyncio.run(basics())

### async_callbacks: Functions and callbacks
# Any function/callback can be sync or async.


# an async scalar function
async def a_add(one, two):
    print("async scalar called")
    return one + two


# and a sync scalar function
def s_add(one, two):
    print("sync scalar called")
    return one + two


# how about an async update hook?
async def my_hook(op: int, dbname: str, table: str, rowid: int):
    print(f"update {op=} {dbname=} {table=} {rowid=}")


async def callbacks():
    db = await apsw.Connection.as_async(":memory:")

    # always close database
    async with contextlib.aclosing(db):
        await db.create_scalar_function("a_add", a_add)
        await db.create_scalar_function("s_add", s_add)

        # use them both in the same query.  we need to await execute
        # to get the cursor, and then await the fetchall on the
        # cursor.
        print(
            await (
                await db.execute("SELECT a_add(1, 2), s_add(3, 4)")
            ).fetchall()
        )

        # A regular callback
        await db.set_update_hook(my_hook)

        await db.execute(
            "CREATE TABLE x(y); INSERT INTO x VALUES(42)"
        )


# use anyio this time
anyio.run(callbacks)

### async_cancellation: Cancellation
# Often a group of tasks are run at the same time.  The frameworks
# provide a way to group tasks, wait until all are complete, and if
# any fail, then cancel uncompleted ones, and raise the resulting
# exceptions.
#
# * :external+python:ref:`except * <except_star>` and
#   :class:`ExceptionGroup` Python syntax for catching multiple
#   exceptions such as from a group of tasks
# * :class:`asyncio.TaskGroup`
# * :external+trio:ref:`Trio tasks <tasks>`
# * :external+anyio:doc:`AnyIO tasks <tasks>`
#
# This example shows asyncio, but the principles are the same across
# all the frameworks.


async def cancellation():
    # this will block SQLite for the sleep duration
    async def sleep(duration):
        await asyncio.sleep(duration)
        return duration

    async def deliberate_error():
        1 / 0
        return 3

    db = await apsw.Connection.as_async(":memory:")

    # always close database
    async with contextlib.aclosing(db):
        await db.create_scalar_function("sleep", sleep)

        start = time.monotonic()

        # create some tasks in a task group that will all
        # run simultaneously
        try:
            async with asyncio.TaskGroup() as tg:
                # this query will sleep for an hour
                task1 = tg.create_task(
                    db.execute("SELECT sleep(3600)")
                )
                # this query will only run after the hour sleep query
                # finishes because we can only do one SQLite query at
                # a time
                task2 = tg.create_task(
                    db.execute("SELECT * FROM sqlite_schema")
                )
                # this will also sleep for an hour
                task3 = tg.create_task(asyncio.sleep(3600))
                # this will have an error
                task4 = tg.create_task(deliberate_error())

                # the TaskGroup with block will now run all the tasks
                # to completion before exiting the block

        # note the * after except which is how you do exception groups
        except* ZeroDivisionError:
            print(
                f"got zero division error after {time.monotonic() - start:.6f} seconds"
            )

        # Lets see what happened to all the tasks.  Note how they are
        # all done (complete) and how all except the deliberate error
        # got cancelled.
        print(f"{task1.done()=} {task1.cancelled()=}")
        print(f"{task2.done()=} {task2.cancelled()=}")
        print(f"{task3.done()=} {task3.cancelled()=}")
        print(f"{task4.done()=} {task4.cancelled()=}")

        # Lets verify SQLite is not still waiting for an hour
        start = time.monotonic()
        functions = await (
            await db.execute(
                "SELECT COUNT(*) FROM pragma_function_list"
            )
        ).get
        print(
            f"After {time.monotonic() - start:.6f} seconds, "
            f"there are {functions} registered SQLite functions"
        )


asyncio.run(cancellation())

### async_timeout: Timeouts
# This demonstrates timeouts for both async and sync code.  The sync
# SQL is `the outlandish fractal
# <https://www.sqlite.org/lang_with.html#outlandish_recursive_query_examples>`__
# but with the ``28`` changed to ``800_000``` and would take days to
# run to completion.
#
# The deadline for async functions is enforced by the async event loop
# and tends to be accurate. The deadline for sync functions is based
# on SQLite periodically calling the :meth:`progress handler
# <apsw.Connection.set_progress_handler>`.
#
# There is a dedicated :attr:`apsw.aio.deadline` which takes priority
# for all frameworks, For |trio| and |anyio| their native timeouts are
# also supported if :attr:`apsw.aio.deadline` has not been set.  The
# :attr:`~apsw.aio.deadline` documentation has more details on setting
# deadlines for each framework, getting their current time, and
# exceptions raised on timeout.
#
# * :meth:`asyncio.get_running_loop().time() <asyncio.loop.time>`
# * :exc:`TimeoutError`
# * :exc:`trio.TooSlowError`
# * :func:`trio.current_time`
# * :func:`trio.current_effective_deadline`
# * :func:`trio.fail_after` :func:`trio.fail_at`
# * :func:`anyio.current_time`
# * :func:`anyio.current_effective_deadline`
# * :func:`anyio.fail_after`


# The query is not reproduced here but is used when running this
# example.
fractal_sql = "outlandish fractal"


async def timeouts():
    async def sleep(amount):
        await trio.sleep(amount)
        return 42

    db = await apsw.Connection.as_async(":memory:")

    # always close database
    async with contextlib.aclosing(db):
        await db.create_scalar_function("sleep", sleep)

        try:
            # This will work with every framework.  Half a second from now.
            with apsw.aio.contextvar_set(
                apsw.aio.deadline, trio.current_time() + 0.5
            ):
                start = trio.current_time()
                await (
                    await db.execute("SELECT sleep(3600)")
                ).fetchall()
        except trio.TooSlowError:
            end = trio.current_time()
            print(
                f"Got async function TooSlowError after {end - start:.6f} seconds"
            )

        # With trio and anyio we can use the native framework timeouts
        try:
            with trio.fail_after(0.5):
                await (await db.execute(fractal_sql)).fetchall()
        except trio.TooSlowError:
            end = trio.current_time()
            print(
                f"Got sync function TooSlowError after {end - start:.6f} seconds"
            )


trio.run(timeouts)

### async_worker_thread: Worker thread
# Async connections work by running the SQLite operations in a
# dedicated background thread.  You can also run your own code there
# which is especially useful if it does many calls before returning a
# final result.  Use :meth:`Connection.async_run`.
#
# Examples shown include :ref:`schema_upgrade` and getting a text
# :ref:`dump <shell-cmd-dump>`.
#
# In the worker thread, the connection is a regular sync connection.


def schema_upgrade(db: apsw.Connection):
    # The user_version is a great way of tracking and upgrading the
    # schema.  Because this is run in the worker thread it is the
    # normal sync approach.

    # Do everything in a single transactions
    with db:
        # database fresh state
        if db.pragma("user_version") == 0:
            db.execute("""
                CREATE TABLE products(id, name, sku, price);
                CREATE TABLE orders(id, product_id, quantity);
                pragma user_version = 1;
            """)
        if db.pragma("user_version") == 1:
            db.execute("""
                ALTER TABLE products ADD COLUMN description;
                CREATE INDEX orders_idx ON orders(id, product_id);
                pragma user_version = 2;
            """)
        # we could exception here if user_version > 2 because it means
        # a more recent schema is present than this code understands.
        # Perhaps a version downgrade happened?


async def worker_thread():
    db = await apsw.Connection.as_async(":memory:")

    # always close database
    async with contextlib.aclosing(db):
        # Do the upgrade
        await db.async_run(schema_upgrade, db)

        # This is doing one operation
        await db.async_run(
            db.execute,
            "INSERT INTO products(id, name) VALUES(?,?)",
            (37, "Banana"),
        )

        # Getting a result
        rows = await db.async_run(
            lambda: db.execute("SELECT COUNT(*) FROM products").get
        )
        print(f"{rows=}")

        # get a dump - to a memory file here, but you'd want to use
        # a real file
        out = io.StringIO()
        await db.async_run(
            lambda: apsw.shell.Shell(
                db=db, stdout=out
            ).process_command(".dump")
        )
        dump = out.getvalue()
        print(f"Dump is {len(dump)} chars starting {repr(dump):.40}")

        # Some stuff from apsw.ext
        usage = await db.async_run(apsw.ext.analyze_pages, db, 2)
        details = await db.async_run(
            apsw.ext.query_info, db, "SELECT * FROM sqlite_schema"
        )


anyio.run(worker_thread)

### async_vtable: Virtual tables
# :ref:`Virtual tables <virtualtables>` are a very good match for
# async, especially if networking is involved.  You can do your own
# :class:`full implementation <VTModule>` using sync and async methods
# as needed, but will find it easier to start with
# :func:`apsw.ext.make_virtual_module` that turns any Python function
# (sync or async) in a virtual table accepting positional and keyword
# arguments.  The sync :ref:`example is here <example_virtual_tables>`
# with the async below being pretty much the same thing.


async def data_table(flags, server="example.com"):
    # one positional and one keyword argument.  note you can't change
    # the number of columns or their order based on the arguments but
    # you can change what rows are returned and their contents
    print(f"data_table called with {flags=} and {server=}")

    # you would do real work here - we just provide some books
    yield ("The Great Gatsby", 1925, 9.2)
    yield ("To Kill a Mockingbird", 1960, 9.5)
    yield ("1984", 1949, 9.8)
    yield ("The Catcher in the Rye", 1951, 8.4)
    yield ("The Hobbit", 1937, 9.6)


# Tell make_virtual_module about the columns
data_table.columns = ("title", "year", "review")
# ... and how to extract them from each row
data_table.column_access = apsw.ext.VTColumnAccess.By_Index


async def virtual_tables():
    db = await apsw.Connection.as_async(":memory:")

    # always close database
    async with contextlib.aclosing(db):
        await apsw.ext.make_virtual_module(db, "books", data_table)

        # regular query
        async for row in await db.execute(
            "SELECT * FROM books WHERE flags=94 AND server=?",
            ("example2.com",),
        ):
            print(row)

        # SQLite will do the query work
        async for row in await db.execute(
            "SELECT * FROM books WHERE server=? AND flags=?AND review > 9.55 "
            "   ORDER BY year DESC",
            ("orange", -2),
        ):
            print(row)


trio.run(virtual_tables)

### async_trace: Tracing in a block
# This is the same as :ref:`sync tracing in a block <example_Trace>`
# adapted to use ``async with`` for :class:`apsw.ext.Trace` and
# transaction control.


async def tracing():
    db = await apsw.Connection.as_async(":memory:")

    # always close database
    async with contextlib.aclosing(db):
        # Use None instead of stdout and no information is printed or gathered
        async with apsw.ext.Trace(
            sys.stdout,
            db=db,
            vtable=True,
            updates=True,
            transaction=True,
        ):
            # APSW does a savepoint behind the scenes to wrap the block
            async with db:
                # Some regular SQL
                await db.execute("create table multi(x)")
                # executemany runs the same statement repeatedly
                await db.executemany(
                    "insert into multi values(?)",
                    ((x,) for x in range(5)),
                )
                # See how many rows were processed
                await (
                    await db.execute("select * from multi limit 2")
                ).fetchall()
                # You can also see how many rows were changed
                await db.execute("delete from multi where x < 4")

            # pragma functions are virtual tables - see how many rows this processes even
            # though only one has 'pow'
            await (
                await db.execute(
                    "SELECT narg FROM pragma_function_list WHERE name='pow'"
                )
            ).get

            # trigger that causes rollback
            await db.execute("""
                create trigger error after insert on multi
                begin
                update multi set rowid=100+new.rowid where rowid=new.rowid;
                select raise(rollback, 'nope');
            end;
            """)

            with contextlib.suppress(apsw.ConstraintError):
                await db.execute("insert into multi values(54)")


asyncio.run(tracing())

### async_resource: Resource usage in a block
# The async equivalent of :ref:`the sync example
# <example_ShowResourceUsage>`.

# The standard outlandish fractal example which is used when running
# but not reproduced here.
query = "fractal"


async def resource_usage():
    db = await apsw.Connection.as_async(":memory:")

    # always close database
    async with contextlib.aclosing(db):
        print("thread (async event loop)")

        async with apsw.ext.ShowResourceUsage(
            sys.stdout, db=db, scope="thread"
        ):
            # some SQLite work
            await (await db.execute(query)).get

            # and take some wall clock time
            await trio.sleep(0.5)

        print("\nprocess (including background SQLite worker)")

        async with apsw.ext.ShowResourceUsage(
            sys.stdout, db=db, scope="process"
        ):
            # some SQLite work
            await (await db.execute(query)).get

            # and take some wall clock time
            await trio.sleep(0.5)


trio.run(resource_usage)

### async_blob: Blob
# Async :doc:`blob` run in the SQLite worker thread.  See the
# :ref:`sync example <example_blob_io>` which this is a direct
# translation to async.


async def blob():
    db = await apsw.Connection.as_async(":memory:")

    # always close database
    async with contextlib.aclosing(db):
        await db.execute("create table blobby(x,y)")
        # Add a blob we will fill in later
        await db.execute(
            "insert into blobby values(1, zeroblob(10000))"
        )
        # Or as a binding
        await db.execute(
            "insert into blobby values(2, ?)", (apsw.zeroblob(20000),)
        )
        # Open a blob for writing.  We need to know the rowid
        rowid = await (
            await db.execute("select ROWID from blobby where x=1")
        ).get
        blob = await db.blob_open("main", "blobby", "y", rowid, True)
        await blob.write(b"hello world")
        # seeking is immediate (no await)
        blob.seek(2000)
        await blob.read(24)
        # seek relative to the end
        blob.seek(-32, 2)
        await blob.write(b"hello world, again")
        # it will be automatically closed when the connection is
        # closed, but explicitly closing chooses transaction
        # boundaries
        await blob.aclose()


anyio.run(blob)

### async_backup: Backup
# :doc:`Backups <backup>` run in the SQLite worker thread of the async
# destination database.  The source can be a sync or async database.
# You do backups by getting the backup object from the destination
# database telling it about the source using
# :meth:`Connection.backup`,
#
# If the destination is sync and you are working with an async source,
# you can run the backup in the async source thread as demonstrated
# below.


async def backup():
    # Setup source and destinations

    async_source = await apsw.Connection.as_async("")
    # ... and fill it with a large amount of data
    await async_source.execute(
        "CREATE TABLE x(y); INSERT INTO x VALUES(randomblob(250000))"
    )
    sync_source = apsw.Connection("")
    sync_source.execute(
        "CREATE TABLE x(y); INSERT INTO x VALUES(randomblob(250000))"
    )

    async_dest = await apsw.Connection.as_async("")
    sync_dest = apsw.Connection("")

    print("async destination, async source")
    async with await async_dest.backup(
        "main", async_source, "main"
    ) as backup:
        while not backup.done:
            await backup.step(42)
            print(
                f"page_count = {backup.page_count} remaining = {backup.remaining}"
            )

    print("async destination, sync source")
    async with await async_dest.backup(
        "main", sync_source, "main"
    ) as backup:
        while not backup.done:
            await backup.step(42)
            print(
                f"page_count = {backup.page_count} remaining = {backup.remaining}"
            )

    print("sync destination, async source")

    # we will run this in the async source thread
    def do_backup():
        with sync_dest.backup("main", async_source, "main") as backup:
            while not backup.done:
                backup.step(42)
                print(
                    f"page_count = {backup.page_count} remaining = {backup.remaining}"
                )

    await async_source.async_run(do_backup)

    # ensure connections get closed
    await async_source.aclose()
    await async_dest.aclose()


asyncio.run(backup())

### async_fts: Full Text Search
# :class:`~apsw.fts5.Table` accesses the database for virtually all
# methods and attributes, so using the :ref:`worker thread
# <example_async_worker_thread>` is needed.  A subset of the
# :doc:`example-fts` is shown.


async def fts():
    db = await apsw.Connection.as_async("recipes.db")

    # always close database
    async with contextlib.aclosing(db):
        if not await db.table_exists("main", "search"):
            search_table: apsw.fts5.Table = await db.async_run(
                apsw.fts5.Table.create,
                db,
                "search",
                content="recipes",
                columns=None,
                generate_triggers=True,
                tokenize=[
                    "simplify",
                    "casefold",
                    "true",
                    "strip",
                    "true",
                    "strip",
                    "true",
                    "unicodewords",
                ],
            )
        else:
            search_table: apsw.fts5.Table = await db.async_run(
                apsw.fts5.Table, db, "search"
            )

        # property access
        print(
            "row_count =",
            await db.async_run(getattr, search_table, "row_count"),
        )

        # we need to do search processing in the worker thread
        def search_processing(query: str, limit: int):
            matches = []
            for match in search_table.search(query):
                matches.append(match)
                if len(matches) >= limit:
                    break
            return matches

        for match in await db.async_run(
            search_processing, "lemon OR guava", 10
        ):
            pprint(match)
            break

        print(
            "First match name is",
            await db.async_run(
                search_table.row_by_id, match.rowid, "name"
            ),
        )

        # query suggestion
        query = "nyme:(minced OR oyl NOT peenut)"
        print(
            query,
            "=>",
            await db.async_run(search_table.query_suggest, query),
        )


asyncio.run(fts())

### async_session: Session
# Use :func:`apsw.aio.make_session` to create the
# :class:`~apsw.Session` object in async mode from an async
# connection.


async def session_example():
    db = await apsw.Connection.as_async(":memory:")

    # always close database
    async with contextlib.aclosing(db):
        await db.execute("CREATE TABLE x(y PRIMARY KEY, z)")

        session = await apsw.aio.make_session(db, "main")

        # We'd like size estimates
        session.config(apsw.SQLITE_SESSION_OBJCONFIG_SIZE, True)

        # all tables
        await session.attach()

        # add some data
        await db.executemany(
            "INSERT INTO x VALUES(?,?)",
            ((i, "a" * i) for i in range(200)),
        )

        print("Size estimate {session.changeset_size}")
        changeset = await session.changeset()
        print(f"Actual size {len(changeset)}")

        # Other than apply, changeset operations don't use a
        # Connection so we'll use trio's mechanism to do invert in a
        # background thread.
        undo = await trio.to_thread.run_sync(
            apsw.Changeset.invert, changeset
        )

        #  Undo the changes
        await apsw.Changeset.apply(undo, db)


trio.run(session_example)
