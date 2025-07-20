#!/usr/bin/env python3

# This code uses Python's optional typing annotations.  You can
# ignore them and do not need to use them.  If you do use them
# then you must include this future annotations line first.
from __future__ import annotations

from typing import Optional, Iterator, Any

import os
import sys
import time
import datetime
import apsw
import apsw.ext
import random
import re
import contextlib
from pathlib import Path

# pretty formatting
from pprint import pprint

### version_check: Checking APSW and SQLite versions

# Where the extension module is on the filesystem
print("      Using APSW file", apsw.__file__)

# From the extension
print("         APSW version", apsw.apsw_version())

# From the sqlite header file at APSW compile time
print("SQLite header version", apsw.SQLITE_VERSION_NUMBER)

# The SQLite code running
print("   SQLite lib version", apsw.sqlite_lib_version())

# If True then SQLite is incorporated into the extension.
# If False then a shared library is being used, or static linking
print("   Using amalgamation", apsw.using_amalgamation)

### bestpractice: Best Practice
# Ensure SQLite usage prevents common mistakes, and gets best
# performance via :doc:`apsw.bestpractice <bestpractice>`

import apsw.bestpractice

apsw.bestpractice.apply(apsw.bestpractice.recommended)

### logging: Logging
# It is a good idea to get SQLite's logs as you will get more
# information about errors. Best practice also includes this.
# :meth:`apsw.ext.log_sqlite` forwards SQLite's log messages to the
# :mod:`logging` module.

apsw.ext.log_sqlite()

# You can also write to SQLite's log
apsw.log(apsw.SQLITE_ERROR, "A message from Python")

### open_db:  Opening the database
# You open the database by using :class:`Connection`

# Default will create the database if it doesn't exist
connection = apsw.Connection("dbfile")

# Open existing read-only
connection = apsw.Connection(
    "dbfile", flags=apsw.SQLITE_OPEN_READONLY
)

# Open existing read-write (exception if it doesn't exist)
connection = apsw.Connection(
    "dbfile", flags=apsw.SQLITE_OPEN_READWRITE
)

### executing_sql: Executing SQL
# Use :meth:`Connection.execute` to execute SQL

connection.execute("create table point(x,y,z)")
connection.execute("insert into point values(1, 2, 3)")
# You can use multiple ; separated statements
connection.execute(
    """
    insert into point values(4, 5, 6);
    create table log(timestamp, event);
    create table foo(a, b, c);
    create table important(secret, data);
"""
)

# read rows
for row in connection.execute("select * from point"):
    print(row)

### why_bindings: Why you use bindings to provide values
# It is tempting to compose strings with the values in them, but it is
# easy to mangle the query especially if values contain punctuation
# and unicode.  It is known as `SQL injection
# <https://en.wikipedia.org/wiki/SQL_injection>`__. Bindings are the
# correct way to supply values to queries.

# a simple value
event = "system started"
# DO NOT DO THIS
query = f"insert into log values(0, '{ event }')"
print("query:", query)

# BECAUSE ... a bad guy could provide a value like this
event = "bad guy here') ; drop table important; -- comment"
# which has effects like this
query = f"insert into log values(0, '{ event }')"
print("bad guy:", query)

### bindings_sequence: Bindings (sequence)
# Bindings can be provided as a sequence such as with
# a tuple or list.  Use **?** to show where the values go.

query = "insert into log values(?, ?)"
data = (7, "transmission started")
connection.execute(query, data)

# You can also use numbers after the ? to select
# values from the sequence.  Note that numbering
# starts at 1
query = "select ?1, ?3, ?2"
data = ("alpha", "beta", "gamma")
for row in connection.execute(query, data):
    print(row)

### bindings_dict: Bindings (dict)
# You can also supply bindings with a dictionary.  Use **:NAME**,
# **@NAME**, or **$NAME**, to provide the key name in the query.
# Names are case sensitive.

query = "insert into point values(:x, @Y, $z)"
data = {"x": 7, "Y": 8, "z": 9}
connection.execute(query, data)

### transaction: Transactions
# By default each statement is its own transaction.  A transaction
# finishes by flushing data to storage and waiting for the operating
# system to confirm it is permanently there (ie will survive a power
# failure) which takes a while.

# 3 separate transactions
connection.execute("insert into point values(2, 2, 2)")
connection.execute("insert into point values(3, 3, 3)")
connection.execute("insert into point values(4, 4, 4)")

# You can use BEGIN / COMMIT to manually make a transaction
connection.execute("BEGIN")
connection.execute("insert into point values(2, 2, 2)")
connection.execute("insert into point values(3, 3, 3)")
connection.execute("insert into point values(4, 4, 4)")
connection.execute("COMMIT")

# Or use `with` that does it automatically
with connection:
    connection.execute("insert into point values(2, 2, 2)")
    connection.execute("insert into point values(3, 3, 3)")
    connection.execute("insert into point values(4, 4, 4)")

# Nested transactions are supported
with connection:
    connection.execute("insert into point values(2, 2, 2)")
    with connection:
        connection.execute("insert into point values(3, 3, 3)")
        connection.execute("insert into point values(4, 4, 4)")

### executemany: executemany
# You can execute the same SQL against a sequence using
# :meth:`Connection.executemany`

data = (
    (1, 1, 1),
    (2, 2, 2),
    (3, 3, 3),
    (4, 4, 4),
    (5, 5, 5),
)
query = "insert into point values(?,?,?)"

# we do it in a transaction
with connection:
    # the query is run for each item in data
    connection.executemany(query, data)

### pragma: Pragmas
# SQLite has a `wide variety of pragmas <https://www.sqlite.org/pragma.html>`__ to control
# the database configuration and library behaviour.  See the :doc:`tips` for maintaining
# your schema.

# WAL mode is good for write performance
connection.pragma("journal_mode", "wal")

# Foreign keys are off by default, so turn them on
connection.pragma("foreign_keys", True)

# You can use this to see if any other connection (including other processes) has
# changed the database
connection.pragma("data_version")

# Useful at startup to detect some database corruption
check = connection.pragma("integrity_check")
if check != "ok":
    print("Integrity check errors", check)

### exectrace: Tracing execution
# You can trace execution of SQL statements and their bindings.  This
# involves code changes and is :ref:`described in more detail here
# <tracing>`.
#
# There are simpler convenient mechanisms for :ref:`individual
# statement tracing <example_Trace>`, :Ref:`summarising a block of
# code <example_ShowResourceUsage>`, and :ref:`SQLite's interface
# <example_trace_v2>` which is used by them.


def my_tracer(
    cursor: apsw.Cursor,
    statement: str,
    bindings: Optional[apsw.Bindings],
) -> bool:
    "Called just before executing each statement"
    print("SQL:", statement.strip())
    print("Bindings:", bindings)
    return True  # if you return False then execution is aborted


# you can trace a single cursor
cursor = connection.cursor()
cursor.exec_trace = my_tracer
cursor.execute(
    """
        drop table if exists bar;
        create table bar(x,y,z);
        select * from point where x=?;
        """,
    (3,),
)

# if set on a connection then all cursors are traced
connection.exec_trace = my_tracer
# and clearing it
connection.exec_trace = None

### rowtrace: Tracing returned rows
# You can trace returned rows, including modifying what is returned or
# skipping it completely.  See :ref:`more about tracing <tracing>`.


def row_tracer(
    cursor: apsw.Cursor, row: apsw.SQLiteValues
) -> apsw.SQLiteValues:
    """Called with each row of results before they are handed off.  You can return None to
    cause the row to be skipped or a different set of values to return"""
    print("Row:", row)
    return row


# you can trace a single cursor
cursor = connection.cursor()
cursor.row_trace = row_tracer
for row in cursor.execute("select x,y from point where x>4"):
    pass

# if set on a connection then all cursors are traced
connection.row_trace = row_tracer
# and clearing it
connection.row_trace = None

### scalar: Defining scalar functions
# Scalar functions take one or more values and return one value.  They
# are registered by calling :meth:`Connection.create_scalar_function`.


def ilove7(*args: apsw.SQLiteValue) -> int:
    "A scalar function"
    print(f"ilove7 got { args } but I love 7")
    return 7


connection.create_scalar_function("seven", ilove7)

for row in connection.execute(
    "select seven(x,y) from point where x>4"
):
    print("row", row)

### aggregate: Defining aggregate functions
# Aggregate functions are called multiple times with matching rows,
# and then provide a final value.  An example is calculating an
# average.  They are registered by calling
# :meth:`Connection.create_aggregate_function`.


class longest:
    # Find which value when represented as a string is
    # the longest

    def __init__(self) -> None:
        self.longest = ""

    def step(self, *args: apsw.SQLiteValue) -> None:
        # Called with each matching row
        for arg in args:
            if len(str(arg)) > len(self.longest):
                self.longest = str(arg)

    def final(self) -> str:
        # Called at the very end
        return self.longest


connection.create_aggregate_function("longest", longest)
print(connection.execute("select longest(event) from log").get)

### window: Defining window functions
# Window functions input values come from a "window" around a row of
# interest.  Four methods are called as the window moves to add,
# remove, get the current value, and finalize.
#
# An example is calculating an average of values in the window to
# compare to the row.  They are registered by calling
# :meth:`Connection.create_window_function`.
#
# This is the Python equivalent to the C based example in the `SQLite
# documentation
# <https://www.sqlite.org/windowfunctions.html#user_defined_aggregate_window_functions>`__


class SumInt:
    def __init__(self):
        self.v = 0

    def step(self, arg):
        print("step", arg)
        self.v += arg

    def inverse(self, arg):
        print("inverse", arg)
        self.v -= arg

    def final(self):
        print("final", self.v)
        return self.v

    def value(self):
        print("value", self.v)
        return self.v


connection.create_window_function("sumint", SumInt)

for row in connection.execute(
    """
        CREATE TABLE t3(x, y);
        INSERT INTO t3 VALUES('a', 4),
                             ('b', 5),
                             ('c', 3),
                             ('d', 8),
                             ('e', 1);
        -- Use the window function
        SELECT x, sumint(y) OVER (
        ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        ) AS sum_y
        FROM t3 ORDER BY x;
    """
):
    print("ROW", row)

### collation: Defining collations (sorting)
# How you sort can depend on the languages or values involved.  You
# register a collation by calling :meth:`Connection.create_collation`.

# This example sorting mechanisms understands some text followed by a
# number and ensures the number portion gets sorted correctly

connection.execute("create table names(name)")
connection.executemany(
    "insert into names values(?)",
    (
        ("file1",),
        ("file7",),
        ("file17",),
        ("file20",),
        ("file3",),
    ),
)

print("Standard sorting")
for row in connection.execute("select * from names order by name"):
    print(row)


def str_num_collate(
    s1: apsw.SQLiteValue, s2: apsw.SQLiteValue
) -> int:
    # return -1 if s1<s2, +1 if s1>s2 else 0 for equal

    def parts(s: str) -> list:
        "Converts str into list of alternating str and int parts"
        return [
            int(v) if v.isdigit() else v
            for v in re.split(r"(\d+)", s)
        ]

    ps1 = parts(str(s1))
    ps2 = parts(str(s2))

    # compare
    if ps1 < ps2:
        return -1
    if ps1 > ps2:
        return 1
    return 0


connection.create_collation("strnum", str_num_collate)

print("\nUsing strnum")
for row in connection.execute(
    "select * from names order by name collate strnum"
):
    print(row)

### colnames: Accessing results by column name
# You can access results by column name using :mod:`dataclasses`.
# APSW provides :class:`apsw.ext.DataClassRowFactory` for names.

import apsw.ext

connection.execute(
    """
    create table books(id, title, author, year);
    insert into books values(7, 'Animal Farm', 'George Orwell', 1945);
    insert into books values(37, 'The Picture of Dorian Gray', 'Oscar Wilde', 1890);
    """
)

# Normally you use column numbers
for row in connection.execute(
    "select title, id, year from books where author=?",
    ("Oscar Wilde",),
):
    # this is very fragile
    print("title", row[0])
    print("id", row[1])
    print("year", row[2])

# Turn on dataclasses - frozen makes them read-only
connection.row_trace = apsw.ext.DataClassRowFactory(
    dataclass_kwargs={"frozen": True}
)

print("\nNow with dataclasses\n")

# Same query - note using AS to set column name
for row in connection.execute(
    """SELECT title,
           id AS book_id,
           year AS book_year
           FROM books WHERE author = ?""",
    ("Oscar Wilde",),
):
    print("title", row.title)
    print("id", row.book_id)
    print("year", row.book_year)

# clear
connection.row_trace = None

### type_conversion: Type conversion into/out of database
# You can use :class:`apsw.ext.TypesConverterCursorFactory` to do
# conversion, both for types you define and for other types.

import apsw.ext

registrar = apsw.ext.TypesConverterCursorFactory()
connection.cursor_factory = registrar


# A type we define - deriving from SQLiteTypeAdapter automatically registers conversion
# to a SQLite value
class Point(apsw.ext.SQLiteTypeAdapter):
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __repr__(self) -> str:
        return f"Point({ self.x }, { self.y })"

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, type(self))
            and self.x == other.x
            and self.y == other.y
        )

    def to_sqlite_value(self) -> str:
        # called to convert Point into something SQLite supports
        return f"{ self.x };{ self.y }"

    # This converter will be registered
    @classmethod
    def convert_from_sqlite(cls, value: str) -> Point:
        return cls(*(float(part) for part in value.split(";")))


# Existing types
def complex_to_sqlite_value(c: complex) -> str:
    return f"{ c.real }+{ c.imag }"


def datetime_to_sqlite_value(dt: datetime.datetime) -> float:
    # Represent as floating point UTC value no matter
    # what timezone is used. Also consider other
    # formats like ISO8601.
    return dt.timestamp()


# ... require manual registration
registrar.register_adapter(complex, complex_to_sqlite_value)
registrar.register_adapter(
    datetime.datetime, datetime_to_sqlite_value
)

# conversion from a SQLite value requires registration
registrar.register_converter("POINT", Point.convert_from_sqlite)


# ... and for stdlib types
def sqlite_to_complex(v: str) -> complex:
    return complex(*(float(part) for part in v.split("+")))


def sqlite_to_datetime(v: float) -> datetime.datetime:
    # Keep the UTC values coming back from the database
    # as UTC
    return datetime.datetime.fromtimestamp(v, datetime.timezone.utc)


registrar.register_converter("COMPLEX", sqlite_to_complex)
registrar.register_converter("TIMESTAMP", sqlite_to_datetime)

# note that the type names are case sensitive and must match the
# registration
connection.execute(
    "create table conversion(p POINT, c COMPLEX, t TIMESTAMP)"
)

# convert going into database
test_data = (Point(5.2, 7.6), 3 + 4j, datetime.datetime.now())
connection.execute(
    "insert into conversion values(?, ?, ?)", test_data
)
print("inserted", test_data)

# and coming back out
print("querying data")
for row in connection.execute("select * from conversion"):
    for i, value in enumerate(row):
        print(f"column {i} = { value !r}")

# clear registrar
connection.cursor_factory = apsw.Cursor

### pyobject: Runtime Python objects
# While only :ref:`5 types <types>` can be stored, you can pass any
# Python objects :ref:`to and from your functions <pyobject>` at
# runtime.

# Python set which isn't a supported SQLite type
# containing items like a complex number and stdout which
# definitely aren't SQLite compatible
py_value = {1, 2, "three", 4 + 5j, sys.stdout}

# Trying to pass it as a value gives TypeError
try:
    print(connection.execute("select ?", (py_value,)).get)
except TypeError as exc:
    print(exc)

# Now wrap it and it works
print(
    "select ?",
    connection.execute("select ?", (apsw.pyobject(py_value),)).get,
)

# It is still null at the SQL level
print(
    "select typeof(?)",
    connection.execute(
        "select typeof(?)", (apsw.pyobject(py_value),)
    ).get,
)


# Lets make a set which SQLite knows nothing about
def make_set(*args):
    print(f"make_set got {args!r}")
    # this will return a set, so we also need to mark it
    return apsw.pyobject(set(args))


connection.create_scalar_function("make_set", make_set)

print(
    "select make_set(?, ?, ?)",
    connection.execute(
        "select make_set(?, ?, ?)",
        (
            # these aren't SQLite types
            apsw.pyobject(3 + 4j),
            apsw.pyobject(sys.stdin),
            # but a string is
            "hello",
        ),
    ).get,
)

### query_limit: Query limiting
# :meth:`apsw.ext.query_limit` limits rows and time in a block
# across all the queries within the block

import apsw.ext

# Use this to make many (virtual) rows
apsw.ext.make_virtual_module(
    connection, "generate_series", apsw.ext.generate_series
)

rows = []

with apsw.ext.query_limit(connection, row_limit=20):
    # 11 rows will come from this
    for (number,) in connection.execute(
        "select * from generate_series(0, 10)"
    ):
        rows.append(number)
    # next query would be 1,000 but we will hit
    # the limit
    for (number,) in connection.execute(
        "select * from generate_series(0, 999)"
    ):
        rows.append(number)

# lets see what we got
print(f"{len(rows)=}")

# We can also time limit
start = time.monotonic()
with apsw.ext.query_limit(connection, timeout=0.2):
    for (number,) in connection.execute(
        "select * from generate_series(0, 1000000000)"
    ):
        pass

print(f"After {time.monotonic() - start:.3f} seconds, we hit {number=}")

# We used the default "no exception" exception.  Lets have an explicit exception.
# with both row and time limits ...
try:
    with apsw.ext.query_limit(
        connection,
        row_limit=1000,
        timeout=1000,
        row_exception=IndexError,
        timeout_exception=TimeoutError,
    ):
        for (number,) in connection.execute(
            "select * from generate_series(0, 1000000000)"
        ):
            pass
except Exception as exc:
    print(f"{exc=}")


### query_details: Query details
# :meth:`apsw.ext.query_info` can provide a lot of information about a
# query (without running it)

import apsw.ext

# test tables
connection.execute(
    """
    create table customers(
        id INTEGER PRIMARY KEY,
        name CHAR,
        address CHAR);
    create table orders(
        id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        item MY_OWN_TYPE);
    create index cust_addr on customers(address);
"""
)

query = """
    SELECT * FROM orders
    JOIN customers ON orders.customer_id=customers.id
    WHERE address = ?;
    SELECT 7;"""

# ask for all information available
qd = apsw.ext.query_info(
    connection,
    query,
    actions=True,  # which tables/views etc and how they are accessed
    explain=True,  # shows low level VDBE
    explain_query_plan=True,  # how SQLite solves the query
)

print("query", qd.query)
print("\nbindings_count", qd.bindings_count)
print("\nbindings_names", qd.bindings_names)
print("\nexpanded_sql", qd.expanded_sql)
print("\nfirst_query", qd.first_query)
print("\nquery_remaining", qd.query_remaining)
print("\nis_explain", qd.is_explain)
print("\nis_readonly", qd.is_readonly)
print("\ndescription")
pprint(qd.description)
if hasattr(qd, "description_full"):
    print("\ndescription_full")
    pprint(qd.description_full)

print("\nquery_plan")
pprint(qd.query_plan)
print("\nFirst 5 actions")
pprint(qd.actions[:5])
print("\nFirst 5 explain")
pprint(qd.explain[:5])

### blob_io: Blob I/O
# BLOBS (binary large objects) are supported by SQLite.  Note that you
# cannot change the size of one, but you can allocate one filled with
# zeroes, and then later open it and read / write the contents similar
# to a file, without having the entire blob in memory.  Use
# :meth:`Connection.blob_open` to open a blob.

connection.execute("create table blobby(x,y)")
# Add a blob we will fill in later
connection.execute("insert into blobby values(1, zeroblob(10000))")
# Or as a binding
connection.execute(
    "insert into blobby values(2, ?)", (apsw.zeroblob(20000),)
)
# Open a blob for writing.  We need to know the rowid
rowid = connection.execute("select ROWID from blobby where x=1").get
blob = connection.blob_open("main", "blobby", "y", rowid, True)
blob.write(b"hello world")
blob.seek(2000)
blob.read(24)
# seek relative to the end
blob.seek(-32, 2)
blob.write(b"hello world, again")
blob.close()

### backup: Backup an open database
# You can :ref:`backup <backup>` a database that is open.  The pages are copied in
# batches of your choosing and allow continued use of the source
# database.

# We will copy a disk database into this memory database
destination = apsw.Connection(":memory:")

# Copy into destination
with destination.backup("main", connection, "main") as backup:
    # The source database can change while doing the backup
    # and the backup will still pick up those changes
    while not backup.done:
        backup.step(7)  # copy up to 7 pages each time
        # monitor progress
        print(backup.remaining, backup.page_count)


### authorizer: Authorizer (control what SQL can do)
# You can allow, deny, or ignore what SQL does.  Use
# :attr:`Connection.authorizer` to set an authorizer.


def auth(
    operation: int,
    p1: Optional[str],
    p2: Optional[str],
    db_name: Optional[str],
    trigger_or_view: Optional[str],
) -> int:
    """Called when each operation is prepared.  We can return SQLITE_OK, SQLITE_DENY or
    SQLITE_IGNORE"""
    # find the operation name
    print(
        apsw.mapping_authorizer_function[operation],
        p1,
        p2,
        db_name,
        trigger_or_view,
    )
    if (
        operation == apsw.SQLITE_CREATE_TABLE
        and p1
        and p1.startswith("private")
    ):
        return apsw.SQLITE_DENY  # not allowed to create tables whose names start with private

    return apsw.SQLITE_OK  # always allow


connection.authorizer = auth
connection.execute("insert into names values('foo')")
connection.execute("select name from names limit 1")
try:
    connection.execute("create table private_stuff(secret)")
    print("Created secret table!")
except Exception as e:
    print(e)

# Clear authorizer
connection.authorizer = None

### progress_handler: Progress handler
# Some operations (eg joins, sorting) can take many operations to
# complete.  Register a progress handler callback with
# :meth:`Connection.set_progress_handler` which lets you provide
# feedback and allows cancelling.

# create a table with random numbers
with connection:
    connection.execute("create table numbers(x)")
    connection.executemany(
        "insert into numbers values(?)",
        ((random.randint(0, 9999999999),) for _ in range(100)),
    )


def progress_handler() -> bool:
    print("progress handler called")
    return False  # returning True aborts


# register handler every 50 vdbe instructions
connection.set_progress_handler(progress_handler, 50)

# Sorting the numbers to find the biggest
for max_num in connection.execute("select max(x) from numbers"):
    print(max_num)

# Clear handler
connection.set_progress_handler(None)

### filecontrol: File Control
# We can get/set low level information using the
# :meth:`Connection.file_control` interface.  In this example we get
# the `data version
# <https://sqlite.org/c3ref/c_fcntl_begin_atomic_write.html#sqlitefcntldataversion>`__.
# There is a `pragma
# <https://sqlite.org/pragma.html#pragma_data_version>`__ but it
# doesn't change for commits on the same connection.

# We use ctypes to provide the correct C level data types and pointers
import ctypes


def get_data_version(db):
    # unsigned 32 bit integer
    data_version = ctypes.c_uint32(0)
    ok = db.file_control(
        "main",  # or an attached database name
        apsw.SQLITE_FCNTL_DATA_VERSION,  # code
        ctypes.addressof(data_version),
    )  # pass C level pointer
    assert ok, "SQLITE_FCNTL_DATA_VERSION was not understood!"
    return data_version.value


# Show starting values
print(
    "fcntl",
    get_data_version(connection),
    "pragma",
    connection.pragma("data_version"),
)

# See the fcntl value versus pragma value
for sql in (
    "create table fcntl_example(x)",
    "begin ; insert into fcntl_example values(3)",
    # we can see the version doesn't change inside a transaction
    "insert into fcntl_example values(4)",
    "commit",
    "pragma user_version=1234",
):
    print(sql)
    connection.execute(sql)
    print(
        "fcntl",
        get_data_version(connection),
        "pragma",
        connection.pragma("data_version"),
    )

### commit_hook: Commit hook
# A commit hook can allow or veto commits.  Register a commit hook
# with  :meth:`Connection.set_commit_hook`.


def my_commit_hook() -> bool:
    print("in commit hook")
    hour = time.localtime()[3]
    if hour >= 8 and hour < 18:
        print("commits okay at this time")
        return False  # let commit go ahead
    print("no commits out of hours")
    return True  # abort commits outside of 8am through 6pm


connection.set_commit_hook(my_commit_hook)
try:
    with connection:
        connection.execute(
            """create table example(x,y,z);
                           insert into example values (3,4,5)"""
        )
except apsw.ConstraintError:
    print("commit was not allowed")

connection.set_commit_hook(None)

### update_hook: Update hook
# Update hooks let you know that data has been added, changed, or
# removed.  For example you could use this to discard cached
# information.  Register a hook using
# :meth:`Connection.set_update_hook`.


def my_update_hook(
    type: int, db_name: str, table_name: str, rowid: int
) -> None:
    op: str = apsw.mapping_authorizer_function[type]
    print(
        f"Updated: { op } db { db_name }, table { table_name }, rowid { rowid }"
    )


connection.set_update_hook(my_update_hook)
connection.execute("insert into names values(?)", ("file93",))
connection.execute(
    "update names set name=? where name=?", ("file94", "file93")
)
connection.execute("delete from names where name=?", ("file94",))

# Clear the hook
connection.set_update_hook(None)

### virtual_tables: Virtual tables
# :ref:`Virtual tables <virtualtables>` let you provide data on demand
# as a SQLite table so you can use SQL queries against that data.
# Writing your own virtual table requires understanding how to return
# less than all the data via the `BestIndex
# <https://www.sqlite.org/vtab.html#the_xbestindex_method>`__ method.
#
# You can export a Python function as a virtual table in 3 lines of
# code using :func:`apsw.ext.make_virtual_module`, being able to
# provide both positional and keyword arguments.
#
# For the first example you'll find :meth:`apsw.ext.generate_series`
# useful instead.


# Yield a row at a time
def table_range(start=1, stop=100, step=1):
    for i in range(start, stop + 1, step):
        yield (i,)


# set column names
table_range.columns = ("value",)
# set how to access what table_range returns
table_range.column_access = apsw.ext.VTColumnAccess.By_Index

# register it
apsw.ext.make_virtual_module(connection, "range", table_range)

# see it work.  we can provide both positional and keyword
# arguments
query = "SELECT * FROM range(90) WHERE step=2"
print(apsw.ext.format_query_table(connection, query))

# the parameters are hidden columns so '*' doesn't select them
# but you can ask
query = "SELECT *, start, stop, step FROM range(89) WHERE step=3"
print(apsw.ext.format_query_table(connection, query))

# Expose the unicode database.
import unicodedata

# A more complex example exporting unicodedata module

# The methods we will call on each codepoint
unicode_methods = (
    "name",
    "decimal",
    "digit",
    "numeric",
    "category",
    "combining",
    "bidirectional",
    "east_asian_width",
    "mirrored",
    "decomposition",
)


# the function we will turn into a virtual table returning
# each row as a dict
def unicode_data(start=0, stop=sys.maxunicode):
    # some methods raise ValueError on some codepoints
    def call(meth: str, c: str):
        try:
            return getattr(unicodedata, meth)(c)
        except ValueError:
            return None

    for c in range(start, stop + 1):
        yield {k: call(k, chr(c)) for k in unicode_methods}


# setup column names and access
unicode_data.columns = unicode_methods
unicode_data.column_access = apsw.ext.VTColumnAccess.By_Name

# register
apsw.ext.make_virtual_module(connection, "unicode_data", unicode_data)

# how many codepoints are in each category?
query = """
    SELECT count(*), category FROM unicode_data
       WHERE stop = 0xffff  -- BMP only
       GROUP BY category
       ORDER BY category
       LIMIT 10"""
print(apsw.ext.format_query_table(connection, query))


# A more complex example - given a list of directories return information
# about the files within them recursively
def get_files_info(
    directories: str,
    sep: str = os.pathsep,
    *,
    ignore_symlinks: bool = True,
) -> Iterator[dict[str, Any]]:
    for root in directories.split(sep):
        with os.scandir(root) as sd:
            for entry in sd:
                if entry.is_symlink() and ignore_symlinks:
                    continue
                if entry.is_dir():
                    yield from get_files_info(
                        os.path.join(root, entry.name),
                        ignore_symlinks=ignore_symlinks,
                    )
                elif entry.is_file():
                    s = entry.stat()
                    yield {
                        "directory": root,
                        "name": entry.name,
                        "extension": os.path.splitext(entry.name)[1],
                        **{
                            k: getattr(s, k)
                            for k in get_files_info.stat_columns
                        },
                    }


# which stat columns do we want?
get_files_info.stat_columns = tuple(
    n for n in dir(os.stat(".")) if n.startswith("st_")
)
# setup columns and access by providing an example of the first entry returned
(
    get_files_info.columns,
    get_files_info.column_access,
) = apsw.ext.get_column_names(next(get_files_info(".")))

apsw.ext.make_virtual_module(connection, "files_info", get_files_info)

# all the sys.path directories
bindings = (
    os.pathsep.join(
        p
        for p in sys.path
        if os.path.isdir(p)
        #  except our current one
        and not os.path.samefile(p, ".")
    ),
)

# Find the 3 biggest files that aren't libraries
query = """SELECT st_size, directory, name
            FROM files_info(?)
            WHERE extension NOT IN ('.a', '.so')
            ORDER BY st_size DESC
            LIMIT 3"""
print(apsw.ext.format_query_table(connection, query, bindings))

# Find the 3 oldest Python files
query = """SELECT DATE(st_ctime, 'auto') AS date, directory, name
            FROM files_info(?)
            WHERE extension='.py'
            ORDER BY st_size DESC
            LIMIT 3"""
print(apsw.ext.format_query_table(connection, query, bindings))

# find space used by filename extension
query = """SELECT extension, SUM(st_size) as total_size
            FROM files_info(?)
            GROUP BY extension
            ORDER BY extension"""
print(apsw.ext.format_query_table(connection, query, bindings))

# unregister a virtual table by passing None
connection.create_module("files_info", None)

### vfs: VFS - Virtual File System
# :ref:`VFS <vfs>` lets you control how SQLite accesses storage.  APSW
# makes it easy to "inherit" from an existing VFS and monitor or alter
# data as it flows through.
#
# :class:`URI <URIFilename>` are shown as a way to receive parameters
# when opening/creating a database file, and :class:`pragmas <VFSFcntlPragma>`
# for receiving parameters once a database is open.

# This example VFS obfuscates the database file contents by xor all
# bytes with 0xa5.


def obfuscate(data: bytes):
    return bytes([x ^ 0xA5 for x in data])


# Inheriting from a base of "" means the default vfs
class ObfuscatedVFS(apsw.VFS):
    def __init__(self, vfsname="obfuscated", basevfs=""):
        self.vfs_name = vfsname
        self.base_vfs = basevfs
        super().__init__(self.vfs_name, self.base_vfs)

    # We want to return our own file implementation, but also
    # want it to inherit
    def xOpen(self, name, flags):
        in_flags = []
        for k, v in apsw.mapping_open_flags.items():
            if isinstance(k, int) and flags[0] & k:
                in_flags.append(v)
        print("xOpen flags", " | ".join(in_flags))

        if isinstance(name, apsw.URIFilename):
            print("   uri filename", name.filename())
            # We can look at uri parameters
            print("   fast is", name.uri_parameter("fast"))
            print("   level is", name.uri_int("level", 3))
            print("   warp is", name.uri_boolean("warp", False))
            print(
                "   notpresent is", name.uri_parameter("notpresent")
            )
            # all of them
            print("   all uris", name.parameters)
        else:
            print("   filename", name)
        return ObfuscatedVFSFile(self.base_vfs, name, flags)


# The file implementation where we override xRead and xWrite to call our
# encryption routine
class ObfuscatedVFSFile(apsw.VFSFile):
    def __init__(self, inheritfromvfsname, filename, flags):
        super().__init__(inheritfromvfsname, filename, flags)

    def xRead(self, amount, offset):
        return obfuscate(super().xRead(amount, offset))

    def xWrite(self, data, offset):
        super().xWrite(obfuscate(data), offset)

    def xFileControl(self, op: int, ptr: int) -> bool:
        if op != apsw.SQLITE_FCNTL_PRAGMA:
            return super().xFileControl(op, ptr)
        # implement our own pragma
        p = apsw.VFSFcntlPragma(ptr)
        print(f"pragma received { p.name } = { p.value }")
        # what do we understand?
        if p.name == "my_custom_pragma":
            p.result = "orange"
            return True
        # We did not understand
        return False


# To register the VFS we just instantiate it
obfuvfs = ObfuscatedVFS()

# Lets see what vfs are now available?
print("VFS available", apsw.vfs_names())

# Make an obfuscated db, passing in some URI parameters
# default open flags
open_flags = apsw.SQLITE_OPEN_READWRITE | apsw.SQLITE_OPEN_CREATE
# add in using URI parameters
open_flags |= apsw.SQLITE_OPEN_URI

# uri parameters are after the ? separated by &
obfudb = apsw.Connection(
    "file:myobfudb?fast=speed&level=7&warp=on&another=true",
    flags=open_flags,
    vfs=obfuvfs.vfs_name,
)

# Check it works
obfudb.execute("create table foo(x,y); insert into foo values(1,2)")

# Check it really is obfuscated on disk
print("What is on disk", repr(Path("myobfudb").read_bytes()[:20]))

# And unobfuscating it
print(
    "Unobfuscated disk",
    repr(obfuscate(Path("myobfudb").read_bytes()[:20])),
)

# Custom pragma
print(
    "pragma returned", obfudb.pragma("my_custom_pragma", "my value")
)

# Tidy up
obfudb.close()
os.remove("myobfudb")

### limits: Limits
# SQLite lets you see and update various limits via
# :meth:`Connection.limit`

# Print some limits
for limit in ("LENGTH", "COLUMN", "ATTACHED"):
    name = "SQLITE_LIMIT_" + limit
    max_name = "SQLITE_MAX_" + limit  # compile time limit
    orig = connection.limit(getattr(apsw, name))
    print(name, orig)
    # To get the maximum, set to 0x7fffffff and then read value back
    connection.limit(getattr(apsw, name), 0x7FFFFFFF)
    max = connection.limit(getattr(apsw, name))
    print(max_name, " ", max)

# Set limit for size of a string
connection.execute("create table testlimit(s)")
connection.execute(
    "insert into testlimit values(?)", ("x" * 1024,)
)  # 1024 char string
connection.limit(apsw.SQLITE_LIMIT_LENGTH, 1023)  # limit is now 1023
try:
    connection.execute(
        "insert into testlimit values(?)", ("y" * 1024,)
    )
    print("string exceeding limit was inserted")
except apsw.TooBigError:
    print("Caught toobig exception")

# reset back to largest value
connection.limit(apsw.SQLITE_LIMIT_LENGTH, 0x7FFFFFFF)

### shell: Shell
# APSW includes a :ref:`shell <shell>`  like the one in `SQLite
# <https://sqlite.org/cli.html>`__, and is also extensible from
# Python.

import apsw.shell

# Here we use the shell to do a csv export and then dump part of the
# database

# Export to a StringIO
import io

output = io.StringIO()
shell = apsw.shell.Shell(stdout=output, db=connection)

# How to execute a dot command
shell.process_command(".mode csv")
shell.process_command(".headers on")

# How to execute SQL
shell.process_sql(
    """
    create table csvtest(column1, column2 INTEGER);
    create index faster on csvtest(column1);
    insert into csvtest values(3, 4);
    insert into csvtest values('a b', NULL);
"""
)

# Or let the shell figure out SQL vs dot command
shell.process_complete_line("select * from csvtest")

# see the result
print(output.getvalue())

# reset output
output.seek(0)

# make a dump of the same table
shell.process_command(".dump csvtest%")

# see the result
print("\nDump output\n")
print(output.getvalue())

### status: Statistics
# SQLite provides statistics by :meth:`status`.  Use :meth:`Connection.status`
# for per connection statistics.

current_usage, max_usage = apsw.status(apsw.SQLITE_STATUS_MEMORY_USED)
print(f"SQLite memory usage { current_usage } max { max_usage }")
schema_used, _ = connection.status(apsw.SQLITE_DBSTATUS_SCHEMA_USED)
print(
    f"{ schema_used } bytes used to store schema for this connection"
)

### trace_v2: Tracing
# This shows using :meth:`Connection.trace_v2`

# From https://www.sqlite.org/lang_with.html
# Outlandish Recursive Query Examples

query = """WITH RECURSIVE
            xaxis(x) AS (VALUES(-2.0) UNION ALL SELECT x+0.05 FROM xaxis WHERE x<1.2),
            yaxis(y) AS (VALUES(-1.0) UNION ALL SELECT y+0.1 FROM yaxis WHERE y<1.0),
            m(iter, cx, cy, x, y) AS (
                SELECT 0, x, y, 0.0, 0.0 FROM xaxis, yaxis
                UNION ALL
                SELECT iter+1, cx, cy, x*x-y*y + cx, 2.0*x*y + cy FROM m
                WHERE (x*x + y*y) < 4.0 AND iter<28
            ),
            m2(iter, cx, cy) AS (
                SELECT max(iter), cx, cy FROM m GROUP BY cx, cy
            ),
            a(t) AS (
                SELECT group_concat( substr(' .+*#', 1+min(iter/7,4), 1), '')
                FROM m2 GROUP BY cy
            )
            SELECT group_concat(rtrim(t),x'0a') FROM a;"""


def trace_hook(trace: dict) -> None:
    # check the sql and connection are as expected and remove from trace
    # so we don't print them
    assert (
        trace.pop("sql", query) == query
        and trace.pop("connection") is connection
    )
    print("code is ", apsw.mapping_trace_codes[trace["code"]])
    pprint(trace)


connection.trace_v2(
    apsw.SQLITE_TRACE_STMT
    | apsw.SQLITE_TRACE_PROFILE
    | apsw.SQLITE_TRACE_ROW,
    trace_hook,
)

# We will get one each of the trace events
for _ in connection.execute(query):
    pass

# Turn off tracing
connection.trace_v2(0, None)

### ShowResourceUsage: System and SQLite resource usage in a block
# Use :meth:`apsw.ext.ShowResourceUsage` to see what resources a
# block of code does.  We use the same query from above.
#
# Only statistics that have changed are shown in the summary. There are
# 20 SQLite values tracked including caching, and 20 system values.

with apsw.ext.ShowResourceUsage(
    sys.stdout, db=connection, scope="thread"
):
    # some SQLite work
    rows = connection.execute(query).get
    # and some non-SQLite work - the imports cause filesystem access
    import statistics, tokenize, uuid, fractions, pydoc, decimal

    # and take some wall clock time
    time.sleep(1.3)

### Trace: SQL statement tracing in a block
# Use :meth:`apsw.ext.Trace` to see SQL statements inside a block of
# code.  This also shows behind the scenes SQL.

# Use None instead of stdout and no information is printed or gathered
with apsw.ext.Trace(
    sys.stdout,
    db=connection,
    vtable=True,
    updates=True,
    transaction=True,
):
    # APSW does a savepoint behind the scenes to wrap the block
    with connection:
        # Some regular SQL
        connection.execute("create table multi(x)")
        # executemany runs the same statement repeatedly
        connection.executemany(
            "insert into multi values(?)", ((x,) for x in range(5))
        )
        # See how many rows were processed
        connection.execute("select * from multi limit 2").fetchall()
        # You can also see how many rows were changed
        connection.execute("delete from multi where x < 4")

    # pragma functions are virtual tables - see how many rows this processes even
    # though only one has 'pow'
    connection.execute("SELECT narg FROM pragma_function_list WHERE name='pow'").get

    # trigger that causes rollback
    connection.execute("""
        create trigger error after insert on multi
        begin
           update multi set rowid=100+new.rowid where rowid=new.rowid;
           select raise(rollback, 'nope');
       end;
    """)

    with contextlib.suppress(apsw.ConstraintError):
        connection.execute("insert into multi values(54)")

### format_query: Formatting query results table
# :meth:`apsw.ext.format_query_table` makes it easy
# to format the results of a query in an automatic
# adjusting table, colour, sanitizing strings,
# truncation etc.

# Create a table with some dummy data
connection.execute(
    """CREATE TABLE dummy(quantity, [spaces in name], last);
    INSERT INTO dummy VALUES(3, 'some regular text to make this row interesting', x'030709');
    INSERT INTO dummy VALUES(3.14, 'Tiếng Việt', null);
    INSERT INTO dummy VALUES('', ?, ' ');
""",
    ("special \t\n\f\0 cha\\rs",),
)

query = "SELECT * FROM dummy"
# default
print(apsw.ext.format_query_table(connection, query))

# no unicode boxes and maximum sanitize the text
kwargs = {"use_unicode": False, "string_sanitize": 2}
print(apsw.ext.format_query_table(connection, query, **kwargs))

# lets have unicode boxes and make things narrow
kwargs = {
    "use_unicode": True,
    "string_sanitize": 0,
    "text_width": 30,
}
print(apsw.ext.format_query_table(connection, query, **kwargs))

# have the values in SQL syntax
kwargs = {"quote": True}
print(apsw.ext.format_query_table(connection, query, **kwargs))

### caching: Caching
# SQLite has a `builtin cache
# <https://www.sqlite.org/pragma.html#pragma_cache_size>`__.  If you
# do your own caching then you can find out if it is invalid via
# `pragma
# <https://www.sqlite.org/pragma.html#pragma_schema_version>`__ for
# schema changes and :meth:`Connection.data_version` for table content
# changes.  Any cache is invalid if the values are different - there
# is no guarantee if they will go up or down.

print(
    "SQLite cache =",
    connection.pragma("cache_size"),
    " page_size = ",
    connection.pragma("page_size"),
)

# Make a second connection to change the same database main
# connection.  These also work if the changes were done in a different
# process.
con2 = apsw.Connection(connection.filename)

# See values before change
print("Before values")
print(f'{connection.pragma("schema_version")=}')
print(f'{connection.data_version()=}')

print("\nAfter values")
# add to table from previous section
con2.execute("insert into dummy values(1, 2, 3)")
print(f'{connection.data_version()=}')

# and add a table.  changing an existing table definition etc also
# bump the schema version
con2.execute("create table more(x,y,z)")
print(f'{connection.pragma("schema_version")=}')

### cleanup:  Cleanup
# As a general rule you do not need to do any cleanup.  Standard
# Python garbage collection will take of everything.  Even if the
# process crashes with a connection in the middle of a transaction,
# the next time SQLite opens that database it will automatically
# rollback the incomplete transaction.

# You can close connections manually
connection.close()
