#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import time
import apsw
import apsw.ext
import random
import ast

# Note: this code uses Python's optional typing annotations.  You can
# ignore them and do not need to use them
from typing import Optional, Iterator, Tuple

### version_check: Checking APSW and SQLite versions

# Where the extension module is on the filesystem
print("      Using APSW file", apsw.__file__)

# From the extension
print("         APSW version", apsw.apswversion())

# From the sqlite header file at APSW compile time
print("SQLite header version", apsw.SQLITE_VERSION_NUMBER)

# The SQLite code running
print("   SQLite lib version", apsw.sqlitelibversion())

# If True then SQLite is incorporated into the extension.
# If False then a shared library is being used, or static linking
print("   Using amalgamation", apsw.using_amalgamation)

### logging: Logging
# It is a good idea to get SQLite's logs as you will get more
# information about errors.  This has to be done first (a SQLite
# limitation, not APSW). :meth:`apsw.ext.log_sqlite` forwards
# SQLite's log messages to the :mod:`logging` module.

apsw.ext.log_sqlite()

# You can also write to SQLite's log
apsw.log(apsw.SQLITE_ERROR, "A message from Python")

### open_db:  Opening the database
# You open the database by using :class:`Connection`

# Default will create the database if it doesn't exist
connection = apsw.Connection("dbfile")

# Open existing read-only
connection = apsw.Connection("dbfile", flags=apsw.SQLITE_OPEN_READONLY)

# Open existing read-write (exception if it doesn't exist)
connection = apsw.Connection("dbfile", flags=apsw.SQLITE_OPEN_READWRITE)

### executing_sql: Executing SQL
# Use :meth:`Connection.execute` to execute SQL

connection.execute("create table point(x,y,z)")
connection.execute("insert into point values(1, 2, 3)")
# You can use multiple ; separated statements
connection.execute("""
    insert into point values(4, 5, 6);
    create table log(timestamp, event);
    create table foo(a, b, c);
    create table important(secret, data);
""")

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
query = "insert into log values(0, '" + event + "')"
print("query:", query)

# BECAUSE ... a bad guy could provide a value like this
event = "bad guy here') ; drop table important; -- "
# which has effects like this
query = "insert into log values(0, '" + event + "')"
print("bad guy:", query)

### bindings_sequence: Bindings (sequence)
# Bindings can be provided as a sequence such as with
# a tuple or list.  Use **?** to show where the values go.

query = "insert into log values(?, ?)"
data = (7, "restart")
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

### types: Using different types
# SQLite supports None, int, float, str, bytes (binary data). If a
# table declaration gives a type then SQLite attempts conversion.
# `Read more <https://www.sqlite.org/flextypegood.html>`__.

connection.execute("""
    create table types1(a, b, c, d, e);
    create table types2(a INTEGER, b REAL, c TEXT, d, e BLOB);
    """)

data = ("12", 3, 4, 5.5, b"\x03\x72\xf4\x00\x9e")
connection.execute("insert into types1 values(?,?,?,?,?)", data)
connection.execute("insert into types2 values(?,?,?,?,?)", data)

for row in connection.execute("select * from types1"):
    print("types1", repr(row))

for row in connection.execute("select * from types2"):
    print("types2", repr(row))

### transaction: Transactions
# By default each statement is its own transaction (3 in the
# example below).  A transaction finishes by flushing data to
# storage and waiting for the operating system to confirm it is
# permanently there (ie will survive a power failure) which takes
# a while.

connection.execute("insert into point values(2, 2, 2)")
connection.execute("insert into point values(3, 3, 3)")
connection.execute("insert into point values(4, 4, 4)")

# You can use BEGIN / END to manually make a transaction
connection.execute("BEGIN")
connection.execute("insert into point values(2, 2, 2)")
connection.execute("insert into point values(3, 3, 3)")
connection.execute("insert into point values(4, 4, 4)")
connection.execute("END")

# Or use `with`` that does it automatically
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

### exectrace: Tracing execution
# You can trace execution of SQL statements.  See :ref:`more about
# tracing <tracing>`.


def my_tracer(cursor: apsw.Cursor, statement: str, bindings: Optional[apsw.Bindings]) -> bool:
    "Called just before executing each statement"
    print("SQL:", statement.strip())
    print("Bindings:", bindings)
    return True  # if you return False then execution is aborted


# you can trace a single cursor
cursor = connection.cursor()
cursor.exectrace = my_tracer
cursor.execute(
    """
        drop table if exists bar;
        create table bar(x,y,z);
        select * from point where x=?;
        """, (3, ))

# if set on a connection then all cursors are traced
connection.exectrace = my_tracer
# and clearing it
connection.exectrace = None

### rowtrace: Tracing returned rows
# You can trace returned rows, including modifying what is returned or
# skipping it completely.  See :ref:`more about tracing <tracing>`.


def row_tracer(cursor: apsw.Cursor, row: apsw.SQLiteValues) -> apsw.SQLiteValues:
    """Called with each row of results before they are handed off.  You can return None to
    cause the row to be skipped or a different set of values to return"""
    print("Row:", row)
    return row


# you can trace a single cursor
cursor = connection.cursor()
cursor.rowtrace = row_tracer
for row in cursor.execute("select x,y from point where x>4"):
    pass

# if set on a connection then all cursors are traced
connection.rowtrace = row_tracer
# and clearing it
connection.rowtrace = None

### scalar: Defining your own functions
# Scalar functions take one or more values and return one value.  They
# are registered by calling :meth:`Connection.createscalarfunction`.


def ilove7(*args: apsw.SQLiteValue) -> int:
    "A scalar function"
    print(f"ilove7 got { args } but I love 7")
    return 7


connection.createscalarfunction("seven", ilove7)

for row in connection.execute("select seven(x,y) from point where x>4"):
    print("row", row)

### aggregate: Defining aggregate functions
# Aggregate functions are called multiple times with matching rows,
# and then provide a final value.  An example is calculating an
# average.  They are registered by calling
# :meth:`Connection.createaggregatefunction`.


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

    @classmethod
    def factory(cls) -> apsw.AggregateCallbacks:
        return cls(), cls.step, cls.final


connection.createaggregatefunction("longest", longest.factory)
for row in connection.execute("select longest(event) from log"):
    print(row)

### window: Defining window functions
# Window functions input values come from a "window" around a row of
# interest.  Four methods are called to add, remove, get the current
# value, and finalize as the window moves.
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
        print("final")
        return self.v

    def value(self):
        print("value", self.v)
        return self.v


connection.create_window_function("sumint", SumInt)

for row in connection.execute("""
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
    """):
        print("ROW", row)

### collation: Defining collations (sorting)
# How you sort can depend on the languages or values involved.  You
# register a collation by calling :meth:`Connection.createcollation`.

# This example sorting mechanisms understands some text followed by a
# number and ensures the number portion gets sorted correctly

connection.execute("create table names(name)")
connection.executemany("insert into names values(?)", (
    ("file1", ),
    ("file7", ),
    ("file17", ),
    ("file20", ),
    ("file3", ),
))

print("Standard sorting")
for row in connection.execute("select * from names order by name"):
    print(row)


def str_num_collate(s1: apsw.SQLiteValue, s2: apsw.SQLiteValue) -> int:
    # return -1 if s1<s2, +1 if s1>s2 else 0

    def parts(v: str) -> tuple[str, int]:
        num = ""
        while v and v[-1].isdigit():
            num = v[-1] + num
            v = v[:-1]
        return v, int(num) if num else 0

    ps1 = parts(str(s1))
    ps2 = parts(str(s2))

    # compare
    if ps1 < ps2:
        return -1
    if ps1 > ps2:
        return 1
    return 0


connection.createcollation("strnum", str_num_collate)

print()
print("Using strnum")
for row in connection.execute("select * from names order by name collate strnum"):
    print(row)

### colnames: Accessing results by column name
# You can access results by column name using :mod:`dataclasses`.
# APSW provides :class:`apsw.ext.DataClassRowFactory` for names
# instead

import apsw.ext

connection.execute("""
    create table books(id, title, author, year);
    insert into books values(7, "Animal Farm", "George Orwell", 1945);
    insert into books values(37, "The Picture of Dorian Gray", "Oscar Wilde", 1890);
    """)

# Normally you use column numbers
for row in connection.execute("select title, id, year from books where author=?", ("Oscar Wilde", )):
    # this is very fragile
    print("title", row[0])
    print("id", row[1])
    print("year", row[2])

# Turn on dataclasses - frozen makes them read-only
connection.rowtrace = apsw.ext.DataClassRowFactory(dataclass_kwargs={"frozen": True})

print("\nNow with dataclasses\n")

# Same query - note using AS to set column name
for row in connection.execute(
        """SELECT title,
           id AS book_id,
           year AS book_year
           FROM books WHERE author = ?""", ("Oscar Wilde", )):
    print("title", row.title)
    print("id", row.book_id)
    print("year", row.book_year)

# clear
connection.rowtrace = None

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

    def __eq__(self, other: Point) -> bool:
        return isinstance(other, Point) and self.x == other.x and self.y == other.y

    def to_sqlite_value(self) -> str:
        # called to convert Point into something SQLite supports
        return f"{ self.x };{ self.y }"

    # This converter will be registered
    @staticmethod
    def convert_from_sqlite(value: str) -> Point:
        return Point(*(float(part) for part in value.split(";")))


# An existing type
def complex_to_sqlite_value(c: complex) -> str:
    return f"{ c.real }+{ c.imag }"


# ... requires manual registration
registrar.register_adapter(complex, complex_to_sqlite_value)

# conversion from a SQLite value requires registration
registrar.register_converter("POINT", Point.convert_from_sqlite)


# ... and for complex
def sqlite_to_complex(v: str) -> complex:
    return complex(*(float(part) for part in v.split("+")))


registrar.register_converter("COMPLEX", sqlite_to_complex)

# note that the type names are case sensitive and must match the
# registration
connection.execute("create table conversion(p POINT, c COMPLEX)")

# convert going into database
test_data = (Point(5.2, 7.6), 3 + 4j)
connection.execute("insert into conversion values(?, ?)", test_data)
print("inserted", test_data)

# and coming back out
for row in connection.execute("select * from conversion"):
    print("back out", row)
    print("equal", row == test_data)

# clear registrar
connection.cursor_factory = apsw.Cursor

### query_details: Query details
# :meth:`apsw.ext.query_info` can provide a lot of information about a
# query (without running it)

import apsw.ext

# test tables
connection.execute("""
    create table customers(
        id INTEGER PRIMARY KEY,
        name CHAR,
        address CHAR);
    create table orders(
        id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        item MY_OWN_TYPE);
    create index cust_addr on customers(address);
""")

query = """
    SELECT * FROM orders
    JOIN customers ON orders.customer_id=customers.id
    WHERE address = ?;
    SELECT 7;"""
bindings = ("123 Main Street", )

# ask for all information available
qd = apsw.ext.query_info(
    connection,
    query,
    bindings=bindings,
    actions=True,  # which tables/views etc and how they are accessed
    expanded_sql=True,  # expands bindings into query string
    explain=True,  # shows low level VDBE
    explain_query_plan=True,  # how SQLite solves the query
)

# help with formatting
import pprint

print("query", qd.query)
print("\nbindings", qd.bindings)
print("\nexpanded_sql", qd.expanded_sql)
print("\nfirst_query", qd.first_query)
print("\nquery_remaining", qd.query_remaining)
print("\nis_explain", qd.is_explain)
print("\nis_readonly", qd.is_readonly)
print("\ndescription\n", pprint.pformat(qd.description))
if hasattr(qd, "description_full"):
    print("\ndescription_full\n", pprint.pformat(qd.description_full))

print("\nquery_plan\n", pprint.pformat(qd.query_plan))
print("\nFirst 5 actions\n", pprint.pformat(qd.actions[:5]))
print("\nFirst 5 explain\n", pprint.pformat(qd.explain[:5]))

### blob_io: Blob I/O
# BLOBS (binary large objects) are supported by SQLite.  Note that you
# cannot change the size of one, but you can allocate one filled with
# zeroes, and then later open it and read / write the contents similar
# to a file, without having the entire blob in memory.  Use
# :meth:`Connection.blobopen` to open a blob.

connection.execute("create table blobby(x,y)")
# Add a blob we will fill in later
connection.execute("insert into blobby values(1, zeroblob(10000))")
# Or as a binding
connection.execute("insert into blobby values(2, ?)", (apsw.zeroblob(20000), ))
# Open a blob for writing.  We need to know the rowid
rowid = connection.execute("select ROWID from blobby where x=1").fetchall()[0][0]
blob = connection.blobopen("main", "blobby", "y", rowid, True)
blob.write(b"hello world")
blob.seek(2000)
blob.write(b"hello world, again")
blob.close()

### authorizer: Authorizer (control what SQL can do)
# You can allow, deny, or ignore what SQL does.  Use
# :attr:`Connection.authorizer` to set an authorizer.


def auth(operation: int, p1: Optional[str], p2: Optional[str], db_name: Optional[str],
         trigger_or_view: Optional[str]) -> int:
    """Called when each operation is prepared.  We can return SQLITE_OK, SQLITE_DENY or
    SQLITE_IGNORE"""
    # find the operation name
    print(apsw.mapping_authorizer_function[operation], p1, p2, db_name, trigger_or_view)
    if operation == apsw.SQLITE_CREATE_TABLE and p1 and p1.startswith("private"):
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
# :meth:`Connection.setprogresshandler` which lets you provide
# feedback and allows cancelling.


def some_numbers(how_many: int) -> Iterator[Tuple[int]]:
    for _ in range(how_many):
        yield (random.randint(0, 9999999999), )


# create a table with random numbers
with connection:
    connection.execute("create table numbers(x)")
    connection.executemany("insert into numbers values(?)", some_numbers(100))


def progress_handler() -> bool:
    print("progress handler called")
    return False  # returning True aborts


# register handler every 50 vdbe instructions
connection.setprogresshandler(progress_handler, 50)

# Sorting the numbers to find the biggest
for max_num in connection.execute("select max(x) from numbers"):
    print(max_num)

# Clear handler
connection.setprogresshandler(None)

### commit_hook: Commit hook
# A commit hook can allow or veto commits.  Register a commit hook
# with  :meth:`Connection.setcommithook`.


def my_commit_hook() -> bool:
    print("in commit hook")
    hour = time.localtime()[3]
    if hour < 8 or hour > 17:
        print("no commits out of hours")
        return True  # abort commits outside of 8am through 6pm
    print("commits okay at this time")
    return False  # let commit go ahead


connection.setcommithook(my_commit_hook)
try:
    with connection:
        connection.execute("create table example(x,y,z); insert into example values (3,4,5)")
except apsw.ConstraintError:
    print("commit was not allowed")

connection.setcommithook(None)

### update_hook: Update hook
# Update hooks let you know that data has been added, changed, or
# removed.  For example you could use this to discard cached
# information.  Register a hook using
# :meth:`Connection.setupdatehook`.


def my_update_hook(type: int, db_name: str, table_name: str, rowid: int) -> None:
    op: str = apsw.mapping_authorizer_function[type]
    print(f"Updated: { op } db { db_name }, table { table_name }, rowid { rowid }")


connection.setupdatehook(my_update_hook)
connection.execute("insert into names values(?)", ("file93", ))
connection.execute("update names set name=? where name=?", ("file94", "file93"))
connection.execute("delete from names where name=?", ("file94", ))

# Clear the hook
connection.setupdatehook(None)

### virtual_tables: Virtual tables
# Virtual tables let you provide data on demand as a SQLite table so
# you can use SQL queries against that data. :ref:`Read more about
# virtual tables <virtualtables>`.

# This example provides information about all the files in Python's
# path.  The minimum amount of code needed is shown, and lets SQLite
# do all the heavy lifting.  A more advanced table would use indices
# and filters to reduce the number of rows shown to SQLite.

# these first columns are used by our virtual table
vtcolumns = ["rowid", "name", "directory"]


def get_file_data(directories):
    "Returns a list of column names, and a list of all the files with their attributes"
    columns = None
    data = []
    counter = 1
    for directory in directories:
        for f in os.listdir(directory):
            if not os.path.isfile(os.path.join(directory, f)):
                continue
            counter += 1
            st = os.stat(os.path.join(directory, f))
            if columns is None:
                # we add on all the fields from os.stat
                columns = vtcolumns + [x for x in dir(st) if x.startswith("st_")]
            data.append([counter, f, directory] + [getattr(st, x) for x in columns[3:]])
    return columns, data


# This gets registered with the Connection
class Source:

    def Create(self, db, modulename, dbname, tablename, *args):
        # the eval strips off layer of quotes
        columns, data = get_file_data([ast.literal_eval(a.replace("\\", "\\\\")) for a in args])
        schema = "create table foo(" + ','.join(["'%s'" % (x, ) for x in columns[1:]]) + ")"
        return schema, Table(columns, data)

    Connect = Create


# Represents a table
class Table:

    def __init__(self, columns, data):
        self.columns = columns
        self.data = data

    def BestIndex(self, *args):
        return None

    def Open(self):
        return Cursor(self)

    def Disconnect(self):
        pass

    Destroy = Disconnect


# Represents a cursor used during SQL query processing
class Cursor:

    def __init__(self, table):
        self.table = table

    def Filter(self, *args):
        self.pos = 0

    def Eof(self):
        return self.pos >= len(self.table.data)

    def Rowid(self):
        return self.table.data[self.pos][0]

    def Column(self, col):
        return self.table.data[self.pos][1 + col]

    def Next(self):
        self.pos += 1

    def Close(self):
        pass


# Register the module as filesource
connection.createmodule("filesource", Source())

# Arguments to module - all directories in sys.path
sysdirs = ",".join(["'%s'" % (x, ) for x in sys.path[1:] if len(x) and os.path.isdir(x)])
connection.execute("create virtual table sysfiles using filesource(" + sysdirs + ")")

print("3 biggest files")
for size, directory, file in connection.execute(
        "select st_size,directory,name from sysfiles order by st_size desc limit 3"):
    print(size, file, directory)

print()
print("3 oldest files")
for ctime, directory, file in connection.execute(
        "select st_ctime,directory,name from sysfiles order by st_ctime limit 3"):
    print(ctime, file, directory)

### vfs: VFS - Virtual File System
# VFS lets you control access to the filesystem from SQLite.  APSW
# makes it easy to "inherit" from an existing VFS and monitor or alter
# data as it flows through.  Read more about :ref:`VFS <vfs>`.

# This example VFS "obfuscates" the database file contents by xor all
# bytes with 0xa5.  URI parameters are also shown as a way you can
# pass additional information for files.


def obfuscate(data):
    if not data: return data
    return bytes([x ^ 0xa5 for x in data])


# Inheriting from a base of "" means the default vfs
class ObfuscatedVFS(apsw.VFS):

    def __init__(self, vfsname="obfuscated", basevfs=""):
        self.vfs_name = vfsname
        self.base_vfs = basevfs
        apsw.VFS.__init__(self, self.vfs_name, self.base_vfs)

    # We want to return our own file implementation, but also
    # want it to inherit
    def xOpen(self, name, flags: int):
        if isinstance(name, apsw.URIFilename):
            print("xOpen of", name.filename())
            # We can look at uri parameters
            print("fast is", name.uri_parameter("fast"))
            print("level is", name.uri_int("level", 3))
            print("warp is", name.uri_boolean("warp", False))
            print("notpresent is", name.uri_parameter("notpresent"))
        else:
            print("xOpen of", name)
        return ObfuscatedVFSFile(self.base_vfs, name, flags)


# The file implementation where we override xRead and xWrite to call our
# encryption routine
class ObfuscatedVFSFile(apsw.VFSFile):

    def __init__(self, inheritfromvfsname, filename, flags):
        apsw.VFSFile.__init__(self, inheritfromvfsname, filename, flags)

    def xRead(self, amount, offset):
        return obfuscate(super().xRead(amount, offset))

    def xWrite(self, data, offset):
        super().xWrite(obfuscate(data), offset)


# To register the VFS we just instantiate it
obfuvfs = ObfuscatedVFS()

# Lets see what vfs are now available?
print("VFS available", apsw.vfsnames())

# Make an obfuscated db, passing in some URI parameters
# default open flags
open_flags = apsw.SQLITE_OPEN_READWRITE | apsw.SQLITE_OPEN_CREATE
# add in using URI parameters
open_flags |= apsw.SQLITE_OPEN_URI

obfudb = apsw.Connection("file:myobfudb?fast=speed&level=7&warp=on&another=true",
                         flags=open_flags,
                         vfs=obfuvfs.vfs_name)

# Check it works
obfudb.execute("create table foo(x,y); insert into foo values(1,2)")

# Check it really is obfuscated on disk
print("What is on disk", repr(open("myobfudb", "rb").read()[:20]))

# And unobfuscating it
print("Unobfuscated disk", repr(obfuscate(open("myobfudb", "rb").read()[:20])))

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
    connection.limit(getattr(apsw, name), 0x7fffffff)
    max = connection.limit(getattr(apsw, name))
    print(max_name, " ", max)

# Set limit for size of a string
connection.execute("create table testlimit(s)")
connection.execute("insert into testlimit values(?)", ("x" * 1024, ))  # 1024 char string
connection.limit(apsw.SQLITE_LIMIT_LENGTH, 1023)  # limit is now 1023
try:
    connection.execute("insert into testlimit values(?)", ("y" * 1024, ))
    print("string exceeding limit was inserted")
except apsw.TooBigError:
    print("Caught toobig exception")

# reset back to largest value
connection.limit(apsw.SQLITE_LIMIT_LENGTH, 0x7fffffff)

### backup: Backup an open database
# You can backup a database that is open.  The pages are copied in
# batches of your choosing and allow continued use of the database.
# :ref:`Read more <backup>`.

# We will copy a disk database into a memory database
memcon = apsw.Connection(":memory:")

# Copy into memory
with memcon.backup("main", connection, "main") as backup:
    backup.step(10)  # copy 10 pages in each batch

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
shell.process_sql("""
    create table csvtest(column1, column2 INTEGER);
    create index faster on csvtest(column1);
    insert into csvtest values(3, 4);
    insert into csvtest values('a b', NULL);
""")

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
# SQLite provides statistics by :meth:`status`

current_usage, max_usage = apsw.status(apsw.SQLITE_STATUS_MEMORY_USED)
print(f"SQLite memory usage { current_usage } max { max_usage }")

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


def trace_hook(trace: Dict) -> None:
    # check the sql and connection are as expected and remove from trace
    # so we don't print them
    assert trace.pop("sql") == query
    print("code is ", apsw.mapping_trace_codes[trace["code"]])
    print(pprint.pformat(trace), "\n")


connection.trace_v2(apsw.SQLITE_TRACE_STMT | apsw.SQLITE_TRACE_PROFILE | apsw.SQLITE_TRACE_ROW, trace_hook)

# We will get one each of the trace events
for _ in connection.execute(query):
    pass

### cleanup:  Cleanup
# As a general rule you do not need to do any cleanup.  Standard
# Python garbage collection will take of everything.  Even if the
# process crashes with a connection in the middle of a transaction,
# the next time SQLite opens that database it will automatically
# rollback the partial data.

# You close connections manually (useful if you want to catch exceptions)
connection.close()
#  You can call close multiple times, and also indicate to ignore exceptions
connection.close(True)