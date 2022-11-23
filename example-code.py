#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import time
import apsw
import random

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

### collations: Defining collations (sorting)
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
        columns, data = get_file_data([eval(a.replace("\\", "\\\\")) for a in args])
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


# Represents a cursor
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
        self.vfsname = vfsname
        self.basevfs = basevfs
        apsw.VFS.__init__(self, self.vfsname, self.basevfs)

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
        return ObfuscatedVFSFile(self.basevfs, name, flags)


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

obfudb = apsw.Connection("file:myobfudb?fast=speed&level=7&warp=on",
                         flags=open_flags,
                         vfs=obfuvfs.vfsname)

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



# Print some limits
for limit in ("LENGTH", "COLUMN", "ATTACHED"):
    name = "SQLITE_LIMIT_" + limit
    maxname = "SQLITE_MAX_" + limit  # compile time
    orig = connection.limit(getattr(apsw, name))
    print(name, orig)
    # To get the maximum, set to 0x7fffffff and then read value back
    connection.limit(getattr(apsw, name), 0x7fffffff)
    max = connection.limit(getattr(apsw, name))
    print(maxname, max)

# Set limit for size of a string
connection.execute("create table testlimit(s)")
connection.execute("insert into testlimit values(?)", ("x" * 1024, ))  # 1024 char string
connection.limit(apsw.SQLITE_LIMIT_LENGTH, 1023)  # limit is now 1023
try:
    connection.execute("insert into testlimit values(?)", ("y" * 1024, ))
    print("string exceeding limit was inserted")
except apsw.TooBigError:
    print("Caught toobig exception")
connection.limit(apsw.SQLITE_LIMIT_LENGTH, 0x7fffffff)


### backup: Backup opened database

# We will copy the disk database into a memory database

memcon = apsw.Connection(":memory:")

# Copy into memory
with memcon.backup("main", connection, "main") as backup:
    backup.step()  # copy whole database in one go

# There will be no disk accesses for this query
for row in memcon.execute("select * from names"):
    pass

###
### Shell  @@ example-shell
###

import apsw.shell

# Here we use the shell to do a csv export providing the existing db
# connection

# Export to a StringIO
import io

output = io.StringIO()
shell = apsw.shell.Shell(stdout=output, db=connection)
# How to execute a dot command
shell.process_command(".mode csv")
shell.process_command(".headers on")
# How to execute SQL
shell.process_sql(
    "create table csvtest(col1,col2); insert into csvtest values(3,4); insert into csvtest values('a b', NULL)")
# Let the shell figure out SQL vs dot command
shell.process_complete_line("select * from csvtest")

# Verify output
#@@CAPTURE
print(output.getvalue())
#@@ENDCAPTURE

###
### Statistics @@example-status
###

#@@CAPTURE
print("SQLite memory usage current %d max %d" % apsw.status(apsw.SQLITE_STATUS_MEMORY_USED))
#@@ENDCAPTURE

###
### Cleanup
###

# We can close connections manually (useful if you want to catch exceptions)
# but you don't have to
connection.close(True)  # force it since we want to exit

# Delete database - we don't need it any more
os.remove("dbfile")
