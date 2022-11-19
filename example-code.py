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
# It is tempting to compose strings with the values in
# them, but it is easy to mangle the query especially
# if values.  It is known as `SQL injection <https://en.wikipedia.org/wiki/SQL_injection>`__
# Bindings are the correct way to supply values to queries.

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
# You can also supply bindings with a dictionary.  Use :NAME, @NAME,
# or $NAME, to provide the key name in the query.

query = "insert into point values(:x, @y, $z)"
data = {"x": 7, "y": 8, "z": 9}
connection.execute(query, data)

### types: Using different types
# SQLite supports None, int, float, str, bytes (binary data) If a
# table declaration gives a type then SQLite attempts conversion.
# `Read more <https://www.sqlite.org/flextypegood.html>`__.

connection.execute("""
    create table types1(a,b,c,d,e);
    create table types2(a INTEGER, b REAL, c TEXT, d, e BLOB);
    """)

data = ("12", 3, 4, 5.5, b"deadbeef")
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
# permanently there (will survive a power failure).

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


def mytrace(cursor: apsw.Cursor, statement: str, bindings: Optional[apsw.Bindings]) -> bool:
    "Called just before executing each statement"
    print("SQL:", statement.strip())
    print("Bindings:", bindings)
    return True  # if you return False then execution is aborted


# you can trace a single cursor
cursor = connection.cursor()
cursor.exectrace = mytrace
cursor.execute(
    """
        drop table if exists bar;
        create table bar(x,y,z);
        select * from point where x=?;
        """, (3, ))

# if set on a connection then all uses are traced
connection.exectrace = mytrace
# and clearing it
connection.exectrace = None

### rowtrace: Tracing returned rows


def rowtrace(cursor: apsw.Cursor, row: apsw.SQLiteValues) -> apsw.SQLiteValues:
    """Called with each row of results before they are handed off.  You can return None to
    cause the row to be skipped or a different set of values to return"""
    print("Row:", row)
    return row


# you can trace a single cursor
cursor = connection.cursor()
cursor.rowtrace = rowtrace
for row in cursor.execute("select x,y from point where x>4"):
    pass

# if set on a connection then all uses are traced
connection.rowtrace = rowtrace
# and clearing it
connection.rowtrace = None

### scalar: Defining your own functions


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
# average

# Here we return the longest item when represented as a string.


class longest:
    # A class is used to hold the current longest value

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
for row in connection.execute("select longest(x,y) from point"):
    print(row)

### collations: Defining collations (sorting)

# The default sorting mechanisms don't understand numbers at the end of strings
# so here we define a collation that does

connection.execute("create table s(str)")
connection.executemany("insert into s values(?)", (
    ("file1", ),
    ("file7", ),
    ("file17", ),
    ("file20", ),
    ("file3", ),
))

print("Standard sorting")
for row in connection.execute("select * from s order by str"):
    print(row)


def strnumcollate(s1: apsw.SQLiteValue, s2: apsw.SQLiteValue) -> int:
    # return -1 if s1<s2, +1 if s1>s2 else 0

    def parts(v):
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


connection.createcollation("strnum", strnumcollate)

print("Using strnum")
for row in connection.execute("select * from s order by str collate strnum"):
    print(row)

### authorizer: Authorizer (control what SQL can do)


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
connection.execute("insert into s values('foo')")
connection.execute("select str from s limit 1")
try:
    connection.execute("create table private_stuff(secret)")
    print("Created secret table!")
except Exception as e:
    print(e)

# Clear authorizer
connection.authorizer = None

### progress_handler: Progress handler


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

connection.setprogresshandler(None)

### blob_io: Blob I/O

connection.execute("create table blobby(x,y)")
# Add a blob we will fill in later
connection.execute("insert into blobby values(1, zeroblob(10000))")
# Or as a binding
connection.execute("insert into blobby values(2,?)", (apsw.zeroblob(20000), ))
# Open a blob for writing.  We need to know the rowid
rowid = connection.execute("select ROWID from blobby where x=1").fetchall()[0][0]
blob = connection.blobopen("main", "blobby", "y", rowid, True)
blob.write(b"hello world")
blob.seek(2000)
blob.write(b"hello world, again")
blob.close()

### commit_hook: Commit hook


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
    connection.execute("create table example(x,y,z); insert into example values (3,4,5)")
except apsw.ConstraintError:
    print("commit was not allowed")

connection.setcommithook(None)

### update_hook: Update hook


def my_update_hook(type: int, db_name: str, table_name: str, rowid: int) -> None:
    op: str = apsw.mapping_authorizer_function[type]
    print(f"Updated: { op } db { db_name }, table { table_name }, rowid { rowid }")


connection.setupdatehook(my_update_hook)
connection.execute("insert into s values(?)", ("file93", ))
connection.execute("update s set str=? where str=?", ("file94", "file93"))
connection.execute("delete from s where str=?", ("file94", ))
connection.setupdatehook(None)

### virtual_tables: Virtual tables

# This virtual table stores information about files in a set of
# directories so you can execute SQL queries


def getfiledata(directories):
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
                columns = ["rowid", "name", "directory"] + [x for x in dir(st) if x.startswith("st_")]
            data.append([counter, f, directory] + [getattr(st, x) for x in columns[3:]])
    return columns, data


# This gets registered with the Connection
class Source:

    def Create(self, db, modulename, dbname, tablename, *args):
        columns, data = getfiledata([eval(a.replace("\\", "\\\\")) for a in args])  # eval strips off layer of quotes
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

#@@CAPTURE
# Which 3 files are the biggest?
for size, directory, file in connection.execute(
        "select st_size,directory,name from sysfiles order by st_size desc limit 3"):
    print(size, file, directory)
#@@ENDCAPTURE

# Which 3 files are the oldest?
#@@CAPTURE
for ctime, directory, file in connection.execute(
        "select st_ctime,directory,name from sysfiles order by st_ctime limit 3"):
    print(ctime, file, directory)
#@@ENDCAPTURE

### @@ example-vfs
### A VFS that "obfuscates" the database file contents.  The scheme
### used is to xor all bytes with 0xa5.  This scheme honours that used
### for MAPI and SQL Server.
###


def encryptme(data):
    if not data: return data
    return bytes([x ^ 0xa5 for x in data])


# Inheriting from a base of "" means the default vfs
class ObfuscatedVFS(apsw.VFS):

    def __init__(self, vfsname="obfu", basevfs=""):
        self.vfsname = vfsname
        self.basevfs = basevfs
        apsw.VFS.__init__(self, self.vfsname, self.basevfs)

    # We want to return our own file implementation, but also
    # want it to inherit
    def xOpen(self, name, flags):
        # We can look at uri parameters
        if isinstance(name, apsw.URIFilename):
            #@@CAPTURE
            print("fast is", name.uri_parameter("fast"))
            print("level is", name.uri_int("level", 3))
            print("warp is", name.uri_boolean("warp", False))
            print("notpresent is", name.uri_parameter("notpresent"))
            #@@ENDCAPTURE
        return ObfuscatedVFSFile(self.basevfs, name, flags)


# The file implementation where we override xRead and xWrite to call our
# encryption routine
class ObfuscatedVFSFile(apsw.VFSFile):

    def __init__(self, inheritfromvfsname, filename, flags):
        apsw.VFSFile.__init__(self, inheritfromvfsname, filename, flags)

    def xRead(self, amount, offset):
        return encryptme(super(ObfuscatedVFSFile, self).xRead(amount, offset))

    def xWrite(self, data, offset):
        super(ObfuscatedVFSFile, self).xWrite(encryptme(data), offset)


# To register the VFS we just instantiate it
obfuvfs = ObfuscatedVFS()
# Lets see what vfs are now available?
#@@CAPTURE
print(apsw.vfsnames())
#@@ENDCAPTURE

# Make an obfuscated db, passing in some URI parameters
obfudb = apsw.Connection("file:myobfudb?fast=speed&level=7&warp=on",
                         flags=apsw.SQLITE_OPEN_READWRITE | apsw.SQLITE_OPEN_CREATE | apsw.SQLITE_OPEN_URI,
                         vfs=obfuvfs.vfsname)
# Check it works
obfudb.execute("create table foo(x,y); insert into foo values(1,2)")

# Check it really is obfuscated on disk
#@@CAPTURE
print(repr(open("myobfudb", "rb").read()[:20]))
#@@ENDCAPTURE

# And unobfuscating it
#@@CAPTURE
print(repr(encryptme(open("myobfudb", "rb").read()[:20])))
#@@ENDCAPTURE

# Tidy up
obfudb.close()
os.remove("myobfudb")

###
### Limits @@example-limit
###

#@@CAPTURE
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

#@@ENDCAPTURE

###
### Backup to memory @@example-backup
###

# We will copy the disk database into a memory database

memcon = apsw.Connection(":memory:")

# Copy into memory
with memcon.backup("main", connection, "main") as backup:
    backup.step()  # copy whole database in one go

# There will be no disk accesses for this query
for row in memcon.execute("select * from s"):
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
