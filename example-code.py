#!/usr/bin/env python
#
# This is the example code. The file should run correctly as is.  coloursrc.py is run over this
# file and produces HTML escaped and coloured code, as well as output where indicated.

#@@BEGIN
import os, sys, time
import apsw

###
### Check we have the expected version of apsw and sqlite
###

#@@CAPTURE
print "Using APSW file",apsw.__file__                # from the extension module
print "   APSW version",apsw.apswversion()           # from the extension module
print " SQLite version",apsw.sqlitelibversion()      # from the sqlite library code
print " SQLite version",apsw.SQLITE_VERSION_NUMBER   # from the sqlite header file at compile time
#@@ENDCAPTURE

###
### Opening/creating database
###

if os.path.exists("dbfile"): os.remove("dbfile")
connection=apsw.Connection("dbfile")
cursor=connection.cursor()

###
### simple statement
###

cursor.execute("create table foo(x,y,z)")

###
### multiple statements
###

cursor.execute("insert into foo values(1,2,3); create table bar(a,b,c) ; insert into foo values(4, 'five', 6.0)")

###
### iterator
###

for x,y,z in cursor.execute("select x,y,z from foo"):
    print cursor.getdescription()  # shows column names and declared types
    print x,y,z

###        
### iterator - multiple statements
###

for m,n,o in cursor.execute("select x,y,z from foo ; select a,b,c from bar"):
    print m,n,o

###
### bindings - sequence
###

cursor.execute("insert into foo values(?,?,?)", (7, 'eight', False))
cursor.execute("insert into foo values(?,?,?1)", ('one', 'two'))  # nb sqlite does the numbers from 1

###
### bindings - dictionary
###

cursor.execute("insert into foo values(:alpha, :beta, :gamma)", {'alpha': 1, 'beta': 2, 'gamma': 'three'})

###
### <a name="example-exectrace">tracing execution</a> <!-@!@->
###

def mytrace(statement, bindings):
    "Called just before executing each statement"
    print "SQL:",statement
    if bindings:
        print "Bindings:",bindings
    return True  # if you return False then execution is aborted

#@@CAPTURE
cursor.setexectrace(mytrace)
cursor.execute("drop table bar ; create table bar(x,y,z); select * from foo where x=?", (3,))
#@@ENDCAPTURE

###
### <a name="example-rowtrace">tracing results</a> <!-@!@->
###

def rowtrace(*results):
    """Called with each row of results before they are handed off.  You can return None to
    cause the row to be skipped or a different set of values to return"""
    print "Row:",results
    return results

#@@CAPTURE
cursor.setrowtrace(rowtrace)
for row in cursor.execute("select x,y from foo where x>3"):
     pass
#@@ENDCAPTURE

# Clear tracers
cursor.setrowtrace(None)
cursor.setexectrace(None)

###
### executemany
###

# (This will work correctly with multiple statements, as well as statements that
# return data.  The second argument can be anything that is iterable.)
cursor.executemany("insert into foo (x) values(?)", ( [1], [2], [3] ) )

# You can also use it for statements that return data
for row in cursor.executemany("select * from foo where x=?", ( [1], [2], [3] ) ):
    print row

###
### defining your own functions
###

def ilove7(*args):
    "a scalar function"
    print "ilove7 got",args,"but I love 7"
    return 7

connection.createscalarfunction("seven", ilove7)

for row in cursor.execute("select seven(x,y) from foo"):
    print row

###
### aggregate functions are more complex
###

# here we return the longest item when represented as a string

def longeststep(context, *args):
    "are any of the arguments longer than our current candidate"
    for arg in args:
        if len( str(arg) ) > len( context['longest'] ):
            context['longest']=str(arg)

def longestfinal(context):
    "return the winner"
    return context['longest']

def longestfactory():
    """called for a new query.  The first item returned can be
    anything and is passed as the context to the step
    and final methods.  We use a dict."""
    return ( { 'longest': '' }, longeststep, longestfinal)

connection.createaggregatefunction("longest", longestfactory)

for row in cursor.execute("select longest(x) from foo"):
    print row

###
### Defining collations.  
###

# The default sorting mechanisms don't understand numbers at the end of strings
# so here we define a collation that does

cursor.execute("create table s(str)")
cursor.executemany("insert into s values(?)", 
                  ( ["file1"], ["file7"], ["file17"], ["file20"], ["file3"] ) )

#@@CAPTURE
for row in cursor.execute("select * from s order by str"):
    print row
#@@ENDCAPTURE

def strnumcollate(s1, s2):
    # return -1 if s1<s2, +1 if s1>s2 else 0
    
    # split values into two parts - the head and the numeric tail
    values=[s1, s2]
    for vn,v in enumerate(values):
        for i in range(len(v), 0, -1):
            if v[i-1] not in "01234567890":
                break
        try:
            v=( v[:i], int(v[i:]) )
        except ValueError:
            v=( v[:i], None )
        values[vn]=v
    # compare
    if values[0]<values[1]:
        return -1
    if values[0]>values[1]:
        return 1
    return 0

connection.createcollation("strnum", strnumcollate)

#@@CAPTURE
for row in cursor.execute("select * from s order by str collate strnum"):
    print row    
#@@ENDCAPTURE

###
### Authorizer (eg if you want to control what user supplied SQL can do)
###

def authorizer(operation, paramone, paramtwo, databasename, triggerorview):
    """Called when each operation is prepared.  We can return SQLITE_OK, SQLITE_DENY or
    SQLITE_IGNORE"""
    # find the operation name
    print apsw.mapping_authorizer_function[operation],
    print paramone, paramtwo, databasename, triggerorview
    if operation==apsw.SQLITE_CREATE_TABLE and paramone.startswith("private"):
        return apsw.SQLITE_DENY  # not allowed to create tables whose names start with private
    
    return apsw.SQLITE_OK  # always allow

connection.setauthorizer(authorizer)
#@@CAPTURE
cursor.execute("insert into s values('foo')")
cursor.execute("select str from s limit 1")
#@@ENDCAPTURE

# Cancel authorizer
connection.setauthorizer(None)

###
### progress handler (SQLite 3 experimental feature)
###

# something to give us large numbers of random numbers
import random
def randomintegers(howmany):
    for i in xrange(howmany):
        yield (random.randint(0,9999999999),)

# create a table with 100 random numbers
cursor.execute("begin ; create table bigone(x)")
cursor.executemany("insert into bigone values(?)", randomintegers(100))
cursor.execute("commit")

# display an ascii spinner
_phcount=0
_phspinner="|/-\\"
def progresshandler():
    global _phcount
    sys.stdout.write(_phspinner[_phcount%len(_phspinner)]+chr(8)) # chr(8) is backspace
    sys.stdout.flush()
    _phcount+=1
    time.sleep(0.1) # deliberate delay so we can see the spinner (SQLite is too fast otherwise!)
    return 0  # returning non-zero aborts

# register progresshandler every 20 instructions
connection.setprogresshandler(progresshandler, 20)

# see it in action - sorting 100 numbers to find the biggest takes a while
print "spinny thing -> ",
for i in cursor.execute("select max(x) from bigone"):
    print # newline
    print i # and the maximum number

connection.setprogresshandler(None)

###
### commit hook (SQLite3 experimental feature)
###

def mycommithook():
    print "in commit hook"
    hour=time.localtime()[3]
    if hour<8 or hour>17:
        print "no commits out of hours"
        return 1  # abort commits outside of 8am through 6pm
    print "commits okay at this time"
    return 0  # let commit go ahead

connection.setcommithook(mycommithook)
try:
    cursor.execute("begin; create table example(x,y,z); insert into example values (3,4,5) ; commit")
except apsw.ConstraintError:
    print "commit was not allowed"

connection.setcommithook(None)

###
### <a name="example-blobio">Blob I/O</a> <!-@!@->
###

cursor.execute("create table blobby(x,y)")
# Add a blob we will fill in later
cursor.execute("insert into blobby values(1,zeroblob(10000))")
# Or as a binding
cursor.execute("insert into blobby values(2,?)", (apsw.zeroblob(20000),))
# Open a blob for writing.  We need to know the rowid
rowid=cursor.execute("select ROWID from blobby where x=1").next()[0]
blob=connection.blobopen("main", "blobby", "y", rowid, 1) # 1 is for read/write
blob.write("hello world")
blob.seek(2000)
blob.write("hello world, again")
blob.close()

###
### Virtual tables
###

# This virtual table stores information about files in a set of
# directories so you can execute SQL queries

def getfiledata(directories):
    columns=None
    data=[]
    counter=1
    for directory in directories:
        for f in os.listdir(directory):
            if not os.path.isfile(os.path.join(directory,f)):
                continue
            counter+=1
            st=os.stat(os.path.join(directory,f))
            if columns is None:
                columns=["rowid", "name", "directory"]+[x for x in dir(st) if x.startswith("st_")]
            data.append( [counter, f, directory] + [getattr(st,x) for x in columns[3:]] )
    return columns, data

# This gets registered with the Connection
class Source:
    def Create(self, db, modulename, dbname, tablename, *args):
        columns,data=getfiledata([eval(a) for a in args]) # eval strips off layer of quotes
        schema="create table foo("+','.join(["'%s'" % (x,) for x in columns[1:]])+")"
        return schema,Table(columns,data)
    Connect=Create

# Represents a table
class Table:
    def __init__(self, columns, data):
        self.columns=columns
        self.data=data

    def BestIndex(self, *args):
        return None

    def Open(self):
        return Cursor(self)

    def Disconnect(self):
        pass

    Destroy=Disconnect

# Represents a cursor
class Cursor:
    def __init__(self, table):
        self.table=table

    def Filter(self, *args):
        self.pos=0

    def Eof(self):
        return self.pos>=len(self.table.data)

    def Rowid(self):
        return self.table.data[self.pos][0]

    def Column(self, col):
        return self.table.data[self.pos][1+col]

    def Next(self):
        self.pos+=1

    def Close(self):
        pass

# Register the module as filesource
connection.createmodule("filesource", Source())

# Arguments to module - all directories in sys.path
sysdirs=",".join(["'%s'" % (x,) for x in sys.path[1:] if len(x) and os.path.isdir(x)])
cursor.execute("create virtual table sysfiles using filesource("+sysdirs+")")

#@@CAPTURE
# Which 3 files are the biggest?
for size,directory,file in cursor.execute("select st_size,directory,name from sysfiles order by st_size desc limit 3"):
    print size,file,directory
#@@ENDCAPTURE

# Which 3 files are the oldest?
#@@CAPTURE
for ctime,directory,file in cursor.execute("select st_ctime,directory,name from sysfiles order by st_ctime limit 3"):
    print ctime,file,directory
#@@ENDCAPTURE

###
### Limits
###

#@@CAPTURE
# Print some limits
for limit in ("LENGTH", "COLUMN", "ATTACHED"):
    name="SQLITE_LIMIT_"+limit 
    max="SQLITE_MAX_"+limit  # compile time
    print name, connection.limit(getattr(apsw, name))
    print max, getattr(apsw, max)

# Set limit for size of a string
cursor.execute("create table testlimit(s)")
cursor.execute("insert into testlimit values(?)", ( "x"*1024, )) # 1024 char string
connection.limit(apsw.SQLITE_LIMIT_LENGTH, 1023) # limit is now 1023
try:
    cursor.execute("insert into testlimit values(?)", ( "y"*1024, ))
    print "string exceeding limit was inserted"
except apsw.TooBigError:
    print "Caught toobig exception"
    
#@@ENDCAPTURE
    
###
### Cleanup
###

# We can close connections manually (useful if you want to catch exceptions)
# but you don't have to
connection.close(True)  # force it since we want to exit

#@@END
