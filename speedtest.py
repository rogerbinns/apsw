#!/usr/bin/env python

# Do speed tests.  The tests try to correspond to http://www.sqlite.org/cvstrac/fileview?f=sqlite/tool/mkspeedsql.tcl

# Check we can do both apsw and pysqlite

import sys
import os
import random
import time
import gc

write=sys.stdout.write
write("                    Python "+sys.executable+" "+str(sys.version_info)+"\n\n")

import apsw

write("    Testing with APSW file "+apsw.__file__+"\n")
write("              APSW version "+apsw.apswversion()+"\n")
write("        SQLite lib version "+apsw.sqlitelibversion()+"\n")
write("    SQLite headers version "+str(apsw.SQLITE_VERSION_NUMBER)+"\n\n")

from pysqlite2 import dbapi2 as pysqlite

write("Testing with pysqlite file "+pysqlite.__file__+"\n")
write("          pysqlite version "+pysqlite.version+"\n")
write("            SQLite version "+pysqlite.sqlite_version+"\n\n")


ones=("zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine",
      "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen",
      "eighteen", "nineteen")
tens=("", "ten", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety")

def number_name(n):
    if n>=1000:
        txt="%s thousand" % (number_name(n/1000),)
        n=n%1000
    else:
        txt=""

    if n>=100:
        txt=txt+" "+ones[n/100]+" hundred"
        n=n%100

    if n>=20:
        txt=txt+" "+tens[n/10]
        n=n%10

    if n>0:
        txt=txt+" "+ones[n]

    txt=txt.strip()

    if txt=="":
        txt="zero"

    return txt

def getlines(scale=50, bindings=False):
    random.seed(0)

    # database schema
    for i in """PRAGMA page_size=1024;
  PRAGMA cache_size=8192;
  PRAGMA locking_mode=EXCLUSIVE;
  CREATE TABLE t1(a INTEGER, b INTEGER, c TEXT);
  CREATE TABLE t2(a INTEGER, b INTEGER, c TEXT);
  CREATE INDEX i2a ON t2(a);
  CREATE INDEX i2b ON t2(b);
  SELECT name FROM sqlite_master ORDER BY 1""".split(";"):
        yield (i,)

    # 50,000 inserts on an unindexed table
    yield ("BEGIN",)
    for i in xrange(1,scale*10000+1):
        r=random.randint(0,500000)
        if bindings:
            yield ("INSERT INTO t1 VALUES(:1, :2, number_name(:2))", (i, r))
        else:
            yield ("INSERT INTO t1 VALUES(%d, %d, '%s')" % (i, r, number_name(r)),)
    yield ("COMMIT",)

    # 50,000 inserts on an indexed table
    t1c_list=[]
    yield ("BEGIN",)
    for i in xrange(1,scale*10000+1):
        r=random.randint(0,500000)
        x=number_name(r)
        t1c_list.append(x)
        if bindings:
            yield ("INSERT INTO t2 VALUES(:1, :2, number_name(:2))", (i, r))
        else:
            yield ("INSERT INTO t2 VALUES(%d, %d, '%s')" % (i, r, x),)
    yield ("COMMIT",)

    # 50 SELECTs on an integer comparison.  There is no index so
    # a full table scan is required.
    for i in xrange(scale):
        yield ("SELECT count(*), avg(b) FROM t1 WHERE b>=%d AND b<%d" % (i*100, (i+10)*100),)

    
    # 50 SELECTs on an LIKE comparison.  There is no index so a full
    # table scan is required.
    for i in xrange(scale):
        yield ("SELECT count(*), avg(b) FROM t1 WHERE c LIKE '%%%s%%'" % (number_name(i),),)

    # Create indices
    yield ("BEGIN",)
    for i in """CREATE INDEX i1a ON t1(a);
                CREATE INDEX i1b ON t1(b);
                CREATE INDEX i1c ON t1(c);""".split(";"):
        yield (i,)
    yield ("COMMIT",)

    # 5000 SELECTs on an integer comparison where the integer is
    # indexed.
    for i in xrange(scale*100):
        yield ("SELECT count(*), avg(b) FROM t1 WHERE b>=%d AND b<%d" % (i*100, (i+10)*100),)

    # 100000 random SELECTs against rowid.
    for i in xrange(1,scale*2000+1):
        yield ("SELECT c FROM t1 WHERE rowid=%d" % (1+random.randint(0,50000),),)

    # 100000 random SELECTs against a unique indexed column.
    for i in xrange(1,scale*2000+1):
        yield ("SELECT c FROM t1 WHERE a=%d" % (1+random.randint(0,50000),),)

    # 50000 random SELECTs against an indexed column text column
    for i in xrange(scale*1000):
        yield ("SELECT c FROM t1 WHERE c='%s'" % (random.choice(t1c_list),),)
        
    # Vacuum
    yield ("VACUUM",)

    # 5000 updates of ranges where the field being compared is indexed.
    yield ("BEGIN",)
    for i in xrange(scale*100):
        yield ("UPDATE t1 SET b=b*2 WHERE a>=%d AND a<%d" % (i*2, (i+1)*2),)
    yield ("COMMIT",)

    # 50000 single-row updates.  An index is used to find the row quickly.
    yield ("BEGIN",)
    for i in xrange(scale*1000):
        yield ("UPDATE t1 SET b=%d WHERE a=%d" % (random.randint(0,500000), i),)
    yield ("COMMIT",)

    # 1 big text update that touches every row in the table.
    yield ("UPDATE t1 SET c=a",)

    # Many individual text updates.  Each row in the table is
    # touched through an index.
    yield ("BEGIN",)
    for i in xrange(1,scale*1000+1):
        yield ("UPDATE t1 SET c='%s' WHERE a=%d" % (number_name(random.randint(0,500000)), i),)
    yield ("COMMIT",)

    # Delete all content in a table.
    yield ("DELETE FROM t1",)

    # Copy one table into another
    yield ("INSERT INTO t1 SELECT * FROM t2",)

    # Delete all content in a table, one row at a time.
    yield ("DELETE FROM t1 WHERE 1",)

    # Refill the table yet again
    yield ("INSERT INTO t1 SELECT * FROM t2",)

    # Drop the table and recreate it without its indices.
    yield ("BEGIN",)
    yield ("DROP TABLE t1",)
    yield ("CREATE TABLE t1(a INTEGER, b INTEGER, c TEXT)",)
    yield ("COMMIT",)

    # Refill the table yet again.  This copy should be faster because
    # there are no indices to deal with.
    yield ("INSERT INTO t1 SELECT * FROM t2",)

    # The three following used "ORDER BY random()" but we can't do that
    # as it causes each run to have different values, and hence different
    # amounts of sorting that have to go on.  The "random()" has been
    # replaced by "c", the column that has the stringified number

    # Select 20000 rows from the table at random.
    yield ("SELECT rowid FROM t1 ORDER BY c LIMIT %d" % (scale*400,),)

    # Delete 20000 random rows from the table.
    yield ("""  DELETE FROM t1 WHERE rowid IN
                     (SELECT rowid FROM t1 ORDER BY c LIMIT %d)""" % (scale*400,),)

    yield ("SELECT count(*) FROM t1",)
    
    # Delete 20000 more rows at random from the table.
    yield ("""DELETE FROM t1 WHERE rowid IN
                 (SELECT rowid FROM t1 ORDER BY c LIMIT %d)""" % (scale*400,),)

    yield ("SELECT count(*) FROM t1",)

text=";".join([x[0] for x in getlines(scale=1)])+";" # pysqlite requires final semicolon
bindings=[line for line in getlines(scale=10, bindings=True)]

def apsw_bigstmt(dbfile):
    "APSW big statement"
    con=apsw.Connection(dbfile)
    b4=time.time()
    for row in con.cursor().execute(text): pass
    after=time.time()
    con.close()
    return after-b4

def pysqlite_bigstmt(dbfile):
    "pysqlite big statement"
    con=pysqlite.connect(dbfile, isolation_level=None)
    b4=time.time()
    for row in con.executescript(text+";"): pass
    after=time.time()
    con.close()
    return after-b4

def apsw_statements(dbfile):
    "APSW individual statements"
    con=apsw.Connection(dbfile)
    con.createscalarfunction("number_name", number_name, 1)
    cursor=con.cursor()
    b4=time.time()
    for b in bindings:
        for row in cursor.execute(*b): pass
    after=time.time()
    con.close()
    return after-b4

def pysqlite_statements(dbfile):
    "pysqlite individual statements"
    con=pysqlite.connect(dbfile, isolation_level=None)
    con.create_function("number_name", 1, number_name)
    cursor=con.cursor()
    b4=time.time()
    for b in bindings:
        for row in cursor.execute(*b): pass
    after=time.time()
    con.close()
    return after-b4

tests=(
    # pysqlite_bigstmt,
    # apsw_bigstmt,
    pysqlite_statements,
    apsw_statements,
    )

dbfile=":memory:"
#dbfile="testdb2"
for i in range(5):
    for t in tests:
        if os.path.exists(dbfile):
            os.remove(dbfile)
        gc.collect()
        print t.__doc__,t(dbfile)
        
