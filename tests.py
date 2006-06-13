#!/usr/bin/env python

# APSW test suite

import apsw

print "Testing with APSW file",apsw.__file__
print "          APSW version",apsw.apswversion()
print "        SQLite version",apsw.sqlitelibversion()


# unittest stuff from here on

import unittest
import os
import sys
import math
import random
import time
import threading
import Queue
import traceback

# helper functions
def randomintegers(howmany):
    for i in xrange(howmany):
        yield (random.randint(0,9999999999),)

# helper class - runs code in a seperate thread
class ThreadRunner(threading.Thread):

    def __init__(self, callable, *args, **kwargs):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.callable=callable
        self.args=args
        self.kwargs=kwargs
        self.q=Queue.Queue()

    def go(self):
        self.start()
        t,res=self.q.get()
        if t: # result
            return res
        else: # exception
            raise res[0], res[1], res[2]

    def run(self):
        try:
            self.q.put( (True, self.callable(*self.args, **self.kwargs)) )
        except:
            self.q.put( (False, sys.exc_info()) )


# main test class/code
class APSW(unittest.TestCase):
    
    def setUp(self, dbname="testdb"):
        # clean out database and journal from last run
        for i in "-journal", "":
            if os.path.exists(dbname+i):
                os.remove(dbname+i)
            assert not os.path.exists(dbname+i)
        self.db=apsw.Connection(dbname)

    def tearDown(self):
        # we don't delete the database file itself.  it will be
        # left around if there was a failure
        del self.db

    def assertTableExists(self, tablename):
        self.failUnlessEqual(self.db.cursor().execute("select count(*) from ["+tablename+"]").next()[0], 0)

    def assertTableNotExists(self, tablename):
        # you get SQLError if the table doesn't exist!
        self.assertRaises(apsw.SQLError, self.db.cursor().execute, "select count(*) from ["+tablename+"]")

    def testSanity(self):
        "Check all parts compiled and are present"
        # check some error codes etc are present - picked first middle and last from lists in code
        apsw.SQLError
        apsw.MisuseError
        apsw.NotADBError
        apsw.ThreadingViolationError
        apsw.BindingsError
        apsw.ExecTraceAbort

    def testConnection(self):
        "Test connection basics"
        # keyword args are not allowed
        self.assertRaises(TypeError, apsw.Connection, ":memory:", user="nobody")
        # too many arguments
        self.assertRaises(TypeError, apsw.Connection, ":memory:", 7)
        # wrong types
        self.assertRaises(TypeError, apsw.Connection, 3)
        # non-unicode
        self.assertRaises(UnicodeDecodeError, apsw.Connection, "\xef\x22\xd3\x9e")

    def testMemoryLeaks(self):
        "MemoryLeaks: Run with a memory profiler such as valgrind and debug Python"
        # make and toss away a bunch of db objects, cursors, functions etc - if you use memory profiling then
        # memory leaks will show up
        c=self.db.cursor()
        c.execute("create table foo(x)")
        c.executemany("insert into foo values(?)", ( [1], [None], [math.pi], ["jkhfkjshdf"], [u"\u1234\u345432432423423kjgjklhdfgkjhsdfjkghdfjskh"],
                                                     [buffer("78696ghgjhgjhkgjkhgjhg\xfe\xdf")]))
        for i in xrange(1000):
            db=apsw.Connection("testdb")
            db.createaggregatefunction("aggfunc", lambda x: x)
            db.createscalarfunction("scalarfunc", lambda x: x)
            db.setbusyhandler(lambda x: False)
            db.setbusytimeout(1000)
            db.setcommithook(lambda x=1: 0)
            db.setrollbackhook(lambda x=2: 1)
            db.setupdatehook(lambda x=3: 2)
            for i in xrange(100):
                c2=db.cursor()
                c2.setrowtrace(lambda x: (x,))
                c2.setexectrace(lambda x,y: True)
                for row in c2.execute("select * from foo"):
                    pass
            del c2
            del db

    def testBindings(self):
        "Check bindings work correctly"
        c=self.db.cursor()
        c.execute("create table foo(x,y,z)")
        vals=(
            ("(?,?,?)", (1,2,3)),
            ("(?,?,?)", [1,2,3]),
            ("(?,?,?)", range(1,4)),
            ("(:a,$b,:c)", {'a': 1, 'b': 2, 'c': 3}),
            ("(1,?,3)", (2,)),
            ("(1,$a,$c)", {'a': 2, 'b': 99, 'c': 3}),
            )
        for str,bindings in vals:
            c.execute("insert into foo values"+str, bindings)
            self.failUnlessEqual(c.execute("select * from foo").next(), (1,2,3))
            c.execute("delete from foo")

        # these ones should cause errors
        vals=(
            (apsw.BindingsError, "(?,?,?)", (1,2)), # too few
            (apsw.BindingsError, "(?,?,?)", (1,2,3,4)), # too many
            (TypeError,          "(?,?,?)", None), # none at all
            (apsw.BindingsError, "(?,?,?)", {'a': 1}), # ? type, dict bindings (note that the reverse will work since all
                                                       # named bindings are alse implicitly numbered
            (TypeError,          "(?,?,?)", 2),    # not a dict or sequence
            )
        for exc,str,bindings in vals:
            self.assertRaises(exc, c.execute, "insert into foo values"+str, bindings)

        # with multiple statements
        c.execute("insert into foo values(?,?,?); insert into foo values(?,?,?)", (99,100,101,102,103,104))
        self.assertRaises(apsw.BindingsError, c.execute, "insert into foo values(?,?,?); insert into foo values(?,?,?)",
                          (100,100,101,1000,103)) # too few
        self.assertRaises(apsw.BindingsError, c.execute, "insert into foo values(?,?,?); insert into foo values(?,?,?)",
                          (101,100,101,1000,103,104,105)) # too many
        # check the relevant statements did or didn't execute as appropriate
        self.failUnlessEqual(self.db.cursor().execute("select count(*) from foo where x=99").next()[0], 1)
        self.failUnlessEqual(self.db.cursor().execute("select count(*) from foo where x=102").next()[0], 1)
        self.failUnlessEqual(self.db.cursor().execute("select count(*) from foo where x=100").next()[0], 1)
        self.failUnlessEqual(self.db.cursor().execute("select count(*) from foo where x=1000").next()[0], 0)
        self.failUnlessEqual(self.db.cursor().execute("select count(*) from foo where x=101").next()[0], 1)
        self.failUnlessEqual(self.db.cursor().execute("select count(*) from foo where x=105").next()[0], 0)

        # regression test
        # self.assertRaises(apsw.BindingsError, c.execute, "create table bar(x,y,z);insert into bar values(?,?,?)")

        # across executemany
        vals=( (1,2,3), (4,5,6), (7,8,9) )
        c.executemany("insert into foo values(?,?,?);", vals)
        for x,y,z in vals:
            self.failUnlessEqual(c.execute("select * from foo where x=?",(x,)).next(), (x,y,z))

        # some errors in executemany
        self.assertRaises(apsw.BindingsError, c.executemany, "insert into foo values(?,?,?)", ( (1,2,3), (1,2,3,4)))
        self.assertRaises(apsw.BindingsError, c.executemany, "insert into foo values(?,?,?)", ( (1,2,3), (1,2)))

    def testCursor(self):
        "Check functionality of the cursor"
        c=self.db.cursor()
        c.execute("create table foo(x,y,z)")
        # table should be empty
        entry=-1
        for entry,values in enumerate(c.execute("select * from foo")):
            pass
        self.failUnlessEqual(entry,-1, "No rows should have been returned")
        # add ten rows
        for i in range(10):
            c.execute("insert into foo values(1,2,3)")
        for entry,values in enumerate(c.execute("select * from foo")):
            # check we get back out what we put in
            self.failUnlessEqual(values, (1,2,3))
        self.failUnlessEqual(entry, 9, "There should have been ten rows")
        # does getconnection return the right object
        self.failUnless(c.getconnection() is self.db)
        # check getdescription - note column with space in name and [] syntax to quote it
        cols=(
            ("x a space", "integer"),
            ("y", "text"),
            ("z", "foo"),
            ("a", "char"),
            (u"\N{LATIN SMALL LETTER E WITH CIRCUMFLEX}\N{LATIN SMALL LETTER A WITH TILDE}", u"\N{LATIN SMALL LETTER O WITH DIAERESIS}\N{LATIN SMALL LETTER U WITH CIRCUMFLEX}"),
            )
        c.execute("drop table foo; create table foo (%s)" % (", ".join(["[%s] %s" % (n,t) for n,t in cols]),))
        c.execute("insert into foo([x a space]) values(1)")
        for row in c.execute("select * from foo"):
            self.failUnlessEqual(cols, c.getdescription())
        # execution is complete ...
        self.assertRaises(apsw.ExecutionCompleteError, c.getdescription)
        self.assertRaises(StopIteration, c.next)
        self.assertRaises(StopIteration, c.next)
        # nulls for getdescription
        for row in c.execute("pragma user_version"):
            self.assertEqual(c.getdescription(), ( (None, None), ))
        # incomplete
        c.execute("select * from foo; create table bar(x)") # we don't bother reading leaving 
        self.assertRaises(apsw.IncompleteExecutionError, c.execute, "select * from foo") # execution incomplete
        self.assertTableNotExists("bar")
        # autocommit
        self.assertEqual(True, self.db.getautocommit())
        c.execute("begin immediate")
        self.assertEqual(False, self.db.getautocommit())
        # pragma
        c.execute("pragma user_version")
        c.execute("pragma pure=nonsense")

    def testTypes(self):
        "Check type information is maintained"
        c=self.db.cursor()
        c.execute("create table foo(row,x)")
        vals=("a simple string",  # "ascii" string
              "0123456789"*200000, # a longer string
              u"a \u1234 unicode \ufe54 string \u0089",  # simple unicode string
              u"\N{BLACK STAR} \N{WHITE STAR} \N{LIGHTNING} \N{COMET} ", # funky unicode
              97, # integer
              2147483647,   # numbers on 31 bit boundary (32nd bit used for integer sign), and then
              -2147483647,  # start using 32nd bit (must be represented by 64bit to avoid losing
              2147483648L,  # detail)
              -2147483648L,
              2147483999L,
              -2147483999L,
              sys.maxint,
              992147483999L,
              -992147483999L,
              9223372036854775807L,
              -9223372036854775808L,
              buffer("a set of bytes"),      # bag of bytes initialised from a string, but don't confuse it with a
              buffer("".join([chr(x) for x in range(256)])), # string
              buffer("".join([chr(x) for x in range(256)])*20000),  # non-trivial size
              None,  # our good friend NULL/None
              1.1,  # floating point can't be compared exactly - failUnlessAlmostEqual is used to check
              10.2, # see Appendix B in the Python Tutorial 
              1.3,
              1.45897589347E97,
              5.987987/8.7678678687676786,
              math.pi,
              True,  # derived from integer
              False
              )
        for i,v in enumerate(vals):
            c.execute("insert into foo values(?,?)", (i, v))

        # now see what we got out
        count=0
        for row,v in c.execute("select * from foo"):
            count+=1
            if type(vals[row]) is float:
                self.failUnlessAlmostEqual(vals[row], v)
            else:
                self.failUnlessEqual(vals[row], v)
        self.failUnlessEqual(count, len(vals))

        # check some out of bounds conditions
        # integer greater than signed 64 quantity (SQLite only supports up to that)
        self.assertRaises(OverflowError, c.execute, "insert into foo values(9999,?)", (922337203685477580799L,))
        self.assertRaises(OverflowError, c.execute, "insert into foo values(9999,?)", (-922337203685477580799L,))

        # invalid character data - non-ascii data must be provided in unicode
        self.assertRaises(UnicodeDecodeError, c.execute, "insert into foo values(9999,?)", ("\xfe\xfb\x80\x92",))

        # not valid types for SQLite
        self.assertRaises(TypeError, c.execute, "insert into foo values(9999,?)", (apsw,)) # a module
        self.assertRaises(TypeError, c.execute, "insert into foo values(9999,?)", (type,)) # type
        self.assertRaises(TypeError, c.execute, "insert into foo values(9999,?)", (dir,))  # function

        # check nothing got inserted
        self.failUnlessEqual(0, c.execute("select count(*) from foo where row=9999").next()[0])
        
    def testAuthorizer(self):
        "Verify the authorizer works"
        def authorizer(operation, paramone, paramtwo, databasename, triggerorview):
            # we fail creates of tables starting with "private"
            if operation==apsw.SQLITE_CREATE_TABLE and paramone.startswith("private"):
                return apsw.SQLITE_DENY
            return apsw.SQLITE_OK
        c=self.db.cursor()
        # this should succeed
        c.execute("create table privateone(x)")
        # this should fail
        self.assertRaises(TypeError, self.db.setauthorizer, 12) # must be callable
        self.db.setauthorizer(authorizer)
        self.assertRaises(apsw.AuthError, c.execute, "create table privatetwo(x)")
        # this should succeed
        self.db.setauthorizer(None)
        c.execute("create table privatethree(x)")

        self.assertTableExists("privateone")
        self.assertTableNotExists("privatetwo")
        self.assertTableExists("privatethree")

        # error in callback
        def authorizer(operation, *args):
            if operation==apsw.SQLITE_CREATE_TABLE:
                1/0
            return apsw.SQLITE_OK
        self.db.setauthorizer(authorizer)
        self.assertRaises(ZeroDivisionError, c.execute, "create table shouldfail(x)")
        self.assertTableNotExists("shouldfail")

        # bad return type in callback
        def authorizer(operation, *args):
            return "a silly string"
        self.db.setauthorizer(authorizer)
        self.assertRaises(TypeError, c.execute, "create table shouldfail(x)")
        self.assertTableNotExists("shouldfail")

        # back to normal
        self.db.setauthorizer(None)
        c.execute("create table shouldsucceed(x)")
        self.assertTableExists("shouldsucceed")

    def testExecTracing(self):
        "Verify tracing of executed statements and bindings"
        c=self.db.cursor()
        cmds=[] # this is maniulated in tracefunc
        def tracefunc(cmd, bindings):
            cmds.append( (cmd, bindings) )
            return True
        c.execute("create table one(x,y,z)")
        self.failUnlessEqual(len(cmds),0)
        self.assertRaises(TypeError, c.setexectrace, 12) # must be callable
        c.setexectrace(tracefunc)
        statements=[
            ("insert into one values(?,?,?)", (1,2,3)),
            ("insert into one values(:a,$b,$c)", {'a': 1, 'b': "string", 'c': None}),
            ]
        for cmd,values in statements:
            c.execute(cmd, values)
        self.failUnlessEqual(cmds, statements)
        self.failUnless(c.getexectrace() is tracefunc)
        c.setexectrace(None)
        self.failUnless(c.getexectrace() is None)
        c.execute("create table bar(x,y,z)")
        # cmds should be unchanged
        self.failUnlessEqual(cmds, statements)
        # tracefunc can abort execution
        count=c.execute("select count(*) from one").next()[0]
        def tracefunc(cmd, bindings):
            return False # abort
        c.setexectrace(tracefunc)
        self.assertRaises(apsw.ExecTraceAbort, c.execute, "insert into one values(1,2,3)")
        # table should not have been modified
        c.setexectrace(None)
        self.failUnlessEqual(count, c.execute("select count(*) from one").next()[0])

    def testRowTracing(self):
        "Verify row tracing"
        c=self.db.cursor()
        c.execute("create table foo(x,y,z)")
        vals=(1,2,3)
        c.execute("insert into foo values(?,?,?)", vals)
        def tracefunc(*result):
            return tuple([7 for i in result])
        # should get original row back
        self.failUnlessEqual(c.execute("select * from foo").next(), vals)
        self.assertRaises(TypeError, c.setrowtrace, 12) # must be callable
        c.setrowtrace(tracefunc)
        self.failUnless(c.getrowtrace() is tracefunc)
        # all values replaced with 7
        self.failUnlessEqual(c.execute("select * from foo").next(), tuple([7]*len(vals)))
        def tracefunc(*result):
            return (7,)
        # a single 7
        c.setrowtrace(tracefunc)
        self.failUnlessEqual(c.execute("select * from foo").next(), (7,))
        # no alteration again
        c.setrowtrace(None)
        self.failUnlessEqual(c.execute("select * from foo").next(), vals)

    def testScalarFunctions(self):
        "Verify scalar functions"
        c=self.db.cursor()
        def ilove7(*args):
            return 7
        self.assertRaises(TypeError, self.db.createscalarfunction, "twelve", 12) # must be callable
        self.db.createscalarfunction("seven", ilove7)
        c.execute("create table foo(x,y,z)")
        for i in range(10):
            c.execute("insert into foo values(?,?,?)", (i,i,i))
        for i in range(10):
            self.failUnlessEqual( (7,), c.execute("select seven(x,y,z) from foo where x=?", (i,)).next())
        # function names are limited to 255 characters - SQLerror is the rather unintuitive error return
        self.assertRaises(apsw.SQLError, self.db.createscalarfunction, "a"*300, ilove7)
        # have an error in a function
        def badfunc(*args):
            return 1/0
        self.db.createscalarfunction("badscalarfunc", badfunc)
        self.assertRaises(ZeroDivisionError, c.execute, "select badscalarfunc(*) from foo")

    def testAggregateFunctions(self):
        "Verify aggregate functions"
        c=self.db.cursor()
        c.execute("create table foo(x,y,z)")
        # aggregate function
        class longest:
            def __init__(self):
                self.result=""
                
            def step(self, context, *args):
                for i in args:
                    if len(str(i))>len(self.result):
                        self.result=str(i)

            def final(self, context):
                return self.result

            def factory():
                v=longest()
                return None,v.step,v.final
            factory=staticmethod(factory)

        self.assertRaises(TypeError, self.db.createaggregatefunction,"twelve", 12) # must be callable
        self.db.createaggregatefunction("longest", longest.factory)

        vals=(
            ("kjfhgk","gkjlfdhgjkhsdfkjg","gklsdfjgkldfjhnbnvc,mnxb,mnxcv,mbncv,mnbm,ncvx,mbncv,mxnbcv,"), # last one is deliberately the longest
            ("gdfklhj",":gjkhgfdsgfd","gjkfhgjkhdfkjh"),
            ("gdfjkhg","gkjlfd",""),
            (1,2,30),
           )

        for v in vals:
            c.execute("insert into foo values(?,?,?)", v)

        v=c.execute("select longest(x,y,z) from foo").next()[0]
        self.failUnlessEqual(v, vals[0][2])

        # SQLite doesn't allow step functions to return an error, so we have to defer to the final
        def badfactory():
            def badfunc(*args):
                1/0
            def final(*args):
                assert False, "This should not be executed"
                return 1
            return None,badfunc,final
        
        self.db.createaggregatefunction("badfunc", badfactory)

        self.assertRaises(ZeroDivisionError, c.execute, "select badfunc(x) from foo")

    def testCollation(self):
        "Verify collations"
        c=self.db.cursor()
        def strnumcollate(s1, s2):
            "return -1 if s1<s2, +1 if s1>s2 else 0.  Items are string head and numeric tail"
            # split values into two parts - the head and the numeric tail
            values=[s1,s2]
            for vn,v in enumerate(values):
                for i in range(len(v),0,-1):
                    if v[i-1] not in "01234567890":
                        break
                try:
                    v=v[:i],int(v[i:])
                except ValueError:
                    v=v[:i],None
                values[vn]=v
            # compare
            if values[0]<values[1]:
                return -1
            if values[0]>values[1]:
                return 1
            return 0

        self.assertRaises(TypeError, self.db.createcollation, "twelve", 12) # must be callable
        self.db.createcollation("strnum", strnumcollate)
        c.execute("create table foo(x)")
        # adding this unicode in front improves coverage
        uni=u"\N{LATIN SMALL LETTER E WITH CIRCUMFLEX}"
        vals=(uni+"file1", uni+"file7", uni+"file9", uni+"file17", uni+"file20")
        valsrev=list(vals)
        valsrev.reverse() # put them into table in reverse order
        c.executemany("insert into foo values(?)", [(x,) for x in valsrev])
        for i,row in enumerate(c.execute("select x from foo order by x collate strnum")):
            self.failUnlessEqual(vals[i], row[0])

        # collation function with an error
        def collerror(*args):
            return 1/0
        self.db.createcollation("collerror", collerror)
	try:
	    c.execute("select x from foo order by x collate collerror")
	except ZeroDivisionError:
	    pass
        self.assertRaises(ZeroDivisionError, c.execute, "select x from foo order by x collate collerror")
        
    def testProgressHandler(self):
        "Verify progress handler"
        c=self.db.cursor()
        phcalledcount=[0]
        def ph():
            phcalledcount[0]=phcalledcount[0]+1
            return 0

        # make 400 rows of random numbers
        c.execute("begin ; create table foo(x)")
        c.executemany("insert into foo values(?)", randomintegers(400))
        c.execute("commit")

        self.assertRaises(TypeError, self.db.setprogresshandler, 12) # must be callable
        self.assertRaises(TypeError, self.db.setprogresshandler, ph, "foo") # second param is steps
        self.db.setprogresshandler(ph, -17) # SQLite doesn't complain about negative numbers
        self.db.setprogresshandler(ph, 20)
        c.execute("select max(x) from foo").next()

        self.assertNotEqual(phcalledcount[0], 0)
        saved=phcalledcount[0]

        # put an error in the progress handler
        def ph(): return 1/0
        self.db.setprogresshandler(ph, 1)
        self.assertRaises(ZeroDivisionError, c.execute, "update foo set x=-10")
        self.db.setprogresshandler(None) # clear ph so next line runs
        # none should have taken
        self.failUnlessEqual(0, c.execute("select count(*) from foo where x=-10").next()[0])
        # and previous ph should not have been called
        self.failUnlessEqual(saved, phcalledcount[0])
                             

    def testChanges(self):
        "Verify reporting of changes"
        c=self.db.cursor()
        c.execute("create table foo (x);begin")
        for i in xrange(100):
            c.execute("insert into foo values(?)", (i+1000,))
        c.execute("commit")
        c.execute("update foo set x=0 where x>=1000")
        self.failUnlessEqual(100, self.db.changes())
        c.execute("begin")
        for i in xrange(100):
            c.execute("insert into foo values(?)", (i+1000,))
        c.execute("commit")
        self.failUnlessEqual(300, self.db.totalchanges())

    def testLastInsertRowId(self):
        "Check last insert row id"
        c=self.db.cursor()
        c.execute("create table foo (x integer primary key)")
        for i in range(10):
            c.execute("insert into foo values(?)", (i,))
            self.failUnlessEqual(i, self.db.last_insert_rowid())
        # get a 64 bit value
        v=2**40
        c.execute("insert into foo values(?)", (v,))
        self.failUnlessEqual(v, self.db.last_insert_rowid())

    def testComplete(self):
        "Completeness of SQL statement checking"
        # the actual underlying routine just checks that there is a semi-colon
        # at the end, not inside any quotes etc
        self.failUnlessEqual(False, self.db.complete("select * from"))
        self.failUnlessEqual(False, self.db.complete("select * from \";\""))
        self.failUnlessEqual(False, self.db.complete("select * from \";"))
        self.failUnlessEqual(True, self.db.complete("select * from foo; select *;"))
        self.failUnlessEqual(False, self.db.complete("select * from foo where x=1"))
        self.failUnlessEqual(True, self.db.complete("select * from foo;"))

    def testBusyHandling(self):
        "Verify busy handling"
        c=self.db.cursor()
        c.execute("create table foo(x); begin")
        c.executemany("insert into foo values(?)", randomintegers(400))
        c.execute("commit")
        # verify it is blocked
        db2=apsw.Connection("testdb")
        c2=db2.cursor()
        c2.execute("begin exclusive")
        self.assertRaises(apsw.BusyError, c.execute, "begin immediate ; select * from foo")

        # close and reopen databases - sqlite will return Busy immediately to a connection
        # it previously returned busy to
        del c
        del c2
        del db2
        del self.db
        self.db=apsw.Connection("testdb")
        db2=apsw.Connection("testdb")
        c=self.db.cursor()
        c2=db2.cursor()
        
        # Put in busy handler
        bhcalled=[0]
        def bh(*args):
            bhcalled[0]=bhcalled[0]+1
            if bhcalled[0]==4:
                return False
            return True
        self.assertRaises(TypeError, db2.setbusyhandler, 12) # must be callable
        self.assertRaises(TypeError, db2.setbusytimeout, "12") # must be int
        db2.setbusytimeout(-77)  # SQLite doesn't complain about negative numbers, but if it ever does this will catch it
        self.assertRaises(TypeError, db2.setbusytimeout, 77,88) # too many args
        self.db.setbusyhandler(bh)

        c2.execute("begin exclusive")
        
        try:
            for row in c.execute("begin immediate ; select * from foo"):
                print row
        except apsw.BusyError:
            pass
        self.failUnlessEqual(bhcalled[0], 4)

        # Close and reopen again
        del c
        del c2
        del db2
        del self.db
        self.db=apsw.Connection("testdb")
        db2=apsw.Connection("testdb")
        c=self.db.cursor()
        c2=db2.cursor()
        
        # Put in busy timeout
        c2.execute("begin exclusive")
        self.assertRaises(TypeError, self.db.setbusyhandler, "foo")
        self.db.setbusytimeout(5000)
        b4=time.time()
        try:
            c.execute("begin immediate ; select * from foo")
        except apsw.BusyError:
            pass
        after=time.time()
        self.failUnless(after-b4>=5.0)

    def testInterruptHandling(self):
        "Verify interrupt function"
        # this is tested by having a user defined function make the interrupt
        c=self.db.cursor()
        c.execute("create table foo(x);begin")
        c.executemany("insert into foo values(?)", randomintegers(400))
        c.execute("commit")
        def ih(*args):
            self.db.interrupt()
            return 7
        self.db.createscalarfunction("seven", ih)
        try:
            for row in c.execute("select seven(x) from foo"):
                pass
        except apsw.InterruptError:
            pass

    def testCommitHook(self):
        "Verify commit hooks"
        c=self.db.cursor()
        c.execute("create table foo(x)")
        c.executemany("insert into foo values(?)", randomintegers(10))
        chcalled=[0]
        def ch():
            chcalled[0]=chcalled[0]+1
            if chcalled[0]==4:
                return 1 # abort
            return 0 # continue
        self.assertRaises(TypeError, self.db.setcommithook, 12)  # not callable
        self.db.setcommithook(ch)
        self.assertRaises(apsw.ConstraintError, c.executemany, "insert into foo values(?)", randomintegers(10))
        self.assertEqual(4, chcalled[0])
        self.db.setcommithook(None)
        def ch():
            chcalled[0]=99
            return 1
        self.db.setcommithook(ch)
        self.assertRaises(apsw.ConstraintError, c.executemany, "insert into foo values(?)", randomintegers(10))
        # verify it was the second one that was called
        self.assertEqual(99, chcalled[0])
        # error in commit hook
        def ch():
            return 1/0
        self.db.setcommithook(ch)
        self.assertRaises(ZeroDivisionError, c.execute, "insert into foo values(?)", (1,))

    def testRollbackHook(self):
        "Verify rollback hooks"
        c=self.db.cursor()
        c.execute("create table foo(x)")
        rhcalled=[0]
        def rh():
            rhcalled[0]=rhcalled[0]+1
            return 1
        self.assertRaises(TypeError, self.db.setrollbackhook, 12) # must be callable
        self.db.setrollbackhook(rh)
        c.execute("begin ; insert into foo values(10); rollback")
        self.assertEqual(1, rhcalled[0])
        self.db.setrollbackhook(None)
        c.execute("begin ; insert into foo values(10); rollback")
        self.assertEqual(1, rhcalled[0])
        def rh():
            1/0
        self.db.setrollbackhook(rh)
        # SQLite doesn't allow reporting an error from a rollback hook, so it will be seen
        # in the next command (eg the select in this case)
        self.assertRaises(ZeroDivisionError, c.execute, "begin ; insert into foo values(10); rollback; select * from foo")
        # check cursor still works
        for row in c.execute("select * from foo"):
            pass

    def testUpdateHook(self):
        "Verify update hooks"
        c=self.db.cursor()
        c.execute("create table foo(x integer primary key, y)")
        uhcalled=[]
        def uh(type, databasename, tablename, rowid):
            uhcalled.append( (type, databasename, tablename, rowid) )
        self.assertRaises(TypeError, self.db.setupdatehook, 12) # must be callable
        self.db.setupdatehook(uh)
        statements=(
            ("insert into foo values(3,4)", (apsw.SQLITE_INSERT, 3) ),
            ("insert into foo values(30,40)", (apsw.SQLITE_INSERT, 30) ),
            ("update foo set y=47 where x=3", (apsw.SQLITE_UPDATE, 3), ),
            ("delete from foo where y=47", (apsw.SQLITE_DELETE, 3), ),
            )
        for sql,res in statements:
            c.execute(sql)
        results=[(type, "main", "foo", rowid) for sql,(type,rowid) in statements]
        self.assertEqual(uhcalled, results)
        self.db.setupdatehook(None)
        c.execute("insert into foo values(99,99)")
        self.assertEqual(len(uhcalled), len(statements)) # length should have remained the same
        def uh(*args):
            1/0
        self.db.setupdatehook(uh)
        self.assertRaises(ZeroDivisionError, c.execute, "insert into foo values(100,100)")
        self.db.setupdatehook(None)
        # check cursor still works
        c.execute("insert into foo values(1000,1000)")
        self.assertEqual(1, c.execute("select count(*) from foo where x=1000").next()[0])

    def testProfile(self):
        "Verify profiling"
        # we do the test by looking for the maximum of 100,000 random
        # numbers with an index present and without.  The former
        # should be way quicker.
        c=self.db.cursor()
        c.execute("create table foo(x); begin")
        c.executemany("insert into foo values(?)", randomintegers(100000))
        profileinfo=[]
        def profile(statement, timing):
            profileinfo.append( (statement, timing) )
        c.execute("commit; create index foo_x on foo(x)")
        self.assertRaises(TypeError, self.db.setprofile, 12) # must be callable
        self.db.setprofile(profile)
        for val1 in c.execute("select max(x) from foo"): pass # profile is only run when results are exhausted
        self.db.setprofile(None)
        c.execute("drop index foo_x")
        self.db.setprofile(profile)
        for val2 in c.execute("select max(x) from foo"): pass
        self.failUnlessEqual(val1, val2)
        self.failUnlessEqual(len(profileinfo), 2)
        self.failUnlessEqual(profileinfo[0][0], profileinfo[1][0])
        self.failUnlessEqual("select max(x) from foo", profileinfo[0][0])
        # the query using the index should take way less time
        self.failUnless(profileinfo[0][1]<profileinfo[1][1])
        def profile(*args):
            1/0
        self.db.setprofile(profile)
        self.assertRaises(ZeroDivisionError, c.execute, "create table bar(y)")

    def testThreading(self):
        "Verify threading behaviour"
        c=self.db.cursor()
        c.execute("create table foo(x,y); insert into foo values(99,100); insert into foo values(101,102)")
        ### Check operations on Connection cause error if executed in seperate thread
        # these should execute fine in any thread
        ThreadRunner(apsw.sqlitelibversion).go()
        ThreadRunner(apsw.apswversion).go()
        # these should generate errors
        nargs={ # number of args for function.  those not listed take zero
            'createaggregatefunction': 2,
            'complete': 1,
            'createcollation': 2,
            'createscalarfunction': 2,
            'setauthorizer': 1,
            'setbusyhandler': 1,
            'setbusytimeout': 1,
            'setcommithook': 1,
            'setprofile': 1,
            'setrollbackhook': 1,
            'setupdatehook': 1,
            'setprogresshandler': 2,
            }
        for func in [x for x in dir(self.db) if not x.startswith("__")]:
            args=("one", "two", "three")[:nargs.get(func,0)]
            try:
                tr=ThreadRunner(getattr(self.db, func), *args)
		tr.go()
            except apsw.ThreadingViolationError:
                pass

        # do the same thing, but for cursor
        nargs={
            'execute': 1,
            'executemany': 1,
            'setexectrace': 1,
            'setrowtrace': 1,
            }
        for func in [x for x in dir(c) if not x.startswith("__")]:
            args=("one", "two", "three")[:nargs.get(func,0)]
            try:
                tr=ThreadRunner(getattr(c, func), *args)
		tr.go()
            except apsw.ThreadingViolationError:
                pass

        # check cursor still works
        for row in c.execute("select * from foo"):
            pass
        del c
	# Do another query in a different thread
	def threadcheck():
	   db=apsw.Connection("testdb")
	   c=db.cursor()
	   return c.execute("select count(*) from foo").next()[0]
	tr=ThreadRunner(threadcheck)
	self.failUnlessEqual(2, tr.go())
        self.db=None
        if False:
            # execute destructor in wrong thread - this is quite difficult to arrange!
            self.db=apsw.Connection("testdb")
            def threadcheck():
                print "here"
                del self.db # python goes into infinite loop here repeatedly running destructor
                print "here2"
                raw_input("...")
            tr=ThreadRunner(threadcheck)
            tr.go()
            self.db=None

    def testSharedCache(self):
        "Verify setting of shared cache"

        ## Several of these should be failing but SQLite isn't
        ## returning an error code as the documentation claims it
        ## will.
        
        # since self.db already exists, we should get error
        db2=apsw.Connection("testdb")
        c=self.db.cursor()
        c2=db2.cursor()
        c.execute("create table foo(x)")
        apsw.enablesharedcache(True) # should fail
        c.executemany("insert into foo values(?)", randomintegers(10))
        c.execute("select * from foo")
        c2.execute("select * from foo")
        apsw.enablesharedcache(False) # should fail
        c.next()
        apsw.enablesharedcache(True) # should fail
        del self.db
        del c
        del db2
        del c2
        # this one should work
        apsw.enablesharedcache(True) # should work
        self.db=None

    def testTracebacks(self):
        "Verify augmented tracebacks"
        return
        def badfunc(*args):
            1/0
        self.db.createscalarfunction("badfunc", badfunc)
        try:
            c=self.db.cursor()
            c.execute("select badfunc()")
            self.fail("Exception should have occurred")
        except ZeroDivisionError:
            tb=sys.exc_info()[2]
            traceback.print_tb(tb)
            del tb
        except:
            self.fail("Wrong exception type")
            


if __name__=='__main__':
    unittest.main()
