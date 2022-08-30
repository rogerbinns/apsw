#!/usr/bin/env python3
#
# See the accompanying LICENSE file.
#
# Do speed tests.  The tests try to correspond to
# https://sqlite.org/cvstrac/fileview?f=sqlite/tool/mkspeedsql.tcl
# Command line options etc were added later hence the
# somewhat weird structuring.

import sys
import os
import random
import time
import gc
import optparse

# Sigh
try:
    maxuni = 0x10ffff
    chr(maxuni)
except ValueError:
    maxuni = 0xffff

timerfn = time.process_time


def doit():
    random.seed(0)
    options.tests = [t.strip() for t in options.tests.split(",")]

    print("         Python", sys.executable, sys.version_info)
    print("          Scale", options.scale)
    print("       Database", options.database)
    print("          Tests", ", ".join(options.tests))
    print("     Iterations", options.iterations)
    print("Statement Cache", options.scsize)

    print("\n")
    if options.apsw:
        import apsw

        print("    Testing with APSW file ", apsw.__file__)
        print("              APSW version ", apsw.apswversion())
        print("        SQLite lib version ", apsw.sqlitelibversion())
        print("    SQLite headers version ", apsw.SQLITE_VERSION_NUMBER, end="\n\n")

        def apsw_setup(dbfile):
            con = apsw.Connection(dbfile, statementcachesize=options.scsize)
            con.createscalarfunction("number_name", number_name, 1)
            return con

    if options.sqlite3:
        import sqlite3

        print("Testing with sqlite3 file ", sqlite3.__file__)
        print("          sqlite3 version ", sqlite3.version)
        print("           SQLite version ", sqlite3.sqlite_version, end="\n\n")

        def sqlite3_setup(dbfile):
            con = sqlite3.connect(dbfile, isolation_level=None, cached_statements=options.scsize)
            con.create_function("number_name", 1, number_name)
            return con

    ones = ("zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven", "twelve",
            "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen", "nineteen")
    tens = ("", "ten", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety")

    others = ("thousand", "hundred", "zero")

    def _number_name(n):
        if n >= 1000:
            txt = "%s %s" % (_number_name(int(n / 1000)), others[0])
            n = n % 1000
        else:
            txt = ""

        if n >= 100:
            txt = txt + " " + ones[int(n / 100)] + " " + others[1]
            n = n % 100

        if n >= 20:
            txt = txt + " " + tens[int(n / 10)]
            n = n % 10

        if n > 0:
            txt = txt + " " + ones[n]

        txt = txt.strip()

        if txt == "":
            txt = others[2]

        return txt

    def unicodify(text):
        if options.unicode and len(text):
            newt = []
            c = options.unicode / 100.0
            for t in text:
                if random.random() > c:
                    newt.append(t)
                    continue
                while True:
                    t = random.randint(0xa1, maxuni)
                    # we don't want the surrogate range or apostrophe
                    if t < 0xd800 or t > 0xdfff: break
                newt.append(chr(t))
            text = "".join(newt)
        return text

    if options.unicode:
        ones = tuple([unicodify(s) for s in ones])
        tens = tuple([unicodify(s) for s in tens])
        others = tuple([unicodify(s) for s in others])

    def number_name(n):
        text = _number_name(n)
        if options.size:
            text = text * int(random.randint(0, options.size) / len(text))
        return text

    def getlines(scale=50, bindings=False):
        random.seed(0)

        # RogerB added two pragmas so that only memory is used.  This means that the
        # vagaries of disk access times don't alter the results

        # database schema
        for i in """PRAGMA page_size=1024;
      PRAGMA cache_size=8192;
      PRAGMA locking_mode=EXCLUSIVE;
      PRAGMA journal_mode = OFF;
      PRAGMA temp_store = MEMORY;
      CREATE TABLE t1(a INTEGER, b INTEGER, c TEXT);
      CREATE TABLE t2(a INTEGER, b INTEGER, c TEXT);
      CREATE INDEX i2a ON t2(a);
      CREATE INDEX i2b ON t2(b);
      SELECT name FROM sqlite_master ORDER BY 1""".split(";"):
            yield (i, )

        # 50,000 inserts on an unindexed table
        yield ("BEGIN", )
        for i in range(1, scale * 10000 + 1):
            r = random.randint(0, 500000)
            if bindings:
                yield ("INSERT INTO t1 VALUES(:1, :2, number_name(:2))", (i, r))
            else:
                yield ("INSERT INTO t1 VALUES(%d, %d, '%s')" % (i, r, number_name(r)), )
        yield ("COMMIT", )

        # 50,000 inserts on an indexed table
        t1c_list = []
        yield ("BEGIN", )
        for i in range(1, scale * 10000 + 1):
            r = random.randint(0, 500000)
            x = number_name(r)
            t1c_list.append(x)
            if bindings:
                yield ("INSERT INTO t2 VALUES(:1, :2, number_name(:2))", (i, r))
            else:
                yield ("INSERT INTO t2 VALUES(%d, %d, '%s')" % (i, r, x), )
        yield ("COMMIT", )

        # 50 SELECTs on an integer comparison.  There is no index so
        # a full table scan is required.
        for i in range(scale):
            yield ("SELECT count(*), avg(b) FROM t1 WHERE b>=%d AND b<%d" % (i * 100, (i + 10) * 100), )

        # 50 SELECTs on an LIKE comparison.  There is no index so a full
        # table scan is required.
        for i in range(scale):
            yield ("SELECT count(*), avg(b) FROM t1 WHERE c LIKE '%%%s%%'" % (number_name(i), ), )

        # Create indices
        yield ("BEGIN", )
        for i in """CREATE INDEX i1a ON t1(a);
                    CREATE INDEX i1b ON t1(b);
                    CREATE INDEX i1c ON t1(c);""".split(";"):
            yield (i, )
        yield ("COMMIT", )

        # 5000 SELECTs on an integer comparison where the integer is
        # indexed.
        for i in range(scale * 100):
            yield ("SELECT count(*), avg(b) FROM t1 WHERE b>=%d AND b<%d" % (i * 100, (i + 10) * 100), )

        # 100000 random SELECTs against rowid.
        for i in range(1, scale * 2000 + 1):
            yield ("SELECT c FROM t1 WHERE rowid=%d" % (1 + random.randint(0, 50000), ), )

        # 100000 random SELECTs against a unique indexed column.
        for i in range(1, scale * 2000 + 1):
            yield ("SELECT c FROM t1 WHERE a=%d" % (1 + random.randint(0, 50000), ), )

        # 50000 random SELECTs against an indexed column text column
        for i in range(scale * 1000):
            if bindings:
                yield (
                    "SELECT c FROM t1 WHERE c=?",
                    (random.choice(t1c_list), ),
                )
            else:
                yield ("SELECT c FROM t1 WHERE c='%s'" % (random.choice(t1c_list), ), )

        # Vacuum
        if options.database != ":memory:":
            # opens a disk file
            yield ("VACUUM", )

        # 5000 updates of ranges where the field being compared is indexed.
        yield ("BEGIN", )
        for i in range(scale * 100):
            yield ("UPDATE t1 SET b=b*2 WHERE a>=%d AND a<%d" % (i * 2, (i + 1) * 2), )
        yield ("COMMIT", )

        # 50000 single-row updates.  An index is used to find the row quickly.
        yield ("BEGIN", )
        for i in range(scale * 1000):
            if bindings:
                yield ("UPDATE t1 SET b=? WHERE a=%d" % (i, ), (random.randint(0, 500000), ))
            else:
                yield ("UPDATE t1 SET b=%d WHERE a=%d" % (random.randint(0, 500000), i), )
        yield ("COMMIT", )

        # 1 big text update that touches every row in the table.
        yield ("UPDATE t1 SET c=a", )

        # Many individual text updates.  Each row in the table is
        # touched through an index.
        yield ("BEGIN", )
        for i in range(1, scale * 1000 + 1):
            if bindings:
                yield ("UPDATE t1 SET c=? WHERE a=%d" % (i, ), (number_name(random.randint(0, 500000)), ))
            else:
                yield ("UPDATE t1 SET c='%s' WHERE a=%d" % (number_name(random.randint(0, 500000)), i), )
        yield ("COMMIT", )

        # Delete all content in a table.
        yield ("DELETE FROM t1", )

        # Copy one table into another
        yield ("INSERT INTO t1 SELECT * FROM t2", )

        # Delete all content in a table, one row at a time.
        yield ("DELETE FROM t1 WHERE 1", )

        # Refill the table yet again
        yield ("INSERT INTO t1 SELECT * FROM t2", )

        # Drop the table and recreate it without its indices.
        yield ("BEGIN", )
        yield ("DROP TABLE t1", )
        yield ("CREATE TABLE t1(a INTEGER, b INTEGER, c TEXT)", )
        yield ("COMMIT", )

        # Refill the table yet again.  This copy should be faster because
        # there are no indices to deal with.
        yield ("INSERT INTO t1 SELECT * FROM t2", )

        # The three following used "ORDER BY random()" but we can't do that
        # as it causes each run to have different values, and hence different
        # amounts of sorting that have to go on.  The "random()" has been
        # replaced by "c", the column that has the stringified number

        # Select 20000 rows from the table at random.
        yield ("SELECT rowid FROM t1 ORDER BY c LIMIT %d" % (scale * 400, ), )

        # Delete 20000 random rows from the table.
        yield ("""  DELETE FROM t1 WHERE rowid IN
                         (SELECT rowid FROM t1 ORDER BY c LIMIT %d)""" % (scale * 400, ), )

        yield ("SELECT count(*) FROM t1", )

        # Delete 20000 more rows at random from the table.
        yield ("""DELETE FROM t1 WHERE rowid IN
                     (SELECT rowid FROM t1 ORDER BY c LIMIT %d)""" % (scale * 400, ), )

        yield ("SELECT count(*) FROM t1", )

    # Do a correctness test first
    if options.correctness:
        print("Correctness test\n")
        if 'bigstmt' in options.tests:
            text = ";\n".join([x[0] for x in getlines(scale=1)]) + ";"
        if 'statements' in options.tests:
            withbindings = [line for line in getlines(scale=1, bindings=True)]
        if 'statements_nobindings' in options.tests:
            withoutbindings = [line for line in getlines(scale=1, bindings=False)]

        res = {}
        for driver in ('apsw', 'sqlite3'):
            if not getattr(options, driver):
                continue

            for test in options.tests:
                name = driver + "_" + test

                print(name + '\t')
                sys.stdout.flush()

                if name == 'sqlite3_bigstmt':
                    print('limited functionality (ignoring)\n')
                    continue

                con = locals().get(driver + "_setup")(":memory:")  # we always correctness test on memory

                if test == 'bigstmt':
                    cursor = con.cursor()
                    if driver == 'apsw':
                        func = cursor.execute
                    else:
                        func = cursor.executescript

                    res[name] = [row for row in func(text)]
                    print(str(len(res[name])) + "\n")
                    continue

                cursor = con.cursor()
                if test == 'statements':
                    sql = withbindings
                elif test == 'statements_nobindings':
                    sql = withoutbindings

                l = []
                for s in sql:
                    for row in cursor.execute(*s):
                        l.append(row)

                res[name] = l
                print(str(len(res[name])) + "\n")

        # All elements of res should be identical
        elements = sorted(res.keys())
        for i in range(0, len(elements) - 1):
            print("%s == %s %s\n" % (elements[i], elements[i + 1], res[elements[i]] == res[elements[i + 1]]))

        del res
    text = None
    withbindings = None
    withoutbindings = None

    if options.dump_filename or "bigstmt" in options.tests:
        text = ";\n".join([x[0] for x in getlines(scale=options.scale)]) + ";"  # sqlite3 requires final semicolon
        if options.dump_filename:
            open(options.dump_filename, "wt", encoding="utf8").write(text)
            sys.exit(0)

    if "statements" in options.tests:
        withbindings = list(getlines(scale=options.scale, bindings=True))

    if "statements_nobindings" in options.tests:
        withoutbindings = list(getlines(scale=options.scale, bindings=False))

    # Each test returns the amount of time taken.  Note that we include
    # the close time as well.  Otherwise the numbers become a function of
    # cache and other collection sizes as freeing members gets deferred to
    # close time.

    def apsw_bigstmt(con):
        "APSW big statement"
        try:
            for row in con.execute(text):
                pass
        except:
            import pdb
            pdb.set_trace()
            pass

    def sqlite3_bigstmt(con):
        "sqlite3 big statement"
        for row in con.executescript(text):
            pass

    def apsw_statements(con, bindings=withbindings):
        "APSW individual statements with bindings"
        cursor = con.cursor()
        for b in bindings:
            for row in cursor.execute(*b):
                pass

    def sqlite3_statements(con, bindings=withbindings):
        "sqlite3 individual statements with bindings"
        cursor = con.cursor()
        for b in bindings:
            for row in cursor.execute(*b):
                pass

    def apsw_statements_nobindings(con):
        "APSW individual statements without bindings"
        return apsw_statements(con, withoutbindings)

    def sqlite3_statements_nobindings(con):
        "sqlite3 individual statements without bindings"
        return sqlite3_statements(con, withoutbindings)

    # Do the work
    print("\nRunning tests - elapsed, CPU (results in seconds, lower is better)\n")

    for i in range(options.iterations):
        print("%d/%d" % (i + 1, options.iterations))
        for test in options.tests:
            # funky stuff is to alternate order each round
            for driver in (("apsw", "sqlite3"), ("sqlite3", "apsw"))[i % 2]:
                if getattr(options, driver):
                    name = driver + "_" + test
                    func = locals().get(name, None)
                    if not func:
                        sys.exit("No such test " + name + "\n")

                    if os.path.exists(options.database):
                        os.remove(options.database)
                    print("\t" + func.__name__ + (" " * (40 - len(func.__name__))), end="")
                    con = locals().get(driver + "_setup")(options.database)
                    gc.collect(2)
                    b4cpu = timerfn()
                    b4 = time.time()
                    func(con)
                    con.close()  # see note above as to why we include this in the timing
                    gc.collect(2)
                    after = time.time()
                    aftercpu = timerfn()
                    print("%0.3f %0.3f" % (after - b4, aftercpu - b4cpu))


parser = optparse.OptionParser()
parser.add_option("--apsw", dest="apsw", action="store_true", default=False, help="Include apsw in testing (%default)")
parser.add_option("--sqlite3", action="store_true", default=False, help="Include sqlite3 module in testing (%default)")
parser.add_option("--correctness", dest="correctness", action="store_true", default=False, help="Do a correctness test")
parser.add_option(
    "--scale",
    dest="scale",
    type="int",
    default=10,
    help=
    "How many statements to execute.  Each unit takes about 2 seconds per test on memory only databases. [Default %default]"
)
parser.add_option("--database", dest="database", default=":memory:", help="The database file to use [Default %default]")
parser.add_option("--tests",
                  dest="tests",
                  default="bigstmt,statements,statements_nobindings",
                  help="What tests to run [Default %default]")
parser.add_option("--iterations",
                  dest="iterations",
                  default=4,
                  type="int",
                  metavar="N",
                  help="How many times to run the tests [Default %default]")
parser.add_option("--tests-detail",
                  dest="tests_detail",
                  default=False,
                  action="store_true",
                  help="Print details of what the tests do.  (Does not run the tests)")
parser.add_option("--dump-sql",
                  dest="dump_filename",
                  metavar="FILENAME",
                  help="Name of file to dump SQL to.  This is useful for feeding into the SQLite command line shell.")
parser.add_option(
    "--sc-size",
    dest="scsize",
    type="int",
    default=100,
    metavar="N",
    help=
    "Size of the statement cache. APSW will disable cache with value of zero.  sqlite3 ensures a minimum of 5 [Default %default]"
)
parser.add_option("--unicode",
                  dest="unicode",
                  type="int",
                  default=0,
                  help="Percentage of text that is non-ascii unicode characters [Default %default]")
parser.add_option(
    "--data-size",
    dest="size",
    type="int",
    default=0,
    metavar="SIZE",
    help=
    "Maximum size in characters of data items - keep this number small unless you are on 64 bits and have lots of memory with a small scale - you can easily consume multiple gigabytes [Default same as original TCL speedtest]"
)

tests_detail = """\
bigstmt:

  Supplies the SQL as a single string consisting of multiple
  statements.  apsw handles this normally via cursor.execute while
  sqlite3 requires that cursor.executescript is called.  The string
  will be several kilobytes and with a factor of 50 will be in the
  megabyte range.  This is the kind of query you would run if you were
  restoring a database from a dump.  (Note that sqlite3 silently
  ignores returned data which also makes it execute faster).

statements:

  Runs the SQL queries but uses bindings (? parameters). eg::

    for i in range(3):
       cursor.execute("insert into table foo values(?)", (i,))

  This test has many hits of the statement cache.

statements_nobindings:

  Runs the SQL queries but doesn't use bindings. eg::

    cursor.execute("insert into table foo values(0)")
    cursor.execute("insert into table foo values(1)")
    cursor.execute("insert into table foo values(2)")

  This test has no statement cache hits and shows the overhead of
       having a statement cache.

  In theory all the tests above should run in almost identical time
  as well as when using the SQLite command line shell.  This tool
  shows you what happens in practise.
    \n"""

if __name__ == "__main__":
    options, args = parser.parse_args()

    if len(args):
        parser.error("Unexpected arguments " + str(args))

    if options.tests_detail:
        print(tests_detail)
        sys.exit(0)

    if not options.apsw and not options.sqlite3 and not options.dump_filename:
        parser.error("You should select at least one of --apsw or --sqlite3")

    doit()
