.. _benchmarking:

Benchmarking
============

Before you do any benchmarking with APSW or other ways of accessing
SQLite, you must understand how and when SQLite does transactions. See
`transaction control
<https://sqlite.org/lockingv3.html#transaction_control>`_.  **APSW does
not alter SQLite's behaviour with transactions.**

Some access layers try to interpret your SQL and manage transactions
behind your back, which may or may not work well with SQLite also
doing its own transactions. You should always manage your transactions
yourself.  For example to insert 1,000 rows wrap it in a single
transaction, otherwise you will have 1,000 transactions, one per row.
A spinning hard drive can't do more than 60 transactions per second.


.. _speedtest:

speedtest
---------

APSW includes a speed tester to compare SQLite performance across
different versions of SQLite, different host systems (hard drives and
controllers matter) as well as between sqlite3 and APSW.  The
underlying queries are based on `SQLite's speed test
<https://sqlite.org/src/file?name=tool/mkspeedsql.tcl>`_.

.. speedtest-begin

.. code-block:: text

    $ python3 -m apsw.speedtest --help
    Usage: apsw.speedtest [options]
    
    Options:
      -h, --help           show this help message and exit
      --apsw               Include apsw in testing (False)
      --sqlite3            Include sqlite3 module in testing (False)
      --correctness        Do a correctness test
      --scale=SCALE        How many statements to execute.  Each unit takes about
                           2 seconds per test on memory only databases. [Default
                           10]
      --database=DATABASE  The database file to use [Default :memory:]
      --tests=TESTS        What tests to run [Default
                           bigstmt,statements,statements_nobindings]
      --iterations=N       How many times to run the tests [Default 4]
      --tests-detail       Print details of what the tests do.  (Does not run the
                           tests)
      --dump-sql=FILENAME  Name of file to dump SQL to.  This is useful for
                           feeding into the SQLite command line shell.
      --sc-size=N          Size of the statement cache. APSW will disable cache
                           with value of zero.  sqlite3 ensures a minimum of 5
                           [Default 100]
      --unicode=UNICODE    Percentage of text that is unicode characters [Default
                           0]
      --data-size=SIZE     Maximum size in characters of data items - keep this
                           number small unless you are on 64 bits and have lots of
                           memory with a small scale - you can easily consume
                           multiple gigabytes [Default same as original TCL
                           speedtest]
    

    $ python3 -m apsw.speedtest --tests-detail
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
        
    

.. speedtest-end