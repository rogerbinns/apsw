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
    usage: apsw.speedtest [-h] [--apsw] [--sqlite3] [--correctness]
                          [--scale SCALE] [--database DATABASE] [--tests TESTS]
                          [--iterations N] [--tests-detail] [--dump-sql FILENAME]
                          [--sc-size N] [--unicode UNICODE] [--data-size SIZE]
                          [--hide-runs]
    
    Tests performance of apsw and sqlite3 packages
    
    options:
      -h, --help           show this help message and exit
      --apsw               Include apsw in testing [False]
      --sqlite3            Include sqlite3 module in testing [False]
      --correctness        Do a correctness test
      --scale SCALE        How many statements to execute. Each 5 units takes
                           about 1 second per test on memory only databases. [10]
      --database DATABASE  The database file to use [:memory:]
      --tests TESTS        What tests to run
                           [bigstmt,statements,statements_nobindings]
      --iterations N       How many times to run the tests [4]
      --tests-detail       Print details of what the tests do. (Does not run the
                           tests)
      --dump-sql FILENAME  Name of file to dump SQL to. This is useful for feeding
                           into the SQLite command line shell.
      --sc-size N          Size of the statement cache. [128]
      --unicode UNICODE    Percentage of text that is non-ascii unicode characters
                           [0]
      --data-size SIZE     Duplicate the ~50 byte text column value up to this
                           many times (amount randomly selected per row)
      --hide-runs          Don't show the individual iteration timings, only final
                           summary
    

    $ python3 -m apsw.speedtest --tests-detail
    bigstmt:
    
      Supplies the SQL as a single string consisting of multiple
      statements.  apsw handles this normally via cursor.execute while
      sqlite3 requires that cursor.executescript is called.  The string
      will be several kilobytes and with a scale of 50 will be in the
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