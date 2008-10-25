.. _benchmarking:

Compatibility and Benchmarking
==============================


nested transactions

also add info about speedtest 

  <h1><a name="SQLiteVersionCompatibilityAndBenchmarking" id="SQLiteVersionCompatibilityAndBenchmarking">SQLite version
  compatibility and benchmarking</a></h1>

  <p>APSW binds to the C interface of SQLite. That interface is stable for each major version of SQLite (ie the C
  interface for any SQLite 3.x is stable, but SQLite 4.x would be an incompatible change). Consequently you can use
  APSW against any revision of SQLite with the same major version number. There are small enhancements to the C api
  over time, and APSW adds support for them as appropriate. The version number of APSW covers the version these
  enhancements were added. The vast majority of changes to SQLite are in the SQL syntax and engine. Those will be
  picked up with any version of APSW. The one exception to this is experimental features in SQLite which may change API
  between revisions. Consequently you will need to turn them off if you want to work against a variety of versions of
  SQLite (see EXPERIMENTAL in <code>setup.py</code>). I strongly recommend you embed the SQLite library into APSW which
  will put you in control of versions.</p>

  <p>Before you do any benchmarking with APSW or other ways of accessing SQLite, you must understand how and when
  SQLite does transactions. See section 7.0, <i>Transaction Control At The SQL Level</i> of <a href=
  "http://sqlite.org/lockingv3.html">sqlite.org/lockingv3.html</a>. <b>APSW does not alter SQLite's behaviour with
  transactions.</b> Some access layers try to interpret your SQL and manage transactions behind your back, which may or
  may not work well with SQLite also doing its own transactions. You should always manage your transactions yourself.
  For example to insert 1,000 rows wrap it in a single transaction else you will have 1,000 transactions. The best clue
  that you have one transaction per statement is having a maximum of 60 statements per second. You need two drive
  rotations to do a transaction - the data has to be committed to the main file and the journal - and 7200 RPM drives
  do 120 rotations a second. On the other hand if you don't put in the transaction boundaries yourself and get more
  than 60 statements a second, then your access mechanism is silently starting transactions for you. This topic also
  comes up fairly frequently in the SQLite mailing list archives.</p>

