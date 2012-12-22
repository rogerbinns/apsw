.. _couchdb:

CouchDB
*******

.. currentmodule:: apsw

`CouchDB <http://couchdb.apache.org>`__ is an increasingly popular
document oriented database, also known as a schema-less database.  It
is also web based using `HTTP
<http://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol>`__ for
access and `JSON <http://json.org/>`__ for data representation.

The APSW source distribution now includes a :ref:`virtual table
implementation <virtualtables>` that lets you access CouchDB databases
from SQLite, including both read and write access.

Some suggested uses:

 * Loading data from SQLite into CouchDB and vice versa.  For example
   you can import CSV data into SQLite and then upload it to CouchDB.

 * Being able to use SQLite based tools for working on CouchDB data.

 * Being able to do `joins
   <http://en.wikipedia.org/wiki/Join_%28SQL%29>`__ between data in
   SQLite and CouchDB.

 * Using the SQLite :ref:`FTS full text search extension <ext-fts3>`
   for data in CouchDB.

 * Using the SQLite :ref:`RTree extension <ext-rtree>` to do spatial
   queries.

Getting it
==========

You can find the code in the :ref:`source distribution
<source_and_binaries>` named :file:`apswcouchdb.py`, or you can get a
copy directly from `source control
<https://code.google.com/p/apsw/source/browse/tools/apswcouchdb.py>`__
(choose "View Raw File").

You will need to have the `Python couchdb module
<https://code.google.com/p/couchdb-python/>`__ installed as well as its
prerequisites.

Usage
=====

To use with your code, place the :file:`apswcouchdb.py` file anywhere
you can import it.  To use it with the APSW :ref:`shell` use the the
``.read`` command specifying the filename.  The virtual table will be
automatically installed via the :attr:`connection_hooks` mechanism and
registering with the shell if appropriate.

A virtual table maps to a CouchDB database.  The database needs to
exist already.  Use the following SQL to create the virtual table::

  create virtual table mytable using couchdb('http://localhost:5984', 'dbname', col1, col2, col3);

From that point on you can do regular SQL operations against *mytable*
where each row will correspond to a document from *dbname*.  When you
drop the table it does not delete the CouchDB database.  If you need
usernames and passwords then specify them as part of the url::

  http://username:password@localhost:5984

Examples
========

Importing CSV data
------------------

We want to import some CSV data to a CouchDB database.  The first step
is to import it into a temporary SQLite table.  It could be imported
directly to CouchDB but then we wouldn't have the ability to specify
`column affinity <https://sqlite.org/datatype3.html>`__ and all
data would end up as strings in CouchDB.  I am using a real estate CSV
file in this example against the :ref:`SQLite shell <shell>`::

     -- Specify affinities in temporary table so numbers from
     -- the CSV end up as numbers and not strings
     create temporary table refixup(street char, city char,
         zip char, state char, beds int, baths int, sqft real,
         type char, sale_date char, price int, latitude real,
         longitude real);

     -- Do the actual import
     .mode csv
     .import realestatetransactions.csv refixup

     -- Create the CouchDB virtual table with the same column names
     create virtual table realestate using couchdb('http://localhost:5984',
         realestate, street, city, zip, state, beds, baths,
         sqft, type, sale_date, price, latitude, longitude);

     -- Copy the data from the temporary table to CouchDB
     insert into realestate select * from refixup;

     -- No longer need the temporary table
     drop table refixup;

Use ``.help import`` for more hints on importing data.

Using FTS
---------

In this example we have CouchDB documents that are recipes.  The
CouchDB _id field is the recipe name::

     -- Virtual table
     create virtual table recipes using couchdb('http://localhost:5984',
        _id, ingredients);

     -- FTS table
     create virtual table recipesearch using fts3(name, ingredients);

     -- Copy the data from CouchDB to FTS3
     insert into recipesearch(name, ingredients)
        select _id,ingredients from recipes;

     -- Which ones have these ingredients
     select name as _id from recipesearch where ingredients
         MATCH 'onions cheese';

Implementation Notes
====================

Automatic column names

  If you don't want to manually specify all columns then you can use
  '+' as a column and the first 1,000 documents in the database will
  be examined to see what keys they have.  Any that you have not
  already specified as a column will be added to the column list in
  alphabetical order.  Only keys without a leading underscore are
  considered::

    create virtual table mytable using couchdb('http://localhost:5984', 'dbname', '+');

Document Ids and Rowids

  CouchDB uses the document id (key _id) as the unique identifier.
  SQLite uses a 64 bit integer rowid.  In order to map the two a
  `temporary table <https://sqlite.org/lang_createtable.html>`__ is
  used behind the scenes. The revision (_rev) is also stored. SQLite
  stores temporary tables separately and discards them when the
  corresponding database is closed.

  By default the temporary tables are stored in a file.  You can use a
  `pragma <https://sqlite.org/pragma.html#pragma_temp_store>`__ to
  change that.  For example ``pragma temp_store=memory`` will use
  memory instead.  My 200,000 document test database resulted in a
  temporary mapping table of 50MB if I accessed all rows/documents.

Scalability

  It is intended that you can use the virtual table with large
  databases.  For example development and profiling were done with a
  200,000 document database using over 2GB of storage.

  This means that behind the scenes the CouchDB `bulk API
  <http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API>`__ is used.
  (Doing an HTTP request per row/document read or written would be far
  too slow.)  Pending updates (adds, changes, deletions) are batched
  up and sent as a group which means there is a delay between when
  your SQL executes and the CouchDB server taking the appropriate
  action.  This can also lead to a delay in error reporting.

  By default documents are read and written in batches of 5,000.  The
  larger the number the fewer HTTP requests are made but more memory
  is consumed. You can change this number like this::

    select couchdb_config('read-batch', 2000);
    select couchdb_config('write-batch', 10000);

  If you are debugging code then setting them to 1 will cause an
  immediate HTTP request per row/document read or written rather than
  waiting till the batch is full or for a transaction boundary.

Updates

  SQLite (and SQL) define a fixed list of columns for each row while
  CouchDB can have zero or more keys per document.  In normal use of
  this module you would have listed a subset of the possible keys as
  columns for SQLite.  CouchDB does an update by supplying a complete
  new document.  If an update specified only the keys/columns declared
  at the SQLite level then other keys would be lost.  Consequently on
  each update this module has to obtain the document and update the
  fields specified via SQL.  Obtaining these documents as needed one
  at a time slows down updates, but does prevent them from losing any
  fields not known to SQL.  If you have specified all the fields in
  SQL then you can off this behaviour saving one HTTP request per
  document updated::

    select couchdb_config('deep-update', 0);

None/null/undefined

  In SQL null means a value is not present.  No two nulls are equal to
  each other plus `other quirks <https://sqlite.org/nulls.html>`__.
  In Python None is a value more like a non-type specific zero
  although it is a singleton.  Javascript has both undefined with a
  SQL null like meaning and null with Python None like meaning.  JSON
  can represent null but not undefined.

  Whenever a key is not present in a document but a value is required
  by SQLite then null is returned.  When creating or updating
  documents, a value of null from SQLite is treated as meaning you do
  not want the key in the document.  (Reading it back will still get
  you a null in SQLite even though technically the document value is
  undefined.)  This means that you cannot use this module to set a
  value in a CouchDB document to null - the key will not be present.

Transactions

  SQLite has the same transaction boundaries as SQL and supports
  transactions.  CouchDB is atomic but does not support transactions.
  Any outstanding batched updates are also flushed on SQLite
  transaction boundaries as well as other points such as when a cursor
  starts from the beginning again.  This means that a rollback can only
  discard unflushed pending updates but not undo earlier updates
  within the SQL transaction.

Types

  Communication between this module and CouchDB uses JSON over HTTP.
  All the JSON types map to Python fundamental types, but only to a
  subset of SQLite types.

  +------------+----------------+--------------+
  | JSON       |  Python        | SQLite       |
  +============+================+==============+
  | Float      | double         | Real         |
  +------------+----------------+--------------+
  | Integer    | int (unlimited)| int (max 64  |
  | (Actually  |                | bit)         |
  | stored as  |                |              |
  | a float)   |                |              |
  +------------+----------------+--------------+
  | String     | unicode        | char         |
  +------------+----------------+--------------+
  | null       | None           | null         |
  +------------+----------------+--------------+
  | No         | buffer / bytes | blob         |
  | equivalent |                |              |
  +------------+----------------+--------------+
  | List       | list           | No           |
  |            |                | equivalent   |
  +------------+----------------+--------------+
  | Object     | dict           | No           |
  |            |                | equivalent   |
  +------------+----------------+--------------+

  Values in JSON that have no SQLite equivalent such as a list are
  `pickled <http://docs.python.org/library/pickle>`__ and supplied to
  SQLite as a blob.  Similarly any blob supplied to SQLite intended
  for CouchDB must be pickled Python data.  Use ``-1`` as the pickle
  version (selects binary encoding).

  Note that this module has no affinity rules.  Whatever type is
  supplied at the SQL level is then sent as the same type to CouchDB.
  You cannot specify types when creating the virtual table.  If you
  need affinity then use an intermediary temporary table as the
  example in the next section shows.

  .. note::

    Although JSON has separate Integer and Float types, Javascript
    itself does not and stores everything as a floating point number
    which has about 15 digits of precision. Python, Erlang and CouchDB
    support arbitrary large numbers but data that passes through a
    Javascript view server will lose precision.  For example
    9223372036854775807 will come back as 9223372036854776000
    (different last 4 digits).

    For Python 2 note that Pickle encodes strings and unicode strings
    differently even when they have same ASCII contents.  If you are
    trying to do an equality check then ensure all strings including
    dictionary keys are unicode before pickling.

Expressions

  SQL is accelerated by using indices.  These are
  precomputed/presorted views of the data and used when evaluating
  queries like ``select * from items where price > 74.99 and
  quantity<=10 and customer='Acme Widgets'`` in order to avoid
  visiting every row in the table.

  If you have constraints like the above then this module uses a
  CouchDB `temporary view
  <http://wiki.apache.org/couchdb/HTTP_view_API>`__ so that CouchDB
  chooses an appropriate subset of documents rather than having to
  transfer all of them and have SQLite do the filtering.

  With a large number of documents the view can take quite a while for
  CouchDB to calculate but is still quicker than sucking down all the
  data for SQLite to calculate in most cases.  Because the views are
  temporary they will eventually be discarded by CouchDB.

  This also means that Javascript's rules are used for evaluation in
  circumstances such as comparing strings to integers.  However SQL
  rules are used for nulls - they are not equal to each other or any
  other value.

  SQLite only supports one index per query.  If all the constraints
  are joined by **and** then this module can tell SQLite it has one
  index covering all of them.  If you use **or** then the index/view
  can only be used for one side of the **or** expression with SQLite
  having to evaluate the other.

  You can get SQLite to do the row/document filtering (which means
  retreiving all documents) like this::

    select couchdb_config('server-eval', 0);

Configuration summary

  You can change behaviour of the module by using the
  ``couchdb_config`` SQL function.  If you call with one argument then
  it returns the current value and with two sets the value.  The
  various options are described in more detail in the relevant
  sections above.

  +-----------------+---------+-------------------------------------------+
  | Option          | Default | Description                               |
  +=================+=========+===========================================+
  | read-batch      | 5,000   | How many documents are retrieved at a time|
  |                 |         | from the server                           |
  +-----------------+---------+-------------------------------------------+
  | write-batch     | 5,000   | How many documents being created/changed  |
  |                 |         | are saved up before sending in one request|
  |                 |         | to the bulk api on the server             |
  +-----------------+---------+-------------------------------------------+
  | deep-update     | 1 (True)| If a change is made to a row/document then|
  |                 |         | the original is retreived from the server |
  |                 |         | (one at a time) so that the keys not      |
  |                 |         | specified as SQL level columns aren't lost|
  +-----------------+---------+-------------------------------------------+
  | server-eval     | 1 (True)| SQL column expressions are evaluated in   |
  |                 |         | the server (by claiming there is an index)|
  |                 |         | rather than downloading all documents     |
  |                 |         | and having SQLite do the evaluation.      |
  +-----------------+---------+-------------------------------------------+

