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

 * Using the SQLite :ref:`FTS3 full text search extension <ext-fts3>`
   for data in CouchDB.
   
Getting it
==========

You can find the code in the :ref:`source distribution
<source_and_binaries>` named :file:`apswcouchdb.py`, or you can get a
copy directly from `source control
<http://code.google.com/p/apsw/source/browse/tools/apswcouchdb.py>`__
(choose "View Raw File").

You will need to have the `Python couchdb module
<http://code.google.com/p/couchdb-python/>`__ installed as well as its
prerequisites.

Usage
=====

To use with your code, place the :file:`apswcouchdb.py` file anywhere
you can import it.  To use it with the APSW :ref:`shell` use the the
.read command specifying the filename.  The virtual table will be
automatically installed via the :attr:`connection_hooks` mechanism and
registering with the shell if appropriate.

Each virtual table maps to a CouchDB database.  The database needs to
exist already.  Use the following SQL to create the virtual table::

  create virtual table mytable using couchdb('http://localhost:5984', 'dbname', col1, col2, col3);

From that point on you can do regular SQL operations against *mytable*
where each row will correspond to a document from *dbname*.  When you
drop the table it does not delete the CouchDB database.  If you need
usernames and passwords then specify them as part of the url::

  http://username:password@localhost:5984


Notes
=====

Automatic column names

  If you don't want to manually specify all columns then you can use
  '+' as a column and the first 1,000 documents in the database will
  be examined to see what keys they have.  Any that you have not
  already specified as a column will be added to the column list in
  alphabetical order.  Only keys without a leading underscore are
  considered.

Document Ids and Rowids

  CouchDB uses the document id (key _id) as the unique identifier.
  SQLite uses a 64 bit integer rowid.  In order to map the two a
  `temporary table <http://www.sqlite.org/lang_createtable.html>`__ is
  used behind the scenes. The revision (_rev) is also stored. SQLite
  stores temporary tables separately and discards them when the
  corresponding database is closed.

  By default the temporary tables are stored in a file.  You can use a
  `pragma <http://www.sqlite.org/pragma.html#pragma_temp_store>`__ to
  change that.  For example ``pragma temp_store=memory`` will use
  memory instead.  My 200,000 document test database resulted in a
  temporary mapping table of 50MB if I accessed all rows/documents.

Scalability

  It is intended that you can use the virtual table with large
  databases.  For example development and profiling were done with a
  200,000 document database using over 2GB of storage.  If you are
  dealing with just a handful of documents then you'll likely find
  `Futon <http://couchdb.apache.org/screenshots.html>`__ a better fit.

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

    select apswcouchdb_config('read-batch', 2000);
    select apswcouchdb_config('write-batch', 10000);

  If you are debugging code then setting them to 1 will cause an
  immediate HTTP request per row/document read or written.

Transactions

  SQLite has the same transaction boundaries as SQL and supports
  transactions.  CouchDB is atomic but does not support transactions.
  Any outstanding batched updates are also flushed on SQLite
  transaction boundaries as well as other points such as when a cursor
  starts from the begining again.  This means that a rollback can only
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
  |            |                | bit)         |
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
  for CouchDB must be pickled Python data.

  Note that this module has no affinity rules.  Whatever type is
  supplied at the SQL level is then sent as the same type to CouchDB.
  You cannot specify types when creating the virtual table.  If you
  need affinity then use an intermediary temporary table as the
  example in the next section shows.

Writing

  The SQLite API requires supplying a value for all columns.  If a
  CouchDB document does not have a particular column/key then
  null/None is used.  On writing back the value for that key will then
  become null.

Indices

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
  data for SQLite to do so in most cases.  Because the views are
  temporary they will eventually be discarded by CouchDB.

  This also means that Javascript's rules are used for evaluation in
  circumstances such as comparing strings to integers.

  SQLite only supports one index per query.  If all the constraints
  are joined by **and** then this module can tell SQLite it has one
  index covering all of them.  If you use **or** then the index/view
  can only be used for one side of the **or** expression with SQLite
  having to evaluate the other.

Examples
========

Importing CSV data
------------------

We want to import some CSV data to a CouchDB database.  The first step
is to import it into a temporary SQLite table.  It could be imported
directly to CouchDB but then we wouldn't have the ability to specify
`column affinity <http://www.sqlite.org/datatype3.html>`__ and all
data would end up as strings in CouchDB.  I am using a real estate CSV
file in this example against the SQLite shell::

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

Using FTS3
----------

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