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
implementation <virtualtables>`__ that lets you access CouchDB
databases from SQLite, including both read and write access.

Some suggested uses:

 * Loading data from SQLite into CouchDB and vice versa.  For example
   you can import CSV data into SQLite and then upload it to CouchDB.

 * Being able to use SQLite based tools for working on CouchDB data.

 * Being able to do `joins
   <http://en.wikipedia.org/wiki/Join_%28SQL%29>`__ between data in
   SQLite and CouchDB.


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

  create virtual table mytable using couchdb('http://localhost:5984', 'col1', 'col2', 'col3');

From that point on you can do regular SQL operations against
*mytable*.  When you drop the table it does not delete the CouchDB
database.  If you need usernames and passwords then specify them as
part of the url::

  http://username:password@localhost:5984



+
undefined
temp table mapping
scaling/batching
binary
pickling
config
