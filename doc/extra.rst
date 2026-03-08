SQLite extra
============

In addition to the main library, SQLite has many additional programs
and loadable extensions.  However these need to be separately compiled
and installed.  They do not have the same level of testing and
documentation as the main library.

Full APSW builds such as those on PyPI include all the ones that
compile for that platform, without any modifications.  This is for
convenience and to help promote these great extras.  They add just
over 1MB to the download and 3MB of disk space.

Access is provided via an :ref:`API <extra_api>`, :ref:`command line
<extra_cli>`, and :ref:`shell <extra_shell>`

vec1
----

There is an experimental under development vector search library by
the SQLite team.  It is also included with the extras if possible.

* `Forum post <https://sqlite.org/forum/forumpost/ceba048877>`__ to provide feedback
* `Site <https://sqlite.org/vec1>`__ with tutorial and
  reference documentation

Dependencies
------------

There are no dependencies for the extensions and programs.  That means
they can be used on other compatible systems.  Some of the programs
require the SQLite library alongside the program which **must** be
placed in the same directory as the program if you copy the program
elsewhere.  (The SQLite library in that directory deliberately has a
different name to avoid interactions with the standard system SQLite
library.)

Extensions and programs that require third party libraries (eg
compression), or TCL are not included, and optional third party
libraries (eg readline) are not used.

Marking
-------

The extensions and programs are all marked as packaged by APSW.  Under
Windows this is indicated in the detailed properties listing.  On
other platforms running :code:`strings` should show it, with ELF
binaries having a :code:`note.apsw` section and MacOS (Mach-O)
binaries having a :code:`apsw` section.

The marking includes the version details.

.. include:: sqlite_extra.rst-inc

.. _extra_cli:

Command line
------------

Programs can be run by giving their name and parameters. For example
:code:`sqlite3_scrub` program::

    python3 -m apsw.sqlite_extra sqlite3_scrub source.db dest.db

You can also get the filename for any program or extension.  For example the
:code:`sqlite3_rsync` program::

    python3 -m apsw.sqlite_extra --path sqlite3_rsync

The :code:`csv` extension path.::

    python3 -m apsw.sqlite_extra --path csv

List what is available::

    python3 -m apsw.sqlite_extra --list

.. _extra_shell:

Shell
-----

The :doc:`shell <shell>` is also integrated.  Use :ref:`.load
<shell-cmd-load>` with :code:`--list` to see all available
extensions, and you can load just giving the name.

.. code-block:: console

    sqlite> .load --list
    sqlite> .load csv

.. _extra_api:

API
---

.. automodule:: apsw.sqlite_extra
    :members:
    :undoc-members:
    :member-order: bysource