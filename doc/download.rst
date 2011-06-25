Download
********

.. _source_and_binaries:

Source and binaries
===================

You can download this release as binaries for Windows.  Just run the
executable corresponding with the Python version you are using.  The
Windows binaries all include the :ref:`FTS <ext-fts3>` and
:ref:`RTree <ext-rtree>` extensions.  (`FTS3_PARENTHESIS
<http://www.sqlite.org/compile.html#enable_fts3_parenthesis>`_ is on.)

Download in source form for other platforms or if you want to compile
yourself on Windows.  See the :ref:`recommended <recommended_build>`
way to build or all the :ref:`options available <building>`.

.. downloads-begin

* `apsw-3.7.7-r1.zip
  <http://apsw.googlecode.com/files/apsw-3.7.7-r1.zip>`_
  (Source, includes this HTML Help)

* `apsw-3.7.7-r1.chm
  <http://apsw.googlecode.com/files/apsw-3.7.7-r1.chm>`_
  (Compiled HTML Help) `Seeing blank content? <http://weblog.helpware.net/?p=36>`_ & `MSKB 902225 <http://support.microsoft.com/kb/902225/>`_

* Windows Python 2.3 `32bit
  <http://apsw.googlecode.com/files/apsw-3.7.7-r1.win32-py2.3.exe>`__

* Windows Python 2.4 `32bit
  <http://apsw.googlecode.com/files/apsw-3.7.7-r1.win32-py2.4.exe>`__

* Windows Python 2.5 `32bit
  <http://apsw.googlecode.com/files/apsw-3.7.7-r1.win32-py2.5.exe>`__

* Windows Python 2.6 `32bit
  <http://apsw.googlecode.com/files/apsw-3.7.7-r1.win32-py2.6.exe>`__
  `64bit 
  <http://apsw.googlecode.com/files/apsw-3.7.7-r1.win-amd64-py2.6.exe>`__

* Windows Python 2.7 `32bit
  <http://apsw.googlecode.com/files/apsw-3.7.7-r1.win32-py2.7.exe>`__
  `64bit 
  <http://apsw.googlecode.com/files/apsw-3.7.7-r1.win-amd64-py2.7.exe>`__

* Windows Python 3.1 `32bit
  <http://apsw.googlecode.com/files/apsw-3.7.7-r1.win32-py3.1.exe>`__
  `64bit 
  <http://apsw.googlecode.com/files/apsw-3.7.7-r1.win-amd64-py3.1.exe>`__

* Windows Python 3.2 `32bit
  <http://apsw.googlecode.com/files/apsw-3.7.7-r1.win32-py3.2.exe>`__
  `64bit 
  <http://apsw.googlecode.com/files/apsw-3.7.7-r1.win-amd64-py3.2.exe>`__

* `apsw-3.7.7-r1-sigs.zip 
  <http://apsw.googlecode.com/files/apsw-3.7.7-r1-sigs.zip>`_
  GPG signatures for all files

.. downloads-end

Some Linux distributions also have packages.

+-------------------+----------------------------------------------------------------------------------+
| Debian            | Install `python-apsw <http://packages.debian.org/python-apsw>`__                 |
+-------------------+----------------------------------------------------------------------------------+
| Ubuntu            | Install `python-apsw <http://packages.ubuntu.com/search?keywords=python-apsw>`__ |
+-------------------+----------------------------------------------------------------------------------+
| Ubuntu PPA        | I maintain a PPA that is up to date at                                           |
|                   | https://launchpad.net/~ubuntu-rogerbinns/+archive/apsw  which has SQLite         |
|                   | embedded statically inside (ie system SQLite is ignored) and has all the         |
|                   | extensions enabled: FTS3/4, RTree, ICU, asyncvfs                                 |
+-------------------+----------------------------------------------------------------------------------+
| Gentoo            | Install `dev-python/apsw <http://www.gentoo-portage.com/dev-python/apsw>`_       |
+-------------------+----------------------------------------------------------------------------------+
| Arch Linux        | Install `python-apsw <http://aur.archlinux.org/packages.php?ID=5537>`__          |
+-------------------+----------------------------------------------------------------------------------+

Note that these (except my Ubuntu PPA) may trail the SQLite and APSW
releases by a year, or more.  It is also possible to build RPMs and
DEB packages from the source, although this involves setting up
package management tools and various dependencies on your build
machine.


.. _verifydownload:

Verifying your download
=======================

Downloads are now digitally signed so you can verify they have not
been tampered with.  Download and extract the zip file of signatures
listed above.  These instructions are for `GNU Privacy Guard
<http://www.gnupg.org/>`__.  (GPG is installed as standard on most
Unix/Linux platforms and can be downloaded for Windows.)

Verify

  To verify a file just use --verify specifying the corresponding
  ``.asc`` filename.  This example verifies the source::

      $ gpg --verify apsw-3.7.7-r1.zip.asc
      gpg: Signature made ... date ... using DSA key ID 0DFBD904
      gpg: Good signature from "Roger Binns <rogerb@rogerbinns.com>"

  If you get a "good signature" then the file has not been tampered with
  and you are good to go.

Getting the signing key

  You may not have the signing key available in which case the last
  line will be something like this::

   gpg: Can't check signature: public key not found

  You can get a copy of the key using this command::

    $ gpg --keyserver hkp://keyserver.ubuntu.com --recv-keys 0DFBD904
    gpg: requesting key 0DFBD904 from hkp server keyserver.ubuntu.com
    gpg: /home/username/.gnupg/trustdb.gpg: trustdb created
    gpg: key 0DFBD904: public key "Roger Binns <rogerb@rogerbinns.com>" imported
    gpg: Total number processed: 1
    gpg:               imported: 1

  Repeat the verify step.

Source code control
===================

The source is controlled by Mercurial documented at
http://code.google.com/p/apsw/source/checkout

easy_install/pip/pypi
=====================

APSW is **not** available at the Python Package Index (pypi) and hence
cannot be installed using easy_install, pip or similar tools.  The
reason for this is that the tools do not provide a way of providing
options to the setup.py included with APSW and hence there is no way
for APSW to know if you want SQLite downloaded, a consistent version
of SQLite or the latest, to use a system SQLite instead, error if an a
system version is not available etc.  I could pick a sensible default
but everyone else using pypi would be disadvantaged or worse get
undesired behaviour (eg different versions of SQLite depending on when
a machine did an install).  Additionally the world of Python packaging
is going through another series of changes (distutils2 aka packaging)
so some solution may come out of that.

I'm happy to work with anyone who has a solution to this problem.