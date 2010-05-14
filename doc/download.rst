Download
********

.. _source_and_binaries:

Source and binaries
===================

You can download this release as binaries for Windows.  Just run the
executable corresponding with the Python version you are using.  The
Windows binaries all include the :ref:`FTS3 <ext-fts3>` and
:ref:`RTree <ext-rtree>` extensions.  (`FTS3_PARENTHESIS
<http://www.sqlite.org/compile.html#enable_fts3_parenthesis>`_ is on.)

Download in source form for other platforms or if you want to compile
yourself on Windows.  See the :ref:`recommended <recommended_build>`
way to build or all the :ref:`options available <building>`.

.. downloads-begin

* `apsw-3.6.23.1-r1.zip
  <http://apsw.googlecode.com/files/apsw-3.6.23.1-r1.zip>`_
  (Source, includes this HTML Help)

* `apsw-3.6.23.1-r1.chm
  <http://apsw.googlecode.com/files/apsw-3.6.23.1-r1.chm>`_
  (Compiled HTML Help) `Seeing blank content? <http://weblog.helpware.net/?p=36>`_ & `MSKB 902225 <http://support.microsoft.com/kb/902225/>`_

* `apsw-3.6.23.1-r1.win32-py2.3.exe
  <http://apsw.googlecode.com/files/apsw-3.6.23.1-r1.win32-py2.3.exe>`_
  (Windows Python 2.3)

* `apsw-3.6.23.1-r1.win32-py2.4.exe
  <http://apsw.googlecode.com/files/apsw-3.6.23.1-r1.win32-py2.4.exe>`_
  (Windows Python 2.4)

* `apsw-3.6.23.1-r1.win32-py2.5.exe
  <http://apsw.googlecode.com/files/apsw-3.6.23.1-r1.win32-py2.5.exe>`_
  (Windows Python 2.5)

* `apsw-3.6.23.1-r1.win32-py2.6.exe
  <http://apsw.googlecode.com/files/apsw-3.6.23.1-r1.win32-py2.6.exe>`_
  (Windows Python 2.6)

* `apsw-3.6.23.1-r1.win32-py3.1.exe
  <http://apsw.googlecode.com/files/apsw-3.6.23.1-r1.win32-py3.1.exe>`_
  (Windows Python 3.1)

* `apsw-3.6.23.1-r1-sigs.zip 
  <http://apsw.googlecode.com/files/apsw-3.6.23.1-r1-sigs.zip>`_
  GPG signatures for all files

.. downloads-end

Some Linux distributions also have packages.

+-------------------+----------------------------------------------------------------------------------+
| Debian            | Install `python-apsw <http://packages.debian.org/python-apsw>`__                 |
+-------------------+----------------------------------------------------------------------------------+
| Ubuntu            | Install `python-apsw <http://packages.ubuntu.com/search?keywords=python-apsw>`__ |
|                   | I maintain a PPA that is up to date at                                           |
|                   |     https://launchpad.net/~ubuntu-rogerbinns/+archive/apsw  which has SQLite     |
|                   | embedded statically inside (ie system SQLite is ignored) and has all the         |
|                   | extensions enabled: FTS3, RTree, ICU, asyncvfs                                   |
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

      $ gpg --verify apsw-3.6.23.1-r1.zip.asc
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
