Download
********

PyPI/pip
========

APSW is `available <https://pypi.org/project/apsw/>`__ at PyPI.

.. code-block:: console

  $ python3 -m pip install apsw

It includes binary downloads for popular platforms, and source for all
others.  SQLite is compiled into the extension, and is not exposed
outside of it.  All :doc:`extensions <extensions>` are enabled.

If a binary is not available for your platform, then pip/Python will
require a C compiler to produce one.

The PyPi release is made using `this github workflow
<https://github.com/rogerbinns/apsw/blob/master/.github/workflows/build-pypi.yml>`__.

.. _source_and_binaries:

Source
======

You can get the source from `PyPI <https://pypi.org/project/apsw/>`__ which is
preconfigured to fetch the corresponding SQLite version, and enables all extensions.
Delete the included :file:`setup.cfg` to remove that.

Use the source form if:

 * You want to control which version of SQLite is used
 * You want to control how SQLite is found, for example if it is provided
   on your platform
 * You want to choose which or if any extensions are enabled
 * You want to control compilation configuration

See the :ref:`recommended <recommended_build>` way to build or all the
:ref:`options available <building>`.

.. downloads-begin

* `apsw-3.39.3.0.zip
  <https://github.com/rogerbinns/apsw/releases/download/3.39.3.0/apsw-3.39.3.0.zip>`__
  (Source, includes this HTML Help)

* `apsw-3.39.3.0-sigs.zip 
  <https://github.com/rogerbinns/apsw/releases/download/3.39.3.0/apsw-3.39.3.0-sigs.zip>`__
  GPG signatures for all files

.. downloads-end

Some Linux distributions also have packages which may trail the SQLite
and APSW releases by a year, or more.  It is also possible to build
RPMs and DEB packages from the source, although this involves setting
up package management tools and various dependencies on your build
machine.

+-------------------+------------------------------------------------------------------------------------------------------+
| Debian            | Install `python3-apsw <http://packages.debian.org/python3-apsw>`__                                   |
+-------------------+------------------------------------------------------------------------------------------------------+
| Fedora            | Install `python3-apsw <https://packages.fedoraproject.org/pkgs/python-apsw/>`__                      |
+-------------------+------------------------------------------------------------------------------------------------------+
| Ubuntu            | Install `python3-apsw <https://packages.ubuntu.com/search?suite=all&searchon=names&keywords=apsw>`__ |
+-------------------+------------------------------------------------------------------------------------------------------+
| Gentoo            | Install `dev-python/apsw <http://packages.gentoo.org/package/dev-python/apsw>`_                      |
+-------------------+------------------------------------------------------------------------------------------------------+
| Arch Linux        | Install `python-apsw <https://www.archlinux.org/packages/?q=apsw>`__                                 |
+-------------------+------------------------------------------------------------------------------------------------------+

.. _verifydownload:

Verifying your download
=======================

Downloads are digitally signed so you can verify they have not been
tampered with.  Download and extract the zip file of signatures listed
above.  These instructions are for `GNU Privacy Guard
<http://www.gnupg.org/>`__.  (GPG is installed as standard on most
Unix/Linux platforms and can be downloaded for Windows.)

Verify

  To verify a file just use --verify specifying the corresponding
  ``.asc`` filename.  This example verifies the source::

      $ gpg --verify apsw-3.39.3.0.zip.asc
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

The source is controlled by Git - start at
https://github.com/rogerbinns/apsw