Best Practice
=============

Explanation
-----------

Because SQLite keeps very strong backwards compatibility, there are
several quirks and settings improvements that are not automatically
done.  This module does them for you, and keeps up to date with SQLite
best practices.  Several are described in the `SQLite documentation
<https://www.sqlite.org/quirks.html>`__.

Functions whose name begin with :code:`library` apply to the SQLite
library as a whole, while those with :code:`connection` apply to
individual connections.

You can call the individual functions, or
:func:`apsw.bestpractice.apply` to apply several.
:func:`~apsw.bestpractice.apply` will setup
:attr:`apsw.connection_hooks` to affect future :class:`connections
<apsw.Connection>` that are made.
:attr:`~apsw.bestpractice.recommended` is recommended::

    import apsw.bestpractice

    apsw.bestpractice.apply(apsw.bestpractice.recommended)

API
---

.. automodule:: apsw.bestpractice
    :members:
    :undoc-members: