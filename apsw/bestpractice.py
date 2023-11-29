#

"""Ensure SQLite usage prevents common mistakes, and get best performance."""

from __future__ import annotations

from typing import Callable

import apsw
import apsw.ext


def connection_wal(connection: apsw.Connection) -> None:
    """Turns on write ahead logging

    Reduces contention and improves write performance.  WAL is
    `described here <https://www.sqlite.org/wal.html>`__.
    """
    connection.pragma("journal_mode", "wal")


def connection_busy_timeout(connection: apsw.Connection, duration_ms: int = 100) -> None:
    """Sets a short busy timeout

    :param duration_ms: How many thousandths of a second to wait

    When another thread or process has locked the database, SQLite
    immediately raises :exc:`apsw.BusyError`.  Changing the `busy
    timeout <https://www.sqlite.org/c3ref/busy_timeout.html>`__ gives
    a grace period during which SQLite retries.
    """
    connection.set_busy_timeout(duration_ms)


def connection_enable_foreign_keys(connection: apsw.Connection) -> None:
    """Enables foreign key constraints

    `Foreign keys <https://www.sqlite.org/foreignkeys.html>`__ need to
    `be enabled <https://www.sqlite.org/foreignkeys.html#fk_enable>`__
    to have an effect.
    """
    connection.pragma("foreign_keys", "ON")


def connection_dqs(connection: apsw.Connection) -> None:
    """Double quotes are for identifiers only, not strings

    Turns off `allowing double quoted strings
    <https://www.sqlite.org/quirks.html#dblquote>`__ if they don't
    match any identifier (column/table names etc), making it an error
    to use double quotes around strings.  SQL strings use single
    quotes.
    """
    connection.config(apsw.SQLITE_DBCONFIG_DQS_DML, 0)
    connection.config(apsw.SQLITE_DBCONFIG_DQS_DDL, 0)


def library_logging() -> None:
    """Forwards SQLite logging to Python logging module

    See :meth:`apsw.ext.log_sqlite`
    """
    apsw.ext.log_sqlite()


recommended: tuple[Callable, ...] = (
    connection_wal,
    connection_busy_timeout,
    connection_enable_foreign_keys,
    connection_dqs,
    library_logging,
)
"All of them"


def apply(which: tuple[Callable, ...]) -> None:
    "Applies library immediately and connection to new connections"
    hooks : list[Callable] = []
    for func in which:
        if func.__name__.startswith("connection_"):
            hooks.append(func)
        else:
            func()

    def best_practise_connection_apply(connection: apsw.Connection) -> None:
        for func in hooks:
            func(connection)

    apsw.connection_hooks.append(best_practise_connection_apply)