import sys

from typing import Optional, Callable, Any, Iterator, Iterable, Sequence, Literal, Protocol, TypeAlias, final
import collections.abc
import array
import types

# Anything that resembles a dictionary
from collections.abc import Mapping

# Anything that resembles a sequence of bytes
if sys.version_info >= (3, 12):
    Buffer: TypeAlias = collections.abc.Buffer
else:
    Buffer: TypeAlias = bytes

SQLiteValue = None | int | float | Buffer | str
"""SQLite supports 5 types - None (NULL), 64 bit signed int, 64 bit
float, bytes (Buffer), and str (unicode text)"""

SQLiteValues = tuple[SQLiteValue, ...]
"A sequence of zero or more SQLiteValue"

Bindings = Sequence[SQLiteValue | zeroblob] | Mapping[str, SQLiteValue | zeroblob]
"""Query bindings are either a sequence of SQLiteValue, or a dict mapping names
to SQLiteValues.  You can also provide zeroblob in Bindings. You can use
dict subclasses or any type registered with :class:`collections.abc.Mapping`
for named bindings"""


class AggregateClass(Protocol):
    "Represents a running aggregate function"

    def step(self, *values: SQLiteValue) -> None:
        "Called with value(s) from a matching row"
        ...

    def final(self) -> SQLiteValue:
        "Called after all matching rows have been processed to get the final value"
        ...


# Neither TypeVar nor ParamSpec work, when either should
AggregateT = Any
"An object provided as first parameter of step and final aggregate functions"

AggregateStep = Callable[[AggregateT, *SQLiteValues], None]

"AggregateStep is called on each matching row with the relevant number of SQLiteValue"

AggregateFinal = Callable[[AggregateT], SQLiteValue]
"Final is called after all matching rows have been processed by step, and returns a SQLiteValue"

AggregateFactory = Callable[[], AggregateClass | tuple[AggregateT, AggregateStep, AggregateFinal]]
"""Called each time for the start of a new calculation using an aggregate function,
returning an object, a step function and a final function"""

ScalarProtocol = Callable[[*SQLiteValues], SQLiteValue]

"""Scalar callbacks take zero or more SQLiteValues, and return a SQLiteValue"""


class WindowClass(Protocol):
    "Represents a running window function"

    def step(self, param: SQLiteValue) -> None:
        "Adds the param(s) to the window"
        ...

    def final(self) -> SQLiteValue:
        "Finishes the function and returns final value"
        ...

    def value(self) -> SQLiteValue:
        "Returns the current value"
        ...

    def inverse(self, param: SQLiteValue) -> None:
        "Removes the param(s) from the window"
        ...


WindowT = Any
"An object provided as first parameter of the 4 window functions, if not using class based callbacks"

WindowStep = Callable[[WindowT, *SQLiteValues], None]
"""Window function step takes zero or more SQLiteValues"""

WindowFinal = Callable[[WindowT, *SQLiteValues], SQLiteValue]
"""Window function final takes zero or more SQLiteValues, and returns a SQLiteValue"""

WindowValue = Callable[[WindowT], SQLiteValue]
"""Window function value returns the current  SQLiteValue"""

WindowInverse = Callable[[WindowT, *SQLiteValues], None]
"""Window function inverse takes zero or more SQLiteValues"""

WindowFactory = Callable[[], WindowClass | tuple[WindowT, WindowStep, WindowFinal, WindowValue, WindowInverse]]
"""Called each time at the start of a new window function execution.  It should return either an object
with relevant methods or an object used as the first parameter and the 4 methods"""

RowTracer = Callable[[Cursor, SQLiteValues], Any]
"""Row tracers are called with the Cursor, and the row that would
be returned.  If you return None, then no row is returned, otherwise
whatever is returned is returned as a result row for the query"""

ExecTracer = Callable[[Cursor, str, Optional[Bindings]], bool]
"""Execution tracers are called with the cursor, sql query text, and the bindings
used.  Return False/None to abort execution, or True to continue"""

Authorizer = Callable[[int, Optional[str], Optional[str], Optional[str], Optional[str]], int]
"""Authorizers are called with an operation code and 4 strings (which could be None) depending
on the operatation.  Return SQLITE_OK, SQLITE_DENY, or SQLITE_IGNORE"""

CommitHook = Callable[[], bool]
"""Commit hook is called with no arguments and should return True to abort the commit and False
to let it continue"""

PreupdateHook = Callable[[PreUpdate], None]
"""The hook is called with information about the update, and has no return value"""

TokenizerResult = Iterable[str | tuple[str, ...] | tuple[int, int, *tuple[str, ...]]]
"""The return from a tokenizer is based on the include_offsets and
include_colocated parameters you provided, both defaulting to
``True``.  See :meth:`FTS5Tokenizer.__call__` for examples.
"""

Tokenizer = Callable[[bytes, int, str | None], TokenizerResult]
"""The tokenizer is called with UTF8 encoded bytes, int flags, and a locale."""

FTS5TokenizerFactory = Callable[[Connection, list[str]], Tokenizer]
"""The factory is called with a list of strings as an argument and should
return a suitably configured Tokenizer"""

FTS5Function = Callable[[FTS5ExtensionApi, *SQLiteValues], SQLiteValue]
"""The first argument is the extension API while the rest are the function parameters
from the SQL"""

FTS5QueryPhrase = Callable[[FTS5ExtensionApi, Any], None]
"""Callback from :meth:`FTS5ExtensionApi.query_phrase`"""

# The Session extension allows streaming of inputs and outputs

SessionStreamInput = Callable[[int], Buffer]
"""Streaming input function that is called with a number of bytes requested
returning up to that many bytes, and zero length for end of file"""

ChangesetInput = SessionStreamInput | Buffer
"""Changeset input can either be a streaming callback or data"""

SessionStreamOutput = Callable[[memoryview], None]
"""Streaming output callable is called with each block of streaming data"""
