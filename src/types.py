
from typing import Union, Tuple, List, Optional, Callable, Any, Dict, \
        Iterator, Sequence, Literal, Set, Protocol
from array import array

SQLiteValue = Union[None, int, float, bytes, str]
"""SQLite supports 5 types - None (NULL), 64 bit signed int, 64 bit
float, bytes, and unicode text"""

Bindings = Union[Sequence[SQLiteValue], Dict[str, SQLiteValue]]
"""Query bindings are either a sequence of SQLiteValue, or a dict mapping names
to SQLiteValues"""

class AggregateProtocol(Protocol):
    "Used to implement aggegrate function callbacks"
    def step(self, *values: SQLiteValue) -> None:
        "Called with value(s) from each matched row"
        ...
    def finalize(self) -> SQLiteValue:
        "Called to get a final result value"
        ...

def AggregateFactory() -> AggregateProtocol:
        "Called each time for the start of a new calculation using an aggregate function"
        ...

ScalarProtocol = Union[
        Callable[[], SQLiteValue],
        Callable[[SQLiteValue, SQLiteValue], SQLiteValue],
        Callable[[SQLiteValue, SQLiteValue, SQLiteValue], SQLiteValue],
        Callable[[SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue], SQLiteValue],
        Callable[[SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue], SQLiteValue],
        Callable[[SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue], SQLiteValue],
        Callable[[SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue], SQLiteValue]
]
"""Scalar callbacks take zero or more SQLiteValues, and return a SQLiteValue"""


RowTracer = Callable[[Cursor, Tuple[SQLiteValue, ...]], Any]
"""Row tracers are called with the Cursor, and the row that would
be returned.  If you return None, then no row is returned, otherwise
whatever is returned is returned as a result row for the query"""

ExecTracer = Callable[[Cursor, str, Union[Dict[str, SQLiteValue], Tuple[SQLiteValue,...], None]], bool]
"""Execution tracers are called with the cursor, sql query text, and the bindings
used.  Return False/None to abort execution, or True to continue"""