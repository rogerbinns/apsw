
from typing import Union, Tuple, List, Optional, Callable, Any, Dict, \
        Iterator, Sequence, Literal, Set
from array import array
from types import TracebackType

SQLiteValue = Union[None, int, float, bytes, str]
"""SQLite supports 5 types - None (NULL), 64 bit signed int, 64 bit
float, bytes, and unicode text"""

SQLiteValues = Union[Tuple[()], Tuple[SQLiteValue, ...]]
"A sequence of zero or more SQLiteValue"

Bindings = Union[Sequence[Union[SQLiteValue, zeroblob]], Dict[str, Union[SQLiteValue, zeroblob]]]
"""Query bindings are either a sequence of SQLiteValue, or a dict mapping names
to SQLiteValues.  You can also provide zeroblob in Bindings."""

# Neither TypeVar nor ParamSpec work, when either should
AggregateT = Any
"An object provided as first parameter of step and final aggregate functions"

AggregateStep = Union[
        Callable[[AggregateT], None],
        Callable[[AggregateT, SQLiteValue], None],
        Callable[[AggregateT, SQLiteValue, SQLiteValue], None],
        Callable[[AggregateT, SQLiteValue, SQLiteValue, SQLiteValue], None],
        Callable[[AggregateT, SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue], None],
        Callable[[AggregateT, SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue], None],
        Callable[[AggregateT, SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue], None],
]
"AggregateStep is called on each matching row with the relevant number of SQLiteValue"

AggregateFinal= Callable[[AggregateT], SQLiteValue]
"Final is called after all matching rows have been processed by step, and returns a SQLiteValue"

AggregateFactory = Callable[[], Tuple[AggregateT, AggregateStep, AggregateFinal]]
"""Called each time for the start of a new calculation using an aggregate function,
returning an object, a step function and a final function"""

ScalarProtocol = Union[
        Callable[[], SQLiteValue],
        Callable[[SQLiteValue], SQLiteValue],
        Callable[[SQLiteValue, SQLiteValue], SQLiteValue],
        Callable[[SQLiteValue, SQLiteValue, SQLiteValue], SQLiteValue],
        Callable[[SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue], SQLiteValue],
        Callable[[SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue], SQLiteValue],
        Callable[[SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue], SQLiteValue],
        Callable[[SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue, SQLiteValue], SQLiteValue]
]
"""Scalar callbacks take zero or more SQLiteValues, and return a SQLiteValue"""


RowTracer = Callable[[Cursor, SQLiteValues], Any]
"""Row tracers are called with the Cursor, and the row that would
be returned.  If you return None, then no row is returned, otherwise
whatever is returned is returned as a result row for the query"""

ExecTracer = Callable[[Cursor, str, Optional[Bindings]], bool]
"""Execution tracers are called with the cursor, sql query text, and the bindings
used.  Return False/None to abort execution, or True to continue"""

