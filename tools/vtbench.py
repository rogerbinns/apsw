#!/usr/bin/env python3

import time
import dataclasses
import random
import statistics

import apsw
import apsw.ext


@dataclasses.dataclass(slots=True, frozen=True)
class Row:
    "A row of data for testing, covering various types"
    name: str
    unit_price: float
    quantity: int
    description: str
    available: bool
    quadrangle: int
    colour: str
    parameter: str
    additional: str
    horizon: bytes


# ensure repeatable runs
random.seed(0)


def gen_value(t):
    if t is str:
        text = "abcdefghijklmnopqrstuvwxya1324324 "
        return text[:random.randrange(5, len(text))]
    if t is bool:
        return random.choice([False, True])
    if t is int:
        return random.randrange(-1_000_000, 1_000_000)
    if t is float:
        return random.uniform(-1_000_000, 1_000_000)
    assert t is bytes
    return b"\0" * random.randrange(8, 128)

# rows holds the data to return
rows: list[Row] = []

for i in range(10):
    rows.append(Row(**{field.name: gen_value(field.type) for field in dataclasses.fields(Row)}))  # type: ignore

columns = tuple(field.name for field in dataclasses.fields(Row))

# the code structuring is to ensure the least amount of code runs to return
# the values, because we want to see the overhead of processing the values
# as much as possible
def data_source_index(count):
    t0, t1, t2, t3, t4, t5, t6, t7, t8, t9 = data_source_index.data
    for _ in range(count // 10):
        yield t0
        yield t1
        yield t2
        yield t3
        yield t4
        yield t5
        yield t6
        yield t7
        yield t8
        yield t9


data_source_index.data = tuple(dataclasses.astuple(row) for row in rows)
data_source_index.columns = columns
data_source_index.column_access = apsw.ext.VTColumnAccess.By_Index


def data_source_dict(count):
    t0, t1, t2, t3, t4, t5, t6, t7, t8, t9 = data_source_dict.data
    for _ in range(count // 10):
        yield t0
        yield t1
        yield t2
        yield t3
        yield t4
        yield t5
        yield t6
        yield t7
        yield t8
        yield t9


data_source_dict.data = tuple(dataclasses.asdict(row) for row in rows)
data_source_dict.columns = columns
data_source_dict.column_access = apsw.ext.VTColumnAccess.By_Name


def data_source_attr(count):
    t0, t1, t2, t3, t4, t5, t6, t7, t8, t9 = data_source_attr.data
    for _ in range(count // 10):
        yield t0
        yield t1
        yield t2
        yield t3
        yield t4
        yield t5
        yield t6
        yield t7
        yield t8
        yield t9


data_source_attr.data = tuple(rows)
data_source_attr.columns = columns
data_source_attr.column_access = apsw.ext.VTColumnAccess.By_Attr

con = apsw.Connection("")
apsw.ext.make_virtual_module(con, "data_source_index", data_source_index)
apsw.ext.make_virtual_module(con, "data_source_dict", data_source_dict)
apsw.ext.make_virtual_module(con, "data_source_attr", data_source_attr)

ROWS = 1_000_000

times: dict[str, list[float]] = {}

for i in range(5):
    for kind in ("index", "dict", "attr"):
        if kind not in times:
            times[kind] = []
        print(f"{kind}\t{ i+ 1}\t", end="", flush=True)
        start = time.perf_counter()
        for _ in con.execute(f"select * from data_source_{kind}(?)", (ROWS, )):
            pass
        end = time.perf_counter()
        times[kind].append(end - start)
        print("%.03f" % (end - start))

print("\nMedians\n")
for k, v in times.items():
    print(k, "\t", "%.03f" % statistics.median(v))
