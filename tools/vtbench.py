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


@dataclasses.dataclass(slots=True, frozen=True)
class RowRepr(Row):
    quadrangle: object


# ensure repeatable runs
random.seed(0)


def gen_value(t):
    if t is str:
        text = "abcdefghijklmnopqrstuvwxya1324324 "
        return text[: random.randrange(5, len(text))]
    if t is bool:
        return random.choice([False, True])
    if t is int:
        return random.randrange(-1_000_000, 1_000_000)
    if t is float:
        return random.uniform(-1_000_000, 1_000_000)
    if t is object:
        return object()
    assert t is bytes
    return b"\0" * random.randrange(8, 128)


# rows holds the data to return
rows: list[Row] = []
rows_repr: list[Row] = []

for i in range(10):
    rows.append(Row(**{field.name: gen_value(field.type) for field in dataclasses.fields(Row)}))  # type: ignore
    klass = RowRepr if i == 5 else Row  # make 10% need repr handling
    rows_repr.append(klass(**{field.name: gen_value(field.type) for field in dataclasses.fields(klass)}))  # type: ignore

columns = tuple(field.name for field in dataclasses.fields(Row))


# the code structuring is to ensure the least amount of code runs to return
# the values, because we want to see the overhead of processing the values
# as much as possible
def data_source(count):
    t0, t1, t2, t3, t4, t5, t6, t7, t8, t9 = data_source.data
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


con = apsw.Connection("")

ROWS = 1_000_000

times: dict[str, list[float]] = {}

counter = 0
for i in range(6):
    for config in ("", "repr", "hidden"):
        for kind in ("index", "dict", "attr"):
            rec = kind
            if config:
                rec += "_" + config
            if rec not in times:
                times[rec] = []
            rows_source = rows if config != "repr" else rows_repr
            if kind == "index":
                data_source.data = tuple(dataclasses.astuple(row) for row in rows_source)
            elif kind == "dict":
                data_source.data = tuple(dataclasses.asdict(row) for row in rows_source)
            else:
                data_source.data = tuple(rows_source)
            data_source.columns = columns
            access = {
                "index": apsw.ext.VTColumnAccess.By_Index,
                "dict": apsw.ext.VTColumnAccess.By_Name,
                "attr": apsw.ext.VTColumnAccess.By_Attr,
            }[kind]
            data_source.column_access = access
            apsw.ext.make_virtual_module(con, f"data_source{counter}", data_source, repr_invalid=config == "repr")
            query = "select *"
            if config == "hidden":
                query += ",count"
            query += f" from data_source{counter}(?)"
            counter += 1
            print(f"{rec:20}{ i+ 1}\t", end="", flush=True)
            start = time.perf_counter()
            for _ in con.execute(query, (ROWS,)):
                pass
            end = time.perf_counter()
            times[rec].append(end - start)
            print("%.03f" % (end - start))

print("\nMedians (stddev)      values per second\n")
for k, v in sorted(times.items()):
    nvalues = (len(columns) + (1 if "hidden" in k else 0)) * ROWS
    print(
        f"{ k:20}%.03f   (%.03f) %s"
        % (statistics.median(v), statistics.stdev(v), format(int(nvalues / statistics.median(v)), "12,d"))
    )
