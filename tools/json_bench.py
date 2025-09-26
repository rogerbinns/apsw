#!/usr/bin/env python3

import pathlib
import sys
import json
import time


topdir = pathlib.Path(__file__).parent.parent.resolve()

words = pathlib.Path(__file__).read_text().split()

randomjson = topdir / "randomjson.so"

if not randomjson.exists():
    sys.exit(f"{randomjson} needs to exist - compile it from sqlite3/ext/misc/randomjson.c")

sys.path.insert(0, str(topdir))

import apsw

con = apsw.Connection(":memory:")
con.enable_load_extension(True)
con.load_extension(str(randomjson))

import argparse

parser = argparse.ArgumentParser(prog="json_bench.py", description="Generates a large data structure and measures how long various operations take")
parser.add_argument("--size", type=int, default=200, help="How much initial JSON text to generate [%(default)s]")

options = parser.parse_args()

data = []

target = options.size * 1024*1024

size = 0

print("Generating json")
seed = 0
while size < target:
    items = con.execute("select random_json(:1), random_json(:1+1), random_json(:1+2), random_json(:1+3),random_json(:1+4)", (seed,)).get
    seed += len(items)
    try:
        [json.loads(i) for i in items]
    except (UnicodeDecodeError, UnicodeEncodeError):
        continue
    data.extend(items)
    size += sum(len(i) for i in items)
    if seed % 100 == 0:
        print(f"\r{size/target*100:5.0f}%", end="", flush=True)

print()

# turn it all into one big dict
big_data = {}
for i in range(len(data)):
    word = words[i % len(words)]
    big_data[f"{word}{i}"] = data[i]

del data

decode = apsw.jsonb_decode
encode = apsw.jsonb_encode

timerfn = time.process_time

b4 = timerfn()
big_data_json = json.dumps(big_data)
duration = timerfn() - b4

print(f"stdlib json.dumps\t{duration:2.2f}")

b4 = timerfn()
x = json.dumps(big_data_json)
duration = timerfn() - b4
del x
print(f"stdlib json.loads\t{duration:2.2f}")


b4 = timerfn()
x = encode(big_data)
duration = timerfn() - b4

print(f"apsw.jsonb.encode\t{duration:2.2f}")

b4 = timerfn()
x2 = decode(x)
duration = timerfn() - b4

del x
del x2

print(f"apsw.jsonb.decode\t{duration:2.2f}")

con.execute("create table one(json); insert into one(rowid,json) values(1, ?)", (big_data_json,))

b4 = timerfn()
con.execute("update one set json = jsonb(json)")
duration = timerfn() - b4

print(f"SQLite json -> jsonb\t{duration:2.2f}")

b4 = timerfn()
con.execute("update one set json = json(json)")
duration = timerfn() - b4

print(f"SQLite jsonb -> json\t{duration:2.2f}")

b4 = timerfn()
x = json.loads(con.execute("select json from one").get)
duration = timerfn() - b4
del x

print(f"SQLite json -> stdlib json decode\t{duration:2.2f}")

con.execute("update one set json = jsonb(json)")

b4 = timerfn()
x = json.loads(con.execute("select json(json) from one").get)
duration = timerfn() - b4
del x

print(f"SQLite jsonb -> json -> stdlib json decode\t{duration:2.2f}")

b4 = timerfn()
x = decode(con.execute("select json from one").get)
duration = timerfn() - b4
del x

print(f"SQLite jsonb -> apsw jsonb decode\t{duration:2.2f}")
