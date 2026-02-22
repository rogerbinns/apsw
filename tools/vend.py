#!/usr/bin/env python3

# Package up all the SQLite extensions and binaries #547
# most likely exposed as apsw.sqlite_extras

# for windows we have to get and extract the full source, followed by
# getting and extracting the amalgamation over the top.  It is too
# painful to build the source otherwise.  may as well do this for
# all platforms based on if fetch is --sqlite or --all --sqlite

# this code works through building and documenting them first
# for later refactoring

# doc for source file pattern is https://www.sqlite.org/src/file?ci=trunk&name=ext%2Fmisc%2Frandomjson.c

extras = {
    "randomjson": {
        "type": "extension",
        "sources": ["ext/misc/randomjson.c"],
        "description": "Generates random json objects",
    },
    "sqlite3_rsync": {
        "type": "executable",
        "sources": ["tool/sqlite3_rsync.c"],
        "description": "Database Remote-Copy Tool",
        "doc": "rsync.html",
        "libsqlite": True,
    },
}

import os
import pathlib
import setuptools._distutils.ccompiler as ccompiler
from setuptools._distutils.sysconfig import customize_compiler

import logging

logging.basicConfig(level=logging.DEBUG, format="    %(message)s")

compiler = ccompiler.new_compiler(verbose=True)
customize_compiler(compiler)

compiler.add_include_dir("sqlite3")

# where the build artifacts go
build_dir = pathlib.Path() / "build" / "sqlite_extras"
compiler.mkpath(str(build_dir))

# where the final binaries go
output_dir = pathlib.Path() / "apsw" / "sqlite_extras_binaries"
compiler.mkpath(str(output_dir))

# for windows we need rc -> res resource file
# start with src/sqlite3.rc, in StringFileInfo block
#   add Comments
#   add FileDescription

# build sqlite3 library
print(">>> sqlite3 library")
lib_enables = "CARRAY COLUMN_METADATA DBPAGE_VTAB DBSTAT_VTAB FTS4 FTS5 GEOPOLY MATH_FUNCTIONS PERCENTILE PREUPDATE_HOOK RTREE SESSION".split()

macros = [(f"SQLITE_ENABLE_{enable}", 1) for enable in lib_enables]
cfg = pathlib.Path("sqlite3") / "sqlite_cfg.h"
if cfg.exists():
    macros.append(("SQLITE_CUSTOM_INCLUDE", "sqlite_cfg.h"))
macros.append(("SQLITE_THREADSAFE", 1))
lib_objs = compiler.compile([str(pathlib.Path("sqlite3") / "sqlite3.c")], output_dir=str(build_dir), macros=macros)


for name, info in extras.items():
    print(f">>> {name}")
    missing = []
    for source in info["sources"]:
        if not (pathlib.Path("sqlite3") / source).exists():
            missing.append(source)
    if missing:
        print("  Skipping due to missing source:", missing)
        print()
        continue

    objs = compiler.compile(
        [str(pathlib.Path("sqlite3") / filename) for filename in info["sources"]], output_dir=str(build_dir)
    )

    if info.get("libsqlite", False):
        objs.extend(lib_objs)

    match info["type"]:
        case "extension":
            # ::TODO:: do we want sqlext vs platform native dll extension?
            out_name = f"{name}.sqlite_extension"
            compiler.link_shared_object(objs, out_name, output_dir=str(build_dir))

        case "executable":
            out_name = f"{name}{compiler.exe_extension if compiler.exe_extension else ''}"
            match compiler.compiler_type:
                case "msvc":
                    libraries = None
                case _:
                    libraries = ["m"]
            compiler.link_executable(objs, name, output_dir=str(build_dir), libraries=libraries)
            # macos xattr -d com.apple.quarantine str(build_dir / out_name)

        case _:
            raise NotImplementedError

    try:
        os.remove(str(output_dir / out_name))
    except FileNotFoundError:
        pass

    compiler.move_file(str(build_dir / out_name), str(output_dir / out_name))

    print()
