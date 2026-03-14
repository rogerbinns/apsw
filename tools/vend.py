#!/usr/bin/env python3

# Package up all the SQLite extensions and binaries #547
# most likely exposed as apsw.sqlite_extra

# for windows we have to get and extract the full source, followed by
# getting and extracting the amalgamation over the top.  It is too
# painful to build the source otherwise.  may as well do this for
# all platforms based on if fetch is --sqlite or --all --sqlite

# this code works through building and documenting them first
# for later refactoring

from __future__ import annotations

import sysconfig
from typing import Literal
import dataclasses
import sys
import pathlib


@dataclasses.dataclass
class Extra:
    name: str
    "unique name for extra"
    description: str
    "what it is"
    doc: str | None = None
    "documentation link relative to sqlite websites.  if none the source will be used"
    lib_sqlite: bool = False
    "true if needing to link against sqlite3.c"
    lib_sqlite_stdio: bool = False
    "true if needing sqlite3_stdio"
    lib_zlib: bool = False
    "true if needing zlib - we statically link in"
    sources: list[str] = dataclasses.field(default_factory=list[str])
    "list of files making source for this extra relative to sqlite3 directory"
    type: Literal["extension"] | Literal["executable"] = "extension"
    "what kind of extra"
    defines: list | None = None
    "additional defines needed"

    def __post_init__(self):
        if self.type == "extension" and not self.sources:
            self.sources = [f"ext/misc/{self.name}.c"]
        assert len(self.sources)
        assert not any("\\" in source for source in self.sources)
        if self.doc is not None:
            assert not self.doc.startswith("/")
            self.doc = f"https://sqlite.org/{self.doc}"
        else:
            self.doc = "https://sqlite.org/src/file?ci=trunk&name=" + "%2f".join(self.sources[0].split("/"))


extras = [
    Extra(
        name="amatch",
        description="Approximate matches virtual table",
    ),
    Extra(
        name="anycollseq",
        description="Fake fallback collating function for any unknown collating sequence",
    ),
    Extra(
        name="appendvfs",
        description="A VFS shim that allows an SQLite database to be appended onto the end of some other file, such as an executable",
    ),
    Extra(
        name="base64",
        description="Convert either direction between base64 blob and text",
    ),
    Extra(
        name="base85",
        description="Convert either direction between base85 blob and text",
    ),
    Extra(
        name="btreeinfo",
        description="btreeinfo virtual table that shows information about all btrees in an SQLite database file",
    ),
    # bytecode: part of library source, can't be separately built
    Extra(
        name="cksumvfs",
        description="A VFS shim that writes a checksum on each page of an SQLite database file",
        doc="cksumvfs.html",
    ),
    Extra(
        name="closure",
        description="A virtual table that finds the transitive closure of a parent/child relationship in a real table",
    ),
    Extra(
        name="completion",
        description="A virtual table that returns suggested completions for a partial SQL input",
        doc="completion.html",
    ),
    Extra(
        name="compress",
        description="SQL compression functions",
        lib_zlib=True,
    ),
    Extra(
        name="csv",
        description="A virtual table for reading CSV files",
        doc="csv.html",
    ),
    # dbstat: part of library source, can't be separately built
    Extra(
        name="decimal",
        description="Routines to implement arbitrary-precision decimal math",
    ),
    Extra(
        name="eval",
        description="Implements SQL function eval() which runs SQL statements recursively",
    ),
    Extra(
        name="fileio",
        description="Implements SQL functions readfile() and writefile(), and eponymous virtual type 'fsdir'",
        sources=["ext/misc/fileio.c", "../src/fileio_win32.c"],
    ),
    # ::TODO:: fossildelta once RBU extension is wrapped
    Extra(
        name="fuzzer",
        description="Virtual table that generates variations on an input word at increasing edit distances from the original",
    ),
    Extra(
        name="ieee754",
        description="functions for the exact display* and input of IEEE754 Binary64 floating-point numbers",
    ),
    Extra(
        name="memstat",
        description="Provides SQL access to the sqlite3_status64() and sqlite3_db_status() interfaces",
        doc="memstat.html",
    ),
    Extra(
        name="nextchar",
        description="Finds all valid 'next' characters for a string given a vocabulary",
    ),
    Extra(
        name="noop",
        description="Implements noop() functions useful for testing",
    ),
    Extra(
        name="prefixes",
        description="Table valued function providing all prefixes of a string",
    ),
    Extra(
        name="randomjson",
        description="Generates random json objects",
    ),
    Extra(
        name="regexp",
        description="Compact reasonably efficient posix extended regular expression matcher",
    ),
    Extra(
        name="rot13",
        description="rot13 function and collating sequence",
    ),
    Extra(
        name="sha1",
        description="SHA1 hash and query results hash",
    ),
    Extra(
        name="shathree",
        description="SHA3 hash and query results hash",
    ),
    Extra(
        name="spellfix",
        description="Search a large vocabulary for close matches",
        doc="spellfix1.html",
    ),
    Extra(
        name="sqlar",
        description="Utility functions for SQL archives",
        doc="sqlar.html#managing_sqlite_archives_from_application_code",
        lib_zlib=True,
    ),
    Extra(
        name="stmt",
        description="Virtual table with information about all prepared statements on a connection",
        doc="stmt.html",
    ),
    Extra(
        name="stmtrand",
        description="Function that returns the same sequence of random integers is returned for each invocation of the statement",
    ),
    # totype: hard codes byte order detection on processors from 2013
    Extra(
        name="uint",
        description="UINT collating sequence",
    ),
    Extra(
        name="unionvtab",
        description="Virtual table combining underlying tables from other databases",
    ),
    Extra(
        name="uuid",
        description="uuid functions",
    ),
    # vfslog: has to be compiled into the amalgamation
    Extra(
        name="vfsstat",
        description="VFS shim tracking call statistics",
    ),
    # vfstrace: has to be used with C code
    Extra(
        name="vtablog",
        description="Virtual table printing diagnostic information for interactive analysis and debugging",
    ),
    # vtshim: not useful
    # wholenumber: use generate_series
    Extra(
        name="zipfile",
        doc="zipfile.html",
        description="Read/Write access to simple archives",
        lib_zlib=True,
    ),
    Extra(
        name="zorder",
        description="Functions for z-order (Morton code) transformations",
    ),
    Extra(
        name="vec1",
        description="Vector search.  !Experimental! !Under development!",
        doc="vec1",
        sources=["vec1/vec1.c"],
    ),
    Extra(
        name="sqlite3_dbdump",
        type="executable",
        description="Converts the content of a SQLite database into UTF-8 text SQL statements that can be used to exactly recreate the original database",
        sources=["ext/misc/dbdump.c"],
        defines=[("DBDUMP_STANDALONE", 1)],
        lib_sqlite=True,
    ),
    Extra(
        name="sqlite3_dbhash",
        type="executable",
        description="Computes SHA1 of the contents of a SQLite database",
        sources=["tool/dbhash.c"],
        lib_sqlite=True,
    ),
    Extra(
        name="sqlite3_dbtotxt",
        sources=["tool/dbtotxt.c"],
        type="executable",
        description="Converts a binary file like a database into a friendly human readable text format",
        doc="src/file?name=tool/dbtotxt.md&ci=trunk",
    ),
    Extra(
        name="sqlite3_diff",
        type="executable",
        sources=["tool/sqldiff.c"]
        + (["tool/winmain.c"] if (sys.platform == "win32" and pathlib.Path("sqlite3/tool/winmain.c").exists()) else []),
        description="Displays content differences between SQLite databases",
        doc="sqldiff.html",
        lib_sqlite_stdio=True,
        lib_sqlite=True,
    ),
    Extra(
        name="sqlite3_expert",
        type="executable",
        sources=["ext/expert/expert.c", "ext/expert/sqlite3expert.c"],
        description="A simple system to propose useful indexes given a database and a set of SQL queries",
        lib_sqlite=True,
    ),
    Extra(
        name="sqlite3_getlock",
        type="executable",
        sources=["tool/getlock.c"],
        description="Unix only shows if and who is holding a database lock",
    ),
    Extra(
        name="sqlite3_index_usage",
        type="executable",
        sources=["tool/index_usage.c"],
        description="Given a database and a log database, shows how many times each index is used",
        lib_sqlite=True,
    ),
    Extra(
        name="sqlite3_normalize",
        type="executable",
        sources=["ext/misc/normalize.c"],
        description="Normalizes SQL text so private information can be removed, and to identify structurally identical queries",
        defines=[("SQLITE_NORMALIZE_CLI", 1)],
        lib_sqlite=True,
    ),
    Extra(
        name="sqlite3_offsets",
        type="executable",
        sources=["tool/offsets.c"],
        description="Shows length and offset for every TEXT or BLOB for a column of a table",
        lib_sqlite=True,
    ),
    Extra(
        name="sqlite3_rsync",
        type="executable",
        sources=["tool/sqlite3_rsync.c"],
        description="Database Remote-Copy Tool",
        doc="rsync.html",
        lib_sqlite=True,
    ),
    Extra(
        name="sqlite3_scrub",
        type="executable",
        sources=["ext/misc/scrub.c"],
        defines=[("SCRUB_STANDALONE", 1)],
        description="Makes a backup zeroing out all deleted content",
        lib_sqlite=True,
    ),
    Extra(
        name="sqlite3_shell",
        type="executable",
        sources=["shell.c"],
        description="Command line shell",
        doc="cli.html",
        lib_sqlite=True,
        # work around sqlite's mistaken handling of utf8 on windows.  it should be
        # using manifest but instead only has a hacky main thing going on
        defines=[("main", "main")],
    ),
    Extra(
        name="sqlite3_showdb",
        type="executable",
        sources=["tool/showdb.c"],
        lib_sqlite=True,
        description="Prints low level details about a database file",
    ),
    Extra(
        name="sqlite3_showjournal",
        type="executable",
        sources=["tool/showjournal.c"],
        description="Prints low level content of a journal",
    ),
    Extra(
        name="sqlite3_showlocks",
        type="executable",
        sources=["tool/showlocks.c"],
        description="Shows all posix advisory locks on a file",
    ),
    Extra(
        name="sqlite3_showshm",
        type="executable",
        sources=["tool/showshm.c"],
        description="Shows low level content of shm and wal-index files",
    ),
    Extra(
        name="sqlite3_showstat4",
        type="executable",
        sources=["tool/showstat4.c"],
        description="Shows contents of stat4 index of a database",
        lib_sqlite=True,
    ),
    Extra(
        name="sqlite3_showwal",
        type="executable",
        sources=["tool/showwal.c"],
        description="Shows low level content of a WAL file",
    ),
    Extra(
        name="sqlite3_sqlar",
        type="executable",
        sources=["sqlar/sqlar.c"],
        doc="sqlar/",
        description="Command line SQL archive tool",
        lib_sqlite=True,
    ),
    # these two are in the withdrawn 3.52.0 release
    #Extra(
    #    name="sqlite3_showtmlog",
    #    type="executable",
    #    sources=["tool/showtmlog.c"],
    #    description="Makes human/csv readable output from a tmstmpvfs log file",
    #),
    #Extra(
    #    name="tmstmpvfs",
    #    description="VFS shim that writes timestamps and other tracing information to the reserved bytes of each page, and also generates corresponding log files",
    #),
]

import os
import re
import setuptools._distutils.ccompiler as ccompiler
from setuptools._distutils.compilers.C.errors import CompileError, LinkError
from setuptools._distutils.sysconfig import customize_compiler
import subprocess
import pprint
import shutil
import platform

import logging


def c_quote(value: str, quote: str = '"'):
    # I originally tried to backslash escape double quotes but rc
    # required double backslashes and there was no way to make
    # everyone happy except to ban them
    assert '"' not in value
    return quote + value + quote


def make_windows_resource(manifest_filename: str | None, **fields):
    assert "FileDescription" in fields
    source = (pathlib.Path() / "sqlite3" / "src" / "sqlite3.rc").read_text()
    out: list[str] = []
    seen_value = False
    for line in source.splitlines():
        if line.strip().startswith("IDI_SQLITE ICON"):
            out.append(r'IDI_SQLITE ICON "sqlite3\\art\\sqlite370.ico"')
            continue
        if line.strip().split() == ["#include", '"sqlite3rc.h"']:
            out.append(f"#define SQLITE_RESOURCE_VERSION {','.join(Version['SQLITE_VERSION'].split('.'))}")
            if manifest_filename:
                # inject manifest here
                location = c_quote(manifest_filename.replace("\\", "\\\\"))
                out.append(f"CREATEPROCESS_MANIFEST_RESOURCE_ID RT_MANIFEST {location}")
            continue
        if line.strip().startswith("VALUE"):
            seen_value = True
            parts = [p.strip() for p in line.split()]
            assert parts[0] == "VALUE"
            assert parts[1].startswith('"') and parts[1].endswith('",')
            name = parts[1][1:-2]
            if name in fields:
                v = fields.pop(name)
                if name == "FileDescription":
                    v = "(APSW packaged) " + v
                out.append(f'      VALUE "{name}", {c_quote(v)}')
            else:
                out.append(line)
            continue

        if seen_value and line.strip() == "END":
            seen_value = False
            for k, v in fields.items():
                out.append(f'      VALUE "{k}", {c_quote(v)}')
        out.append(line)
    return "\r\n".join(out) + "\r\n"


unix_resource_header = """
#ifdef APSW_SUPPORTS_ATTRIBUTE
#if defined(__APPLE__)
/* Mach-O section */
__attribute__((section("__TEXT,__apsw"), used))
#else
/* ELF note section */
__attribute__((section(".note.apsw"), used))
#endif
#endif
const char apsw_resource_metadata[] =
    "APSW-Note: Unmodified SQLite project artifact. Packaged by APSW for convenience.\\n"
"""

windows_manifest = """
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<assembly xmlns="urn:schemas-microsoft-com:asm.v1" manifestVersion="1.0">
  <application>
    <windowsSettings>
      <activeCodePage xmlns="http://schemas.microsoft.com/SMI/2019/WindowsSettings">UTF-8</activeCodePage>
    </windowsSettings>
  </application>
</assembly>
"""


def resource_file(build_dir, compiler, extra: Extra) -> str:
    if compiler.compiler_type == "msvc":
        if extra.type == "executable":
            with open(build_dir / "utf8_manifest", "wt") as mf:
                mf.write(windows_manifest)
        with open(build_dir / f"{extra.name}.rc", "wt") as f:
            f.write(
                make_windows_resource(
                    mf.name if extra.type == "executable" else None,
                    FileDescription=f"SQLite {extra.description}",
                    InternalName=f"sqlite3 - {extra.name}",
                    Comment="Unmodified SQLite project artifact. Packaged by APSW for convenience",
                )
            )

    else:
        with open(build_dir / f"{extra.name}_rsrc.c", "wt") as f:
            f.write(f"""{unix_resource_header}
                "Copyright: https://sqlite.org/copyright.html\\n"
                "Description: {c_quote(extra.description, quote="")}\\n"
                "Documentation: {extra.doc}\\n"
            """)
            for k, v in sorted(Version.items()):
                f.write(f'"{k}: {v}\\n"')
            f.write(";\n")
    f.close()
    return f.name


Version = {
    "SQLITE_VERSION": None,
    "SQLITE_SOURCE_ID": None,
    "SQLITE_SCM_BRANCH": None,
    "SQLITE_SCM_TAGS": None,
    "SQLITE_SCM_DATETIME": None,
}


def get_version():
    pat = r"#define\s+(" + "|".join(Version.keys()) + r')\s+"(.*)"\s*$'
    for line in pathlib.Path("sqlite3/sqlite3.c").read_text(encoding="utf8").splitlines():
        if mo := re.match(pat, line):
            Version[mo.group(1)] = mo.group(2)

    for k, v in Version.items():
        if v is None:
            raise Exception(f"Version {k} not found")


def exc_type(exc: Exception) -> str:
    # we have to do this because using Compile/Link Error in the
    # except doesn't work because distutils is all messed up via
    # setuptools shimming
    if type(exc).__name__ not in (CompileError.__name__, LinkError.__name__):
        raise
    return type(exc).__name__


@dataclasses.dataclass
class CompilerImplementation:
    """What is the actual compiler

    Sometimes call we know is it is cc so the implementation
    details are determined by compile_check"""

    name: Literal["gcc"] | Literal["clang"] | Literal["msvc"] | Literal["unknown"]
    version: str
    "random text"
    misc: set[str]
    "the various other things"


def do_build(what: set[str], verbose: bool, fail_fast: bool = False):
    get_version()
    compiler = ccompiler.new_compiler(verbose=True)
    # this configures compiler to have the same flags as python was built with
    customize_compiler(compiler)
    # we also want the same libraries which the above doesn't do and
    # we need pthread/dl on various platforms.
    if compiler.compiler_type == "unix" and (libs := sysconfig.get_config_var("LIBS")):
        for lib in libs.split():
            if lib.startswith("-l"):
                compiler.add_library(lib[2:])

    compile_extra_preargs = None
    link_extra_preargs = None

    compiler.add_include_dir("sqlite3")
    compiler.add_include_dir("sqlite3/zlib")

    # where the build artifacts go
    build_dir = (
        pathlib.Path()
        / "build"
        / "sqlite_extra"
        / f"{sysconfig.get_platform()}-{sysconfig.get_python_version()}{getattr(sys, 'abiflags', '')}"
    )
    shutil.rmtree(build_dir, ignore_errors=True)
    compiler.mkpath(str(build_dir))

    # where the final binaries go
    output_dir = pathlib.Path() / "apsw" / "sqlite_extra_binaries"
    for p in output_dir.glob("*"):
        if p.name.lower() == "readme.md":
            continue
        p.unlink()

    print("Checking if compiler works")
    compile_check_name = get_compile_check(build_dir)
    try:
        objs = compiler.compile([compile_check_name], output_dir=str(build_dir))
        compiler.link_executable(objs, "compile_check", output_dir=str(build_dir))
        out_name = f"{build_dir}/compile_check{compiler.exe_extension if compiler.exe_extension else ''}"
        p = subprocess.run([out_name], capture_output=True, encoding="utf8", text=True)
        p.check_returncode()
        info = p.stdout.splitlines()
        ci = CompilerImplementation(info[0], info[1], set(info[2:]))
        print(ci)
        print("   OK")
    except Exception as exc:
        print(f"      Failed to compile hello world because {exc}")
        return

    # we want the linker to strip debug rather than trying to do so at
    # copy time which caused various problems
    if ci.name in ("gcc", "clang"):
        if sys.platform == "darwin":
            link_extra_preargs = ["-Wl,-S"]
        else:
            link_extra_preargs = ["-Wl,--strip-debug"]

    # figure out if compile and link pre args work, and remove them if not
    new_objs = objs
    if compile_extra_preargs:
        print(f"Checking {compile_extra_preargs=}")
        try:
            new_objs = compiler.compile(
                [compile_check_name], output_dir=str(build_dir), extra_preargs=compile_extra_preargs
            )
            print("   OK")
        except Exception as exc:
            if exc_type(exc):
                print(f"   Failed - skipping them")
                compile_extra_preargs = None
    if link_extra_preargs:
        print(f"Checking {link_extra_preargs=}")
        try:
            compiler.link_executable(
                new_objs, "compile_check_link_args", output_dir=str(build_dir), extra_preargs=link_extra_preargs
            )
            print("   OK")
        except Exception as exc:
            if exc_type(exc):
                print(f"    Failed - skipping them")
                link_extra_preargs = None

    # figure out if attribute is understood
    if compiler.compiler_type != "msvc":
        print("Checking if attribute section and used are supported")
        with open(build_dir / "attr_test.c", mode="wt") as f:
            f.write(unix_resource_header + ";")

        try:
            compiler.compile(
                [f.name],
                output_dir=str(build_dir),
                macros=[("APSW_SUPPORTS_ATTRIBUTE", 1)],
                extra_preargs=compile_extra_preargs,
            )
            compiler.macros.append(("APSW_SUPPORTS_ATTRIBUTE", 1))
            print("   Supported")
        except Exception as exc:
            if exc_type(exc):
                print("   NOT SUPPORTED")

    # build sqlite3 library
    print(">>> sqlite3 library")

    SQLITE_LIB_NAME = "sqlite3_tool"

    lib_enables = "CARRAY COLUMN_METADATA DBPAGE_VTAB DBSTAT_VTAB FTS4 FTS5 GEOPOLY MATH_FUNCTIONS PERCENTILE PREUPDATE_HOOK RTREE SESSION STAT4".split()

    macros = [(f"SQLITE_ENABLE_{enable}", 1) for enable in lib_enables]

    # add in others
    macros.extend(
        [
            ("SQLITE_USE_URI", 1),
            ("SQLITE_THREADSAFE", 2),
            ("SQLITE_ENABLE_COLUMN_METADATA", 1),
        ]
    )

    cfg = pathlib.Path("sqlite3") / "sqlite_cfg.h"
    if cfg.exists():
        macros.append(("_HAVE_SQLITE_CONFIG_H", 1))

    # we need a subset of zlib source files
    zlib_sources = [
        str(pathlib.Path("sqlite3/zlib") / f"{name}.c")
        for name in "adler32 crc32 deflate inflate inffast inftrees zutil trees compress uncompr".split()
    ]

    if compiler.compiler_type == "msvc":
        macros.append(("SQLITE_API", "__declspec(dllexport)"))
    try:
        lib_resource = resource_file(
            build_dir, compiler, Extra(name="libsqlite3", description="SQLite 3 library", doc="")
        )
        lib_objs = compiler.compile(
            [str(pathlib.Path("sqlite3") / "sqlite3.c"), lib_resource] + zlib_sources,
            output_dir=str(build_dir),
            macros=macros,
            extra_preargs=compile_extra_preargs,
        )
        # dlopen libraries are different than shared libraries so flags have to be given
        so_link_flags = None
        match compiler.compiler_type:
            case "msvc":
                so_link_flags = ["/DLL"]
            case "unix":
                linker_so_orig = compiler.linker_so
                if sys.platform == "darwin":
                    so_link_flags = ["-dynamiclib", "-install_name", f"@rpath/lib{SQLITE_LIB_NAME}.dylib"]
                    compiler.linker_so = [l for l in linker_so_orig if l != "-bundle"]

        # this ensures the library has debug stripped
        if link_extra_preargs:
            so_link_flags = (so_link_flags or []) + link_extra_preargs

        # we have to figure out the library filename
        before = set(build_dir.glob("*"))

        compiler.link_shared_lib(lib_objs, SQLITE_LIB_NAME, output_dir=str(build_dir), extra_preargs=so_link_flags)
        if compiler.compiler_type == "unix":
            compiler.linker_so = linker_so_orig
        new_files = set(build_dir.glob("*")) - before
        if len(new_files) > 1 and compiler.compiler_type == "msvc":
            new_files = {f for f in new_files if str(f).lower().endswith(".dll")}
        assert len(new_files) == 1
        sqlite_lib_filename = str(list(new_files)[0])
        if sys.platform == "darwin":
            # compiler above does .so as extension but macos requires .dylib
            new_name = str(pathlib.Path(sqlite_lib_filename).with_suffix(".dylib"))
            os.rename(sqlite_lib_filename, new_name)
            sqlite_lib_filename = new_name
        shutil.copy2(sqlite_lib_filename, str(output_dir))

    except Exception as exc:
        print(f"Compiling SQLite failed {exc_type(exc)} - giving up")
        return

    # sqlite stdio
    print(">>> sqlite3 stdio library")
    try:
        lib_stdio_objs = compiler.compile(
            [str(pathlib.Path("sqlite3") / "ext" / "misc" / "sqlite3_stdio.c")],
            output_dir=str(build_dir),
            extra_preargs=compile_extra_preargs,
        )
    except Exception as exc:
        print(f"Compiling sqlite3_stdio failed {exc_type(exc)} - giving up")
        return

    lib_stdio_include = pathlib.Path("sqlite3") / "ext" / "misc"

    failed: list[tuple[Extra, str]] = []

    try:
        for extra in extras:
            if extra.type not in what:
                continue
            print(f">>> {extra.name:30}({extra.type})")
            missing = []
            for source in extra.sources:
                if not (pathlib.Path("sqlite3") / source).exists():
                    missing.append(source)
            if missing:
                failed.append((extra, f"Missing source files {missing}"))
                if fail_fast:
                    return
                continue

            resource = resource_file(build_dir, compiler, extra)
            include_dirs = [str(lib_stdio_include)] if extra.lib_sqlite_stdio else None

            avx_pre_args = None

            if extra.name == "vec1":
                avx_pre_args = []
                match platform.machine().lower():
                    case "x86_64" | "amd64" | "i386" | "i686" | "x86":
                        is_x86 = True

                    case _:
                        is_x86 = False

                match compiler.compiler_type:
                    case "msvc":
                        if is_x86:
                            avx_pre_args.append("/arch:AVX2")
                        avx_pre_args.append("/fp:fast")

                    case "unix":
                        if ci.name in ("gcc", "clang"):
                            avx_pre_args.append("-O3")
                            if is_x86:
                                avx_pre_args.extend(("-mavx2", "-mfma"))

            try:
                if avx_pre_args:
                    # we have to compile the file twice with different defines and compiler flags
                    # first scalar and then avx2
                    macros1 = extra.defines or []
                    if is_x86:
                        macros1.append(("VEC1SIMD", "SCALAR"))

                    macros2 = extra.defines or []
                    macros2.append(("VEC1SIMD", "AVX2"))
                    preargs_2 = compile_extra_preargs or []
                    preargs_2.extend(avx_pre_args)

                    # we have to copy the source file
                    assert len(extra.sources) == 1

                    avx_c = build_dir / "vec1_avx.c"
                    shutil.copy2(pathlib.Path("sqlite3") / extra.sources[0], avx_c)

                    objs = compiler.compile(
                        [str(pathlib.Path("sqlite3") / filename) for filename in extra.sources]
                        + [resource]
                        + (zlib_sources if extra.lib_zlib else []),
                        output_dir=str(build_dir),
                        include_dirs=include_dirs,
                        extra_preargs=compile_extra_preargs,
                        macros=macros1,
                    )

                    if is_x86:
                        objs.extend(
                            compiler.compile(
                                [avx_c],
                                output_dir=str(build_dir),
                                include_dirs=include_dirs,
                                extra_preargs=preargs_2,
                                macros=macros2,
                            )
                        )

                else:
                    objs = compiler.compile(
                        [str(pathlib.Path("sqlite3") / filename) for filename in extra.sources]
                        + [resource]
                        + (zlib_sources if extra.lib_zlib else []),
                        output_dir=str(build_dir),
                        include_dirs=include_dirs,
                        extra_preargs=compile_extra_preargs,
                        macros=extra.defines,
                    )

            except Exception as exc:
                failed.append((extra, exc_type(exc)))
                if fail_fast:
                    return
                continue

            match extra.type:
                case "extension":
                    # we don't support these for extensions
                    assert not extra.lib_sqlite and not extra.lib_sqlite_stdio

                    # .so works just fine on macos, but sqlite only looks
                    # for ,dylib if you don't give the extension
                    out_name = f"{extra.name}{compiler.shared_lib_extension if sys.platform != 'darwin' else '.dylib'}"
                    try:
                        compiler.link_shared_object(
                            objs, out_name, output_dir=str(build_dir), extra_preargs=link_extra_preargs
                        )
                    except Exception as exc:
                        failed.append((extra, f"Linking as extension {exc_type(exc)} error"))
                        if fail_fast:
                            return
                        continue

                case "executable":
                    if extra.lib_sqlite_stdio:
                        objs.extend(lib_stdio_objs)

                    out_name = f"{extra.name}{compiler.exe_extension if compiler.exe_extension else ''}"

                    libraries = None

                    runtime_library_dirs = None
                    if extra.lib_sqlite:
                        match compiler.compiler_type:
                            case "msvc":
                                libraries = [SQLITE_LIB_NAME]
                            case "unix":
                                libraries = [SQLITE_LIB_NAME, "m"]
                                runtime_library_dirs = ["@loader_path" if sys.platform == "darwin" else "$ORIGIN"]
                            case _:
                                failed.append((extra, "Don't know how to set runtime directory to same as executable"))
                                if fail_fast:
                                    return
                                continue
                    try:
                        compiler.link_executable(
                            objs,
                            extra.name,
                            output_dir=str(build_dir),
                            libraries=libraries,
                            extra_preargs=link_extra_preargs,
                            library_dirs=[str(build_dir)] if libraries else None,
                            runtime_library_dirs=runtime_library_dirs,
                            # we provide a proper manifest in the resource and distutils
                            # tells it to automatically create one, so that has to be overridden
                            extra_postargs=["/MANIFEST:NO"] if compiler.compiler_type == "msvc" else None,
                        )
                    except Exception as exc:
                        failed.append((extra, f"Linking executable {exc_type(exc)}"))
                        if fail_fast:
                            return
                        continue
                    # ::TODO:: is this needed? macos xattr -d com.apple.quarantine str(build_dir / out_name)

                case _:
                    raise NotImplementedError

            try:
                os.remove(str(output_dir / out_name))
            except FileNotFoundError:
                pass

            shutil.copy2(str(build_dir / out_name), str(output_dir / out_name))

            logging.info("")

    finally:
        if failed:
            print(f"\n{len(failed)} failures\n")
            for extra, reason in failed:
                print(reason)
                pprint.pprint(extra)
                print()


def get_compile_check(build_dir):
    with open(build_dir / "compile_check.c", mode="wt") as f:
        f.write(r"""
#include <stdio.h>

int main(int argc, char **argv)
{
#ifdef __clang__
    printf("clang\n");
    printf("%d.%d.%d\n",__clang_major__, __clang_minor__, __clang_patchlevel__);
#elif defined(_MSC_VER)
    printf("msvc\n");
    printf("%d\n", _MSC_FULL_VER);
#elif defined(__GNUC__)
    printf("gcc\n");
    printf("%d.%d.%d\n", __GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__);
#else
    printf("unknown\n");
    printf("unknown\n");
#endif
#ifdef __STDC_VERSION__
    printf("__STDC_VERSION__=%ld\n", (long)__STDC_VERSION__);
#endif
""")
        for item in ("char", "int", "long", "long long", "int*", "void*"):
            f.write(rf""" printf("sizeof({item})=%d\n", (int)sizeof({item}));""" + "\n")

        for define in "NDEBUG __OPTIMIZE__ __OPTIMIZE_SIZE__ __NO_INLINE__ __STDC_HOSTED__ __FAST_MATH__ __STDC_NO_ATOMICS__ __STDC_NO_THREADS__ __PIC__ __PIE__ __SANITIZE_ADDRESS__ _MT _DLL _DEBUG __ELF__ __GLIBC__ __MUSL__ __GNUC_GNU_INLINE__ __STRICT_ANSI__".split():
            f.write(rf"""
#ifdef {define}
    printf("{define}\n");
#endif
            """)

        f.write("return 0;}")
    return f.name


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser()
    parser.set_defaults(function=None)

    subparsers = parser.add_subparsers()
    p = subparsers.add_parser("compile", help="Build things")
    p.set_defaults(function="compile")
    p.add_argument("-v", default=False, action="store_true", dest="verbose", help="Show compiler command")
    p.add_argument("-f", default=False, action="store_true", dest="fail_fast", help="Stop on first failure")
    p.add_argument("--only", choices=["extension", "executable"], help="Only build one type")

    p = subparsers.add_parser("json", help="Generate JSON description file")
    p.set_defaults(function="json")
    p.add_argument("outfile", help="JSON file to output")

    p = subparsers.add_parser("rst", help="Generate rst inclusion file")
    p.set_defaults(function="rst")
    p.add_argument("outfile", help="rst file to output")

    options = parser.parse_args()

    match options.function:
        case "compile":
            logging.basicConfig(level=logging.DEBUG if options.verbose else logging.WARNING, format="    %(message)s")
            what = {"extension", "executable"} if not options.only else {options.only}
            do_build(what, options.verbose, options.fail_fast)

        case "json":
            out = {}
            for extra in extras:
                out[extra.name] = {"description": extra.description, "type": extra.type}

            with open(options.outfile, "wt") as f:
                json.dump(out, f, indent=4, sort_keys=True)

        case "rst":
            with open(options.outfile, "wt") as f:
                print(
                    """
.. generated by tools/vend.py - edit that not this

Programs
--------

                """,
                    file=f,
                )

                for extra in sorted(extras, key=lambda x: x.name):
                    if extra.type != "executable":
                        continue

                    print(f"{extra.name} (`doc <{extra.doc}>`__)", file=f)
                    print(file=f)
                    print(f"   {extra.description}", file=f)
                    print(file=f)

                print(
                    """
Extensions
----------

.. list-table::
    :header-rows: 1
    :widths: auto

    * - Name
      - Doc
      - Description
      - Registers
                """,
                    file=f,
                )

                import apsw
                import apsw.sqlite_extra

                db = apsw.Connection(":memory:")
                # cause it to be created now
                db.execute("select * from pragma_collation_list").get

                def details(name):
                    fn_before = set(row[0] for row in db.execute("select name from pragma_function_list"))
                    vfs_before = set(apsw.vfs_names())
                    mod_before = set(row[0] for row in db.execute("SELECT name FROM pragma_module_list"))
                    col_before = set(row[0] for row in db.execute("SELECT name FROM pragma_collation_list"))
                    apsw.sqlite_extra.load(db, name)
                    fn_after = set(db.execute("select name from pragma_function_list").get)
                    vfs_after = set(apsw.vfs_names())
                    mod_after = set(row[0] for row in db.execute("SELECT name FROM pragma_module_list"))
                    col_after = set(row[0] for row in db.execute("SELECT name FROM pragma_collation_list"))

                    if name == "anycollseq":
                        print("      - Fallback collation", file=f)
                        return
                    print("      -", file=f)
                    for kind, diff in (
                        ("Collation", col_after - col_before),
                        ("Function", fn_after - fn_before),
                        ("VFS", vfs_after - vfs_before),
                        ("VTable", mod_after - mod_before),
                    ):
                        if diff:
                            print(f"        * {kind}:", " ".join(f":code:`{v}`" for v in sorted(diff)), file=f)

                for extra in sorted(extras, key=lambda x: x.name):
                    if extra.type != "extension":
                        continue

                    print(f"    * - {extra.name}", file=f)
                    print(f"      - `link <{extra.doc}>`__", file=f)
                    print(f"      - {extra.description}", file=f)
                    details(extra.name)
                    print(file=f)

        case _:
            parser.error("You must specify a sub-command to run")
