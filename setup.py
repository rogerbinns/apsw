#!/usr/bin/env python3

# See the accompanying LICENSE file.

from __future__ import annotations

import os
import sys
import shlex
import glob
import re
import time
import zipfile
import tarfile
import subprocess
import sysconfig
import shutil
import pathlib
import contextlib
from dataclasses import dataclass

from setuptools import setup, Extension, Command
from setuptools.command import build_ext, sdist

try:
    # current setuptools has build
    from setuptools.command import build
except ImportError:
    # but older pythons with older versions don't
    from distutils.command import build

# This is used to find the compiler and flags for building the test extension
import distutils.ccompiler

include_dirs = ["src"]
library_dirs = []
define_macros = []
libraries = []

##
## Workaround for https://github.com/pypa/cibuildwheel/issues/1487
##

patch = False
try:
    import distutils.dist

    if hasattr(distutils.dist.Distribution, "find_config_files"):
        patch = True
except Exception:
    pass

if patch:

    def monkey_patched_find_config_files(self):
        res = orig_find_local_files(self)
        if os.path.isfile("setup.apsw"):
            res.append("setup.apsw")
        return res

    orig_find_local_files = distutils.dist.Distribution.find_config_files
    distutils.dist.Distribution.find_config_files = monkey_patched_find_config_files

##
## End of customizations
##

project_urls = {
    "Changelog": "https://rogerbinns.github.io/apsw/changes.html",
    "Documentation": "https://rogerbinns.github.io/apsw/",
    "Issue Tracker": "https://github.com/rogerbinns/apsw/issues",
    "Code": "https://github.com/rogerbinns/apsw",
    "Example": "https://rogerbinns.github.io/apsw/example.html",
}


# python 2 and 3 print equivalent
def write(*args):
    # py2 won't allow optional keyword arg on end, so work around it
    dest = sys.stdout
    if args[-1] == sys.stderr:
        dest = args[-1]
        args = args[:-1]
    dest.write(" ".join(args) + "\n")
    dest.flush()


# ensure files are closed
def read_whole_file(name, mode):
    assert mode == "rt"
    f = open(name, mode, encoding="utf8")
    try:
        return f.read()
    finally:
        f.close()


def write_whole_file(name, mode, data):
    assert mode == "wt"
    f = open(name, mode, encoding="utf8")
    try:
        f.write(data)
    finally:
        f.close()


# work out version number
version = read_whole_file(os.path.join("src", "apswversion.h"), "rt").split()[2].strip('"')


def sqliteversion(v):
    assert len(v.split(".")) >= 4
    return ".".join(v.split(".")[:3])


# They keep messing with where files are in URI
def fixup_download_url(url):
    ver = re.search("3[0-9]{6}", url)
    if ver:
        ver = int(ver.group(0))
        if ver >= 3480000:
            year = "2025"
        elif ver >= 3450000:
            year = "2024"
        elif ver >= 3410000:
            year = "2023"
        elif ver >= 3370200:
            year = "2022"
        elif ver >= 3340100:
            year = "2021"
        if "/" + year + "/" not in url:
            url = url.split("/")
            url.insert(3, year)
            return "/".join(url)
    return url


# Run test suite
class run_tests(Command):
    description = "Run test suite"

    # I did originally try using 'verbose' as the option but it turns
    # out that is builtin and defaults to 1 (--quiet is also builtin
    # and forces verbose to 0)
    user_options = [
        ("show-tests", "v", "Show each test being run"),
        ("locals", None, "Show local variables in test failure"),
    ]

    # see if you can find boolean_options documented anywhere
    boolean_options = ["show-tests", "locals"]

    def initialize_options(self):
        self.show_tests = 0
        self.locals = False

    def finalize_options(self):
        pass

    def run(self):
        import unittest
        import apsw.tests

        apsw.tests.setup()
        suite = unittest.TestLoader().loadTestsFromModule(apsw.tests)
        # verbosity of zero doesn't print anything, one prints a dot
        # per test and two prints each test name
        result = unittest.TextTestRunner(verbosity=self.show_tests + 1, tb_locals=self.locals).run(suite)
        if not result.wasSuccessful():
            sys.exit(1)


# A hack we dont't document
class build_test_extension(Command):
    description = "Compiles APSW test loadable extension"

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        name = "testextension.sqlext"

        def v(n):
            return sysconfig.get_config_var(n)

        # unixy platforms have this, and is necessary to match the 32/64 bitness of Python itself
        if v("CC"):
            cc = f"{ v('CC') } { v('CFLAGS') } { v('CCSHARED') } -Isqlite3 -c src/testextension.c"
            ld = f"{ v('LDSHARED') } testextension.o -o { name }"

            for cmd in cc, ld:
                print(cmd)
                subprocess.run(cmd, shell=True, check=True)
        else:
            # windows mostly
            compiler = distutils.ccompiler.new_compiler(verbose=True)
            compiler.add_include_dir("sqlite3")
            compiler.add_include_dir(".")
            preargs = ["/Gd"] if "msvc" in str(compiler.__class__).lower() else ["-fPIC"]
            objs = compiler.compile(["src/testextension.c"], extra_preargs=preargs)
            compiler.link_shared_object(objs, name)


# deal with various python version compatibility issues with how
# to treat returned web data as lines of text
def fixupcode(code):
    if type(code) != bytes:
        code = code.read()
    if type(code) == bytes:
        code = code.decode("iso8859-1")

    if type(code) == str:
        return [l + "\n" for l in code.split("\n")]

    return code


fetch_parts = []


class fetch(Command):
    description = "Automatically downloads SQLite and components"
    user_options = [
        ("version=", None, f"Which version of SQLite/components to get (default { sqliteversion(version) })"),
        ("missing-checksum-ok", None, "Continue on a missing checksum (default abort)"),
        ("sqlite", None, "Download SQLite amalgamation"),
        ("all", None, "Download all downloadable components"),
    ]
    fetch_options = ["sqlite"]
    boolean_options = fetch_options + ["all", "missing-checksum-ok"]

    def initialize_options(self):
        self.version = None
        self.sqlite = False
        self.all = False
        self.missing_checksum_ok = False

    def finalize_options(self):
        global fetch_parts
        if self.version in ("self", None):
            self.version = sqliteversion(version)
        if self.all:
            for i in self.fetch_options:
                setattr(self, i, True)
        for i in self.fetch_options:
            fetch_parts.append(i)

    def run(self):
        # work out the version
        if self.version == "latest":
            write("  Getting download page to work out latest SQLite version")
            page = self.download("https://sqlite.org/download.html", text=True, checksum=False)
            match = re.search(r"sqlite-amalgamation-3([0-9][0-9])([0-9][0-9])([0-9][0-9])\.zip", page)
            if match:
                self.version = "3.%d.%d.%d" % tuple([int(match.group(n)) for n in range(1, 4)])
                assert self.version.endswith(".0")  # sqlite doesn't use last component so we do now
                self.version = sqliteversion(self.version)
            else:
                write("Unable to determine latest SQLite version.  Use --version=VERSION", sys.stderr)
                write("to set version - eg setup.py fetch --version=3.6.18", sys.stderr)
                sys.exit(17)
            write("    Version is " + self.version)
        # now get each selected component
        downloaded = 0

        v = [int(x) for x in self.version.split(".")]
        assert len(v) == 3
        v.append(0)
        self.webversion = "%d%02d%02d%02d" % tuple(v)

        ## The amalgamation
        if self.sqlite:
            write("  Getting the SQLite amalgamation")

            AURL = "https://sqlite.org/sqlite-autoconf-%s.tar.gz" % (self.webversion,)

            AURL = fixup_download_url(AURL)

            data = self.download(AURL, checksum=True)

            if os.path.exists("sqlite3"):
                shutil.rmtree("sqlite3")

            # if you get an exception here it is likely that you don't have the python zlib module
            import zlib

            tar = tarfile.open("nonexistentname to keep old python happy", "r", data)
            configmember = None
            kwargs = {}
            if sys.version_info >= (3, 11, 4):
                kwargs["filter"] = "tar"
            for member in tar.getmembers():
                tar.extract(member, **kwargs)
                # find first file named configure
                if not configmember and member.name.endswith("/configure"):
                    configmember = member
            tar.close()
            # the directory name has changed a bit with each release so try to work out what it is
            if not configmember:
                write("Unable to determine directory it extracted to.", dest=sys.stderr)
                sys.exit(19)
            dirname = configmember.name.split("/")[0]
            os.rename(dirname, "sqlite3")
            if sys.platform != "win32":
                write("    Running configure to work out SQLite compilation flags")
                subprocess.check_call(["./configure"], cwd="sqlite3")
            downloaded += 1

        if not downloaded:
            write("You didn't specify any components to fetch.  Use")
            write("   setup.py fetch --help")
            write("for a list and details")
            raise ValueError("No components downloaded")

    # A function for verifying downloads
    def verifyurl(self, url, data):
        d = ["%s" % (len(data),)]
        import hashlib

        d.append(hashlib.sha256(data).hexdigest())
        d.append(hashlib.sha3_256(data).hexdigest())

        write("    Length:", d[0], " SHA256:", d[1], " SHA3_256:", d[2])
        sums = os.path.join(os.path.dirname(__file__), "checksums")
        for line in read_whole_file(sums, "rt").split("\n"):
            line = line.strip()
            if len(line) == 0 or line[0] == "#":
                continue
            l = [l.strip() for l in line.split()]
            if len(l) != 4:
                write("Invalid line in checksums file:", line, sys.stderr)
                raise ValueError("Bad checksums file")
            if l[0] == url:
                if l[1:] == d:
                    write("    Checksums verified")
                    return
                if l[1] != d[0]:
                    write("Length does not match.  Expected", l[1], "download was", d[0])
                if l[2] != d[1]:
                    write("SHA256 does not match.  Expected", l[2], "download was", d[1])
                if l[3] != d[2]:
                    write("SHA3_256 does not match.  Expected", l[3], "download was", d[2])
                write(
                    "The download does not match the checksums distributed with APSW.\n"
                    "The download should not have changed since the checksums were\n"
                    "generated.  The cause could be anything from network corruption\n"
                    "to a malicious attack."
                )
                raise ValueError("Checksums do not match")
        # no matching line
        write("    (Not verified.  No match in checksums file)")
        if not self.missing_checksum_ok:
            raise ValueError("No checksum available.  Use --missing-checksum-ok option to continue")

    # download a url
    def download(self, url, text=False, checksum=True):
        import urllib.request

        urlopen = urllib.request.urlopen
        import io

        bytesio = io.BytesIO

        write("    Fetching", url)
        count = 0
        while True:
            try:
                if count:
                    write("        Try #", str(count + 1))
                try:
                    page = urlopen(url).read()
                except:
                    # Degrade to http if https is not supported
                    e = sys.exc_info()[1]
                    if count >= 4 and url.startswith("https:"):
                        write("        [Python has https issues? - using http instead]")
                        page = urlopen(url.replace("https://", "http://")).read()
                    else:
                        raise
                break
            except:
                write("       Error ", str(sys.exc_info()[1]))
                time.sleep(3.14 * count)
                count += 1
                if count >= 10:
                    raise

        if text:
            page = page.decode("iso8859_1")

        if checksum:
            self.verifyurl(url, page)

        if not text:
            page = bytesio(page)

        return page


# We allow enable/omit to be specified to build and then pass them to build_ext
build_enable = None
build_omit = None
build_enable_all_extensions = False

bparent = build.build


class apsw_build(bparent):
    user_options = bparent.user_options + [
        ("enable=", None, "Enable SQLite options (comma separated list)"),
        ("omit=", None, "Omit SQLite functionality (comma separated list)"),
        ("enable-all-extensions", None, "Enable all SQLite extensions"),
        ("fetch", None, "Fetches SQLite for pypi based build"),
    ]
    boolean_options = bparent.boolean_options + ["enable-all-extensions", "fetch"]

    def __init__(self, dist):
        self._saved_dist = dist
        bparent.__init__(self, dist)

    def initialize_options(self):
        v = bparent.initialize_options(self)
        self.enable = None
        self.omit = None
        self.enable_all_extensions = build_enable_all_extensions
        self.fetch = False
        return v

    def finalize_options(self):
        global build_enable, build_omit, build_enable_all_extensions
        build_enable = self.enable
        build_omit = self.omit
        build_enable_all_extensions = self.enable_all_extensions
        if self.fetch:
            fc = fetch(self._saved_dist)
            fc.initialize_options()
            fc.all = True
            fc.finalize_options()
            fc.run()
        return bparent.finalize_options(self)


def findamalgamation():
    amalgamation = (
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqlite3.c"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqlite3", "sqlite3.c"),
    )
    for path in amalgamation:
        if os.path.exists(path):
            return path
    return None


def update_type_stubs_old_names(include_old: bool) -> None:
    stubs = pathlib.Path("apsw/__init__.pyi").read_text(encoding="utf8")
    new_stubs = []
    for line in stubs.split("\n"):
        if line.endswith("## OLD-NAME") and include_old == line.startswith("#"):
            line = ("#" + line) if not include_old else line[1:]
        new_stubs.append(line)
    new_stubs = "\n".join(new_stubs)
    if new_stubs != stubs:
        print("Updating type stubs to", "include" if include_old else "exclude", "old names")
        pathlib.Path("apsw/__init__.pyi").write_text(new_stubs, encoding="utf8")


beparent = build_ext.build_ext


class apsw_build_ext(beparent):
    user_options = beparent.user_options + [
        ("enable=", None, "Enable SQLite options (comma separated list)"),
        ("omit=", None, "Omit SQLite functionality (comma separated list)"),
        ("enable-all-extensions", None, "Enable all SQLite extensions"),
        ("use-system-sqlite-config", None, "Uses system SQLite library config (enabled/omitted APIs etc)"),
        (
            "definevalues=",
            None,
            "Additional defines eg --definevalues SQLITE_MAX_ATTACHED=37,SQLITE_EXTRA_INIT=mycore_init",
        ),
        ("apsw-no-old-names", None, "Old non-PEP8 names are excluded"),
    ]
    boolean_options = beparent.boolean_options + [
        "enable-all-extensions",
        "use-system-sqlite-config",
        "apsw-no-old-names",
    ]

    def initialize_options(self):
        v = beparent.initialize_options(self)
        self.enable = build_enable
        self.omit = build_omit
        self.enable_all_extensions = build_enable_all_extensions
        self.definevalues = None
        self.use_system_sqlite_config = False
        self.apsw_no_old_names = False
        return v

    def finalize_options(self):
        v = beparent.finalize_options(self)

        if self.enable_all_extensions:
            exts = [
                "fts4",
                "fts3",
                "fts3_parenthesis",
                "rtree",
                "stat4",
                "fts5",
                "geopoly",
                "math_functions",
                "dbstat_vtab",
            ]
            if not self.omit or "icu" not in self.omit.split(","):
                if get_icu_config():
                    exts.append("icu")
            if not self.enable:
                self.enable = ",".join(exts)
            else:
                self.enable = self.enable + "," + ",".join(exts)

        ext = self.extensions[0]

        if not ext.define_macros:
            ext.define_macros = []
        if not ext.depends:
            ext.depends = []
        if not ext.include_dirs:
            ext.include_dirs = []
        if not ext.library_dirs:
            ext.library_dirs = []
        if not ext.libraries:
            ext.libraries = []

        if self.apsw_no_old_names:
            ext.define_macros.append(("APSW_OMIT_OLD_NAMES", "1"))

        update_type_stubs_old_names(include_old=not self.apsw_no_old_names)

        if self.definevalues:
            for define in self.definevalues.split(","):
                define = define.split("=", 1)
                if len(define) != 2:
                    define.append("1")
                ext.define_macros.append(tuple(define))

        # Fixup debug setting
        if self.debug:
            # distutils forces NDEBUG even with --debug so overcome that
            ext.undef_macros.append("NDEBUG")
            ext.define_macros.append(("APSW_DEBUG", "1"))  # extra test harness code
            ext.define_macros.append(("SQLITE_DEBUG", "1"))
        else:
            ext.define_macros.append(("NDEBUG", "1"))

        # fork checker?
        if hasattr(os, "fork"):
            ext.define_macros.append(("APSW_FORK_CHECKER", "1"))

        # SQLite 3
        # Look for amalgamation in sqlite3 subdirectory

        path = findamalgamation()
        if path:
            ext.define_macros.append(("APSW_USE_SQLITE_AMALGAMATION", "1"))
            # we also add the directory to include path since icu tries to use it
            ext.include_dirs.insert(0, os.path.dirname(path))
            write("SQLite: Using amalgamation", path)
        else:
            sqlite3_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqlite3")
            inc = False
            if self.use_system_sqlite_config:
                set_config_from_system(os.path.join(sqlite3_dir, "sqlite3config.h"))
                inc = True
            if os.path.isdir(sqlite3_dir) and len(glob.glob(os.path.join(sqlite3_dir, "*"))) > 3:
                write("SQLite: Using include/libraries in sqlite3 subdirectory")
                inc = True
                ext.library_dirs.append(sqlite3_dir)
            else:
                write("SQLite: Using system sqlite include/libraries")
            if inc:
                ext.include_dirs.insert(0, sqlite3_dir)
            ext.libraries.append("sqlite3")

        # sqlite3config.h used to be generated from configure output - now optional override file
        s3config = os.path.join(ext.include_dirs[0], "sqlite3config.h")
        if os.path.exists(s3config):
            write(f"SQLite: Using your configuration { s3config }")
            ext.define_macros.append(("APSW_USE_SQLITE_CONFIG", "1"))

        # autosetup makes this file
        s3config = os.path.join(ext.include_dirs[0], "sqlite_cfg.h")
        if os.path.exists(s3config):
            write(f"SQLite: Using configure generated { s3config }")
            ext.define_macros.append(("APSW_USE_SQLITE_CFG_H", "1"))

        # enables
        addicuinclib = False
        if self.enable:
            for e in self.enable.split(","):
                e = e.strip()
                ext.define_macros.append(("SQLITE_ENABLE_" + e.upper(), 1))
                if e.upper() == "ICU":
                    addicuinclib = True
                else:
                    os.putenv("APSW_TEST_" + e.upper(), "1")
                # See issue #55 where I had left off the 3 in fts3.  This code
                # tries to catch misspelling the name of an extension.
                # However the SQLITE_ENABLE prefix is also used by other
                # options - see https://sqlite.org/compile.html but almost
                # all of those have _ in them, so our abbreviated and
                # hopefully future proof test
                if (
                    "_" not in e.lower()
                    and "memsys" not in e.lower()
                    and e.lower()
                    not in (
                        "fts4",
                        "fts3",
                        "rtree",
                        "icu",
                        "iotrace",
                        "stat2",
                        "stat3",
                        "stat4",
                        "dbstat_vtab",
                        "fts5",
                        "json1",
                        "rbu",
                        "geopoly",
                    )
                ):
                    write("Unknown enable " + e, sys.stderr)
                    raise ValueError("Bad enable " + e)

        # omits
        if self.omit:
            for e in self.omit.split(","):
                e = e.strip()
                ext.define_macros.append(("SQLITE_OMIT_" + e.upper(), 1))

        # icu
        if addicuinclib:
            icc = get_icu_config()
            if icc:
                # if posix is true then quotes get stripped such as from -Dfoo="bar"
                kwargs = {"posix": False}
                for part in shlex.split(icc.cflags, **kwargs):
                    if part.startswith("-I"):
                        ext.include_dirs.append(part[2:])
                    elif part.startswith("-D"):
                        part = part[2:]
                        if "=" in part:
                            part = tuple(part.split("=", 1))
                        else:
                            part = (part, "1")
                        ext.define_macros.append(part)

                for part in shlex.split(icc.ldflags, **kwargs):
                    if part.startswith("-L"):
                        ext.library_dirs.append(part[2:])
                    elif part.startswith("-l"):
                        ext.libraries.append(part[2:])

                write("ICU: Added includes, flags and libraries from " + icc.tool)
                os.putenv("APSW_TEST_ICU", "1")
            else:
                write("ICU: Unable to determine includes/libraries for ICU using pkg-config or icu-config")

        # done ...
        return v

    def run(self):
        v = beparent.run(self)
        return v


sparent = sdist.sdist


class apsw_sdist(sparent):
    user_options = sparent.user_options + [
        ("add-doc", None, "Includes built documentation from doc/build/html into source"),
        ("for-pypi", None, "Configure for pypi distribution"),
    ]

    boolean_options = sparent.boolean_options + ["add-doc", "for-pypi"]

    def initialize_options(self):
        sparent.initialize_options(self)
        self.add_doc = False
        self.for_pypi = False
        self.use_defaults = False  # they are useless

        # Make sure the manifest is regenerated
        self.force_manifest = True

    def run(self):
        cfg = "pypi" if self.for_pypi else "default"
        shutil.copy2(f"tools/setup-{ cfg }.cfg", "setup.apsw")
        v = sparent.run(self)

        if self.add_doc:
            if len(list(help_walker(""))) < 20:
                raise Exception("The help is not built")
            for archive in self.get_archive_files():
                add_doc(archive, self.distribution.get_fullname())
        return v


def set_config_from_system(outputfilename: str):
    import ctypes, ctypes.util

    try:
        # ensure file does not exist if we fail (it could exist from
        # previous run)
        os.remove(outputfilename)
    except FileNotFoundError:
        pass

    libpath = ctypes.util.find_library("sqlite3")
    if not libpath:
        sys.exit("Could not find system sqlite3 library using ctypes.util.find_library")
    print("Extracting configuration from", libpath)

    lib = ctypes.cdll.LoadLibrary(libpath)

    func = lib.sqlite3_compileoption_get
    func.argtypes = [ctypes.c_int]
    func.restype = ctypes.c_char_p

    configs = {}

    i = 0
    while True:
        s = func(i)
        if not s:
            break
        s = "SQLITE_" + s.decode("utf8")
        s = s.split("=", 1)
        if len(s) == 1:
            s.append(1)
        try:
            # intify value if str(int(value)) == value
            v = int(s[1])
            if str(v) == s[1]:
                s[1] = v
        except ValueError:
            pass
        configs[s[0]] = s[1]
        i += 1

    # we have to do some cleanup ...
    if configs["SQLITE_THREADSAFE"] and not configs.get("SQLITE_MUTEX_NOOP"):
        # remove individual muitex configs because sqlite always redefines them
        for k in {"SQLITE_MUTEX_PTHREADS", "SQLITE_MUTEX_W32"}:
            if k in configs:
                del configs[k]
    # always remove these
    for k in {"SQLITE_COMPILER"}:
        if k in configs:
            del configs[k]

    # write out the results
    os.makedirs(os.path.dirname(outputfilename), exist_ok=True)
    with open(outputfilename, "wt", encoding="utf8") as f:
        for c, v in sorted(configs.items()):
            print(f"#undef  { c }", file=f)
            print(f"#define { c } { v }", file=f)


def help_walker(arcdir):
    # Provides a list of (archive name, disk name) for all the help files
    assert os.path.isfile("doc/build/html/_sources/about.rst.txt")
    topdir = "doc/build/html/"
    for dirpath, _, filenames in os.walk(topdir):
        prefix = dirpath[len(topdir) :]
        for f in filenames:
            yield os.path.join(arcdir, "doc", prefix, f), os.path.join(dirpath, f)


def add_doc(archive, topdir):
    write("Add help files to", archive)
    if archive.endswith(".tar") or ".tar." in archive:
        fmt = ""
        if archive.endswith(".gz") or archive.endswith(".tgz"):
            fmt = ":gz"
        elif archive.endswith(".bz2") or archive.endswith(".tbz2"):
            fmt = ":bz2"
        oldarchive = tarfile.open(archive)
        newarchive = tarfile.open(archive + "-", mode="w" + fmt)
        for mem in oldarchive.getmembers():
            newarchive.addfile(mem, oldarchive.extractfile(mem))
        oldarchive.close()
        for arcname, fname in help_walker(topdir):
            newarchive.add(fname, arcname)
        newarchive.add("doc/build/apsw.1", f"{topdir}/man/apsw.1")
        newarchive.close()
        os.rename(archive + "-", archive)
    elif archive.endswith(".zip"):
        ofile = zipfile.ZipFile(archive, "a", zipfile.ZIP_DEFLATED)
        for arcname, fname in help_walker(topdir):
            ofile.write(fname, arcname)
        ofile.write("doc/build/apsw.1", f"{topdir}/man/apsw.1")
        ofile.close()
    else:
        raise Exception("Don't know what to do with " + archive)


@dataclass
class IcuConfig:
    tool: str
    cflags: str
    ldflags: str


def get_icu_config() -> IcuConfig | None:
    skw = {"text": True, "capture_output": True}
    cflags = ldflags = ""

    if shutil.which("pkg-config"):
        with contextlib.suppress(subprocess.CalledProcessError):
            cflags = subprocess.run(["pkg-config", "--cflags", "icu-io"], **skw).stdout.strip()
        with contextlib.suppress(subprocess.CalledProcessError):
            ldflags = subprocess.run(["pkg-config", "--libs", "icu-io"], **skw).stdout.strip()
        if cflags or ldflags:
            return IcuConfig(tool="pkg-config", cflags=cflags, ldflags=ldflags)
    if shutil.which("icu-config"):
        cflags = subprocess.run(["icu-config", "--cppflags"], **skw).stdout.strip()
        ldflags = subprocess.run(["icu-config", "--ldflags"], **skw).stdout.strip()
        return IcuConfig(tool="icu-config", cflags=cflags, ldflags=ldflags)

    return None


# We depend on every .[ch] file in src except unicode
depends = [f for f in glob.glob("src/*.[ch]") if f != "src/apsw.c" and "unicode" not in f]

if __name__ == "__main__":
    setup(
        name="apsw",
        version=version,
        python_requires=">=3.9",
        description="Another Python SQLite Wrapper",
        long_description=pathlib.Path("README.rst").read_text(encoding="utf8"),
        long_description_content_type="text/x-rst",
        author="Roger Binns",
        author_email="rogerb@rogerbinns.com",
        url="https://github.com/rogerbinns/apsw",
        project_urls=project_urls,
        classifiers=[
            "Development Status :: 5 - Production/Stable",
            "Intended Audience :: Developers",
            "Programming Language :: C",
            "Programming Language :: Python :: 3",
            "Topic :: Database :: Front-Ends",
        ],
        keywords=["database", "sqlite"],
        license="any-OSI",
        license_files=["LICENSE"],
        platforms="any",
        ext_modules=[
            Extension(
                "apsw.__init__",
                ["src/apsw.c"],
                include_dirs=include_dirs,
                library_dirs=library_dirs,
                libraries=libraries,
                define_macros=define_macros,
                depends=depends,
            ),
            Extension(
                "apsw._unicode",
                ["src/unicode.c"],
                depends=["src/_unicodedb.c"],
                undef_macros=["NDEBUG"] if os.environ.get("UNICODE_DEBUG") else [],
            ),
        ],
        packages=["apsw"],
        package_data={"apsw": ["__init__.pyi", "py.typed", "fts_test_strings"]},
        cmdclass={
            "test": run_tests,
            "build_test_extension": build_test_extension,
            "fetch": fetch,
            "build_ext": apsw_build_ext,
            "build": apsw_build,
            "sdist": apsw_sdist,
        },
    )
