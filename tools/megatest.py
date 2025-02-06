#!/usr/bin/env python3

# See the accompanying LICENSE file.
"""This file runs the test suite against several versions of SQLite
and Python to make sure everything is ok in the various combinations.
It only runs on a UNIX like environment.

All the work is done in parallel rather than serially.  This allows
for it to finish a lot sooner.

"""

import os
import sys
import argparse
import subprocess
import re
import shutil
import time
import concurrent.futures
import random

if os.path.isdir("/usr/lib/ccache"):
    os.putenv("PATH", "/usr/lib/ccache:" + os.environ["PATH"])
    print(f"{os.environ.get('CCACHE_DIR')=}")

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# disable testfileprefix
os.putenv("APSWTESTPREFIX", "")
try:
    del os.environ["APSWTESTPREFIX"]
except KeyError:
    pass


def run(cmd):
    subprocess.run(cmd, shell=True, check=True)


def dotest(pyver, logdir, pybin, pylib, workdir, sqlitever, debug, sysconfig):
    pyflags = "-X warn_default_encoding  -X dev -X tracemalloc=5" if debug else ""
    extdebug = "--debug" if debug else ""
    logf = os.path.abspath(os.path.join(logdir, "buildruntests.txt"))
    # this is used to alternate support for full metadata and test --definevalues flags
    build_ext_flags = (
        "--definevalues SQLITE_ENABLE_COLUMN_METADATA,SQLITE_DEFAULT_CACHE_SIZE=-1"
        if random.choice((False, True))
        else ""
    )
    if pyver == "system" or sysconfig:
        build_ext_flags += " --use-system-sqlite-config"

    run(
        f"""(
            set -ex ;
            cd { workdir } ;
            { pybin } -m venv venv
            venv/bin/python3 -m ensurepip || true ;
            venv/bin/python3 -m pip install --upgrade --upgrade-strategy eager pip wheel setuptools ;
            env LD_LIBRARY_PATH={ pylib } venv/bin/python3 -bb -Werror { pyflags } setup.py fetch \
                --version={ sqlitever } --all build_test_extension build_ext --inplace --force --enable-all-extensions \
                { extdebug } { build_ext_flags } test -v --locals;"""
        + (
            f"""
            cp tools/setup-pypi.cfg setup.apsw ;
            venv/bin/python3 -m pip wheel -v . ;
            venv/bin/python3 -m pip install --no-index --force-reinstall --find-links=. apsw ;
            venv/bin/python3 -m apsw.tests --locals"""
            if not debug
            else ""
        )
        + f"""   ) >{ logf }  2>&1"""
    )


def runtest(workdir, pyver, bits, sqlitever, logdir, debug, sysconfig):
    pybin, pylib = buildpython(workdir, pyver, bits, debug, os.path.abspath(os.path.join(logdir, "pybuild.txt")))
    dotest(pyver, logdir, pybin, pylib, workdir, sqlitever, debug, sysconfig)


def main(PYVERS, SQLITEVERS, BITS, concurrency):
    try:
        del os.environ["APSWTESTPREFIX"]
    except KeyError:
        pass
    print("Test starting")
    os.system("rm -rf apsw/.*so megatestresults 2>/dev/null ; mkdir megatestresults")
    print("  ... removing old work directory")
    topworkdir = os.path.abspath("../apsw-test")
    os.system(f"rm -rf { topworkdir }/* 2>/dev/null ; mkdir -p { topworkdir }")
    os.system("rm -rf $HOME/.local/lib/python*/site-packages/apsw* 2>/dev/null")
    print("      done")

    jobs = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        for debug in False, True:
            for sqlitever in SQLITEVERS:
                for pyver in PYVERS:
                    for sysconfig in False, True:
                        for bits in BITS:
                            # we only have 64 bit system python
                            if pyver == "system" and bits != 64:
                                continue
                            if sysconfig and bits != 64:
                                continue
                            # python 3.14 alpha on 32 bit gets some SIMD intrinsics wrong
                            if pyver == "3.14.0a1" and bits == 32:
                                continue

                            print(
                                f"Python { pyver } { bits }bit  SQLite { sqlitever }  debug { debug } sysconfig { sysconfig }"
                            )
                            workdir = os.path.abspath(
                                os.path.join(
                                    topworkdir,
                                    "py%s-%d-sq%s%s%s"
                                    % (
                                        pyver,
                                        bits,
                                        sqlitever,
                                        "-debug" if debug else "",
                                        "-sysconfig" if sysconfig else "",
                                    ),
                                )
                            )
                            logdir = os.path.abspath(
                                os.path.join(
                                    "megatestresults",
                                    "py%s-%d-sq%s%s%s"
                                    % (
                                        pyver,
                                        bits,
                                        sqlitever,
                                        "-debug" if debug else "",
                                        "-sysconfig" if sysconfig else "",
                                    ),
                                )
                            )
                            os.makedirs(logdir)
                            os.makedirs(workdir)
                            copy_git_files(workdir)
                            job = executor.submit(
                                runtest,
                                workdir=workdir,
                                bits=bits,
                                pyver=pyver,
                                sqlitever=sqlitever,
                                logdir=logdir,
                                debug=debug,
                                sysconfig=sysconfig,
                            )
                            job.info = f"py { pyver } sqlite { sqlitever } debug { debug } bits { bits } sysconfig { sysconfig }"
                            jobs.append(job)

        print(f"\nAll { len(jobs) } builds started, now waiting for them to finish ({ concurrency } concurrency)\n")
        start = time.time()
        for job in concurrent.futures.as_completed(jobs):
            print(job.info, "-> ", end="", flush=True)
            try:
                job.result()
                print("\t OK", flush=True)
            except Exception as e:
                print("\t FAIL", e, flush=True)

        print(f"\nFinished in { int(time.time() - start) } seconds")


def copy_git_files(destdir):
    for line in subprocess.run(["git", "ls-files"], text=True, capture_output=True, check=True).stdout.split("\n"):
        if not line:
            continue
        fn = line.split("/")
        if fn[0] in {".github", "doc"}:
            continue
        if len(fn) > 1:
            os.makedirs(os.path.join(destdir, "/".join(fn[:-1])), exist_ok=True)
        shutil.copyfile(line, os.path.join(destdir, line))


def getpyurl(pyver):
    dirver = pyver
    if "a" in dirver:
        dirver = dirver.split("a")[0]
    elif "b" in dirver:
        dirver = dirver.split("b")[0]
    elif "rc" in dirver:
        dirver = dirver.split("rc")[0]

    # Upper or lower case 'p' in download filename is somewhat random
    p = "P"
    ext = "xz"
    return "https://www.python.org/ftp/python/%s/%sython-%s.tar.%s" % (dirver, p, pyver, ext)


def buildpython(workdir, pyver, bits, debug, logfilename):
    if pyver == "system":
        return "/usr/bin/python3", ""
    url = getpyurl(pyver)
    tarx = "J"
    run(
        'set -e ; cd %s ; mkdir pyinst ; ( echo "Getting %s"; wget -q %s -O - | tar xf%s -  ) > %s 2>&1'
        % (workdir, url, url, tarx, logfilename)
    )
    full = ""
    if sys.platform.startswith("linux"):
        ldflags = 'LDFLAGS="-L/usr/lib/$(dpkg-architecture -qDEB_HOST_MULTIARCH)"; export LDFLAGS;'
    else:
        ldflags = ""
    configure_flags = "--with-pydebug  --without-freelists --with-assertions" if debug else ""
    run(f"""(set -ex ;
            cd { workdir } ;
            cd Python-{ pyver } ;
            env CC='gcc -m{ bits }' ./configure --prefix={ workdir }/pyinst  --with-ensure-pip=yes --disable-test-modules {configure_flags} >> { logfilename } 2>&1 ;
            env ASAN_OPTIONS=detect_leaks=false nice nice nice make  -j 4 install ;
            # a lot of effort to reduce disk space
            rm -rf  {workdir}/pyinst/lib/*/test { workdir}/pyinst/lib/*/idlelib { workdir}/pyinst/lib/*/lib2to3 { workdir}/pyinst/lib/*/tkinter ;
            rm -rf lib/test lib/idlelib lib/encodings ;
            find { workdir } -type d -name __pycache__ -print0 | xargs -0 --no-run-if-empty rm -rf ;
            make distclean >/dev/null ) > { logfilename} 2>&1
    """)
    suf = "3"
    pybin = os.path.join(workdir, "pyinst", "bin", "python" + suf)
    return pybin, os.path.join(workdir, "pyinst", "lib")


def natural_compare(a, b):
    # https://stackoverflow.com/a/8408177
    convert = lambda text: int(text) if text.isdigit() else text.lower()
    alphanum_key = lambda key: [convert(c) for c in re.split("([0-9]+)", key)]

    return cmp(alphanum_key(a), alphanum_key(b))


def cmp(a, b):
    if a < b:
        return -1
    if a > b:
        return +1
    assert a == b
    return 0


# Default versions we support
PYVERS = (
    "3.14.0a4",
    "3.13.2",
    "3.12.9",
    "3.11.11",
    "3.10.16",
    "3.9.21",
    "system",
)

SQLITEVERS = ("3.49.0", )

BITS = (64, 32)

if __name__ == "__main__":
    nprocs = 0
    try:
        # try and work out how many processors there are - this works on linux
        for line in open("/proc/cpuinfo", "rt"):
            line = line.split()
            if line and line[0] == "processor":
                nprocs += 1
    except:
        pass
    # well there should be at least one!
    if nprocs == 0:
        nprocs = 1

    concurrency = nprocs * 2
    if concurrency > 24:
        concurrency = 24

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--pyvers", help="Which Python versions to test against [%(default)s]", default=",".join(PYVERS)
    )
    parser.add_argument(
        "--sqlitevers",
        dest="sqlitevers",
        help="Which SQLite versions to test against [%(default)s]",
        default=",".join(SQLITEVERS),
    )
    parser.add_argument("--bits", default=",".join(str(b) for b in BITS), help="Bits [%(default)s]")
    parser.add_argument(
        "--tasks",
        type=int,
        dest="concurrency",
        help="Number of simultaneous builds/tests to run [%(default)s]",
        default=concurrency,
    )

    options = parser.parse_args()

    pyvers = options.pyvers.split(",")
    sqlitevers = options.sqlitevers.split(",")
    bits = tuple(int(b.strip()) for b in options.bits.split(","))
    concurrency = options.concurrency
    sqlitevers = [x for x in sqlitevers if x]
    main(pyvers, sqlitevers, bits, concurrency)
