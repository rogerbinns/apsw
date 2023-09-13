#!/usr/bin/env python3
#
# Checks the checksums file

import urllib.request
import hashlib
import os
import sys

# prevent setuptools.setup from running.  It runs because of
# https://github.com/pypa/cibuildwheel/issues/1611
import setuptools
setuptools.setup = lambda *args, **kwargs: 0
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import setup

sqlitevers = (
    '3430100',
    '3430000',
    '3420000',
    '3410200',
    '3410100',
    '3410000',
    '3400100',
    '3400000',
    '3390400',
    '3390300',
    '3390200',
    '3390100',
    '3390000',
    '3380500',
    '3380400',
    '3380300',
    '3380200',
    '3380100',
    '3380000',
    '3370200',
    '3370100',
    '3370000',
)

fixup_download_url = setup.fixup_download_url


def getline(url):
    for line in open("checksums", "rt"):
        line = line.strip()
        if len(line) == 0 or line[0] == "#":
            continue
        l = [l.strip() for l in line.split()]
        if len(l) != 4:
            print("Invalid line in checksums file:", line)
            raise ValueError("Bad checksums file")
        if l[0] == url:
            return l[1:]
    return None


def check(url, data):
    d = ["%s" % (len(data), ), hashlib.sha1(data).hexdigest(), hashlib.md5(data).hexdigest()]
    line = getline(url)
    if line:
        if line != d:
            print("Checksums mismatch for", url)
            print("checksums file is", line)
            print("Download is", d)
    else:
        print(url, d[0], d[1], d[2])


for v in sqlitevers:
    # All platforms amalgamation
    AURL = "https://sqlite.org/sqlite-autoconf-%s.tar.gz" % (v, )
    AURL = fixup_download_url(AURL)
    try:
        data = urllib.request.urlopen(AURL).read()
    except:
        print(AURL)
        raise
    check(AURL, data)
