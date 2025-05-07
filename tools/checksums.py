#!/usr/bin/env python3
#
# Checks the checksums file

import urllib.request
import hashlib
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import setup

sqlitevers = (
    "3490200",
    "3490100",
    "3490000",
    "3480000",
    "3470200",
    "3470100",
    "3470000",
    "3460100",
    "3460000",
    "3450300",
    "3450200",
    "3450100",
    "3450000",
    "3440200",
    "3440100",
    "3440000",
    "3430200",
    "3430100",
    "3430000",
    "3420000",
    "3410200",
    "3410100",
    "3410000",
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
    d = ["%s" % (len(data),), hashlib.sha256(data).hexdigest(), hashlib.sha3_256(data).hexdigest()]
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
    AURL = "https://sqlite.org/sqlite-autoconf-%s.tar.gz" % (v,)
    AURL = fixup_download_url(AURL)
    try:
        data = urllib.request.urlopen(AURL).read()
    except:
        print(AURL)
        raise
    check(AURL, data)
