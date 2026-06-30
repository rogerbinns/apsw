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
    "3530300",
    "3530200",
    "3530100",
)

other_urls = (
    # vec1
    "https://sqlite.org/vec1/zip/vec1-20260526165101-762865e44d.zip",
    "https://sqlite.org/vec1/zip/vec1-20260505104119-eb38e10fef.zip",
    "https://sqlite.org/vec1/zip/vec1-20260409204746-4b73767df0.zip",
    "https://sqlite.org/vec1/zip/vec1-20260306155250-d070184523.zip",
    # sqlar
    "https://sqlite.org/sqlar/zip/sqlar-src-20180107193712-4824e73896.zip",
    # zlib
    "https://github.com/madler/zlib/releases/download/v1.3.2/zlib132.zip",
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
    for filename in ("sqlite-autoconf-%s.tar.gz", "sqlite-src-%s.zip"):
        AURL = f"https://sqlite.org/{filename}" % (v,)
        AURL = fixup_download_url(AURL)
        try:
            data = urllib.request.urlopen(AURL).read()
        except:
            print(AURL)
            raise
        check(AURL, data)

for url in other_urls:
    try:
        data = urllib.request.urlopen(url).read()
    except:
        print(url)
        raise
    check(url, data)
