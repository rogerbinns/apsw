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
    "3510300",
    "3510200",
    "3510100",
    "3510000",
)

other_urls = ("https://sqlite.org/vec1/zip/vec1-20260306155250-d070184523.zip",
              "https://sqlite.org/sqlar/zip/sqlar-src-20180107193712-4824e73896.zip",
              "https://www.zlib.net/zlib132.zip",)

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

for url in other_urls:
    try:
        data = urllib.request.urlopen(url).read()
    except:
        print(url)
        raise
    check(url, data)

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
