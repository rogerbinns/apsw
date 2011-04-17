#!/usr/bin/env python
#
# See the accompanying LICENSE file.
#
import urllib2
import hashlib

sqlitevers=(
    '3070400',
    '3070500',
    '3070600',
    '3070601',
    )

# Checks the checksums file

def getline(url):
    for line in open("checksums", "rtU"):
        line=line.strip()
        if len(line)==0 or line[0]=="#":
            continue
        l=[l.strip() for l in line.split()]
        if len(l)!=4:
            print "Invalid line in checksums file:", line
            raise ValueError("Bad checksums file")
        if l[0]==url:
            return l[1:]
    return None

def check(url, data):
    d=["%s" % (len(data),), hashlib.sha1(data).hexdigest(), hashlib.md5(data).hexdigest()]
    line=getline(url)
    if line:
        if line!=d:
            print "Checksums mismatch for", url
            print "checksums file is", line
            print "Download is", d
    else:
        print url,
        if url.endswith(".zip"):
            print "  ",
        print d[0], d[1], d[2]


for v in sqlitevers:
    # Windows amalgamation
    AURL="http://www.sqlite.org/sqlite-amalgamation-%s.zip" % (v,)
    try:
        data=urllib2.urlopen(AURL).read()
    except:
        print AURL
        raise
    check(AURL, data)
    # All other platforms amalgamation
    AURL="http://www.sqlite.org/sqlite-autoconf-%s.tar.gz" % (v,)
    try:
        data=urllib2.urlopen(AURL).read()
    except:
        print AURL
        raise
    check(AURL, data)
    # asyncvfs
    AURL="http://www.sqlite.org/sqlite-src-%s.zip" % (v,)
    try:
        data=urllib2.urlopen(AURL).read()
    except:
        print AURL
        raise
    check(AURL, data)
    
