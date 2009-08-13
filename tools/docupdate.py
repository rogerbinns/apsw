# python
#
# See the accompanying LICENSE file.
#
# various automagic documentation updates

import sys


# get the download file names correct

version=sys.argv[1]
url="http://apsw.googlecode.com/files"

download=open("doc/download.rst", "rtU").read()

op=[]
incomment=False
for line in open("doc/download.rst", "rtU"):
    line=line.rstrip()
    if line==".. downloads-begin":
        op.append(line)
        incomment=True
        op.append("")
        op.append("* `apsw-%s.zip" % (version,))
        op.append("  <%s/apsw-%s.zip>`_" % (url, version))
        op.append("  (Source, includes this HTML Help)")
        op.append("")
        op.append("* `apsw-%s.chm" % (version,))
        op.append("  <%s/apsw-%s.chm>`_" % (url, version))
        op.append("  (Compiled HTML Help) `Seeing blank content? <http://weblog.helpware.net/?p=36>`_ & `MSKB 902225 <http://support.microsoft.com/kb/902225/>`_")
        op.append("")
        for pyver in ("2.3", "2.4", "2.5", "2.6", "3.0", "3.1"):
            op.append("* `apsw-%s.win32-py%s.exe" % (version, pyver))
            op.append("  <%s/apsw-%s.win32-py%s.exe>`_" % (url, version, pyver))
            op.append("  (Windows Python %s)" % (pyver,))
            op.append("")
        continue
    if line==".. downloads-end":
        incomment=False
    if incomment:
        continue
    op.append(line)

op="\n".join(op)
if op!=download:
    open("doc/download.rst", "wt").write(op)

# put usage and description for speedtest into benchmark

import speedtest

benchmark=open("doc/benchmarking.rst", "rtU").read()

op=[]
incomment=False
for line in open("doc/benchmarking.rst", "rtU"):
    line=line.rstrip()
    if line==".. speedtest-begin":
        op.append(line)
        incomment=True
        op.append("")
        op.append(".. code-block:: text")
        op.append("")
        op.append("    $ python speedtest.py --help")
        speedtest.parser.set_usage("Usage: speedtest.py [options]")
        for line in speedtest.parser.format_help().split("\n"):
            op.append("    "+line)
        op.append("")
        op.append("    $ python speedtest.py --tests-detail")
        for line in speedtest.tests_detail.split("\n"):
            op.append("    "+line)
        op.append("")
        continue
    if line==".. speedtest-end":
        incomment=False
    if incomment:
        continue
    op.append(line)

op="\n".join(op)
if op!=benchmark:
    open("doc/benchmarking.rst", "wt").write(op)
