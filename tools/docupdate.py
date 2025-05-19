# python
#
# See the accompanying LICENSE file.
#
# various automagic documentation updates

import sys
import os
import textwrap
import re

# get the download file names correct

version = sys.argv[1]
releasedate = sys.argv[2]
url = "  <https://github.com/rogerbinns/apsw/releases/download/" + version + "/%s>`__"
version_no_r = version.split("-r")[0]

download = open("doc/install.rst", "rt").read()

op = []
indownload = inverify = False
for line in open("doc/install.rst", "rt"):
    line = line.rstrip()
    if line == ".. downloads-begin":
        op.append(line)
        indownload = True
        op.append("")
        op.append("* `apsw-%s.zip" % (version,))
        op.append(url % ("apsw-%s.zip" % version))
        op.append("  (Source as zip, includes this HTML Help)")
        op.append("")
        op.append("* `apsw-%s.tar.gz" % (version,))
        op.append(url % ("apsw-%s.tar.gz" % version))
        op.append("  (Source as tar.gz, includes this HTML Help)")
        op.append("")
        op.append("* `apsw-%s.zip.cosign-bundle" % (version,))
        op.append(url % ("apsw-%s.zip.cosign-bundle" % version))
        op.append("  cosign signature for zip source")
        op.append("")
        op.append("* `apsw-%s.tar.gz.cosign-bundle" % (version,))
        op.append(url % ("apsw-%s.tar.gz.cosign-bundle" % version))
        op.append("  cosign signature for tar.gz source")
        op.append("")
        continue
    if line == "  .. verify-begin":
        op.append(line)
        inverify = True
        op.append("")
        op.append("  .. code-block:: console")
        op.append("")

        def back(s):
            return s + " " * (62 - len(s)) + "\\"

        op.append("    " + back(f"$ cosign verify-blob apsw-{version}.zip"))
        op.append("    " + back("    --new-bundle-format"))
        op.append("    " + back(f"    --bundle apsw-{version}.zip.cosign-bundle"))
        op.append("    " + back("    --certificate-identity=rogerb@rogerbinns.com"))
        op.append("    " + "    --certificate-oidc-issuer=https://github.com/login/oauth")
        op.append("    " + "Verified OK")
        op.append("")
        op.append("    " + back(f"$ python3 -m sigstore verify identity apsw-{version}.zip"))
        op.append("    " + back(f"    --bundle apsw-{version}.zip.cosign-bundle"))
        op.append("    " + back("    --cert-identity=rogerb@rogerbinns.com"))
        op.append("    " + "    --cert-oidc-issuer=https://github.com/login/oauth")
        op.append("    " + f"OK: apsw-{version}.zip")

        op.append("")
    if line == ".. downloads-end":
        indownload = False
    if line == "  .. verify-end":
        inverify = False
    if indownload or inverify:
        continue
    op.append(line)

"""
      $ cosign verify-blob --new-bundle-format apsw.3.46.0.0.zip \
            --bundle apsw.3.46.0.0-cosign.bundle                 \
            --certificate-identity=rogerb@rogerbinns.com         \
            --certificate-oidc-issuer=https://github.com/login/oauth
      Verified OK

"""


op = "\n".join(op)
if op != download:
    open("doc/install.rst", "wt").write(op)

# put usage and description for speedtest into benchmark

import apsw.speedtest

benchmark = open("doc/benchmarking.rst", "rt").read()

op = []
incomment = False
for line in open("doc/benchmarking.rst", "rt"):
    line = line.rstrip()
    if line == ".. speedtest-begin":
        op.append(line)
        incomment = True
        op.append("")
        op.append(".. code-block:: text")
        op.append("")
        op.append("    $ python3 -m apsw.speedtest --help")
        cols = os.environ.get("COLUMNS", None)
        os.environ["COLUMNS"] = "80"
        for line in apsw.speedtest.parser.format_help().split("\n"):
            op.append("    " + line)
        if cols is None:
            del os.environ["COLUMNS"]
        else:
            os.environ["COLUMNS"] = cols
        op.append("")
        op.append("    $ python3 -m apsw.speedtest --tests-detail")
        for line in apsw.speedtest.tests_detail.split("\n"):
            op.append("    " + line)
        op.append("")
        continue
    if line == ".. speedtest-end":
        incomment = False
    if incomment:
        continue
    op.append(line)

op = "\n".join(op)
if op != benchmark:
    open("doc/benchmarking.rst", "wt").write(op)

# shell stuff

import apsw, io, apsw.shell

shell = apsw.shell.Shell()

# Generate help info
s = io.StringIO()
shell.stderr = s


def tw(*args):
    return 80


shell._terminal_width = tw
shell.command_help([])

# shell._help_info is now a dict where key is the command, and value
# is a list. [0] is the command and parameters, [1] is the one liner
# description shown in overall .help, and v[2] is multi-paragraph text
# detailed help info.  The following methods help turn that into
# rst


def backtickify(s):
    s = s.group(0)
    if s in {"A", "I", "O", "SQL", "APSW", "TCL", "C", "HTML", "JSON", "CSV", "TSV", "US", "VFS", "FLAGS"}:
        return s
    if s == "'3'":  # example in command_parameter
        return "``'3'``"
    if s.startswith("'") and s.endswith("'"):
        s = s.strip("'")
        return f"``{ s }``"
    if all(c.upper() == c and not c.isdigit() for c in s):
        return f"``{ s }``"
    return s


def long_help_to_rst(long_help):
    # pass v[2] from above or other long text
    res = []
    if long_help:
        for i, para in enumerate(long_help):
            if not para:
                res.append("")
            else:
                para = para.replace("\\", "\\\\")
                if para.lstrip() == para:
                    # no indent
                    para = re.sub(r"'?[\w%]+'?", backtickify, para)
                if para.endswith(":"):
                    # we have to double up final : if the next
                    # section is indented further
                    c = i + 1
                    # skip blanks
                    while not long_help[c]:
                        c += 1
                    # indented?
                    if long_help[c].lstrip() != long_help[c]:
                        para += ":"
                res.extend(textwrap.wrap(para, width=80))
        res.append("")
    return res


def backtick_each_word(s):
    out = []
    for word in s.split():
        out.append(f"``{word}``")
    return " ".join(out)


incomment = False
op = []
for line in open("doc/shell.rst", "rt"):
    line = line.rstrip()
    if line == ".. help-begin:":
        op.append(line)
        incomment = True
        op.append("")

        op.append(".. hlist::")
        op.append("  :columns: 3")
        op.append("")
        for k in shell._help_info:
            op.append(f"  * :ref:`{ k } <shell-cmd-{ k.replace("_", "-")  }>`")
        op.append("")

        for k, v in shell._help_info.items():
            op.append(f".. _shell-cmd-{ k.replace("_", "-") }:")
            op.append(".. index::")
            op.append("    single: " + v[0].lstrip(".").split()[0] + " (Shell command)")
            op.append("")
            op.append(v[0].lstrip("."))
            op.append("-" * len(v[0].lstrip(".")))
            op.append("")
            op.append("*" + v[1] + "*")
            op.append("")
            op.extend(long_help_to_rst(v[2]))

        continue
    if line == ".. usage-begin:":
        op.append(line)
        incomment = True
        op.append("")
        op.append(".. code-block:: text")
        op.append("")
        op.extend(["  " + x for x in shell.usage().split("\n")])
        op.append("")
        continue
    if line == ".. help-end:":
        incomment = False
    if line == ".. usage-end:":
        incomment = False
    if incomment:
        continue
    op.append(line)

op = "\n".join(op)
if op != open("doc/shell.rst", "rt").read():
    open("doc/shell.rst", "wt").write(op)


# cli shell man page
incomment = False
op = []
for line in open("doc/cli.rst", "rt"):
    line = line.rstrip()
    if line.startswith(":version:"):
        op.append(f":version: apsw {version}")
        continue
    if line.startswith(":date:"):
        op.append(f":date: {releasedate}")
        continue
    if line == ".. options-begin:":
        op.append(line)
        op.append("")
        incomment = True
        usage = shell.usage().split("\n\n")
        # [0] is invocation
        # [1] is description
        # [2] is "Options include"
        # [3] are the options
        op.extend(long_help_to_rst(usage[1].splitlines()))
        assert usage[3].lstrip().startswith("-")
        assert len(usage) == 4

        each = []
        for line in usage[3].splitlines():
            line = line.lstrip()
            if line.startswith("-"):
                each.append(re.split(r"  \s*", line))
            else:
                each[-1][1] += " " + line

        for option, desc in each:
            op.append(backtick_each_word(option))
            for line in long_help_to_rst([desc]):
                op.append("    " + line)

        op.append("")
        continue

    if line == ".. commands-begin:":
        op.append(line)
        op.append("")
        incomment = True

        for command in shell._help_info.values():
            op.append(backtick_each_word(command[0]))
            for line in long_help_to_rst([command[1]]):
                op.append("    " + line)

        op.append("")
        op.append("COMMANDS")
        op.append("========")
        op.append("")

        for command in shell._help_info.values():
            op.append(command[0])
            op.append("-" * len(command[0]))
            op.append("")
            op.extend(long_help_to_rst([command[1]]))
            if command[2]:
                op.extend(long_help_to_rst(command[2]))
                op.append("")
        continue

    if line == ".. copyright-begin:":
        op.append(line)
        op.append("")
        incomment = True

        for i, line in enumerate(open("doc/copyright.rst", "rt")):
            line = line.rstrip()
            if i == 0:
                op.append(line.upper())
            else:
                op.append(line)

        continue

    if line in {".. options-end:", ".. commands-end:", ".. copyright-end:"}:
        # rst behaves badly if there is no blank line before the label
        op.append("")
        incomment = False

    if incomment:
        continue
    op.append(line)

# ensure another blank line at end otherwise weird formatting can happen
op.append("")

op = "\n".join(op)
if op != open("doc/cli.rst", "rt").read():
    open("doc/cli.rst", "wt").write(op)
