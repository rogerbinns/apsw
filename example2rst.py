# Python code

# The purpose of this file is to produce rst output interspersed into
# the the text of the example code

# Imports
import string, sys, cStringIO

def docapture(filename):
    code=[]
    code.append(outputredirector)
    counter=0
    for line in open(filename, "rU"):
        line=line[:-1] # strip off newline
        if line.startswith("#@@CAPTURE"):
            code.append("opto('.tmpop-%s-%d')" % (filename, counter))
            counter+=1
        elif line.startswith("#@@ENDCAPTURE"):
            code.append("opnormal()")
        else:
            code.append(line)
    code="\n".join(code)
    # open("xx.py", "wt").write(code)
    exec code in {}

outputredirector="""
import sys
origsysstdout=None
def opto(fname):
  global origsysstdout
  origsysstdout=sys.stdout,fname
  sys.stdout=open(fname, "wt")
def opnormal():
  sys.stdout.close()
  sys.stdout=origsysstdout[0]
  sys.stdout.write(open(origsysstdout[1], "rb").read())
"""

def rstout(filename):
    op=[]
    op.extend("""
Example
=======

::
""".split("\n"))
    counter=0
    for line in open(filename, "rtU"):
        line=line.rstrip()
        if "@@CAPTURE" in line:
            continue
        if "@@ENDCAPTURE" not in line:
            op.append("    "+line)
            continue
        op.append("")
        op.append("::")
        op.append("")
        for line in open(".tmpop-%s-%d" % (filename, counter), "rtU"):
            line=line.rstrip()
            op.append("   "+line)
        op.append("")
        op.append("::")
        op.append("")
        os.remove(".tmpop-%s-%d" % (filename, counter))
        counter+=1

    return op

if __name__ == "__main__":
  docapture("example-code.py")
  op=rstout("example-code.py")
  open("doc/example.rst", "wt").write("\n".join(op))
