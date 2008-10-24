# python

# Work out how much coverage we actually have

import glob

linesexecuted=0
linestotal=0

for f in glob.glob("*.c.gcov"):
    if f=="sqlite3.c.gcov":
        continue
    for line in open(f, "rtU"):
        line=line.split(":", 1)[0].strip()
        if line=="-":
            continue
        if line!="#####":
            linesexecuted+=1
        linestotal+=1

print "Lines executed: %0.2f of %d" % (linesexecuted*100.0/linestotal, linestotal)
        
