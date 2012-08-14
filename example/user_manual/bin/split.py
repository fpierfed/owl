#!/usr/bin/env python
import os
import sys



# The input file is sys.argv[1], the number of lines per chunk is sys.argv[2], 
# otherwise we have an error.
if(len(sys.argv) != 3):
    sys.stderr.write('Usage: split.py <input file> <# of lines per chunk>\n')
    sys.exit(1)

file_name = sys.argv[1]
try:
    N = int(sys.argv[2])
except:
    sys.stderr.write('ERROR: unable to cast %s to an integer\n' % (sys.argv[2]))
    sys.exit(2)

root, ext = os.path.splitext(os.path.basename(file_name))
try:
    f = open(file_name)
except:
    sys.stderr.write('ERROR: unable to open %s for reading\n' % (file_name))
    sys.exit(3)

# Since the file might be huge...
i = 0
index = 0
out = open('%s_%d%s' % (root, index, ext),'w')
for line in f:
    out.write(line)
    i += 1
    if(i >= N):
        out.close()
        i = 0
        index += 1
        out = open('%s_%d%s' % (root, index, ext),'w')
if(not out.closed):
    out.close()
f.close()
