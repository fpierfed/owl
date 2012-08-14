#!/usr/bin/env python
import os
import re
import sys


# The input search term is sys.argv[1], the input file is sys.argv[2], otherwise
# we have an error.
if(len(sys.argv) != 3):
    sys.stderr.write('Usage: grep.py <word> <input file>\n')
    sys.exit(1)

pattern = sys.argv[1]
file_name = sys.argv[2]
root, ext = os.path.splitext(os.path.basename(file_name))
try:
    f = open(file_name)
except:
    sys.stderr.write('ERROR: unable to open %s for reading\n' % (file_name))
    sys.exit(2)

out = open('%s%s' % (root, '.result'),'w')
for line in f:
    if(re.search(pattern, line)):
        out.write(line)
out.close()
f.close()
