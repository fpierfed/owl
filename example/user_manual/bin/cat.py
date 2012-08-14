#!/usr/bin/env python
import os
import sys


# The input files are sys.argv[1:], otherwise we have an error.
if(len(sys.argv) < 2):
    sys.stderr.write('Usage: grep.py <input file> <input file> ...\n')
    sys.exit(1)

# The input file names all have the form text_<i>.result
file_names = sys.argv[1:]
root, ext = os.path.splitext(os.path.basename(file_names[0]))
root, dummy = root.rsplit('_', 1)

out = open('%s%s' % (root, '.result'),'w')
for file_name in file_names:
    for line in open(file_name):
        out.write(line)
out.close()
