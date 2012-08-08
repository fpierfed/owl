#!/usr/bin/env python
import datetime
import re
import sys



if(len(sys.argv) != 2):
    sys.stderr.write('Usage: analyze.py <log file>\n')
    sys.exit(1)
f = open(sys.argv[1])

ls = f.readlines()

submit = re.compile('^[0-9]{3} \([0-9]+\.[0-9]+\.[0-9]+\) ([0-9]{2})/([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2}) Job submitted from host:')
end = re.compile('^[0-9]{3} \([0-9]+\.[0-9]+\.[0-9]+\) ([0-9]{2})/([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2}) Job terminated.')

[m, d, hh, mm, ss] = [int(x) for x in submit.match(ls[0]).groups()]
d0 = datetime.datetime(2012, m, d, hh, mm, ss)

m = None
for l in ls[-12:]:
    match = end.search(l)
    if(match):
        [m, d, hh, mm, ss] = [int(x) for x in match.groups()]
        break
if(m is None):
    sys.stderr.write('Malformed log?')
    sys.exit(2)

d1 = datetime.datetime(2012, m, d, hh, mm, ss)
print('From submit to finish: %d seconds' % (int((d1 - d0).total_seconds())))
sys.exit(0)
