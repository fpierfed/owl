#!/usr/bin/env python
import os
import sys
import time

try:
    sleep_time = float(sys.argv[1])
except:
    sleep_time = 30.
time.sleep(sleep_time)


print('Executed on %s' % (os.environ.get('HOST', 'UNKNOWN')))
sys.exit(0)

