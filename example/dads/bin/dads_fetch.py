#!/usr/bin/env python
import sys

import dads


try:
    dataset = sys.argv[1]
except:
    print('Usage: dads_fetch.py dataset name')
    print('Example: dads_fetch.py j9am01070')
    sys.exit(1)
    
conn = dads.Connection()
response = conn.fetch([dataset],['raw','aux','bestref'])
if(not response.success):
    print(response)
    sys.exit(2)

print(response)
sys.exit(0)
