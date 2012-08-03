#!/usr/bin/env python
"""
run_setup.py

Make sure that the setup looks fine. This means checking environment variables,
file paths etc. For now this entails making sure that numpy and pyfits are
indeed installed.
"""
import os
try:
    import numpy
except:
    print('Error: please install numpy.')
try:
    import pyfits
except:
    print('Error: please install pyfits.')

# Now touch all files in the current directory to make Condor transfer them back
for f in os.listdir('.'):
    if(not f.endswith('.fits')):
        continue
    os.utime(f, None)
