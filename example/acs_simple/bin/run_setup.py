#!/usr/bin/env python
"""
run_setup.py

Make sure that the setup looks fine. This means checking environment variables,
file paths etc.

The following environment variables have to defined and pointing to real
directories:
    iraf
    jref
    mtab
    crotacomp
    cracscomp
In addition, IRAFARCH might be useful to make PyRAF happy. Remember to always
add a trailing slash ('/') to directory paths.


Example (note the traling slashes):
    setenv iraf /jwst/iraf/iraf/
    setenv IRAFARCH redhat
    source $iraf/unix/hlib/irafuser.csh
    setenv jref /home/fpierfed/data/jref/
    setenv mtab /home/fpierfed/data/mtab/
    setenv crotacomp /home/fpierfed/data/ota/
    setenv cracscomp /home/fpierfed/data/acs/
"""
import os



# Make sure that the environment makes sense.
for var in ('iraf', 'jref', 'mtab', 'crotacomp', 'cracscomp', ):
    try:
        path = os.environ[var]
    except:
        print('Error: the environment variable %s is not defined.' % (var))
        sys.exit(1)
    if(not os.path.isdir(path)):
        print('Error: %s ($%s) is not a directory.' % (path, var))
        sys.exit(2)

# Now touch all files in the current directory to make Condor transfer them back
for f in os.listdir('.'):
    if(not f.endswith('.fits')):
        continue
    os.utime(f, None)



















































