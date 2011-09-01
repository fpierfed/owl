#!/usr/bin/env python
# Copyright (C) 2010 Association of Universities for Research in Astronomy(AURA)
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
#     1. Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
# 
#     2. Redistributions in binary form must reproduce the above
#       copyright notice, this list of conditions and the following
#       disclaimer in the documentation and/or other materials provided
#       with the distribution.
# 
#     3. The name of AURA and its representatives may not be used to
#       endorse or promote products derived from this software without
#       specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY AURA ``AS IS'' AND ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL AURA BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
# OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
# TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
# USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
# DAMAGE.
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



















































