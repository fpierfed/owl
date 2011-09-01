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
run_multidrizzle.py

OWL wrapper around Multidrizzle. Process a full set of ACS exposures and produce
the drizzled mosaic (i.e. _drz.fits) file.

We can safely (i.e. given the fact that we run under OWL) assume that:
    1. All the input files are in the current working directory.
    2. All the support Python modules (i.e. stsci_python) have been installed.
    3. All the relevant reference files are present.
    4. All relevant environment vcariables (e.g. $jref, $mtab etc.) are defined.
    5. All the input files can be safely drizzled together.
    6. The environment variable DATASET is set to the root of the _drz file.

Usage
    shell> run_multidrizzle.py <association _asn.fits>
"""
import os
import sys

import pyfits


def asn2exp(asn_file, n=None):
    """
    Given an association file and an integer n, return the name of the nth 
    exposure file belonging to that association. If n is omitted, return all the
    members of the association. If n is outside its vaid range (0-indexed), 
    raise an exception.
    
    The return value is either an exception bein risen or a list. If n is in its
    valid range, the list has a single member.
    """
    try:
        t = pyfits.open(asn_file)
    except:
        msg = '%s is not a valid association FITS file.' % (asn_file)
        raise(Exception(msg))
    
    # The association file has a global header as extension 0 and a binary table
    # as extension 1.
    try:
        exps = [row[0].lower() for row in t[1].data if row[1] == 'EXP-DTH']
    except:
        msg = '%s is not a valid association table FITS file.' % (asn_file)
        raise(Exception(msg))
    t.close()
    
    # Does n make sense?
    valid = range(len(exps))
    if(n != None and n not in valid):
        raise(Exception('%d is outside the valid range %s.' % (n, valid)))
    
    # If n == None, return the whole list. Otherwise the nth exposure file.
    if(n == None):
        return(exps)
    return([exps[n], ])





if(__name__ == '__main__'):
    # Run mkiraf so that PyRAF is happy.
    os.system('rm -rf login.cl uparm')
    os.system('echo vt100 | mkiraf')
    from pyraf import iraf
    from iraf import stsdas, hst_calib, acs, multidrizzle
    
    
    
    # Get the value of $DATASET.
    root = os.environ.get('DATASET', 'final').lower()
    
    # Run Multidrizzle on the input files.
    asn_file = sys.argv[1]
    exps = asn2exp(asn_file)
    input_str = ','.join(['%s_flt.fits' % (e) for e in exps])
    multidrizzle(input=input_str, output=root)
    
    # Make sure that the _flt.fits file was indeed produced. I guess there is no 
    # other way of catching an error...
    if(not os.path.exists('%s_drz.fits' % (root))):
        print('Error: Multidrizzle("%s") failed.' % (input_str))
        sys.exit(1)
    sys.exit(0)


















































