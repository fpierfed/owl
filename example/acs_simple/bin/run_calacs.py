#!/usr/bin/env python
"""
run_calacs.py

OWL wrapper around CALACS. Process a single ACS exposure and produce the
calibrated (i.e. _flt.fits) file.

We can safely (i.e. given the fact that we run under OWL) assume that:
    1. All the input files are in the current working directory.
    2. All the support Python modules (i.e. stsci_python) have been installed.
    3. All the relevant reference files are present.
    4. All relevant environment vcariables (e.g. $jref, $mtab etc.) are defined.

Usage
    shell> run_calacs.py <association _asn.fits> <exposure number>

Exposure numbers start with 0.
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
    from iraf import stsdas, hst_calib, acs, calacs



    # Run CALACS on the input file.
    asn_file = sys.argv[1]
    exp_id = int(sys.argv[2])
    raw_file = '%s_raw.fits' % (asn2exp(asn_file, exp_id)[0])
    calacs(input=raw_file)

    # Make sure that the _flt.fits file was indeed produced. I guess there is no
    # other way of catching an error...
    if(not os.path.exists(raw_file.replace('_raw.fits', '_flt.fits'))):
        print('Error: CALACS("%s") failed.' % (raw_file))
        sys.exit(1)
    sys.exit(0)


















































