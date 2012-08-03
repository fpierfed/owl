#!/usr/bin/env python
"""
asn2exp.py

Given an association FIST file (i.e. an HST _asn.fits file) and an integer n,
print out the root name of the nth exposure file belonging to that association.
If n is omitted, print the root names of all the members of the association. If
n is outside its vaid range (0-indexed), print an error message and return an
error code.

Examples
    shell> asn2exp.py j9am01070_asn.fits
    j9am01edq
    j9am01egq
    j9am01ejq
    j9am01emq

    shell> asn2exp.py j9am01070_asn.fits 0
    j9am01edq

    shell> asn2exp.py j9am01070_asn.fits 2
    j9am01ejq

    shell> asn2exp.py j9am01070_asn.fits -1
    Error

    shell> asn2exp.py j9am01070_asn.fits 4
    Error
"""
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
    import os
    import sys



    # Constants
    USAGE = 'Usage: asn2exp.py <asn file> [<exposore index>]'


    # Parse command line arguments.
    nargs = len(sys.argv)
    if(nargs < 2):
        # Not enough arguments.
        print('Error: please specify at least one association file.')
        print(USAGE)
        sys.exit(1)
    elif(nargs == 2):
        # We just have the association file.
        asn_file = sys.argv[1]
        if(not os.path.exists(asn_file)):
            print('Error: %s does not exist.' % (asn_file))
            print(USAGE)
            sys.exit(2)
        n = None
    else:
        # Get the first two and ignore the rest.
        asn_file = sys.argv[1]
        if(not os.path.exists(asn_file)):
            print('Error: %s does not exist.' % (asn_file))
            print(USAGE)
            sys.exit(2)
        try:
            n = int(sys.argv[2])
        except:
            print('Error: the exposure index is not an integer.')
            print(USAGE)
            sys.exit(3)

    # Run it and quit.
    try:
        exps = asn2exp(asn_file, n)
    except Exception as e:
        print('Error: %s ' % (e.args[0]))
        print(USAGE)
        sys.exit(255)

    for exp in exps:
        print(exp)
    sys.exit(0)















































