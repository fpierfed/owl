#!/usr/bin/env python
"""
Process phase

The input datum is a full visit made of N HST ACS exposures. The visit
identifier is given in the $OSF_DATASET environment variable.

The process phase is made of steps that:
    - Run CALACS on each raw exposure in a round-robin.
    - Run Multidrizzle on all calibrated exposures.
    - Compute absolute astrometric solution for the output of Multidrizzle.

Environment variables.
This script assumes that the following environment variables are defined:
    - $OSF_DATASET (the identifier of the visit to process).
    - $DADS (root of the data repository).
    - $IRAF (root to the IRAF installation directory: see IRAF docs).
    - $ARCHIVE_DATA (path to the ACS raw exposure repository, under $DADS).
    - $RAW (path to the working area for raw exposures).


This script is the aggregation of
    acsDgCal
    acsDgDrz
    acsDgAstrometry
from acs-l2-dg.xml
"""
import os
import sys

from hlalib import *



# Constants. We override those defined in hlalib.
# TODO: Put all of these in a config file.
# Environment variables assumed to be defined.
ENV_VARS = ('OSF_DATASET', 'SOFTWARE', 'REF', 'jref', 'CAL', 'RAW', 'WORK',
            'mtab', 'crotacomp', 'cracscomp', 'hostmachine', 'CONTROL', 'LOG',
            'DADS', 'FINAL', 'HLAPipelineBin', 'FITSGeneration')
# Where do we find the code to execute?
CODE_ROOT = os.environ['HLAPipelineBin']
CODE_ROOT2 = os.environ['FITSGeneration']
# Which scripts shall we execute (in order!)?
# TODO: This is more effient than a class, but ugly...
SCRIPT_DEFS = ({'exe': os.path.join(CODE_ROOT, 'execPipelineStep'),
                'args': [os.path.join(CODE_ROOT2, 'do_calacs.py'), ],
                'timeout': None},
               {'exe': os.path.join(CODE_ROOT, 'execPipelineStep'),
                'args': [os.path.join(CODE_ROOT2, 'do_multidrizzle.py'), ],
                'timeout': None},
               {'exe': os.path.join(CODE_ROOT, 'execPipelineStep'),
                'args': [os.path.join(CODE_ROOT2, 'do_astrometry.py'), ],
                'timeout': None})






if(__name__ == '__main__'):
    # Make sure that the environment makes sense: better to fail early.
    checkEnvironment(vars=ENV_VARS)

    # Now that we knoe that the environment makes some sense, let's run the four
    # scripts we need to execute. They do not take arguments and work in a separate
    # disk location, not the current directory necessarily. They also take all
    # inputs from the environment.

    # Execute the four scripts in a row. Break if any returned a non 0 exit
    # code.
    sys.exit(executeScripts(scriptInfoList=SCRIPT_DEFS))










































