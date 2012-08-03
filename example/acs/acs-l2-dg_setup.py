#!/usr/bin/env python
"""
Setup phase

The input datum is a full visit made of N HST ACS exposures. The visit
identifier is given in the $OSF_DATASET environment variable.

The setup phase is made of steps that:
    - Make sure that the visit is entirely made of public exposures.
    - Make sure that all the raw exposures are there. If the raw exposures are
      not on HLA file systems, a request is made for the files to HST DADS
      system to a partial calibration pipeline. DADS processes the files, ftp's
      them to an HLA dir and then they are moved to the HLA datalinks area
      /hladev/datalinks/instrument/proposal/proposal_visistId e.g
      /hladev/datalinks/acs/11013/11013_01  In the datalinks area there is a sub
      dir for each instrument/category of data, in each of those subdirs is a
      symbolic link to the actual data.
    - Make sure that none of the raw exposures have valid science data.
    - Copy all raw exposures to a work area ($RAW).

Environment variables.
This script assumes that the following environment variables are defined:
    - $OSF_DATASET (the identifier of the visit to process).
    - $DADS (root of the data repository).
    - $IRAF (root to the IRAF installation directory: see IRAF docs).
    - $ARCHIVE_DATA (path to the ACS raw exposure repository, under $DADS).
    - $RAW (path to the working area for raw exposures).


This script is the aggregation of
    acsDgIsprop
    acsDgFetch
    acsDgFilter
    acsDgStage
from acs-l2-dg.xml
"""
import os
import sys

from hlalib import *



# Constants. We override those defined in hlalib.
# TODO: Put all of these in a config file.
# Environment variables assumed to be defined.
ENV_VARS = ('OSF_DATASET', 'DADS', 'IRAF', 'ARCHIVE_DATA', 'RAW',
            'HLAPipelineBin')
# Where do we find the code to execute?
CODE_ROOT = os.environ['HLAPipelineBin']
# Which scripts shall we execute (in order!)?
# TODO: This is more effient than a class, but ugly...
SCRIPT_DEFS = ({'exe': os.path.join(CODE_ROOT, 'isProprietary'),
                'args': [],
                'timeout': None},
               {'exe': os.path.join(CODE_ROOT, 'execPipelineStep'),
                'args': [os.path.join(CODE_ROOT, 'fetchAcsRaw'), ],
                'timeout': 2 * 3600},                               # 2 hours.
               {'exe': os.path.join(CODE_ROOT, 'execPipelineStep'),
                'args': [os.path.join(CODE_ROOT, 'filterBadAcs.py'), ],
                'timeout': None},
               {'exe': os.path.join(CODE_ROOT, 'execPipelineStep'),
                'args': [os.path.join(CODE_ROOT, 'stageAcsVisit'), ],
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










































