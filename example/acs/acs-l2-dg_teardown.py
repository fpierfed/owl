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
Teardown phase

The input datum is a full visit made of N HST ACS exposures. The visit 
identifier is given in the $OSF_DATASET environment variable.

The teardown phase is made of steps that:
    - Copy all FITS and log files to an output location (/hladev/datalinks/'+ 
      INST + '/' + FINAL_DIR)
    - Create a zoomed image.
    - Create previews.
    - Update the footprint information in the database (using a cluster-level
      lock file).
    - Invoke the acs-l2-sl pipeline for catalog generation.
    - Update the file status in the database.
    - Cleanup and quit.

Environment variables.
This script assumes that the following environment variables are defined:
    - $OSF_DATASET (the identifier of the visit to process).
    - $DADS (root of the data repository).
    - $IRAF (root to the IRAF installation directory: see IRAF docs).
    - $ARCHIVE_DATA (path to the ACS raw exposure repository, under $DADS).
    - $RAW (path to the working area for raw exposures).


This script is the aggregation of 
    acsDgStore
    acsDgZoom
    acsMakePreviews
    acsDgFootprint
    acsDgSlTrig
    acsDgUpdateFileStatus
    acsDgClean
from acs-l2-dg.xml
"""
import os
import sys

from hlalib import *



# Constants. We override those defined in hlalib.
# TODO: Put all of these in a config file.
# Environment variables assumed to be defined.
ENV_VARS = ('OSF_DATASET', 'FINAL', 'WORK', 'IRAF', 'HLA_DATA_PATHS', 
            'HLAPipelineBin', )
# Where do we find the code to execute?
CODE_ROOT = os.environ['HLAPipelineBin']
# Which scripts shall we execute (in order!)?
# TODO: This is more effient than a class, but ugly...
SCRIPT_DEFS = ({'exe': os.path.join(CODE_ROOT, 'execPipelineStep'),
                'args': [os.path.join(CODE_ROOT, 'storeProduct'), 'acs'],
                'timeout': None},
               {'exe': os.path.join(CODE_ROOT, 'execPipelineStep'),
                'args': [os.path.join(CODE_ROOT, 'callMakeZoom'), 'acs'],
                'timeout': None},
               {'exe': os.path.join(CODE_ROOT, 'execPipelineStep'),
                'args': [os.path.join(CODE_ROOT, 'callMakePreviews'), 'acs'],
                'timeout': None}, 
               {'exe': os.path.join(CODE_ROOT, 'execPipelineStep'),
                'args': [os.path.join(CODE_ROOT, 'callAcsPipelineUpdateSingle'), ],
                'timeout': None},
               {'exe': os.path.join(CODE_ROOT, 'execPipelineStep'),
                'args': [os.path.join(CODE_ROOT, 'callAcsPhot'), ],
                'timeout': None},
               {'exe': os.path.join(CODE_ROOT, 'execPipelineStep'),
                'args': [os.path.join(CODE_ROOT, 'updateFileStatus'), ],
                'timeout': None},
               {'exe': os.path.join(CODE_ROOT, 'cleanupFitsGeneration'),
                'args': ['acs', ],
                'timeout': None},)






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










































