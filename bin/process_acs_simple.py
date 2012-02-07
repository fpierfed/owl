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
Given the name of an ACS association, create the DAG for processing it (meaning
running CALACS on each exposure of the visit and multidrizzle on the total 
output of CALACS) and submit it to the grid.

Variables used by the job/workflow templates are

    code_root           path to the root of the pipeline code
    repository          path to the raw data repository
    dataset             name of the association to process (9 characters)
    exposures           root names of the exposures in the visit (internal)

Different types of middleware can be used to execute the workflow on the user 
data. The middleware is specified using the -g option and defaults to 'condor'.
Supported middleware is 'condor', 'makefile' or 'xgrid'.
"""
import os
import time

from owl import config
from owl import workflow





# Constants
USAGE = '''\
process_acs_simple.py OPTIONS <association name>

OPTIONS
    -r, --repository=PATH               path to the raw data respository
    -g, --grid-middleware=MIDDLEWARE    middleware name
    -e,--env=KEY=VAL(,KEY=VAL)*
    
'''
TEMPLATE_ROOT = config.DIRECTORIES_TEMPLATE_ROOT
CODE_ROOT = config.DIRECTORIES_PIPELINE_ROOT
WORK_ROOT = config.DIRECTORIES_WORK_ROOT
INSTRUMENT = 'acs'
MODE = 'simple'





def process(datasets, repository, templateRoot, codeRoot=CODE_ROOT, extraEnv={},
            workRoot=WORK_ROOT, middleware='condor', verbose=False):
    """
    Given a list of datasets (i.e. association names) to process and a directory 
    of templates, for each association determine the number of exposures, render
    the full workflow template set and submit the instantiated workflow to the 
    grid. Return immediately upon workflow submission.
    """
    # Create a simple work directory path: workRoot/<user>_<timestamp>
    dirName = '%s_%f' % (os.environ.get('USER', 'UNKNOWN'), time.time())
    workDir = os.path.join(workRoot, dirName)
    
    for dataset in datasets:
        # Create a instrument/mode Workflow instance (dataset independent)...
        wflow = workflow.AcsSimpleWorkflow(templateRoot=templateRoot)
        # ... and submit it to the grid (for this particular piece of data).
        _id = wflow.execute(codeRoot=codeRoot, 
                            repository=repository, 
                            dataset=dataset, 
                            workDir=workDir,
                            flavour=middleware)
        print('Dataset %s submitted as job %s' % (dataset, _id))
    return(0)
    


def _parseExtraEnvironmentInfo(rawUSerEnvInfo):
    """
    Given a string of the form
    
        KEY=VAL(,KEY=VAL)*
    
    where KEYs are environment variable names and VARs their respective values,
    parse the input string and return a dictionary of the form
    
        {KEY: VAL, }
    
    Only simple parsing is supported. This means, among other things, that VALs
    are assumed to be strings. No arrays, list, tuples are supported. The 
    returned dictionary is assumed to be usable to agument the user environment.
    """
    def parseToken(token):
        """
        Parse a 
            KEY=VAL
        string, allowing for spaces on either side of the = sign. Return the
        parsed (KEY, VAL) tuple.
        """
        if(not token or '=' not in token):
            return(())
        items = token.split('=', 1)
        return((items[0].strip(), items[1].strip()))
    
    if(not rawUSerEnvInfo):
        return({})
    
    return(dict(map(parseToken, rawUSerEnvInfo.split(','))))







if(__name__ == '__main__'):
    import optparse
    import sys
    
    
    
    # Setup the command line option parser and do the parsing.
    parser = optparse.OptionParser(USAGE)
    parser.add_option('-r', '--repository',
                      dest='repository',
                      type='str',
                      default=None,
                      help='path to the raw data respository')
    parser.add_option('-g', '--grid-middleware',
                      dest='middleware',
                      type='str',
                      default='condor',
                      help='grid/local middleware to use')
    parser.add_option('-e', '--env',
                      dest='env',
                      type='str',
                      default='',
                      help='any extra environment variable to use')
    # Verbose flag
    parser.add_option('-v',
                      action='store_true',
                      dest='verbose',
                      default=False)
    
    # Parse the command line args.
    (options, args) = parser.parse_args()
    
    # Sanity check: all the opions (apart from verbose) are required.
    if(not options.repository):
        parser.error('Please specify the repository path.')
    if(not args):
        parser.error('Please specify the name of the association(s).')
    
    # Make sure that that suff actually exists.
    if(not os.path.exists(options.repository)):
        parser.error('Please specify a valid repository path.')
    
    instrumentPath = os.path.join(TEMPLATE_ROOT, INSTRUMENT)
    if(not os.path.exists(instrumentPath)):
        print('Unable to find templates for %s' % (INSTRUMENT))
        sys.exit(1)
    
    templateDir = os.path.join(instrumentPath, MODE)
    if(not os.path.exists(templateDir)):
        print('Unable to find templates for %s/%s' % (INSTRUMENT, MODE))
        sys.exit(2)
    
    # Now see if we have to do any environment variable parsing/setting up.
    env = _parseExtraEnvironmentInfo(options.env)
    
    # Run!
    sys.exit(process(datasets=args, 
                     repository=options.repository, 
                     templateRoot=templateDir, 
                     middleware=options.middleware,
                     extraEnv=env,
                     verbose=options.verbose))










































