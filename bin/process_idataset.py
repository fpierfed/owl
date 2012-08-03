#!/usr/bin/env python
"""
Given a workflow template and a dataset name, create the DAG for the processing
of the dataset and submit it to the grid.

Workflow templates are arranged by instrument and observing mode:
    templates/
        instrument1/
            modeA/
                instrument1_modeA.dag
                dagNodeX.job
                dagNodeY.job
                dagNodeZ.job
                ...
            modeB/
                instrument1_modeA.dag
                dagNodeI.job
                dagNodeJ.job
                ...
        instrument2/
            modeC
                instrument2_modeC.dag
                dagNodeX1.job
                dagNodeY1.job
                dagNodeZ1.job
                dagNodeW1.job
                ...
            ...
        ...
Workflow templates use the Jinja2 Python template framework.

Variables used by the job/workflow templates are

    code_root           path to the root of the pipeline code
    repository          path to the raw data repository
    dataset             root name of the dataset to process
    num_ccds            number of CCDs to process

Different types of middleware can be used to execute the workflow on the user
data. The middleware is specified using the -g option and defaults to 'condor'.
Supported middleware is 'condor', 'makefile' or 'xgrid'.

File transfer uses iRODS.
"""
import os
import time

from owl import config
from owl import workflow





# Constants
USAGE = '''\
process_dataset.py OPTIONS <dataset root>

OPTIONS
    -r, --repository=PATH               path to the raw data respository
    -i, --instrument=INS                instrument name
    -i, --mode=MODE                     instrument mode
    -g, --grid-middleware=MIDDLEWARE    middleware name
    -e,--env=KEY=VAL(,KEY=VAL)*

'''
TEMPLATE_ROOT = config.DIRECTORIES_TEMPLATE_ROOT
CODE_ROOT = config.DIRECTORIES_PIPELINE_ROOT
WORK_ROOT = config.DIRECTORIES_WORK_ROOT




def process(datasets, repository, templateRoot, codeRoot=CODE_ROOT, extraEnv={},
            workRoot=WORK_ROOT, middleware='condor', verbose=False):
    """
    Given a list of datasets to process and a directory of templates, for each
    dataset, determine the number of CCDs, render the full workflow template set
    and submit the instantiated workflow to the grid. Return immediately upon
    workflow submission.
    """
    # Create a simple work directory path: workRoot/<user>_<timestamp>
    dirName = '%s_%f' % (os.environ.get('USER', 'UNKNOWN'), time.time())
    workDir = os.path.join(workRoot, dirName)

    for dataset in datasets:
        # Create a instrument/mode Workflow instance (dataset independent)...
        wflow = workflow.BcwIrodsWorkflow(templateRoot=templateRoot)
        # ... and submit it to the grid (for this particular piece of data).
        wflow.execute(codeRoot=codeRoot,
                      repository=repository,
                      dataset=dataset,
                      workDir=workDir,
                      flavour=middleware)
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
    parser.add_option('-i', '--instrument',
                      dest='instrument',
                      type='str',
                      default=None,
                      help='instrument name')
    parser.add_option('-m', '--mode',
                      dest='mode',
                      type='str',
                      default=None,
                      help='instrument mode')
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
    if(not options.instrument):
        parser.error('Please specify the instrument name.')
    if(not options.mode):
        parser.error('Please specify the instrument mode.')
    if(not args):
        parser.error('Please specify the dataset name(s).')

    # Make sure that that suff actually exists.
    # FIXME: make sure that the iRODS repository exists!!!!
    instrumentPath = os.path.join(TEMPLATE_ROOT, options.instrument)
    if(not os.path.exists(instrumentPath)):
        parser.error('Please specify a valid instrument name.')

    templateDir = os.path.join(instrumentPath, options.mode)
    if(not os.path.exists(templateDir)):
        parser.error('Please specify a valid instrument mode.')

    # Now see if we have to do any environment variable parsing/setting up.
    env = _parseExtraEnvironmentInfo(options.env)

    # Run!
    sys.exit(process(datasets=args,
                     repository=options.repository,
                     templateRoot=templateDir,
                     middleware=options.middleware,
                     extraEnv=env,
                     verbose=options.verbose))










































