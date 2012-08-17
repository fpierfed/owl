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
    -m, --mode=MODE                     instrument mode
    -g, --grid-middleware=MIDDLEWARE    middleware name
    -e,--env=KEY=VAL(,KEY=VAL)*

'''
TEMPLATE_ROOT = getattr(config, 'DIRECTORIES_TEMPLATE_ROOT')
CODE_ROOT = getattr(config, 'DIRECTORIES_PIPELINE_ROOT')
WORK_ROOT = getattr(config, 'DIRECTORIES_WORK_ROOT')





class BcwWorkflow(workflow.Workflow):
    """
    Simple BCW workflow.
    """
    def get_extra_keywords(self, code_root, repository, dataset, work_dir,
                           flavour, extra_env):
        return({'num_ccds': _get_number_of_ccds(repository, dataset)})



def _get_number_of_ccds(repository, dataset):
    """
    Return the number of extensions/CCDs in the given dataset (i.e. in the FITS
    file `repository`/`dataset`.fits). In this case we assume that the FITS file
    is simply a BCW text file and its CCDs are lines in the file.
    """
    lines = [l
             for l in open(os.path.join(repository,
                                        dataset + '.fits')).readlines()
             if(l.strip())]
    return(len(lines))


def process(datasets, repository, template_root, code_root=CODE_ROOT,
            extra_env=None, work_root=WORK_ROOT, middleware='condor',
            verbose=False):
    """
    Given a list of datasets to process and a directory of templates, for each
    dataset, determine the number of CCDs, render the full workflow template set
    and submit the instantiated workflow to the grid. Return immediately upon
    workflow submission.
    """
    if(extra_env is None):
        extra_env = {}

    # Create a simple work directory path: work_root/<user>_<timestamp>
    dir_name = '%s_%f' % (os.environ.get('USER', 'UNKNOWN'), time.time())
    work_dir = os.path.join(work_root, dir_name)

    for dataset in datasets:
        # Create a instrument/mode Workflow instance (dataset independent)...
        wflow = workflow.BcwWorkflow(template_root=template_root)
        # ... and submit it to the grid (for this particular piece of data).
        _id = wflow.execute(code_root=code_root,
                            repository=repository,
                            dataset=dataset,
                            work_dir=work_dir,
                            flavour=middleware)
        print('Dataset %s submitted as job %s' % (dataset, _id))
        print('  Work directory: %s' % (work_dir))
    return(0)



def _parse_extra_environment_info(raw_user_env):
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
    def parse_token(token):
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

    if(not raw_user_env):
        return({})

    return(dict(map(parse_token, raw_user_env.split(','))))







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
    if(not os.path.exists(options.repository)):
        parser.error('Please specify a valid repository path.')

    instrumentPath = os.path.join(TEMPLATE_ROOT, options.instrument)
    if(not os.path.exists(instrumentPath)):
        parser.error('Please specify a valid instrument name.')

    templateDir = os.path.join(instrumentPath, options.mode)
    if(not os.path.exists(templateDir)):
        parser.error('Please specify a valid instrument mode.')

    # Now see if we have to do any environment variable parsing/setting up.
    env = _parse_extra_environment_info(options.env)

    # Run!
    sys.exit(process(datasets=args,
                     repository=options.repository,
                     template_root=templateDir,
                     middleware=options.middleware,
                     extra_env=env,
                     verbose=options.verbose))










































