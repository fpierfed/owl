#!/usr/bin/env python
"""
This is a single-entry OWL blackboard management Condor Job Hook.

It is assumed that the reader is familiar with Condor and the Job Hook mechanics
(described in section 4.4 of the Condor manual).

Instead of defining three different scripts for the 'Prepare Job', 'Update Job
Info' and 'Job Exit' Starter Hooks, we only have this one. Differentiating
between the three roles is done by noting that a 'Job Exit' Hook is the only one
which is passed arguments on the command-line. In addition, ClassAds passed as
STDIN to 'Prepare Job' Hooks lack these attributes: JobState, JobPid, NumPids,
JobStartDate, RemoteSysCpu, RemoteUserCpu and ImageSize. OWL does add JobState
if not present and sets it to 'Starting'.
"""
import logging
import os
import sys




def get_owl_nvironment(job_ad):
    """
    Look into the given job ClassAd for any extra environment variables that
    might be defined there but not in our shell environment. Return the
    corresponding Python {key: val} dictionary that we will then use to agument
    our environment.
    """
    environment = {}

    # Find the Environment = ... line.
    job_env_str = ''
    for line in job_ad.split('\n'):
        if(line.startswith('Environment = ')):
            job_env_str = line.split('Environment = ', 1)[1]
            job_env_str = job_env_str.strip()
            job_env_str = job_env_str[1:-1]

    # Replace ' ' with a placeholder.
    job_env_str = job_env_str.replace("' '", 'OWL_CONDOR_SPACE_SPLACEHOLDER')
    tokens = job_env_str.split()
    for token in tokens:
        # If this fails is because we have screwed up badly and we need to know.
        key, val = token.split('=', 1)
        if(key.startswith('OWL')):
            environment[key] = val.replace('OWL_CONDOR_SPACE_SPLACEHOLDER', ' ')
    return(environment)



def setup_logger(name):
    """
    Create and customize our logger.
    """
    # Get the path to the GRID-provided temp directory.
    tmp_path = '/tmp'
    for key in ('TMP', 'TMPDIR', 'TEMP'):
        if(os.environ.has_key(key)):
            tmp_path = os.environ[key]
            break

    log_name = name + '-' + os.environ.get('USER', 'UNKNOWN') + '.log'
    LOG_FILE_NAME = os.path.join(tmp_path, log_name)
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    LOG_LEVEL = logging.DEBUG

    # Configure logging.
    mylogger = logging.getLogger(name)
    mylogger.setLevel(LOG_LEVEL)
    fh = logging.FileHandler(LOG_FILE_NAME)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter(LOG_FORMAT)
    fh.setFormatter(formatter)
    mylogger.addHandler(fh)
    return(mylogger)



def update_blackboard_entry(job_ad):
    """
    Update the Blackboard entry corresponding to the given Job ClassAd and
    create it if it is not there.
    """
    from owl.classad import Job
    from owl import blackboard


    # Create a Job instance from the ClassAd.
    job = Job.new_from_classad(job_ad)

    # Now, we could be in a rescue DAG, meaning that the blackboard entry might
    # already be there. If this is the case, we just update that entry and not
    # create a new one.
    entry = None
    try:
        entry = blackboard.getEntry(entryId=job.GlobalJobId)
    except:
        pass

    if(entry):
        # Tell the Blackboard that we have a new Job starting up.
        blackboard.updateEntry(job)
    else:
        # Tell the Blackboard that we have a new Job starting up.
        blackboard.createEntry(job)
    return


def close_blackboard_entry(job_ad):
    """
    Close the Blackboard entry corresponding to the given Job ClassAd.
    """
    from owl.classad import Job
    from owl import blackboard


    # Create a Job instance from the ClassAd.
    job = Job.new_from_classad(job_ad)

    # Close the Blackboard entry corresponding to job.
    blackboard.closeEntry(job)
    return





if(__name__ == '__main__'):
    EXTRA_ATTRS = ('JobState', 'JobPid', 'NumPids', 'JobStartDate',
                   'RemoteSysCpu', 'RemoteUserCpu', 'ImageSize')


    # Setup logging.
    logger = setup_logger('owl_blackboard_hooks')

    # Read the raw ClassAd from STDIN and create a Job instance.
    logger.debug("Parsing STDIN.")
    classad = sys.stdin.read()

    # Determine the role of this hook.
    role = None
    if(len(sys.argv) > 1):
        exitReason = sys.argv[1]
        role = 'job_exit'
    else:
        exitReason = None
        n = 0
        for attr in EXTRA_ATTRS:
            if(attr + ' =' in classad):
                n += 1
        if(n > len(EXTRA_ATTRS) / 2.):
            role = 'update_job'
        else:
            role = 'prepare_job'
    logger.debug('Script invoked as %s hook.' % (role))

    # Agument the (very restricted) environment with OWL specific variables
    # defined in the job/ClassAd Environment (if any).
    owlEnv = get_owl_nvironment(classad)
    for k in owlEnv.keys():
        v = owlEnv[k]
        logger.debug('Adding ENV(%s) = %s' % (k, v))
        os.environ[k] = v

    from owl.config import DATABASE_CONNECTION_STR
    logger.debug('OWL DATABASE_CONNECTION_STR = %s' % (DATABASE_CONNECTION_STR))


    if(role == 'job_exit'):
        # Close the Blackboard entry and be happy.
        fn = close_blackboard_entry
    else:
        # create (or uodate if already there) a Blackboard entry for job.
        fn = update_blackboard_entry

    # What are you waiting for?
    logger.debug('Upodating the blackboard.')
    try:
        fn(classad)
    except:
        logger.exception('Error updating the database.')
    else:
        logger.debug('Update done.')

    # Just exit.
    sys.exit(0)
