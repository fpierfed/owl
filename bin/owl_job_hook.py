#!/usr/local/owl-dev/bin/python
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
import datetime
import logging
import os
import sys
import time




def get_owl_environment(job_ad):
    """
    Look into the given job ClassAd for any extra environment variables that
    might be defined there but not in our shell environment. Return the
    corresponding Python {key: val} dictionary that we will then use to agument
    our environment.
    """
    from owl import classad

    # Find the Environment = ... line.
    job_env_str = ''
    for line in job_ad.split('\n'):
        if(line.startswith('Environment = ')):
            job_env_str = line.split('Environment = ', 1)[1]
            break

    return(dict([(k, v) for (k, v) \
                 in classad.parse_classad_environment(job_env_str).items() \
                 if k.startswith('OWL')]))



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
    tmp_path = '/tmp'
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



def update_blackboard_entry(job_ad, logger=None):
    """
    Update the Blackboard entry corresponding to the given Job ClassAd.
    """
    from owl.classad import Job
    from owl import blackboard


    # Create a Job instance from the ClassAd and use it to update the BB.
    blackboard.updateEntry(Job.new_from_classad(job_ad))
    return


def create_blackboard_entry(job_ad, logger=None):
    """
    Create a new Blackboard entry from `job_ad`. In case a Blackboard entry with
    the same GlobalJobId already exists (which would happen e.g. in a rescue
    DAG), fallback to an update.
    """
    from owl.classad import Job
    from owl import blackboard

    # Now, we could be in a rescue DAG, meaning that the blackboard entry might
    # already be there. If this is the case, we just update that entry and not
    # create a new one.
    entry = None
    job = Job.new_from_classad(job_ad)
    try:
        entry = blackboard.getEntry(entryId=job.GlobalJobId)
    except:
        pass

    if(entry):
        update_blackboard_entry(job_ad, logger)
    else:
        blackboard.createEntry(job)
    return


def close_blackboard_entry(job_ad, logger=None):
    """
    Close the Blackboard entry corresponding to the given Job ClassAd.
    """
    from owl.classad import Job
    from owl import blackboard


    # Create a Job instance from the ClassAd.
    job = Job.new_from_classad(job_ad)

    # Make sure that the job CompletionDate is not 0 (as it seems to be all the
    # time).
    if(not hasattr(job, 'CompletionDate') or job.CompletionDate == 0):
        job.CompletionDate = job.JobStartDate + job.JobDuration
        if(logger):
            logger.debug('CompletionDate = 0 in an exit hook: setting it to %d'
                         % (job.CompletionDate))

    # Close the Blackboard entry corresponding to job.
    blackboard.closeEntry(job)
    return





if(__name__ == '__main__'):
    EXTRA_ATTR = 'JobPid'


    # Setup logging.
    logger = setup_logger('owl_blackboard_hooks')

    logger.debug('This script has been invoked as %s' % (' '.join(sys.argv)))
    logger.debug('The current UTC datetime is %s'
                 % (datetime.datetime.utcnow().isoformat()))

    # Read the raw ClassAd from STDIN and create a Job instance.
    logger.debug("Parsing STDIN.")
    classad = sys.stdin.read()
    logger.debug('Raw ClassAd:\n---\n%s---' % (classad))

    # Determine the role of this hook.
    role = None
    if(len(sys.argv) > 1):
        exitReason = sys.argv[1]
        role = 'job_exit'
    else:
        exitReason = None
        n = 0
        if(EXTRA_ATTR + ' =' in classad):
            role = 'update_job'
        else:
            role = 'prepare_job'
    logger.debug('Script invoked as %s hook.' % (role))

    # Agument the (very restricted) environment with OWL specific variables
    # defined in the job/ClassAd Environment (if any).
    owlEnv = get_owl_environment(classad)
    for k in owlEnv.keys():
        v = owlEnv[k]
        logger.debug('Adding ENV(%s) = %s' % (k, v))
        os.environ[k] = v

    from owl.config import DATABASE_CONNECTION_STR
    logger.debug('OWL DATABASE_CONNECTION_STR = %s' % (DATABASE_CONNECTION_STR))


    if(role == 'job_exit'):
        fn = close_blackboard_entry
    elif(role == 'prepare_job'):
        fn = create_blackboard_entry
    elif(role == 'update_job'):
        fn = update_blackboard_entry
    else:
        # What did they ask us to do again?????
        logger.warning('Unsupported hook role: exiting with no action.')
        sys.exit(0)

    # What are you waiting for?
    logger.debug('Upodating the blackboard.')
    ok = False
    retries = 5
    while(retries > 0 and not ok):
        try:
            fn(classad, logger)
            ok = True
        except:
            retries -= 1
            logger.debug('Update failed: trying again (%d times to go).' \
                         % (retries))
            time.sleep(.1)

    if(not ok):
        logger.exception('Error updating the database.')
    else:
        logger.debug('Update done.')

    # Just exit.
    sys.exit(0)
