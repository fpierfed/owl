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




def getOwlEnvironment(ad):
    env = {}

    # Find the Environment = ... line.
    jobEnvStr = ''
    for line in ad.split('\n'):
        if(line.startswith('Environment = ')):
            jobEnvStr = line.split('Environment = ', 1)[1]
            jobEnvStr = jobEnvStr.strip()
            jobEnvStr = jobEnvStr[1:-1]

    # Replace ' ' with a placeholder.
    jobEnvStr = jobEnvStr.replace("' '", 'OWL_CONDOR_SPACE_SPLACEHOLDER')
    tokens = jobEnvStr.split()
    for token in tokens:
        # If this fails is because we have screwed up badly and we need to know.
        key, val = token.split('=', 1)
        if(key.startswith('OWL')):
            env[key] = val.replace('OWL_CONDOR_SPACE_SPLACEHOLDER', ' ')
    return(env)



def setupLogger(name):
    # Get the path to the GRID-provided temp directory.
    tmpPath = '/tmp'
    for k in ('TMP', 'TMPDIR', 'TEMP'):
        if(os.environ.has_key(k)):
            tmpPath = os.environ[k]
            break

    logName = name + '-' + os.environ.get('USER', 'UNKNOWN') + '.log'
    LOG_FILE_NAME = os.path.join(tmpPath, logName)
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    LOG_LEVEL = logging.DEBUG

    # Configure logging.
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)
    fh = logging.FileHandler(LOG_FILE_NAME)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter(LOG_FORMAT)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return(logger)



def createOrUpdateBlackboardEntry(ad):
    from owl.classad import Job
    from owl import blackboard


    # Create a Job instance from the ClassAd.
    job = Job.newFromClassAd(ad)

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


def closeBlackboardEntry(ad):
    from owl.classad import Job
    from owl import blackboard


    # Create a Job instance from the ClassAd.
    job = Job.newFromClassAd(ad)

    # Close the Blackboard entry corresponding to job.
    blackboard.closeEntry(job)
    return





if(__name__ == '__main__'):
    EXTRA_ATTRS = ('JobState', 'JobPid', 'NumPids', 'JobStartDate',
                   'RemoteSysCpu', 'RemoteUserCpu', 'ImageSize')


    # Setup logging.
    logger = setupLogger('owl_blackboard_hooks')

    # Read the raw ClassAd from STDIN and create a Job instance.
    logger.debug("Parsing STDIN.")
    ad = sys.stdin.read()

    # Determine the role of this hook.
    role = None
    if(len(sys.argv) > 1):
        exitReason = sys.argv[1]
        role = 'job_exit'
    else:
        exitReason = None
        n = 0
        for attr in EXTRA_ATTRS:
            if(attr + ' =' in ad):
                n += 1
        if(n > len(EXTRA_ATTRS) / 2.):
            role = 'update_job'
        else:
            role = 'prepare_job'
    logger.debug('Script invoked as %s hook.' % (role))

    # Agument the (very restricted) environment with OWL specific variables
    # defined in the job/ClassAd Environment (if any).
    owlEnv = getOwlEnvironment(ad)
    for k in owlEnv.keys():
        v = owlEnv[k]
        logger.debug('Adding ENV(%s) = %s' % (k, v))
        os.environ[k] = v

    from owl.config import *
    logger.debug('OWL DATABASE_CONNECTION_STR = %s' % (DATABASE_CONNECTION_STR))


    if(role == 'job_exit'):
        # Close the Blackboard entry and be happy.
        fn = closeBlackboardEntry
    else:
        # create (or uodate if already there) a Blackboard entry for job.
        fn = createOrUpdateBlackboardEntry

    # What are you waiting for?
    logger.debug('Upodating the blackboard.')
    try:
        fn(ad)
    except:
        logger.exception('Error updating the database.')
    else:
        logger.debug('Update done.')

    # Just exit.
    sys.exit(0)
