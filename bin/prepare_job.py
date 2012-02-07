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
import logging
import os
import sys




def getOwlEnvironment(job):
    env = {}
    try:
        jobEnvStr = job.Environment
    except:
        jobEnvStr = ''
    # Replace ' ' with a placeholder.
    jobEnvStr = jobEnvStr.replace("' '", 'OWL_CONDOR_SPACE_SPLACEHOLDER')
    tokens = jobEnvStr.split()
    for token in tokens:
        # If this fails is because we have screwed up badly and we need to know.
        key, val = token.split('=', 1)
        if(key.startswith('OWL')):
            env[key] = val.replace('OWL_CONDOR_SPACE_SPLACEHOLDER', ' ')
    return(env)



def getJob(stream=sys.stdin):
    from owl.job import Job
    
    return(Job.newFromClassAd(stream.read()))



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



def createBlackboardEntry(job):
    from owl import blackboard
    
    
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





if(__name__ == '__main__'):
    logger = setupLogger('prepare_job')
    
    # Read the raw ClassAd from STDIN and create a Job instance.
    logger.debug("Parsing STDIN to create a Job instance.")    
    job = getJob(sys.stdin)
    logger.debug("Created job instance.")
    
    # Agument the (very restricted) environment with OWL specific variables defined
    # in job.Environment (if any).
    owlEnv = getOwlEnvironment(job)
    for k in owlEnv.keys():
        os.environ[k] = owlEnv[k]
    
    # Now create (or uodate if already there) a Blackboard entry for job.
    logger.debug('Upodating the blackboard.')
    try:
        createBlackboardEntry(job)
    except:
        logger.exception('Error updating the database.')
    else:
        logger.debug('Update done.')
        
    # Just exit.
    sys.exit(0)
