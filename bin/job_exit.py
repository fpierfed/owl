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

from owl import blackboard
from owl import job



# Get the path to the GRID-provided temp directory.
tmpPath = '/tmp'
for k in ('TMP', 'TMPDIR', 'TEMP'):
    if(os.environ.has_key(k)):
        tmpPath = os.environ[k]
        break

# Constants
logName = 'job_exit-' + os.environ.get('USER', 'UNKNOWN') + '.log'
LOG_FILE_NAME = os.path.join(tmpPath, logName)
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_LEVEL = logging.DEBUG



# Configure logging.
logger = logging.getLogger('job_exit')
logger.setLevel(LOG_LEVEL)
fh = logging.FileHandler(LOG_FILE_NAME)
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter(LOG_FORMAT)
fh.setFormatter(formatter)
logger.addHandler(fh)

# Read the raw ClassAd from STDIN
logger.debug("Reading STDIN.")
ad = sys.stdin.read()
logger.debug("Read STDIN.")

# Create a Job instance
logger.debug("Creating job instance.")
j = job.Job.newFromClassAd(ad)
logger.debug("Created job instance.")

# Tell the Blackboard that we have a new Job starting up.
try:
    blackboard.closeEntry(j)
except:
    logger.exception('Error updating the database.')
else:
    logger.debug("Updated database.")

# Just exit.
sys.exit(0)
