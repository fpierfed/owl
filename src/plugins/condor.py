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
Define all Condor-specific DRMAA properties here.
"""
import os

import drmaa
import jinja2

from owl.utils import which



# Constants
NATIVE_SPEC_TMPL = jinja2.Template('''\
universe        = scheduler
log             = {{ dag_name }}.dagman.log
remove_kill_sig = SIGUSR1
getenv          = True
on_exit_remove	= ( ExitSignal =?= 11 || (ExitCode =!= UNDEFINED && ExitCode >=0 && ExitCode <= 2))
copy_to_spool	= False
arguments       = "-f -l . -Debug 3 -Lockfile {{ dag_name }}.lock -AutoRescue 1 -DoRescueFrom 0 -Dag {{ dag_name }} -CsdVersion $CondorVersion:' '7.4.2' 'May' '20' '2010' 'BuildID:' 'Fedora-7.4.2-1.fc13' '$"''')






def createJobTemplate(drmaaSession, dagName, workDir):
    dagMan = which('condor_dagman')
    if(not dagMan):
        raise(Exception('Unable to find condor_dagman in $PATH'))
    
    ad = drmaaSession.createJobTemplate()
    ad.remoteCommand = dagMan
    ad.jobEnvironment = {'_CONDOR_DAGMAN_LOG': '%s.dagman.out' % (dagName),
                         '_CONDOR_MAX_DAGMAN_LOG': 0}
    ad.workingDirectory = workDir
    ad.nativeSpecification = NATIVE_SPEC_TMPL.render(dag_name=dagName)
    ad.blockEmail = True
    ad.jobName = dagName
    ad.outputPath = ':' + os.path.join(workDir, dagName + '.lib.out')
    ad.errorPath = ':' + os.path.join(workDir, dagName + '.lib.err')
    ad.joinFiles = False
    return(ad)


def submit(dagName, workDir):
    """
    Create a DRMAA session and submit the DAGMan job within it.
    """
    # Start the DRMAA session.
    session = drmaa.Session()
    session.initialize()
    
    # Create the DRMAA ClassAd.
    ad = createJobTemplate(session, dagName, workDir)
    
    # Submit the job.
    jobId = session.runJob(ad)
    
    # Cleanup.
    session.deleteJobTemplate(ad)
    
    # Close the DRMAA session.
    session.exit()
    # Mannage jobId so that it corresponds more closely to what is stored in the
    # database: 
    #   <hostname>#<cluster id>.<job id> instead of
    #   <hostname>.<cluster id>.<job id> that we have here.
    # The database version of this id appends #<timestamp> to what we have here
    # as well as replacing the hostname with the fully qualified host name.
    hostname, clusterId, jobId = jobId.rsplit('.', 2)
    return('%s#%s.%s' % (hostname, clusterId, jobId))
