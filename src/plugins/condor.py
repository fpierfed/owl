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
    ad.workingDirectory = workDir
    ad.nativeSpecification = NATIVE_SPEC_TMPL.render(dag_name=dagName)
    ad.blockEmail = True
    ad.jobName = dagName
    ad.outputPath = ':' + os.path.join(workDir, dagName + '.lib.out')
    ad.errorPath = ':' + os.path.join(workDir, dagName + '.lib.err')
    ad.joinFiles = False
    return(ad)


def submit(dagName, workDir, wait=False):
    """
    Create a DRMAA session and submit the DAGMan job within it. If wait=False,
    submit the job asyncronously. Otherwise, wait for the job to complete and
    return its exit code as well.

    Return
        jobId                   if wait == False
        (jobId, exit_code)      if wait == True
    """
    # TODO: homogenize the return type.

    # Start the DRMAA session.
    session = drmaa.Session()
    session.initialize()

    # Create the DRMAA ClassAd.
    ad = createJobTemplate(session, dagName, workDir)

    # Submit the job.
    jobId = session.runJob(ad)

    # Wait?
    if(wait):
        try:
            err = session.wait(jobId, drmaa.Session.TIMEOUT_WAIT_FOREVER)
        except drmaa.errors.InvalidArgumentException:
            # This is due to the fact that Condor drmaa 1.6 set stat=200 when
            # exit code = 0 (see WISDOM file in drmaa-1.6).
            err = 0
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
    sanitizedId = '%s#%s.%s' % (hostname, clusterId, jobId)
    if(wait):
        return((sanitizedId, err))
    return(sanitizedId)



