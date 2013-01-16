"""
Define all Condor-specific DRMAA properties here.

For more information on DRMAA, please see http://www.drmaa.org
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






def create_job_template(drmaa_session, dag_name, work_dir):
    """
    Create a DRMAA Condor DAGMan job given the already instantiated OWL Workflow
    templates for `dag_name` found in `work_dir`. Return the DRMAA job as a
    DRMAA template (confusing terminology, I know).
    """
    dagman = which('condor_dagman')
    if(not dagman):
        raise(Exception('Unable to find condor_dagman in $PATH'))

    drmaa_tmplt = drmaa_session.createJobTemplate()
    drmaa_tmplt.remoteCommand = dagman
    drmaa_tmplt.workingDirectory = work_dir
    drmaa_tmplt.nativeSpecification = NATIVE_SPEC_TMPL.render(dag_name=dag_name)
    drmaa_tmplt.blockEmail = True
    drmaa_tmplt.jobName = dag_name
    drmaa_tmplt.outputPath = ':' + os.path.join(work_dir, dag_name + '.lib.out')
    drmaa_tmplt.errorPath = ':' + os.path.join(work_dir, dag_name + '.lib.err')
    drmaa_tmplt.joinFiles = False
    return(drmaa_tmplt)


def submit(dag_name, work_dir, wait=False):
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
    drmaa_tmplt = create_job_template(session, dag_name, work_dir)

    # Submit the job.
    job_id = session.runJob(drmaa_tmplt)

    # Wait?
    if(wait):
        try:
            err = session.wait(job_id, drmaa.Session.TIMEOUT_WAIT_FOREVER)
        except drmaa.errors.InvalidArgumentException:
            # This is due to the fact that Condor drmaa 1.6 set stat=200 when
            # exit code = 0 (see WISDOM file in drmaa-1.6).
            err = 0
    # Cleanup.
    session.deleteJobTemplate(drmaa_tmplt)

    # Close the DRMAA session.
    session.exit()
    # Mannage job_id so that it corresponds more closely to what is stored in
    # the database:
    #   <hostname>#<ClusterId>.<ProcId> instead of
    #   <hostname>.<ClusterId>.<ClusterId> that we have here.
    # The database version of this id appends #<timestamp> to what we have here
    # as well as replacing the hostname with the fully qualified host name.
    hostname, cluster_id, proc_id = job_id.rsplit('.', 2)
    sanitized_id = '%s#%s.%s' % (hostname, cluster_id, proc_id)
    if(wait):
        return((sanitized_id, err))
    return(sanitized_id)



