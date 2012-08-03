"""
A collection of utilities for interfacing with Condor.
"""
import os
import subprocess
import tempfile
import time

from owl.classad import ClassAd
from owl.utils import which




# Constants
# TODO: We should be using condor_quill or similar system.
# Timeout in seconds for the call to EXE.
TIMEOUT = 5.



def _parse_classads(stdout):
    """
    Parse a (list) of ClassAds in a single file object `stdout`. Return the list
    of parsed ClassAd instances.
    """
    ads = []

    # Machine ClassAds are separated by an empty line.
    ad = ''
    for line in stdout:
        if(line.strip() == '' and ad):
            # New ClassAd: parse the last one and start a new one.
            ads.append(ClassAd.newFromClassAd(ad))
            ad = ''
            continue
        ad += line
    # The file ended and hence we must have a last ClassAd: parse it and quit.
    if(ad):
        ads.append(ClassAd.newFromClassAd(ad))
    return(ads)


def _parse_globaljobid(gjob_id):
    """
    A Condor GlobalJobId has the following format:
        <submit host>#<ClusterId>.<ProcId>#<unix timestamp>
    where
        <submit host> is the name of the machine the submit machine.
        ClusterId and ProcId for the standard Condor Job ID = ClusterId.ProcId.
        <unix timestamp> is the UNIX timestamp of the job insertion in the
            Condor queue.
    Return
        [<submit host>, <ClusterId>.<ProcId>, <unix timestamp>]
    """
    if(not is_globaljobid(gjob_id)):
        raise(ValueError('%s is not a valid Condor GlobalJobId' \
                         % (str(gjob_id))))

    tokens = gjob_id.strip().split('#')
    tokens[-1] = int(tokens[-1])
    return(tokens)


def is_globaljobid(gjob_id):
    """
    Is the input `gjob_id` a valid Condor GlobalJobId?
    """
    if(gjob_id.count('#') != 2):
        return(False)
    tokens = gjob_id.split('#')
    try:
        _ = int(tokens[2])
    except:
        return(False)
    if(not is_localjobid(tokens[1])):
        return(False)
    return(True)


def is_localjobid(job_id):
    """
    Is the given `job_id` a local Job ID of the form <ClusterId>.<ProcId>?
    """
    try:
        (cluster, proc) = job_id.split('.')
        _ = int(cluster)
        _ = int(proc)
    except:
        return(False)
    return(True)


def _run_and_get_stdout(args, timeout=TIMEOUT):
    """
    Simple wrapper around subprocess.Popen() with support for timeouts. Return
    the path of the file with the STDOUT content. In case the process had to be
    killed or returned an error, the file is removed and None returned.

    One possibly controversial thing here: we run the process and have it write
    STDOUT to a file, which is potentially inefficient. At the same time, we
    could have a very large cluster with thousands of cores in which case STDOUT
    could be huge...
    """
    # Open the file for writing.
    (fid, file_name) = tempfile.mkstemp()
    os.close(fid)
    f = open(file_name, 'w')

    # Execute the external process and redirect STDOUT to f.
    proc = subprocess.Popen(args,
                            stdout=f,
                            shell=False)

    # Since proc.wait() can deadlock, it is prudent to jut poll...
    start_time = time.time()
    while(time.time() - start_time < timeout):
        exit_code = proc.poll()
        if(exit_code is None):
            # The process is still running.
            time.sleep(.1)
            continue

        # If we are here, the process finished. Remove the file.
        if(exit_code != 0):
            f.close()
            os.remove(file_name)
            return(None)

        # Everything is cool. Return the file name.
        f.close()
        return(file_name)
    # If the process is still tunning at this point, kill it. Remove the file.
    proc.kill()
    f.close()
    os.remove(file_name)
    return(None)


def _run(args, timeout=TIMEOUT):
    """
    Simple wrapper around subprocess.Popen() with support for timeouts. Return
    the exit code of the invoked command. In case the process had to be killed,
    return None.
    """
    # Execute the external process.
    proc = subprocess.Popen(args, shell=False)

    # Since proc.wait() can deadlock, it is prudent to jut poll...
    start_time = time.time()
    while(time.time() - start_time < timeout):
        exit_code = proc.poll()
        if(exit_code is None):
            # The process is still running.
            time.sleep(.1)
            continue

        # If we are here, the process finished. Return exit_code.
        return(exit_code)

    # If the process is still tunning at this point, kill it. Return None.
    proc.kill()
    return(None)


def _run_condor_cmd(argv, error_result, timeout=TIMEOUT):
    """
    Internal: execute the command specified in `argv` and either return its
    parsed STDOUT (as list of ClassAd instances) or `error_result` in case of
    error.
    """
    ads = error_result
    stdout = _run_and_get_stdout([which(argv.pop(0)), ] + argv, timeout)
    if(stdout is not None):
        f = open(stdout, 'r')
        ads = _parse_classads(f)
        f.close()
        os.remove(stdout)
    return(ads)


def condor_status(machine_name=None, timeout=TIMEOUT):
    """
    Wrapper around condor_status: retrieve the list of machines in the pool and
    their current status. If `machine_name` == None, then return information on
    all machines that belong to the pool. Otherwise just return info on that one
    machine.

    Return
        [classad, ...]
    """
    m = ClassAd(MyType='Machine',
                Name='Error communicating with condor please try again later.')
    machines = [m, ]

    args = [which('condor_status'), '-long']
    if(machine_name):
        args.append(machine_name)
    return(_run_condor_cmd(args, machines, timeout=timeout))


def condor_stats(timeout=TIMEOUT):
    """
    Return the Condor Schedd stats as a ClassAd instance.
    """
    ad = ClassAd(MyType='Scheduler',
                 Name='Error communicating with the Condor Schedd.')
    args = [which('condor_status'), '-long', '-schedd']
    res = _run_condor_cmd(args, [ad, ], timeout=timeout)
    return(res[0])


def _run_condor_job_cmd(cmd, extra_argv=None, job_id=None, owner=None,
                        timeout=TIMEOUT):
    """
    Internal convenience function to handle all Condor commands that deal with
    jobs, schedds and owner input.

    Return
        255 if both `job_id` and `owner` == None or
        254 if job_id is not a valid Condor (local or global) job ID or
        `cmd` exit code
    """
    schedd_argv = []
    if(extra_argv is None):
        extra_argv = []
    elif(not isinstance(extra_argv, (list, tuple))):
        # We are being very tolerant here: others would probably throw and
        # exception and would be right in doing so.
        # FIXME: Throw an exception instead?
        extra_argv = [extra_argv, ]

    if(job_id is not None):
        # Start assuming that job_id is a local ID.
        arg = job_id
        # Then check if it is global.
        if(is_globaljobid(job_id)):
            [schedd, job_id, _] = _parse_globaljobid(job_id)
            schedd_argv = ['-n', schedd]
        # Then, if it is neither global not local, error out.
        elif(not is_localjobid(job_id)):
            return(254)
    elif(owner is not None):
        arg = owner
    else:
        return(255)

    # Invoke condot_hold.
    return(_run([which(cmd)] + schedd_argv + extra_argv + [str(arg)], timeout))


def condor_hold(job_id=None, owner=None, timeout=TIMEOUT):
    """
    Wrapper around condor_hold: put the job with the given `job_id` or all jobs
    of the given `owner` (depending on which one is not None) on hold.  If both
    `job_id` and `owner` are specified, `owner` is ignored.

    Return
        255 if both `job_id` and `owner` == None or
        254 if job_id is not a valid Condor (local or global) job ID or
        condor_hold exit code
    """
    # Invoke condot_hold.
    return(_run_condor_job_cmd('condor_hold', [], job_id, owner, timeout))


def condor_release(job_id=None, owner=None, timeout=TIMEOUT):
    """
    Wrapper around condor_release: release the job with the given `job_id` or
    all the jobs of the given `owner` (depending on which one is not None). If
    both `job_id` and `owner` are specified, `owner` is ignored.

    Return
        255 if both `job_id` and `owner` == None or
        condor_release exit code
    """
    # Invoke condot_release.
    return(_run_condor_job_cmd('condor_release', [], job_id, owner, timeout))


def condor_prio(priority, job_id=None, owner=None, timeout=TIMEOUT):
    """
    Wrapper around condor_prio: set the priority of the job with the given
    `job_id` or of all the jobs of the given `owner` (depending on which one is
    not None). If both `job_id` and `owner` are specified, `owner` is ignored.

    `priority` can be any positive integer (or 0), with higher numbers
    corresponding to greater priority. For reference, job priority defaults to
    0.

    Return
        255 if both `job_id` and `owner` == None or
        254 if job_id is not a valid Condor (local or global) job ID or
        253 if `priority` is not a positive (or 0) integer or
        condor_prio exit code
    """
    try:
        priority = int(priority)
    except (ValueError, TypeError):
        return(253)
    if(priority < 0):
        return(253)

    # Invoke condot_release.
    return(_run_condor_job_cmd('condor_prio', ['-p', str(priority)], job_id,
                               owner, timeout))


def condor_getprio(job_id):
    """
    Use condor_q to retrieve the priority of the job with the given `job_id`.
    Return the job priority as positive integer (or 0 , which is the default job
    priority in Condor) or None in case of error.

    Return
        job priority as positive integer
        None if the job could not be found.
    """
    # Just make sure we query the right Schedd.
    schedd_argv = []
    if(is_globaljobid(job_id)):
        [schedd, job_id, _] = _parse_globaljobid(job_id)
        schedd_argv = ['-name', schedd]

    stdout = _run_and_get_stdout([which('condor_q')] +
                                  schedd_argv +
                                  ['-format',
                                   '"%d\n"',
                                   'JobPrio',
                                   str(job_id)])
    if(stdout is None):
        return
    res = open(stdout).readline().strip()
    if(not res):
        return
    try:
        priority = int(res)
    except (ValueError, TypeError):
        return
    return(priority)




































