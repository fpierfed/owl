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



def _parse_machine_classad(stdout):
    """
    Parse a machine ClassAd list and return an object per machine. We re-use the
    Job class from owl since it is general enough (for now).
    """
    machines = []

    # Machine ClassAds are separated by an empty line.
    ad = ''
    for line in stdout:
        if(line.strip() == '' and ad):
            # New ClassAd: parse the last one and start a new one.
            machines.append(ClassAd.newFromClassAd(ad))
            ad = ''
            continue
        ad += line
    # The file ended and hence we must have a last ClassAd: parse it and quit.
    if(ad):
        machines.append(ClassAd.newFromClassAd(ad))
    return(machines)


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



def condor_status(machine_name=None, timeout=TIMEOUT):
    """
    Wrapper around condor_status: retrieve the list of machines in the pool and
    their current status. If `machine_name` == None, then return information on
    all machines that belong to the pool. Otherwise just return info on that one
    machine.

    Return
        (dict(*classad), )
    """
    machines = ({'Name':
                 'Error communicating with condor please try again later.'}, )

    # Invoke condot_status and write its output to a temp file.
    args = [which('condor_status'), '-long']
    if(machine_name):
        args.append(machine_name)
    stdout = _run_and_get_stdout(args, timeout)

    if(stdout is not None):
        f = open(stdout, 'r')
        machines = _parse_machine_classad(f)
        f.close()
        os.remove(stdout)
    return(machines)


def condor_hold(job_id, timeout=TIMEOUT):
    """
    Wrapper around condor_hold: put the job with the given `job_id` on hold.

    Return
        condor_hold exit code
    """
    # Invoke condot_hold.
    return(_run((which('condor_hold'), str(job_id)), timeout))


def condor_release(job_id, timeout=TIMEOUT):
    """
    Wrapper around condor_release: release the job with the given `job_id`.

    Return
        condor_release exit code
    """
    # Invoke condot_release.
    return(_run((which('condor_release'), str(job_id)), timeout))


































