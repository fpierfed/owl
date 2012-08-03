"""
HLA Lib

A set of convenience functions and constants to handle HLA processing from
Condor/OWL. User code building upon this library is supposed to override
constants where appropriate.
"""
import os
import subprocess
import sys
import time




# Constants.
# TODO: Put all of these in a config file.
# Environment variables assumed to be defined.
ENV_VARS = ('OSF_DATASET', )
# Where do we find the code to execute?
CODE_ROOT = '/bin'
# Which scripts shall we execute (in order!)?
# TODO: This is more effient than a class, but ugly...
SCRIPT_DEFS = ({'exe': os.path.join(CODE_ROOT, 'sleep'),
                'args': ['10', ],
                'timeout': None}, )
# How long to sleep before polling again for the executable to be done.
POLL_TIME = 0.1                                                     # seconds.
# What to return in case we kill a process and/or get an error.
ERR_RETURN_CODE = -999





def syncExecute(exe, args=[], timeout=None, pollTime=POLL_TIME,
                errCode=ERR_RETURN_CODE):
    """
    Invoke the executable `exe` and wait for it to finish. Return its exit code.

    Assume that `exe` is just the executable name (possibly with full path) with
    no arguments. Arguments are given in the string list args. A timeout in
    seconds can also be specified. In case a timeout is given and it passes
    before `exe` is done, kill `exe` and return `errCode`.
    """
    if(args == None):
        args = []
    elif(isinstance(args, string)):
        args = [args, ]
    elif(isinstance(args, tuple)):
        args = list(args)

    t0 = time.time()
    job = subprocess.Popen(args=[exe, ] + args, shell=False)
    while(true):
        # Check if `exe` is still running.
        job.poll()
        if(job.returncode != None):
            # `exe` has finished running.
            return(job.returncode)

        # Now see if we have to worry about timeouts.
        if(timeout and time.time() - t0 >= timeout):
            # We have a timeout and we have exceeded it. Kill the process and
            # return `errCode`.
            job.terminate()
            return(errCode)
    # If we get here, something weird has happened. Return our standard error
    # code.
    return(errCode)


def checkEnvironment(vars=ENV_VARS):
    """
    Make sure that each of the environment variables in the list `vars` is
    defined. Raise an exception if any of them is not defined.
    """
    for var in vars:
        try:
            dummy = os.environ[var]
        except:
            raise(Exception('Please define $%s' % (var)))
    return


def executeScripts(scriptInfoList=SCRIPT_DEFS):
    """
    Execute each of the scripts in scriptInfoList one at the time in order.
    If any one of them returns an error (i.e. a non 0 exit code) or comes with a
    timeout and the timeout passes, interrupt the call chain and return an error
    code.

        scriptInfoList = [{'exe': <executable>,
                           'args': <arguments to exe or None>,
                           'timeout': <timeout in seconds or None>}]
    """
    for scriptDef in scriptInfoList:
        err = syncExecute(scriptDef)
        if(err):
            return(err)
    return(0)











































