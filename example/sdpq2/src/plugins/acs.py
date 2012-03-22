"""
ACS plugin

Exports just one function: 

    `workflow_id`, `exit_code` = process(`SQPQueue entry`)
"""
import os
import subprocess

from owl.utils import which
from owl.config import DIRECTORIES_REPOSITORY

# Needed for PodBayDoors.py
ENV = {'OPUS_DB_SERVER': 'HARPOSQLA', 
       'OPUS_DB': 'opus_rep', 
       'DADS_DB_SERVER': 'HARPOSQLA', 
       'DADS_DB': 'dadsops_rep',
       'DELIVERY_HOST': 'dmsdevvm1.stsci.edu', 
       'DELIVERY_DIR': '', 
       'DELIVERY_USER': 'anonymous', 
       'DELIVERY_PW': 'anonymous@stsci.edu', 
       'ARCHIVE_USER': 'anonymous', 
       'ARCHIVE_PASS': 'anonymous@stsci.edu', 
       'SDPQ_DB_SERVER': 'dummy',
       'SDPQ_DB': 'dummy'}




def _setup_environment(datasetName):
    """
    Make sure that the environment variables that PodBayDoors.py needs are
    defined. If not, get them from the constant ENV defined abuve. This does not
    override existing definitions in os.environ, with the exception of 
    $DELIVERY_DIR, which is always set to 
        owl.config.DIRECTORIES_REPOSITORY/datasetName.
    """
    defined = os.environ.key()
    for k, v in [(ky, vl) for (ky, vl) in ENV.items() \
                 if ky not in defined and ky != 'DELIVERY_DIR']:
        os.environ[k] = v
    os.environ['DELIVERY_DIR'] = os.path.join(DIRECTORIES_REPOSITORY, 
                                              datasetName)
    return


def process(entry):
    """
    process the sdpq2.SDPQueue entry `entry` and return the corresponding
    OWL workflow ID as well as the workflow submission exit code.
    
    At the moment, we just fetch the POD files associated to the given dataset 
    from DADS, drop them in a directory and quit. We do this by calling 
    
        PodBayDoors.py <dataset name>
    
    Return None as workflow ID and the exit code of PodBayDoors.py
    """
    _setup_environment(entry.datasetName)
    
    # POD/RAW files for dataset XXX are stored by PodBayDoors.py in the dir
    # $DELIVERY_DIR, which we set to be owl.config.DIRECTORIES_REPOSITORY/XXX. 
    # It is important to remember that $DELIVERY_DIR is the only environment 
    # variable that we do override.
    repo_dir = os.path.realpath(DIRECTORIES_REPOSITORY)
    dst_dir = os.path.join(repo_dir, entry.datasetName)
    if(os.path.isdir(repo_dir)):
        os.makedirs(repo_dir)
    
    # Now is dst_dir exists, we have to choose what to do. We chose not to 
    # download the files again and assume instead that they have been put there
    # already.
    if(os.path.exists(dst_dir)):
        return((None, 0))
    
    args = [which('PodBayDoors.py'), entry.datasetName]
    proc = subprocess.Popen(args, shell=False)
    err = proc.wait()
    return((None, err))
