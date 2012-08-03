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
       'ACAREA': '/usr/local/sybase/stbin'}




def _setup_environment(datasetName):
    """
    Make sure that the environment variables that PodBayDoors.py needs are
    defined. If not, get them from the constant ENV defined abuve. This does not
    override existing definitions in os.environ.
    """
    defined = os.environ.keys()
    for k, v in [(ky, vl) for (ky, vl) in ENV.items() if ky not in defined]:
        os.environ[k] = v
    return


def _get_pod_names(datasetName):
    """
    If `datasetName` is an association, determine all the corresponding 
    exposures; otherwise set the exposure list to contain `datasetName` only.
    Then, for each exposure in the exposure list, determine the POD/EDT dataset
    names and return them all in a flat list.
    
    This is a wrapper around PodBayDoors.getExposures and PodBayDoors.findPod.
    """
    from PodBayDoors import getExposures, findPod
    
    exps = getExposures([datasetName, ])
    allfiles = []
    for key in exps.iterkeys():
        results = findPod(exps[key])
        allfiles.extend(results["podfiles"])
        allfiles.extend(results["edtfiles"])
    return(allfiles)


def _icopy_files(root_names, dst_dir):
    """
    For each of the root file names `root_names` copy <file name>.fits from 
    iRODS to `dst_dir`. Return 0 is all the copy operations succeeded, n where n
    is the number of files that failed otherwise.
    """
    # FIXME: make this more generic by fetching the iRODS env from a config file
    err = 0
    URL = 'irods://dadsops.hlspZone:irods@dmslab1.stsci.edu:1247'
    URL += '/hlspZone/home/rods/dads/pipeline/inputs/'
    
    cmd = which('irods.py')
    if(not dst_dir.endswith('/')):
        dst_dir += '/'
    
    # The iRODS directory structure for a POD file called 
    #   lz_bdd9_143_0001748545.pod
    # is
    #   /hlspZone/home/rods/dads/pipeline/inputs/lz_bdd9/143_0001/
    # and the actual POD file is stored there.
    for root_name in root_names:
        root_name = root_name.lower()
        tokens = root_name.split('_')
        full_url = os.path.join(URL, 
                                '%s_%s' % (tokens[0], tokens[1]), 
                                '%s_%s' % (tokens[2], tokens[3][:4]), 
                                '%s.pod' % root_name)
        proc = subprocess.Popen([cmd, full_url, dst_dir], shell=False)
        _error = proc.wait()
        if(_error):
            err += 1
    return(err)


def process(entry):
    """
    process the sdpq2.SDPQueue entry `entry` and return the corresponding
    OWL workflow ID as well as the workflow submission exit code.
    
    At the moment, we just fetch the POD files associated to the given dataset 
    from DADS, drop them all in the `DIRECTORIES_REPOSITORY`/pods directory and 
    quit. We do this by calling routines in 
    
        PodBayDoors.py
    
    Return None as workflow ID and the exit code of the file fetching process.
    """
    err = 0
    _setup_environment(entry.datasetName)
    
    # POD/EDT files are all stored in owl.config.DIRECTORIES_REPOSITORY/pods
    repo_dir = os.path.join(os.path.realpath(DIRECTORIES_REPOSITORY), 'pods')
    if(not os.path.isdir(repo_dir)):
        os.makedirs(repo_dir)
    
    # Retrieve the POD/EDT dataset names.
    dataset_names = _get_pod_names(entry.datasetName)
    
    # Copy them over and quit.
    err = _icopy_files(dataset_names, repo_dir)
    return((None, err))
