"""
ACS plugin

Exports just one function: 

    `workflow_id`, `exit_code` = process(`SQPQueue entry`)
"""
import os
import subprocess

from owl.utils import which




def process(entry):
    """
    process the sdpq2.SDPQueue entry `entry` and return the corresponding
    OWL workflow ID as well as the workflow submission exit code.
    
    At the moment, we just fetch the POD files associated to the given dataset 
    from DADS, drop them in a directory and quit. We do this by calling 
    
        PodBayDoors.py <dataset name>
    
    Return None as workflow ID and the exit code of PodBayDoors.py
    """
    args = [which('PodBayDoors.py'), entry.datasetName]
    proc = subprocess.Popen(args, shell=False)
    return(None, proc.wait())



def process_list(entries):
    """
    process the list of sdpq2.SDPQueue entries `entries` and return the 
    corresponding OWL workflow IDs as well as the workflow submission exit 
    codes.
    
    At the moment, we just fetch the POD files associated to the given datasets 
    from DADS, drop them all in the same directory and quit. We do this by 
    calling 
    
        PodBayDoors.py <dataset name> <dataset name> <dataset name> ...
    
    Return None as workflow IDs and the exit code of PodBayDoors.py
    """
    args = [which('PodBayDoors.py'), ] + [e.datasetName for e in entries]
    proc = subprocess.Popen(args, shell=False)
    err = proc.wait()
    return([(None, err) for e in entries])



