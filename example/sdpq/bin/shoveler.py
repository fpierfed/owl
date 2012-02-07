#!/usr/bin/env python
import datetime
import os

import elixir
from sqlalchemy import desc



# dataset_name[0] -> instrument mapping.
# I=WFPC3 L=COS I=WFC3 J=ACS N=NICMOS O=STIS U=WFPC2 W=WFPC X=FOC Y=FOS Z=GHRS 
# F=FGS V=HSP
DATASET_INSTRUMENT = {'i': 'WFPC3',
                      'l': 'COS',
                      'j': 'ACS',
                      'n': 'NICMOS',
                      'o': 'STIS',
                      'u': 'WFPC2',
                      'w': 'WFPC',
                      'x': 'FOC',
                      'y': 'FOS',
                      'z': 'GHRS',
                      'f': 'FGS',
                      'v': 'HSP'}



def dataset_to_instrument(dataset, mapping=DATASET_INSTRUMENT):
    instr = mapping.get(dataset.lower()[0], None)
    if(instr is None):
        raise(NotImplementedException('Unknown instrument for %s' % (dataset)))
    return(instr)


def is_association(instrument, dataset):
    return(dataset.endswith('0'))






    
if(__name__ == '__main__'):
    import time
    import sdpq
    from owl import config
    
    
    w = None
    n = 100
    sleep_time = 60.
    REPO_ROOT = '/jwst/data/repository/raw'
    # Workflow classes are named
    #   uppercase(<instrument>)AsnWorkflow or
    #   uppercase(<instrument>)ExpWorkflow or
    W_TEMPLATE = '%(inst)s%(typ)sWorkflow'
    TEMPLATE_ROOT = os.path.join(os.path.dirname(sdpq.__file__), 'templates')
    CODE_ROOT = '/jwst'
    try:
        while(True):
            entries = sdpq.pop(limit=100)
            if(not entries):
                print('Nothing to do at the moment.')
            
            for e in entries:
                print('About to process dataset %s' % (e.datasetName))
                
                # Determine instrument type.
                instrument = dataset_to_instrument(e.datasetName)
                
                # Determine whether this is an association or a simple exposure.
                dataset_type = 'Exp'
                if(is_association(instrument, e.datasetName)):
                    dataset_type = 'Asn'
                
                # Create a simple work directory path: workRoot/<user>_<timestamp>
                dir_name = '%s_%f' % (os.environ.get('USER', 'UNKNOWN'), 
                                      time.time())
                work_dir = os.path.join(config.DIRECTORIES_WORK_ROOT, dir_name)
                
                # Run!
                repo_dir = os.path.join(REPO_ROOT, instrument.lower())
                templ_dir = os.path.join(TEMPLATE_ROOT, 
                                         instrument.lower(), 
                                         dataset_type.lower())
                W = getattr(sdpq.workflow, W_TEMPLATE \
                            % {'inst': instrument.upper(), 'typ': dataset_type})
                _id = W(templ_dir).execute(dataset=e.datasetName,
                                           codeRoot=CODE_ROOT,
                                           repository=repo_dir,
                                           workDir=work_dir,
                                           flavour='condor')
                e.workflowId = unicode(_id)
                e.shoveledDate = datetime.datetime.now()
                print('Submitted dataset %s as job %s' % (e.datasetName, _id))
                
            # Now that we have submitted all the entries to OWL/Condor, they 
            # have been assigned a new workflow ID (by the execute method above)
            # Simply write the changs to the DB and then sleep so that we do not
            # saturate the cluster.
            elixir.session.commit()
            time.sleep(sleep_time)
    except KeyboardInterrupt:
        print('All done.')




