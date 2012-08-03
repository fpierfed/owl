#!/usr/bin/env python
import datetime
import os

import elixir

from sdpq2 import *



    
if(__name__ == '__main__'):
    import sys
    
    
    
    # Insert a dataset and quit.
    try:
        dataset = unicode(sys.argv[1])
    except:
        dataset = unicode('j9am01070')
    
    elixir.setup_all()
    
    e = SdpQueue.get_by(datasetName=dataset)
    if(e):
        e.shoveledDate = None
        e.workflowId = None
    else:
        e = SdpQueue(datasetName=unicode(dataset),
                     insertDate=datetime.datetime.now(),
                     priority=1,
                     shoveledDate=None,
                     workflowId=None,
                     wasReprioritized=0)
    elixir.session.commit()





