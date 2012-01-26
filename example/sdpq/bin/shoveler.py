#!/usr/bin/env python
import datetime
import os

import elixir
from sqlalchemy import desc

from eunomia import config

# Define the database connection.
# We use SQLite3 for testing and small installations...
if(config.DATABASE_FLAVOUR == 'sqlite'):
    elixir.metadata.bind = 'sqlite:///%s' %(os.path.abspath(config.DATABASE_DB))
else:
    has_mssql = config.DATABASE_FLAVOUR.startswith('mssql')
    port_info = ''
    connection_str = '%(flavour)s://%(user)s:%(passwd)s@%(host)s'
    
    # We need to handle a few special cases.
    # 0. The password miught contain characters that need to be escaped.
    pwd = urllib.quote_plus(config.DATABASE_PASSWORD)
    
    # 1. Database separator
    db_info = '/' + config.DATABASE_DB
    
    # 2. Yes/No port onformation and yes/no MSSQL.
    if(config.DATABASE_PORT and config.DATABASE_PORT != -1 and not has_mssql):
        port_info += ':' + str(config.DATABASE_PORT)
    elif(config.DATABASE_PORT and config.DATABASE_PORT != -1):
        port_info += '?port=' + str(config.DATABASE_PORT)
    
    # 3. MSSSQL wants a different connection string if a port is specified. Bug?
    if(has_mssql):
        connection_str += '%(db_info)s%(port_info)s'
    else:
        connection_str += '%(port_info)s%(db_info)s'
    
    elixir.metadata.bind = connection_str % {'flavour': config.DATABASE_FLAVOUR,
                                             'user': config.DATABASE_USER,
                                             'passwd': pwd,
                                             'host': config.DATABASE_HOST,
                                             'port_info': port_info,
                                             'db_info': db_info}
elixir.metadata.bind.echo = False





class SDPQ(elixir.Entity):
    """
    Interface to the SDPQ database table(s).
    """
    elixir.using_options(tablename='queue')
    
    datasetName = elixir.Field(elixir.Unicode(255))
    insertDate = elixir.Field(elixir.DateTime)
    priority = elixir.Field(elixir.Integer)
    shoveledDate = elixir.Field(elixir.DateTime)
    workflowId = elixir.Field(elixir.Unicode(255))
    


class CollateralDamage(elixir.Entity):
    """
    Entries in SDPQ are the highest possible data product that can be built from
    a dataset that need reprocessing. For instance, if an exposure member of an
    association needs to be reprocessed, it means that the parent association 
    will need to get reprocessed. In that case we put the association in the 
    queue, not the exposure. This also means that we would put the exposure here
    just to keep track of things (mostly for UI-related reasons).
    """
    elixir.using_options(tablename='PotentialProcessedDatasets')
    
    datasetName = elixir.Field(elixir.Unicode(255))
    insertDate = elixir.Field(elixir.DateTime)
    insertCount = elixir.Field(elixir.Integer)
    queueEntry = elixir.ManyToOne('SDPQ')



class History(elixir.Entity):
    """
    Interface to the SDPQ database table(s).
    """
    elixir.using_options(tablename='history')
    
    datasetName = elixir.Field(elixir.Unicode(255))
    insertDate = elixir.Field(elixir.DateTime)
    priority = elixir.Field(elixir.Integer)
    shoveledDate = elixir.Field(elixir.DateTime)
    workflowId = elixir.Field(elixir.Unicode(255))
    completionDate = elixir.Field(elixir.DateTime)






def pop(limit=None, offset=0):
    """
    Get datasets from SDPQ in decreasing priority order. If limit is not None,
    return at most (limit-offset) datasets. Only fetch entries with a 
    shoveledDate == None.
    
    Update the SDPQ entries for the datasets returned by setting their 
    shoveledDate field to now's date and time.
    
    Return the fetched entries or the empty list if none were available to be
    fetched.
    
    Rise an exception in case of error.
    """
    elixir.setup_all()
    
    q = SDPQ.query.filter_by(shoveledDate=None).order_by(desc(SDPQ.priority))
    entries = q[offset:]
    if(limit is not None):
        entries = entries[:limit]
    
    # Mark them as shoveled.
    return(update(entries, 'shoveledDate', datetime.datetime.now()))


def update(entries, key, val):
    """
    given a list of (SDPQ) entries, an attribute name and a value, do a mass
    update and commit it to the database. Return the newly updated entries.
    
    Rise an exception in case of error.
    """
    for e in entries:
        setattr(e, key, val)
    elixir.session.commit()
    return(entries)



    
if(__name__ == '__main__'):
    import time
    import workflow
    
    
    
    w = None
    n = 100
    sleep_time = 60.
    # Workflow classes are named
    #   uppercase(<instrument>)AsnWorkflow or
    #   uppercase(<instrument>)ExpWorkflow or
    W_TEMPLATE = '%(inst)s%(typ)sWorkflow'
    try:
        while(True):
            entries = pop(limit=100)
            for e in entries:
                print('About to process dataset %s' % (e.datasetName))
                
                # Determine instrument type.
                instrument = dataset_to_instrument(e.datasetName)
                
                # Determine whether this is an association or a simple exposure.
                dataset_type = 'Exp'
                if(is_association(instrument, e.datasetName)):
                    dataset_type = 'Asn'
                
                # Instantiate!
                W = getattr(workflow, W_TEMPLATE % {'inst': instrument.upper(),
                                                    'typ': dataset_type})
                _id = W().execute(dataset=e.datasetName)
                e.workflowId = _id
                print('Submitted dataset %s as job %s' % (e.datasetName, _id))
                
            # Now that we have submitted all the entries to OWL/Condor, they 
            # have been assigned a new workflow ID (by the execute method above)
            # Simply write the changs to the DB and then sleep so that we do not
            # saturate the cluster.
            elixir.session.commit()
            time.sleep(sleep_time)
    except KeyboardInterrupt:
        print('All done.')




