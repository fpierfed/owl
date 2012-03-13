import os
import urllib

import elixir
from sqlalchemy import desc

from owl.config import DATABASE_FLAVOUR
from owl.config import DATABASE_USER
from owl.config import DATABASE_PASSWORD
from owl.config import DATABASE_HOST
from owl.config import DATABASE_PORT
from owl.utils import dbConnectionStr



# Connect to the database.
elixir.metadata.bind = dbConnectionStr(DATABASE_FLAVOUR,
                                       DATABASE_USER,
                                       DATABASE_PASSWORD,
                                       DATABASE_HOST,
                                       DATABASE_PORT,
                                       dbName='sdpqdev')
elixir.metadata.bind.echo = False



# This is the sdpqdev database structure. In operations we use SQL Server and so
# when manually interacting with it, some commands come in handy (in tsql):
#   list all tables in the current database: select * from sys.Tables
#   describe a table: exec sp_columns <table name>
class SdpQueue(elixir.Entity):
    """
    Interface to the SdpQueue database table(s).
    """
    elixir.using_options(tablename='SdpQueue')
    
    datasetName = elixir.Field(elixir.Unicode(80), primary_key=True)
    insertDate = elixir.Field(elixir.DateTime)
    priority = elixir.Field(elixir.Integer, default=1)
    shoveledDate = elixir.Field(elixir.DateTime, required=False)
    workflowId = elixir.Field(elixir.Unicode(100), required=False)
    wasReprioritized = elixir.Field(elixir.Integer, default=0)
    


class ImpactedDatasets(elixir.Entity):
    """
    Entries in SdpQueue are the highest possible data product that can be built 
    from a dataset that need reprocessing. For instance, if an exposure member 
    of an association needs to be reprocessed, it means that the parent 
    association will need to get reprocessed. In that case we put the 
    association in the queue, not the exposure. This also means that we would 
    put the exposure here just to keep track of things (mostly for UI-related 
    reasons).
    """
    elixir.using_options(tablename='ImpactedDatasets')
    
    datasetName = elixir.Field(elixir.Unicode(80), primary_key=True)
    insertDate = elixir.Field(elixir.DateTime)
    queueEntry = elixir.ManyToOne('SdpQueue')
    status = elixir.Field(elixir.Unicode(10), required=False)



class SdpHistory(elixir.Entity):
    """
    Mirror of the SDPQueue table where we move old data for performance reasons.
    """
    elixir.using_options(tablename='SdpHistory')
    
    datasetName = elixir.Field(elixir.Unicode(80), primary_key=True)
    insertDate = elixir.Field(elixir.DateTime, primary_key=True)
    priority = elixir.Field(elixir.Integer)
    shoveledDate = elixir.Field(elixir.DateTime, required=False)
    workflowId = elixir.Field(elixir.Unicode(100), required=False)
    wasReprioritized = elixir.Field(elixir.Integer, default=0)
    completionDate = elixir.Field(elixir.DateTime)



class ImpactedHistory(elixir.Entity):
    """
    Mirror of the ImpactedDatasets table where we move old data for performance 
    reasons.
    """
    elixir.using_options(tablename='ImpactedHistory')
    
    datasetName = elixir.Field(elixir.Unicode(80), primary_key=True)
    insertDate = elixir.Field(elixir.DateTime, primary_key=True)
    historyEntry = elixir.ManyToOne('SdpHistory')
    status = elixir.Field(elixir.Unicode(10), required=False)






def pop(limit=None, offset=0):
    """
    Get datasets from SDPQueue in decreasing priority order. If limit is not 
    None, return at most (limit-offset) datasets. Only fetch entries with a 
    shoveledDate == None.
    
    Update the SdpQueue entries for the datasets returned by setting their 
    shoveledDate field to now's date and time.
    
    Return the fetched entries or the empty list if none were available to be
    fetched.
    
    Rise an exception in case of error.
    """
    elixir.setup_all()
    
    q = SdpQueue.query.filter_by(shoveledDate=None).order_by(desc(SdpQueue.priority))
    entries = q[offset:]
    if(limit is not None):
        entries = entries[:limit]
    
    # Mark them as shoveled.
    return(entries)


def update(entries, key, val):
    """
    given a list of (SdpQueue) entries, an attribute name and a value, do a mass
    update and commit it to the database. Return the newly updated entries.
    
    Rise an exception in case of error.
    """
    for e in entries:
        setattr(e, key, val)
    elixir.session.commit()
    return(entries)


def save():
    """
    Just do a commit.
    """
    return(elixir.session.commit())
    
