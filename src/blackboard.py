#!/usr/bin/env python
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
Blackboard database tables/Python classes


The Blackboard maps the Condor Job data model with the addition of a Dataset
column:

    MyType = "(unknown type)"
    TargetType = "(unknown type)"
    *GlobalJobId = "fedora13vm0.localdomain#19.0#1286911040"
    ProcId = 0
    AutoClusterId = 2
    AutoClusterAttrs = "JobUniverse,LastCheckpointPlatform,NumCkpts,DiskUsage,ImageSize,RequestMemory,FileSystemDomain,Requirements,NiceUser,ConcurrencyLimits"
    WantMatchDiagnostics = TRUE
    LastMatchTime = 1286911058
    NumJobMatches = 1
    OrigMaxHosts = 1
    LastJobStatus = 1
    JobStatus = 2
    EnteredCurrentStatus = 1286911058
    LastSuspensionTime = 0
    CurrentHosts = 1
    ClaimId = "<192.168.240.129:54723>#1286905548#30#3c3d63872ec23f4c0743eca1cdea0ea8ca266764"
    PublicClaimId = "<192.168.240.129:54723>#1286905548#30#..."
    StartdIpAddr = "<192.168.240.129:54723>"
    RemoteHost = "slot1@fedora13vm0.localdomain"
    RemoteSlotID = 1
    StartdPrincipal = "192.168.240.129"
    ShadowBday = 1286911058
    JobStartDate = 1286911058
    JobCurrentStartDate = 1286911058
    NumShadowStarts = 1
    JobRunCount = 1
    ClusterId = 19
    QDate = 1286911040
    CompletionDate = 0
    Owner = "fpierfed"
    RemoteWallClockTime = 0.000000
    LocalUserCpu = 0.000000
    LocalSysCpu = 0.000000
    RemoteUserCpu = 0.000000
    RemoteSysCpu = 0.000000
    ExitStatus = 0
    NumCkpts_RAW = 0
    NumCkpts = 0
    NumJobStarts = 0
    NumRestarts = 0
    NumSystemHolds = 0
    CommittedTime = 0
    TotalSuspensions = 0
    CumulativeSuspensionTime = 0
    ExitBySignal = FALSE
    CondorVersion = "$CondorVersion: 7.4.2 May 20 2010 BuildID: Fedora-7.4.2-1.fc13 $"
    CondorPlatform = "$CondorPlatform: X86_64-LINUX_F13 $"
    RootDir = "/"
    Iwd = "/home/fpierfed/bcw/condor/bcw/dag"
    JobUniverse = 5
    Cmd = "/hstdev/project/condor_bcw/bcw/python/processMef.py"
    MinHosts = 1
    MaxHosts = 1
    WantRemoteSyscalls = FALSE
    WantCheckpoint = FALSE
    RequestCpus = 1
    JobPrio = 0
    User = "fpierfed@fedora13vm0.localdomain"
    NiceUser = FALSE
    JobNotification = 0
    WantRemoteIO = TRUE
    UserLog = "/home/fpierfed/bcw/condor/bcw/dag/DATASET.log"
    CoreSize = 0
    KillSig = "SIGTERM"
    Rank = 0.000000
    In = "/dev/null"
    TransferIn = FALSE
    Out = "processMef.out"
    StreamOut = FALSE
    Err = "processMef.err"
    StreamErr = FALSE
    BufferSize = 524288
    BufferBlockSize = 32768
    ShouldTransferFiles = "IF_NEEDED"
    WhenToTransferOutput = "ON_EXIT"
    TransferFiles = "ONEXIT"
    ImageSize_RAW = 2
    ImageSize = 2
    ExecutableSize_RAW = 2
    ExecutableSize = 2
    DiskUsage_RAW = 2
    DiskUsage = 2
    RequestMemory = ceiling(ifThenElse(JobVMMemory =!= UNDEFINED, JobVMMemory, ImageSize / 1024.000000))
    RequestDisk = DiskUsage
    Requirements = (Arch == "X86_64") && (OpSys == "LINUX") && (Disk >= DiskUsage) && (((Memory * 1024) >= ImageSize) && ((RequestMemory * 1024) >= ImageSize)) && ((HasFileTransfer) || (TARGET.FileSystemDomain == MY.FileSystemDomain))
    FileSystemDomain = "fedora13vm0.localdomain"
    JobLeaseDuration = 1200
    PeriodicHold = FALSE
    PeriodicRelease = FALSE
    PeriodicRemove = FALSE
    OnExitHold = FALSE
    OnExitRemove = TRUE
    LeaveJobInQueue = FALSE
    DAGNodeName = "PROC_MEF"
    DAGParentNodeNames = ""
    DAGManJobId = 18
    HookKeyword = "FOO"
    Environment = ""
    Arguments = "-i /hstdev/project/condor_bcw/repository/raw/dataset_001/raw-000002.fits -o raw-000002_%(ccdId)s.fits"
    MyAddress = "<192.168.240.129:38226>"
    LastJobLeaseRenewal = 1286911058
    TransferKey = "1#4cb4b4521f05fa55203feb37"
    TransferSocket = "<192.168.240.129:38226>"
    ShadowIpAddr = "<192.168.240.129:38226>"
    ShadowVersion = "$CondorVersion: 7.4.2 May 20 2010 BuildID: Fedora-7.4.2-1.fc13 $"
    UidDomain = "fedora13vm0.localdomain"
    OrigCmd = "/hstdev/project/condor_bcw/bcw/python/processMef.py"
    OrigIwd = "/home/fpierfed/bcw/condor/bcw/dag"
    ExitCode = 0
    Dataset = "raw-000002.fits"
    Instances = 4
"""
import datetime
import os
import urllib
import elixir
from sqlalchemy import desc

from classad import Job
from config import DATABASE_CONNECTION_STR




# Classes/Tables.
class Blackboard(elixir.Entity):
    elixir.using_options(tablename='blackboard')

    MyType = elixir.Field(elixir.Unicode(255))
    TargetType = elixir.Field(elixir.Unicode(255))
    GlobalJobId = elixir.Field(elixir.Unicode(255), primary_key=True)
    ProcId = elixir.Field(elixir.Integer)
    AutoClusterId = elixir.Field(elixir.Integer)
    AutoClusterAttrs = elixir.Field(elixir.Unicode(255))
    WantMatchDiagnostics = elixir.Field(elixir.Boolean)
    LastMatchTime = elixir.Field(elixir.DateTime)
    LastRejMatchTime = elixir.Field(elixir.DateTime)
    NumJobMatches = elixir.Field(elixir.Integer)
    OrigMaxHosts = elixir.Field(elixir.Integer)
    LastJobStatus = elixir.Field(elixir.Integer)
    JobStatus = elixir.Field(elixir.Integer)
    EnteredCurrentStatus = elixir.Field(elixir.DateTime)
    LastSuspensionTime = elixir.Field(elixir.DateTime)
    CurrentHosts = elixir.Field(elixir.Integer)
    ClaimId = elixir.Field(elixir.Unicode(255))
    PublicClaimId = elixir.Field(elixir.Unicode(255))
    StartdIpAddr = elixir.Field(elixir.Unicode(255))
    RemoteHost = elixir.Field(elixir.Unicode(255))
    RemoteSlotID = elixir.Field(elixir.Integer)
    StartdPrincipal = elixir.Field(elixir.Unicode(255))
    ShadowBday = elixir.Field(elixir.DateTime)
    JobStartDate = elixir.Field(elixir.DateTime)
    JobCurrentStartDate = elixir.Field(elixir.DateTime)
    NumShadowStarts = elixir.Field(elixir.Integer)
    JobRunCount = elixir.Field(elixir.Integer)
    ClusterId = elixir.Field(elixir.Integer)
    QDate = elixir.Field(elixir.DateTime)
    CompletionDate = elixir.Field(elixir.DateTime)
    Owner = elixir.Field(elixir.Unicode(255))
    RemoteWallClockTime = elixir.Field(elixir.Float)
    LocalUserCpu = elixir.Field(elixir.Float)
    LocalSysCpu = elixir.Field(elixir.Float)
    RemoteUserCpu = elixir.Field(elixir.Float)
    RemoteSysCpu = elixir.Field(elixir.Float)
    ExitStatus = elixir.Field(elixir.Integer)
    NumCkpts_RAW = elixir.Field(elixir.Integer)
    NumCkpts = elixir.Field(elixir.Integer)
    NumJobStarts = elixir.Field(elixir.Integer)
    NumRestarts = elixir.Field(elixir.Integer)
    NumSystemHolds = elixir.Field(elixir.Integer)
    CommittedTime = elixir.Field(elixir.DateTime)
    TotalSuspensions = elixir.Field(elixir.Integer)
    CumulativeSuspensionTime = elixir.Field(elixir.Integer)
    ExitBySignal = elixir.Field(elixir.Boolean)
    CondorVersion = elixir.Field(elixir.Unicode(255))
    CondorPlatform = elixir.Field(elixir.Unicode(255))
    RootDir = elixir.Field(elixir.Unicode(255))
    Iwd = elixir.Field(elixir.Unicode(255))
    JobUniverse = elixir.Field(elixir.Integer)
    Cmd = elixir.Field(elixir.Unicode(255))
    MinHosts = elixir.Field(elixir.Integer)
    MaxHosts = elixir.Field(elixir.Integer)
    WantRemoteSyscalls = elixir.Field(elixir.Boolean)
    WantCheckpoint = elixir.Field(elixir.Boolean)
    RequestCpus = elixir.Field(elixir.Integer)
    JobPrio = elixir.Field(elixir.Integer)
    User = elixir.Field(elixir.Unicode(255))
    NiceUser = elixir.Field(elixir.Boolean)
    JobNotification = elixir.Field(elixir.Integer)
    WantRemoteIO = elixir.Field(elixir.Boolean)
    UserLog = elixir.Field(elixir.Unicode(255))
    CoreSize = elixir.Field(elixir.Integer)
    KillSig = elixir.Field(elixir.Unicode(255))
    Rank = elixir.Field(elixir.Float)
    In = elixir.Field(elixir.Unicode(255))
    TransferIn = elixir.Field(elixir.Boolean)
    Out = elixir.Field(elixir.Unicode(255))
    StreamOut = elixir.Field(elixir.Boolean)
    Err = elixir.Field(elixir.Unicode(255))
    StreamErr = elixir.Field(elixir.Boolean)
    BufferSize = elixir.Field(elixir.Integer)
    BufferBlockSize = elixir.Field(elixir.Integer)
    ShouldTransferFiles = elixir.Field(elixir.Unicode(255))
    WhenToTransferOutput = elixir.Field(elixir.Unicode(255))
    TransferFiles = elixir.Field(elixir.Unicode(255))
    ImageSize_RAW = elixir.Field(elixir.Integer)
    ImageSize = elixir.Field(elixir.Integer)
    ExecutableSize_RAW = elixir.Field(elixir.Integer)
    ExecutableSize = elixir.Field(elixir.Integer)
    DiskUsage_RAW = elixir.Field(elixir.Integer)
    DiskUsage = elixir.Field(elixir.Integer)
    RequestMemory = elixir.Field(elixir.Unicode(255))
    RequestDisk = elixir.Field(elixir.Unicode(255))
    Requirements = elixir.Field(elixir.Unicode(255))
    FileSystemDomain = elixir.Field(elixir.Unicode(255))
    JobLeaseDuration = elixir.Field(elixir.Integer)
    PeriodicHold = elixir.Field(elixir.Boolean)
    PeriodicRelease = elixir.Field(elixir.Boolean)
    PeriodicRemove = elixir.Field(elixir.Boolean)
    OnExitHold = elixir.Field(elixir.Boolean)
    OnExitRemove = elixir.Field(elixir.Boolean)
    LeaveJobInQueue = elixir.Field(elixir.Boolean)
    DAGNodeName = elixir.Field(elixir.Unicode(255))
    DAGParentNodeNames = elixir.Field(elixir.Unicode(255))
    DAGManJobId = elixir.Field(elixir.Integer)
    HookKeyword = elixir.Field(elixir.Unicode(255))
    Environment = elixir.Field(elixir.UnicodeText())
    Arguments = elixir.Field(elixir.Unicode(255))
    MyAddress = elixir.Field(elixir.Unicode(255))
    LastJobLeaseRenewal = elixir.Field(elixir.DateTime)
    TransferKey = elixir.Field(elixir.Unicode(255))
    TransferSocket = elixir.Field(elixir.Unicode(255))
    ShadowIpAddr = elixir.Field(elixir.Unicode(255))
    ShadowVersion = elixir.Field(elixir.Unicode(255))
    UidDomain = elixir.Field(elixir.Unicode(255))
    OrigCmd = elixir.Field(elixir.Unicode(255))
    OrigIwd = elixir.Field(elixir.Unicode(255))
    StarterIpAddr = elixir.Field(elixir.Unicode(255))
    JobState = elixir.Field(elixir.Unicode(255))
    NumPids = elixir.Field(elixir.Integer)
    JobPid = elixir.Field(elixir.Integer)
    JobDuration = elixir.Field(elixir.Float)
    ExitCode = elixir.Field(elixir.Integer)
    Dataset = elixir.Field(elixir.Unicode(255))
    Instances = elixir.Field(elixir.Integer)

    def __repr__(self):
        return('Blackboard instance.')

    def todict(self):
        return(self.__dict__)




def _extractDatasetName(job):
    """
    Extract dataset name from job.Arguments, unless job.InputDataset is defined,
    in which case use that.
    """
    if(hasattr(job, 'InputDataset')):
        return(getattr(job, 'InputDataset'))

    args = job.Arguments.split()
    for i in range(len(args)):
        if(args[i] == '-i'):
            return(args[i+1])
    return('UNKNOWN')


def _convertTimeStamps(job):
    """
    Given a Job instance, convert all the timestamps we care about into DateTime
    instances.
    """
    fieldNames = ('LastMatchTime', 'EnteredCurrentStatus', 'LastSuspensionTime',
                  'ShadowBday', 'JobStartDate', 'JobCurrentStartDate', 'QDate',
                  'CompletionDate', 'CommittedTime', 'LastJobLeaseRenewal',
                  'LastRejMatchTime')

    for fieldName in fieldNames:
        if(hasattr(job, fieldName)):
            utc = datetime.datetime.utcfromtimestamp(getattr(job, fieldName))
            setattr(job, fieldName, utc)
    return



def createEntry(job):
    """
    Insert the corresponding Blackboard entry in the database. Derive the
    Dataset name form the job.Arguments string, if job.Dataset is not defined.
    """
    # Define the database connection.
    elixir.metadata.bind = DATABASE_CONNECTION_STR
    elixir.metadata.bind.echo = False
    elixir.setup_all()

    # Fix timestamps and dataset name.
    _convertTimeStamps(job)
    job.Dataset = _extractDatasetName(job)
    attrs = job.__dict__

    entry = Blackboard(**attrs)
    elixir.session.commit()
    return


def updateEntry(job):
    """
    Fetch the existing entry for job and update it.
    """
    # Define the database connection.
    elixir.metadata.bind = DATABASE_CONNECTION_STR
    elixir.metadata.bind.echo = False
    elixir.setup_all()

    # Fix timestamps.
    _convertTimeStamps(job)

    # Update the old entry. Remember that the information on the number of job
    # instances is only available to the prepare_job hook. This means that we
    # should not update entry.Instances.
    entry = Blackboard.query.filter_by(GlobalJobId=job.GlobalJobId).one()
    modified = 0
    for attr in job.__dict__.keys():
        if(attr == "Instances"):
            # Never update Instances.
            continue

        if(not hasattr(entry, attr)):
            # Not in our data model: move on.
            continue

        if(getattr(job, attr) != getattr(entry, attr)):
            setattr(entry, attr, getattr(job, attr))
            modified += 1
    if(modified):
        elixir.session.commit()
    return


def listEntries(owner=None, dataset=None):
    """
    List all known Blackboard entries and return them to the caller.
    """
    # Define the database connection.
    elixir.metadata.bind = DATABASE_CONNECTION_STR
    elixir.metadata.bind.echo = False
    elixir.setup_all()

    query = Blackboard.query
    if(dataset):
        query = query.filter_by(Dataset=dataset)
    if(owner):
        query = query.filter_by(Owner=owner)
    return(query.order_by(desc(Blackboard.JobStartDate)).all())


def getEntry(entryId):
    """
    Retrieve a single blackboard entry given its GlobalJobId.
    """
    # Define the database connection.
    elixir.metadata.bind = DATABASE_CONNECTION_STR
    elixir.metadata.bind.echo = False
    elixir.setup_all()

    query = Blackboard.query.filter_by(GlobalJobId=entryId)
    return(query.one())


def getOSFEntry(dagManJobId):
    """
    Given a DAGManJobId `dagManJobId`, find all the blackboard entries that are
    associated with that DAG and present them in an OSF-like manner:
        Dataset, Owner, DAGManJobId, [Nodei JobState, Nodei ExitCode, ]
    """
    # Define the database connection.
    elixir.metadata.bind = DATABASE_CONNECTION_STR
    elixir.metadata.bind.echo = False
    elixir.setup_all()

    osfEntries = []

    # Get all the relevant entries, grouped by their DAGManJobId.
    if(not dagManJobId):
        return

    query = Blackboard.query.filter_by(DAGManJobId=dagManJobId)
    entries = query.order_by(Blackboard.ClusterId, Blackboard.ProcId).all()

    # Now build the OSF-like entry.
    osfEntry = (entries[0].Dataset,
                entries[0].Owner,
                entries[0].DAGManJobId,
                entries)
    return(osfEntry)



# Aliases
closeEntry = updateEntry



























