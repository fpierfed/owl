create table blackboard (
    GlobalJobId              varchar(255) not null,
    MyType                   varchar(255),
    TargetType               varchar(255),
    ProcId                   integer,
    AutoClusterId            integer,
    AutoClusterAttrs         varchar(255),
    WantMatchDiagnostics     boolean,
    LastMatchTime            timestamp without time zone,
    LastRejMatchTime         timestamp without time zone,
    NumJobMatches            integer,
    OrigMaxHosts             integer,
    LastJobStatus            integer,
    JobStatus                integer,
    EnteredCurrentStatus     timestamp without time zone,
    LastSuspensionTime       timestamp without time zone,
    CurrentHosts             integer,
    ClaimId                  varchar(255),
    PublicClaimId            varchar(255),
    StartdIpAddr             varchar(255),
    RemoteHost               varchar(255),
    RemoteSlotID             integer,
    StartdPrincipal          varchar(255),
    ShadowBday               timestamp without time zone,
    JobStartDate             timestamp without time zone,
    JobCurrentStartDate      timestamp without time zone,
    NumShadowStarts          integer,
    JobRunCount              integer,
    ClusterId                integer,
    QDate                    timestamp without time zone,
    CompletionDate           timestamp without time zone,
    Owner                    varchar(255),
    RemoteWallClockTime      real,
    LocalUserCpu             real,
    LocalSysCpu              real,
    RemoteUserCpu            real,
    RemoteSysCpu             real,
    ExitStatus               integer,
    NumCkpts_RAW             integer,
    NumCkpts                 integer,
    NumJobStarts             integer,
    NumRestarts              integer,
    NumSystemHolds           integer,
    CommittedTime            timestamp without time zone,
    TotalSuspensions         integer,
    CumulativeSuspensionTime integer,
    ExitBySignal             boolean,
    CondorVersion            varchar(255),
    CondorPlatform           varchar(255),
    RootDir                  varchar(255),
    Iwd                      varchar(255),
    JobUniverse              integer,
    Cmd                      varchar(255),
    MinHosts                 integer,
    MaxHosts                 integer,
    WantRemoteSyscalls       boolean,
    WantCheckpoint           boolean,
    RequestCpus              integer,
    JobPrio                  integer,
    User                     varchar(255),
    NiceUser                 boolean,
    JobNotification          integer,
    WantRemoteIO             boolean,
    UserLog                  varchar(255),
    CoreSize                 integer,
    KillSig                  varchar(255),
    Rank                     real,
    In                       varchar(255),
    TransferIn               boolean,
    Out                      varchar(255),
    StreamOut                boolean,
    Err                      varchar(255),
    StreamErr                boolean,
    BufferSize               integer,
    BufferBlockSize          integer,
    ShouldTransferFiles      varchar(255),
    WhenToTransferOutput     varchar(255),
    TransferFiles            varchar(255),
    ImageSize_RAW            integer,
    ImageSize                integer,
    ExecutableSize_RAW       integer,
    ExecutableSize           integer,
    DiskUsage_RAW            integer,
    DiskUsage                integer,
    RequestMemory            varchar(255),
    RequestDisk              varchar(255),
    Requirements             varchar(255),
    FileSystemDomain         varchar(255),
    JobLeaseDuration         integer,
    PeriodicHold             boolean,
    PeriodicRelease          boolean,
    PeriodicRemove           boolean,
    OnExitHold               boolean,
    OnExitRemove             boolean,
    LeaveJobInQueue          boolean,
    DAGNodeName              varchar(255),
    DAGParentNodeNames       varchar(255),
    DAGManJobId              integer,
    HookKeyword              varchar(255),
    Environment              text,
    Arguments                varchar(255),
    MyAddress                varchar(255),
    LastJobLeaseRenewal      timestamp without time zone,
    TransferKey              varchar(255),
    TransferSocket           varchar(255),
    ShadowIpAddr             varchar(255),
    ShadowVersion            varchar(255),
    UidDomain                varchar(255),
    OrigCmd                  varchar(255),
    OrigIwd                  varchar(255),
    StarterIpAddr            varchar(255),
    JobState                 varchar(255),
    NumPids                  integer,
    JobPid                   integer,
    JobDuration              real,
    ExitCode                 integer,
    Dataset                  varchar(255),
    Instances                integer
);
