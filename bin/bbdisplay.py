#!/usr/bin/env python
"""
Print out a summary of the Blackboard content corresponding to the given user
and dataset.


Usage
    shell> bbdisplay.py [Options] <user> <dataset>

Options
    --from <start date>     select from <start date>, defaults to start of times
    --to <end date>         select until <end date>, defaults to now
    --exit_code <err>       select entries with exit code=<err>, defaults to 0
    --status <stat>         select where status is <stat>, default 'Exited'



Note
All times in UTC. Input times in UTC ISO 8601 format (i.e. YYYY-MM-DDTHH:MM:SS)

ISO 8601 parsing copyright 2007 Michael Twomey, released under MIT lincense.
"""
from datetime import datetime, timedelta, tzinfo
import re

import elixir
from sqlalchemy import desc, asc

from owl.blackboard import Blackboard
from owl.config import DATABASE_CONNECTION_STR





# Constants
KEYS = ('JobStartDate', 'ExitStatus', 'JobDuration', 'CompletionDate',
        'JobUniverse', 'RequestCpus', 'LocalSysCpu', 'OrigIwd', 'ExitCode',
        'GlobalJobId', 'Iwd', 'QDate', 'RemoteSysCpu', 'JobStatus',
        'RemoteUserCpu', 'Owner', 'NumJobStarts', 'KillSig',
        'CumulativeSuspensionTime', 'JobState', 'User', 'UserLog',
        'NumRestarts', 'NumJobMatches', 'DAGManJobId', 'DiskUsage',
        'DAGNodeName', 'NumShadowStarts', 'ExitBySignal', 'JobPid',
        'RemoteHost', 'CurrentHosts', 'JobRunCount', 'ClusterId', 'HookKeyword',
        'RemoteWallClockTime', 'LocalUserCpu', 'JobPrio', 'DAGParentNodeNames',
        'EnteredCurrentStatus', 'Dataset', 'Instances', 'LastMatchTime')



# ISO 8601 Parsing START
# Adapted from http://delete.me.uk/2005/03/iso8601.html
ISO8601_REGEX = re.compile(r"(?P<year>[0-9]{4})(-(?P<month>[0-9]{1,2})(-(?P<day>[0-9]{1,2})"
    r"((?P<separator>.)(?P<hour>[0-9]{2}):(?P<minute>[0-9]{2})(:(?P<second>[0-9]{2})(\.(?P<fraction>[0-9]+))?)?"
    r"(?P<timezone>Z|(([-+])([0-9]{2}):([0-9]{2})))?)?)?)?"
)
TIMEZONE_REGEX = re.compile("(?P<prefix>[+-])(?P<hours>[0-9]{2}).(?P<minutes>[0-9]{2})")

class ParseError(Exception):
    """Raised when there is a problem parsing a date string"""

# Yoinked from python docs
ZERO = timedelta(0)
class Utc(tzinfo):
    """UTC

    """
    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO
UTC = Utc()

class FixedOffset(tzinfo):
    """Fixed offset in hours and minutes from UTC

    """
    def __init__(self, offset_hours, offset_minutes, name):
        self.__offset = timedelta(hours=offset_hours, minutes=offset_minutes)
        self.__name = name

    def utcoffset(self, dt):
        return self.__offset

    def tzname(self, dt):
        return self.__name

    def dst(self, dt):
        return ZERO

    def __repr__(self):
        return "<FixedOffset %r>" % self.__name

def parse_timezone(tzstring, default_timezone=UTC):
    """Parses ISO 8601 time zone specs into tzinfo offsets

    """
    if tzstring == "Z":
        return default_timezone
    # This isn't strictly correct, but it's common to encounter dates without
    # timezones so I'll assume the default (which defaults to UTC).
    # Addresses issue 4.
    if tzstring is None:
        return default_timezone
    m = TIMEZONE_REGEX.match(tzstring)
    prefix, hours, minutes = m.groups()
    hours, minutes = int(hours), int(minutes)
    if prefix == "-":
        hours = -hours
        minutes = -minutes
    return FixedOffset(hours, minutes, tzstring)

def parse_date(datestring, default_timezone=UTC):
    """Parses ISO 8601 dates into datetime objects

    The timezone is parsed from the date string. However it is quite common to
    have dates without a timezone (not strictly correct). In this case the
    default timezone specified in default_timezone is used. This is UTC by
    default.
    """
    if not isinstance(datestring, basestring):
        raise ParseError("Expecting a string %r" % datestring)
    m = ISO8601_REGEX.match(datestring)
    if not m:
        raise ParseError("Unable to parse date string %r" % datestring)
    groups = m.groupdict()
    tz = parse_timezone(groups["timezone"], default_timezone=default_timezone)

    if groups['hour'] is None:
        groups['hour'] = '0'

    if groups['minute'] is None:
        groups['minute'] = '0'

    if groups['second'] is None:
        groups['second'] = '0'

    if groups["fraction"] is None:
        groups["fraction"] = 0
    else:
        groups["fraction"] = int(float("0.%s" % groups["fraction"]) * 1e6)

    return datetime(int(groups["year"]), int(groups["month"]), int(groups["day"]),
        int(groups["hour"]), int(groups["minute"]), int(groups["second"]),
        int(groups["fraction"]), tz)
# ISO 8601 Parsing END





def fetchData(username, dataset, mindate=None, maxdate=None, exitcode=0,
              status='Exited', keys=KEYS):
    """
    Do what I say :-) But remove None values.
    """
    # Define the database connection.
    elixir.metadata.bind = DATABASE_CONNECTION_STR
    elixir.session.configure(bind=elixir.metadata.bind)
    elixir.metadata.bind.echo = False
    elixir.setup_all()

    fields = [getattr(Blackboard, k) for k in keys]

    query = elixir.session.query(*fields)

    query = query.filter_by(Owner=unicode(username))
    query = query.filter_by(Dataset=unicode(dataset))
    query = query.filter_by(ExitCode=exitcode)
    query = query.filter_by(JobState=unicode(status))
    if(mindate is not None):
        query = query.filter(Blackboard.JobStartDate>=mindate)
    if(maxdate is not None):
        query = query.filter(Blackboard.JobStartDate<=maxdate)
    query = query.order_by(desc(Blackboard.JobStartDate))
    return(query.all())


def printData(rows, keys=KEYS):
    print('# ' + ','.join(keys))
    for row in rows:
        print(','.join([str(x) for x in row]))
    return






if(__name__ == '__main__'):
    import optparse
    import sys



    # Setup the command line option parser and do the parsing.
    parser = optparse.OptionParser(__doc__)
    parser.add_option('--from',
                      dest='start_time',
                      type='str',
                      default=None,
                      help='start time for the query')
    parser.add_option('--to',
                      dest='end_time',
                      type='str',
                      default=None,
                      help='end time for the query')
    parser.add_option('--exit_code',
                      dest='exit_code',
                      type='int',
                      default=0,
                      help='select only entries with the given exit code')
    parser.add_option('--status',
                      dest='status',
                      type='str',
                      default='Exited',
                      help='select only entries with the given status')

    # Parse the command line args.
    (options, args) = parser.parse_args()

    # Sanity check: all options are optional, args must be 2:
    if(len(args) != 2):
        parser.error('Please specify both username and dataset name.')
    username = args[0]
    dataset = args[1]

    # Final sanity checks: from and to, if specified must parse to valid Python
    # datetime instances.
    start_time = options.start_time
    end_time = options.end_time
    if(start_time):
        try:
            start_time = parse_date(options.start_time)
        except:
            parser.error('Incvalid from timestamp.')
    if(end_time):
        try:
            end_time = parse_date(options.end_time)
        except:
            parser.error('Incvalid to timestamp.')

    res = fetchData(username, dataset,
                    mindate=start_time,
                    maxdate=end_time,
                    exitcode=options.exit_code,
                    status=options.status)
    printData(res)
    sys.exit(0)