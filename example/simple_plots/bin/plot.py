#!/usr/bin/env python
"""
Plot date-dependent Blackboard fields in the terminal as simple quicklook. The
plot is generated on a per DAG node basis, i.e. plot all Job duration times for
CALACS.


Usage
    shell> plot.py [OPTIONS] <Blackboard field> <DAG Node name>

Options
    --from <start date>     plot starting from <start date>, defaults to now
    --to <end date>         plot up until <end date>, defaults to start of times

    --xfield <field>        use <field> as abscissa, defaults to JobStartDate


Note
All times in UTC. Input times in UTC ISO 8601 format (i.e. YYYY-MM-DDTHH:MM:SS)

ISO 8601 parsing copyright 2007 Michael Twomey, released under MIT lincense.
"""
from datetime import datetime, timedelta, tzinfo
import os
import re
import subprocess
import tempfile
import time

import elixir
from sqlalchemy import desc, asc

from owl.blackboard import Blackboard
from owl.config import DATABASE_CONNECTION_STR
from owl.utils import which





# Constants
DEFAULT_X = 'JobStartDate'




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



def _fetchData(name, x, y, tmin=None, tmax=None, exit_code=0):
    """
    Do what I say :-) But remove None values.
    """
    # Define the database connection.
    elixir.metadata.bind = DATABASE_CONNECTION_STR
    elixir.session.configure(bind=elixir.metadata.bind)
    elixir.metadata.bind.echo = False
    elixir.setup_all()

    xfield = getattr(Blackboard, x)
    yfield = getattr(Blackboard, y)

    query = elixir.session.query(xfield, yfield)
    query = query.filter_by(DAGNodeName=unicode(name))
    if(exit_code is not None):
        query = query.filter_by(ExitCode=exit_code)

    query = query.filter(xfield != None)
    query = query.filter(yfield != None)

    if(tmin):
        query = query.filter(xfield >= tmin)
    if(tmax):
        query = query.filter(xfield <= tmax)
    query = query.order_by(asc(xfield))
    result = query.all()
    if(not result):
        return(result)

    # Now for something fun: some SQLServer drivers do not convert datetimes to
    #  Python datetime instances. We do it here.
    if(isinstance(result[0][0], str)):
        fnx = lambda x: parse_date(x)
    else:
        fnx = lambda x: x
    if(isinstance(result[0][1], str)):
        fny = lambda y: parse_date(y)
    else:
        fny = lambda y: y
    return([(fnx(x), fny(y)) for (x, y) in result])


class Plot(object):
    def __init__(self, node_name, yfield, xfield=DEFAULT_X,
                 start_time=None, end_time=None):
        """
        Initialize a plot:
            node_name is the name of the DAG nodes to retrieve.
            yfield is the name of the Blackboard field to plot.

            xfield is the name of the x Blackboard field (default=JobStartDate).
            start_time is the min datetime for the plot (default=0).
            end_time is the max datetime for the plot (default=now).
        """
        self.node_name = node_name
        self.xfield = xfield
        self.yfield = yfield
        self.start_time = start_time
        self.end_time = end_time

        self.data = _fetchData(self.node_name, self.xfield, self.yfield,
                               self.start_time, self.end_time)
        return

    def show(self, format='ascii'):
        """
        Print out the plot to STDOUT, specifying the plot format. At the moment
        ASCII plots are the only ones which are supported.
        """
        if(format.upper() != 'ASCII'):
            print('Warning: %s format not supported. Reverting to ASCII' \
                  % (format))

        # Convert the datetime instances to UNIX timestamps. We assume that
        # self.data is a list of 2 element tuples [(x, y), (x, y), ...] and is
        # homogeneous, i.e. all x are of teh same type and all y are of the same
        # (usually different) type.
        xistime = False
        yistime = False
        convert_x = lambda x: x
        convert_y = lambda y: y
        if(isinstance(self.data[0][0], datetime)):
            convert_x = lambda x: time.mktime(x.timetuple())
            xistime = True
        if(isinstance(self.data[0][1], datetime)):
            convert_y = lambda y: time.mktime(y.timetuple())
            yistime = True

        # write the (converted) data to a temp file.
        (fid, file_name) = tempfile.mkstemp()
        os.close(fid)
        data_file = open(file_name, 'w')
        for (x, y) in self.data:
            data_file.write('%s,%s\n' % (str(convert_x(x)),
                                         str(convert_y(y))))
        data_file.close()

        gnuplot = which('gnuplot')
        if(not gnuplot):
            gnuplot = '/Users/fpierfed/NoBalckup/gnuplot-4.6.0/src/gnuplot'
        proc = subprocess.Popen([gnuplot, ],
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE)
        proc.stdin.write('set terminal dumb size 100,24\n')
        proc.stdin.write('set title "%s/%s"\n' % (self.yfield, self.xfield))
        proc.stdin.write('set ylabel "%s"\n' % (self.yfield))
        proc.stdin.write('set xlabel "%s"\n' % (self.xfield))
        proc.stdin.write('set timefmt "%s"\n')
        if(xistime):
            proc.stdin.write('set xdata time\n')
            proc.stdin.write('set format x "%m/%y"\n')
        if(yistime):
            proc.stdin.write('set ydata time\n')
            proc.stdin.write('set format y "%m/%y"\n')
        proc.stdin.write('set datafile separator ","\n')
        proc.stdin.write('plot "%s" using 1:2 with lines title ""\n' \
                         % (file_name))
        proc.stdin.write('quit\n')

        done = False
        while(not done):
            err = proc.poll()
            if(err is not None):
                done = True
        print(proc.stdout.read())

        proc.stdout.close()
        proc.stdin.close()
        del(proc)
        os.remove(file_name)
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
                      help='start time')
    parser.add_option('--to',
                      dest='end_time',
                      type='str',
                      default=None,
                      help='end time')
    parser.add_option('--xfield',
                      dest='xfield',
                      type='str',
                      default=DEFAULT_X,
                      help='what to use for x variable')

    # Parse the command line args.
    (options, args) = parser.parse_args()

    # Sanity check: all options are optional, args must be 2:
    if(len(args) != 2):
        parser.error('Please specify the Blackboard field and the DAG Node.')
    yfield = args[0]
    node_name = args[1]

    # Make sure that that suff actually exists.
    if(not hasattr(Blackboard, yfield)):
        parser.error('Invalid Blackboard field %s' % (yfield))
    if(options.xfield and not hasattr(Blackboard, options.xfield)):
        parser.error('Invalid Blackboard field %s' % (options.xfield))

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

    # Run!
    plot = Plot(node_name, yfield,
                xfield=options.xfield,
                start_time=start_time,
                end_time=end_time)
    plot.show()