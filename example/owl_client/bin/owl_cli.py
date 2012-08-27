#!/usr/bin/env python
"""
OWL Command Line Interface

Communicate with an OWL system to both retrieve informations (query mode) or
control it (command mode). For detailed API method description, please see the
OWL API documentation at
    https://github.com/fpierfed/owl/blob/master/docs/OWL_API.txt


Installation
    shell> python setup.py install


Requirements
Python 2.6 or later.


Usage
    shell> owl_cli.py -H <OWLD host> [-P <port>] <command> [<argument list>]
"""
import optparse
import socket

from owl_client import OwlClient, OWLD_PORT


# We create names at the module level that pylint does not like.
# pylint: disable=C0103
USAGE = __doc__
parser = optparse.OptionParser(USAGE)
parser.add_option('-H', '--host',
                  dest='host',
                  type='str',
                  help='OWLD hostname or IP address')
parser.add_option('-P', '--port',
                  dest='port',
                  type='int',
                  help='OWLD port. Defaults to %s' % OWLD_PORT,
                  default=int(OWLD_PORT))
# Verbose flag
parser.add_option('-v',
                  action='store_true',
                  dest='verbose',
                  default=False)

# Parse the command line args.
(options, args) = parser.parse_args()

# Sanity check: we need at least one command and we need to make sure that
# the OWLD hostname makes sense.
if(not options.host):
    parser.error('Please specify the address/hostname of the OWLD.')
try:
    # If this works, host is an IP.
    dummy = socket.inet_aton(options.host)
    ipaddr = options.host
except socket.error:
    # Did not work: host might be a hostname. Convert it to IP.
    try:
        ipaddr = socket.gethostbyname(options.host)
    except socket.gaierror:
        parser.error('Unable to parse %s into a valid IP address.'
                     % (options.host))
if(not args):
    parser.error('Please specify a command.')

owl = OwlClient(ipaddr, options.port)
method = args.pop(0)

# Handle any None
for i in range(len(args)):
    if(args[i] == 'None'):
        args[i] = None
print(getattr(owl, method)(*args))
