#!/usr/bin/env python
"""
OWL Client Module

Communicate with an OWL system to both retrieve informations (query mode) or
control it (command mode). For detailed API method description, please see the
OWL API documentation at
    https://github.com/fpierfed/owl/blob/master/docs/OWL_API.txt


Installation
    shell> python setup.py install


Requirements
Python 2.6 or later.
"""
import functools
import json
import socket



__version__ = '1.0'



# Constants
OWLD_PORT = 9999
ERROR = {255: 'All required arguments are None.',
         254: 'Not a valid OWL Job ID.',
         253: 'Invalid priority value.'}



class OwlClient(object):
    """
    Proxy object for a remote OWLD.
    """
    def __init__(self, addr, port=OWLD_PORT):
        self.addr = addr
        self.port = port
        self.sock = None
        return

    def __getattr__(self, name):
        return(functools.partial(self.execute, name))

    def execute(self, *argv, **kwds):
        """
        Internal method: this is the implementation of the Proxy pattern in
        Python.
        """
        # Connect to (addr, port) and send argv in JSON format.
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.connect((self.addr, self.port))

        # Agument argv with the keyword arguments.
        argv = list(argv)
        argv.append(kwds)

        # We need to send the command and its arguments as a list (i.e. the full
        # argv list) in JSON format, new-line terminated.
        cmd_spec = json.dumps(argv) + '\n'
        self.sock.sendall(cmd_spec)

        # Now wait for the answer from the remote OWLD.
        # res = recv(sock, 1024)
        res = ''
        while(True):
            tmp = self.sock.recv(1024)
            if(not tmp):
                break
            res += tmp

        # Cleanup and quit.
        self.sock.close()
        return(json.loads(res))

    def isalive(self):
        """
        Return whether or not the remote OWLD is alive.
        """
        msg = 'foo'
        try:
            res = self.echo(msg)
        except socket.error:
            # print('The remote OWLD on %s:%d does not seem to ba running.' \
            #       % (self.addr, self.port))
            return(False)
        except:
            # print('The remote process on %s:%d does not seem to be an OWLD.' \
            #       % (self.addr, self.port))
            return(False)
        if(res == msg):
            return(True)
        # print('The remote OWLD on %s:%d does not seem to be right.' \
        #       % (self.addr, self.port))
        return(False)




