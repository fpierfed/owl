#!/usr/bin/env python
"""
OWL Daemon

Introduction
This long-lived daemon handles the communication between OWL and external code
such as web interfaces or third-party utilities. It is part of the full OWL
installation and relies on OWL to do its job.

Usage
While it could run standalone, it is assumed that this daemon is started and
kept alive by the local Condor Master. As such it relies on a local Condor
installation. Typically it runs on a Condor submit node.

Installation
Define the daemon in the local Condor configuration and add it to DAEMON_LIST
therein. For instance (in /etc/condor/condor_config.local):
    OWLD = /usr/local/bin/owld.py
    DAEMON_LIST   = MASTER, COLLECTOR, NEGOTIATOR, STARTD, SCHEDD, OWLD

API
The OWL daemon implements the following JSON HTTP API


"""
import array
import asyncore
import asynchat
import inspect
import json
import os
try:
    import cPickle as pickle
except:
    import pickle
import Queue
import select
import socket
import sys
import time

import owl.condorutils as condor


# Constants
OWLD_METHOD_PREFIX = 'owlapi_'
HEARTBEAT_TIMEOUT = 10



# Commands are of the form ((method name, arg1, arg2, ..., argN),
#                           (command_id, callback))
# Where method_name is the name of the OWLD method to invoke. It is followed
# by 0 or more arguments.
# callback is a RequestHandler method which simply send data back to the Client.
# The strategy is as follows:
#   1. Client connects to OWLD
#   2. Client sends a command in a single JSON string.
#   3. OWLD (via Command Monitor and Request Handler) parses the string and
#      creates one entry in the CommandQueue. It assigns that entry a unique ID
#   4. OWLD executes the command (at some later time).
#   5. OWLD sends results to Client.
class CommandQueue(Queue.Queue):
    """
    Thread-safe queue holding user-specified OWLD commands.
    """
    pass


class CommandIDQueue(Queue.Queue):
    """
    Utility, thread-safe queue holding internal IDs for OWLD commands.
    """
    pass


class RequestHandler(asynchat.async_chat):
    """
    Handle the communication with the client, be it a commandline tool or
    another process.
    """
    # asynchat.async_chat has a lot of publich methods, nothing we can do about
    # that, hence:
    # pylint: disable=R0904
    def __init__(self, request, cmd_queue, cmd_id_queue):
        asynchat.async_chat.__init__(self, request)
        self.set_terminator('\n')

        self.indata = ''
        self.request = request
        self.cmd_queue = cmd_queue
        self.cmd_id_queue = cmd_id_queue
        return

    def collect_incoming_data(self, data):
        """
        This gets called for us when there is new data on the wire. We simply
        append the additional data to whatever we already have in self.indata.
        """
        self.indata += data
        return

    def found_terminator(self):
        """
        This gets called every time the client send us a terminator char. We
        put the command (encoded in self.indata) into the appropriate queue.
        """
        print('Got %s' % (self.indata))

        # Parse the JSON string and get a new ID for the command. Remember:
        # what we get from the client is a simple JSON string of the form
        #   "[method name, arg1, arg2, ...]"
        # Where all strings are unicode and the argument list might be empty.
        # What we put in the Command Queue is
        #   ((method name, arg1, arg2, ...), (cmd_id, callback))
        # Where callback is self.replay and cmd_id is dynamically generated and
        # are for internal consumption only.
        try:
            cmd_spec = json.loads(self.indata)
        except:
            # TODO: We have an error but we do not close the connection?
            print('Warning: ignored malformed JSON command %s' \
                  % (str(self.indata)))
            self.indata = ''
            return
        cmd_id = new_command_id(self.cmd_id_queue)

        # Remember that we only want tuples in our queues.
        # TODO: does the above make sense?
        self.cmd_queue.put((tuple(cmd_spec), (cmd_id, self.reply)))
        self.indata = ''
        return

    def reply(self, message):
        self.push(json.dumps(message) + '\n')
        self.close_when_done()
        return


class CommandMonitor(asyncore.dispatcher):
    """
    Separate "thread" to monitor user-input to the OWLD.
    """
    # asyncore.dispatcher has a lot of publich methods, nothing we can do about
    # that, hence:
    # pylint: disable=R0904
    def __init__(self, ipaddr, port, cmd_queue):
        asyncore.dispatcher.__init__(self)

        self.cmd_queue = cmd_queue
        self.cmd_id_queue = CommandIDQueue()

        # Start the actual service.
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((ipaddr, port))
        self.listen(1000)

        print('*** Monitoring user input.')
        return

    def handle_accept(self):
        """
        Called after a connect() has been called by the remote client to the
        local endpoint. We are ready to read data over the socket.
        """
        pair = self.accept()
        if(pair is None):
            return

        # request, client_address = pair
        RequestHandler(pair[0], self.cmd_queue, self.cmd_id_queue)
        return


class Daemon(object):
    """
    OWL Daemon

    It does two things: listens on the network for incoming commands (in which
    case it sends the results back to the client on the same connection) and
    relays those commands to OWL (and the results back to the client).
    """
    def __init__(self, ipaddr, port, heartbeat_timeout,
                 apiprefix=OWLD_METHOD_PREFIX):
        # Send a heartbeat every heartbeat_timeout seconds.
        self.hb_timeout = heartbeat_timeout

        # All public API methods are prefixed with self.prefix
        self.prefix = apiprefix

        # Init a command queue.
        self.cmd_queue = CommandQueue()

        # Monitor for user commands.
        self.cmd_monitor = CommandMonitor(ipaddr, port, self.cmd_queue)
        return

    def run(self, use_poll=False):
        """
        Sit is an infinite loop waiting for a command to answer.
        """
        # This is just a merge of our event loop, monitoring the queue, and
        # ayncore.loop.
        if use_poll and hasattr(select, 'poll'):
            poll_fun = asyncore.poll2
        else:
            poll_fun = asyncore.poll
        asyncmap = asyncore.socket_map
        timeout = 1.

        last_heartbeat = 0
        pid = os.getpid()
        while(True):
            # Poll the sockets.
            if(asyncmap):
                poll_fun(timeout, asyncmap)

            # Check the command queue and see if there is anything we to do.
            self._handle_command()

            # Do we need to send a heartbeat?
            now = time.time()
            if(now - last_heartbeat >= self.hb_timeout):
                send_alive(pid, timeout)
            time.sleep(.1)
        return

    def _handle_command(self):
        """
        Look at self.cmd_queue to see if there is anything to do. If so, parse
        the command specification found there, potentially act on it and quit.
        """
        try:
            cmd_spec = self.cmd_queue.get(block=True, timeout=.1)
        except Queue.Empty:
            return

        # raw_cmd_spec is a tuple of the form
        #   ((method name, arg1, arg2, ..., argN), (id, callback))
        # All strings are unicode.
        # We prefix method name with self.prefix as those are the only methods
        # we export to the outside world.
        if(not (isinstance(cmd_spec, tuple) and len(cmd_spec) == 2)):
            # TODO: We have an error but we do not close the connection?
            print('Warning: ignored malformed command %s' \
                  % (str(cmd_spec)))
            return

        (meth_spec, (cmd_id, callback)) = cmd_spec
        method_name = self.prefix + meth_spec[0]
        if(not hasattr(self, method_name)):
            # TODO: We have an error but we do not close the connection?
            callback('Warning: ignored unsupported command %s' \
                     % (str(meth_spec[0])))
            return

        result = getattr(self, method_name)(*meth_spec[1:])
        callback(str(result))
        return

    def stop(self):
        """
        Cleanup and quit.
        """
        return

    def owlapi_echo(self, message):
        """
        Simple echo service.

        Usage
            echo(message)

        Return
            unicode(message)
        """
        return(message)

    def owlapi_list_methods(self):
        """
        Introspection: list all the supported API methods and their doc strings.

        Usage
            list_methods()

        return
            [(unicode(method name), unicode(doc string)), ...]
        """
        res = []
        for (name, method) in inspect.getmembers(self, inspect.ismethod):
            if(name.startswith(self.prefix)):
                res.append((name, method.__doc__.strip()))
        return(res)

    def owlapi_resources_get_list(self):
        """
        Return the list of the names of available compute resources.

        Usage
            resources_get_list()

        Return
            [resource name, ...]
        """
        ads = condor.condor_status()
        return([ad.Name for ad in ads])

    def owlapi_resources_get_info(self, name):
        """
        Return the full ClassAd for the given resource name.

        Usage
           resources_get_info(resource name)

        Return
            ClassAd instance as dictionary
        """
        ads = condor.condor_status(name)
        if(not ads):
            return
        return(ads[0].todict())


def new_command_id(queue):
    """
    Based on the content of `queue`, create a new ID and store it back into
    `queue`. For now IDs are simply increasing integers, starting from 0.
    """
    try:
        old_id = queue.get(timeout=.1)
    except Queue.Empty:
        old_id = -1
    new_id = old_id + 1
    queue.put(new_id)
    return(new_id)

def send_alive(pid, timeout=300, master_host="127.0.0.1", master_port=1271):
    """
    Send a UDP packet to the condor_master containing the
    DC_CHILDALIVE command.

    This will have the master register a trigger to fire in timeout
    seconds. When the trigger fires the pid will be killed. Each time
    the DC_CHILDALIVE is sent the trigger's timer is reset to fire in
    timeout seconds.

    DC_CHILDALIVE should be sent every timeout/3 seconds.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(build_message(pid, int(timeout)), (master_host, master_port))
    return

def build_message(pid, timeout):
    """
    Build a datagram packet to send to the condor_master.

    The package format is (command, pid, timeout). The command is
    always DC_CHILDALIVE (the integer 60008). The pid is the pid of
    the process the master is monitoring, i.e. getpid if this
    script. The timeout is the amount of time, in seconds, the master
    will wait before killing the pid. Each field in the packet must be
    8 bytes long, thus the padding.
    """
    DC_CHILDALIVE = 60008

    message=array.array('H')
    message.append(0); message.append(0); message.append(0) # padding
    message.append(socket.htons(DC_CHILDALIVE))
    message.append(0); message.append(0); message.append(0) # padding
    message.append(socket.htons(pid))
    message.append(0); message.append(0); message.append(0) # padding
    message.append(socket.htons(timeout))
    return(message.tostring())


if(__name__ == '__main__'):
    try:
        from owl import config
    except:
        config = object()
        config.OWLD_PORT = 9999


    port = int(config.OWLD_PORT)
    hostname = socket.gethostname()
    this_ip = socket.gethostbyname(hostname)

    print('Starting OWLD on %s:%d' % (this_ip, port))
    daemon = Daemon(ipaddr=this_ip, port=port,
                    heartbeat_timeout=HEARTBEAT_TIMEOUT)
    try:
        daemon.run()
    except KeyboardInterrupt:
        daemon.stop()
        print('\nAll done.')
    sys.exit(0)
