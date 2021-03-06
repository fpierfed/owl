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
The OWL daemon implements the following JSON HTTP API described in
    docs/OWL_API.txt
"""
import array
import asyncore
import asynchat
import datetime
import inspect
import json
import logging
import os
import Queue
import select
import socket
import sys
import time

import owl.condorutils as condor
from owl import blackboard



# Constants
OWLD_METHOD_PREFIX = 'owlapi_'
HEARTBEAT_TIMEOUT = 10
LOG_NAME = 'owlddev.log'



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
    def __init__(self, request, cmd_queue, cmd_id_queue, logger,
                 max_msg_size=None):
        asynchat.async_chat.__init__(self, request)
        self.set_terminator('\n')

        self.indata = ''
        self.request = request
        self.cmd_queue = cmd_queue
        self.cmd_id_queue = cmd_id_queue
        self.max_msg_bytes = max_msg_size
        self._logger = logger
        return

    def collect_incoming_data(self, data):
        """
        This gets called for us when there is new data on the wire. We simply
        append the additional data to whatever we already have in self.indata.
        """
        self.indata += data
        if(self.max_msg_bytes and len(self.indata) > self.max_msg_bytes):
            # Too much data!
            msg = 'too much data (%d bytes > %d bytes)' \
                  % (len(self.indata), self.max_msg_bytes)
            self._logger.warn(msg)

            self.indata = msg + self.terminator
            self.reply(msg)
        return

    def found_terminator(self):
        """
        This gets called every time the client send us a terminator char. We
        put the command (encoded in self.indata) into the appropriate queue.
        """
        self._logger.debug('Got "%s"' % (self.indata))

        # Parse the JSON string and get a new ID for the command. Remember:
        # what we get from the client is a simple JSON string of the form
        #   "[method name, arg1, arg2, ..., keyword_dict]"
        # Where all strings are unicode and the argument list might be empty.
        # What we put in the Command Queue is
        #   ((method name, arg1, arg2, ..., keyword_dict), (cmd_id, callback))
        # Where callback is self.replay and cmd_id is dynamically generated and
        # are for internal consumption only.
        try:
            cmd_spec = json.loads(self.indata)
        except:
            self._logger.warn('Warning: ignored malformed JSON command %s' \
                              % (str(self.indata[:-1])))
            self.indata = ''
            return
        cmd_id = new_command_id(self.cmd_id_queue)

        # Remember that we only want tuples in our queues.
        self.cmd_queue.put((tuple(cmd_spec), (cmd_id, self.reply)))
        self.indata = ''
        return

    def reply(self, message):
        """
        Simply send `message` back to the client.
        """
        self._logger.debug('Replying "%s"' % (message))
        self.push(json.dumps(message, default=datetime_handler) + '\n')
        self.close_when_done()
        return


class CommandMonitor(asyncore.dispatcher):
    """
    Separate "thread" to monitor user-input to the OWLD.
    """
    # asyncore.dispatcher has a lot of publich methods, nothing we can do about
    # that, hence:
    # pylint: disable=R0904
    def __init__(self, ipaddr, port, cmd_queue, logger, max_msg_size=None):
        asyncore.dispatcher.__init__(self)

        self.cmd_queue = cmd_queue
        self.cmd_id_queue = CommandIDQueue()

        self.max_msg_bytes = max_msg_size

        self._logger = logger

        # Start the actual service.
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((ipaddr, port))
        self.listen(1000)

        self._logger.debug('Monitoring user input.')
        return

    def handle_accept(self):
        """
        Called after a connect() has been called by the remote client to the
        local endpoint. We are ready to read data over the socket.
        """
        pair = self.accept()
        if(pair is None):
            return

        self._logger.debug('Client connected from %s:%d' % pair[1])

        # request, client_address = pair
        RequestHandler(request=pair[0],
                       cmd_queue=self.cmd_queue,
                       cmd_id_queue=self.cmd_id_queue,
                       max_msg_size=self.max_msg_bytes,
                       logger=self._logger)
        return


class Daemon(object):
    """
    OWL Daemon

    It does two things:
        1. Listens on the network for incoming commands (in which case it sends
           the results back to the client on the same connection) and relays
           those commands to OWL (and the results back to the client).
        2. Sends keepalive messages to the Condor Master (is present).
    """
    def __init__(self, ipaddr, port, heartbeat_timeout, logger,
                 max_msg_size=None, max_rows=None,
                 apiprefix=OWLD_METHOD_PREFIX):
        """
        Initialize an OWL Daemon.

            ipadr: IP address to listen on (hint: use 0.0.0.0) - string
            port: network port to listen to - integer
            heartbeat_timeout: the number of seconds to wait before sending out
                a heartbeat signal (to the Condor Master) - float
            logger: a (required) logging.logger instance.
            max_msg_size: if neither None nor 0, the maximum size in bytes for
                a message sent over `port`. If specified and if the incoming
                data packet is larger that that, it will get ignored - integer
            max_rows: if neither None nor 0, the maximum number of rows API
                methods can return - integer
            apiprefix: each method which starts with apiprefix is exposed as an
                API method - string
        """
        self._max_rows = max_rows
        self._logger = logger

        # Send a heartbeat every heartbeat_timeout seconds.
        self.hb_timeout = heartbeat_timeout

        # All public API methods are prefixed with self.prefix
        self.prefix = apiprefix

        # Init a command queue.
        self.cmd_queue = CommandQueue()

        self._logger.info('OWLD initialized.')

        # Monitor for user commands.
        self.cmd_monitor = CommandMonitor(ipaddr=ipaddr,
                                          port=port,
                                          cmd_queue=self.cmd_queue,
                                          max_msg_size=max_msg_size,
                                          logger=self._logger)
        return

    def run(self, use_poll=False, timeout=1., sleep_time=.1):
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
            time.sleep(sleep_time)
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
        #   ((method name, arg1, arg2, ..., keyword_dict), (id, callback))
        # All strings are unicode.
        # We prefix method name with self.prefix as those are the only methods
        # we export to the outside world.
        if(not (isinstance(cmd_spec, tuple) and len(cmd_spec) == 2)):
            self._logger.warn('Warning: ignored malformed command %s' \
                              % (str(cmd_spec)))
            return

        (meth_spec, (cmd_id, callback)) = cmd_spec
        meth_spec = list(meth_spec)
        method_name = self.prefix + meth_spec[0]
        if(not hasattr(self, method_name)):
            msg = 'Warning: ignored unsupported command %s' \
                  % (str(meth_spec[0]))
            self._logger.warn(msg)
            callback(msg)
            return

        # Do we have keyword arguments or just positional args?
        kwds = {}
        if(isinstance(meth_spec[-1], dict)):
            # Yes!
            kwds = meth_spec.pop()

        result = getattr(self, method_name)(*meth_spec[1:], **kwds)
        callback(result)
        return

    def stop(self):
        """
        Cleanup and quit.
        """
        self._logger.info('OWLD stopping.')
        logging.shutdown()
        return

    def owlapi_echo(self, message=''):
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

    def owlapi_resources_get_list(self, timeout=condor.TIMEOUT):
        """
        Return the list of the names of available compute resources.

        Usage
            resources_get_list()

        Return
            [resource name, ...]
        """
        ads = condor.condor_status(timeout=timeout)
        return([ad.Name for ad in ads])

    def owlapi_resources_get_info(self, name=None, timeout=condor.TIMEOUT):
        """
        Return the full ClassAd for the given resource name as a dictionary.

        Usage
           resources_get_info(resource name)

        Return
            ClassAd instance as dictionary
        """
        if(name is None):
            return

        ads = condor.condor_status(name, timeout=timeout)
        if(not ads):
            return
        return(ads[0].todict())

    def owlapi_resources_get_stats(self, timeout=condor.TIMEOUT):
        """
        Return the full Condor Schedd statistics as a Python dictionary.

        Usage
            resources_get_stats()

        Return
            Collected statistics as a dictionary.
        """
        stats = condor.condor_stats(timeout=timeout)
        if(not stats):
            return
        return(stats.todict())

    def owlapi_jobs_get_list(self, owner=None, dataset=None,
                             offset=None, limit=20, newest_first=True):
        """
        Return the list of all Blackboard entries, optionally restricting to
        those corresponding to a given dataset (when `dataset` is not None)
        and/or a given user (when `owner` is not None). Pagination is performed
        using `limit` and `offset` (and is disabled by default).
        `offset` can be either None (default) or a positive integer. If `offset`
        is 0 or None (or anything but a positive integer), return results
        starting from index 0, otherwise form index `offset`.
        `limit` can be either None (default=20) or a positive integer. If
        `limit` is None (or anything but a positive integer), return all the
        entries to the end of the list (and from `offset`). Otherwise return at
        most `limit` results (again starting from `offset`).

        If newest_first=True, then the results are sorted by descending
        JobStartDate. The sorting is reversed otherwise.

        Usage
            jobs_get_list(owner=None, datasset=None, offset=None, limit=20,
                          newest_first=True)

        Return
            [{GlobalJobId, DAGManJobId, Dataset, Owner, JobStartDate,
              DAGNodeName, DAGParentNodeNames, ExitCode, JobState, JobDuration}]
        """
        fields = ('GlobalJobId',
                  'DAGManJobId',
                  'Dataset',
                  'Owner',
                  'JobStartDate',
                  'DAGNodeName',
                  'DAGParentNodeNames',
                  'ExitCode',
                  'JobState',
                  'JobDuration')
        # Do we have a limit on the number of rows we can return?
        if(self._max_rows and self._max_rows < limit):
            limit = self._max_rows

        return([dict([(f, getattr(e, f, None)) for f in fields]) \
                for e in blackboard.listEntries(owner=owner,
                                                dataset=dataset,
                                                limit=limit,
                                                offset=offset,
                                                newest_first=newest_first)])

    def owlapi_jobs_get_dag(self, dagId):
        """
        Return the list of all Blackboard entries associated to the given DAG id
        `dagId`. The DAG id can be either a global Id or a local id. Global ids
        are safer as they ensure that only jobs submitted from the same host as
        the input DAG are returned.

        The results are sorted by ascending local Job id.

        It is important to realize that further checks in the calling code are
        needed as (even when using global DAG ids) since Condor stored only the
        ClusterId of the parent dagman job, there is a degeneracy in the ClassAd
        as well as database content. In particular, different DAGs submitted on
        different hosts sharing the same Blackboard database could end up having
        the same ClusterId. While this case is solved by using global ids,
        another situation is not: reinstalling Condor resets the job id counter
        and therefore can cause different DAGs to have the same ClusterId and
        the same submit host. In that case, eiter wipe the Blackboard database
        after reinstalling Condor or filter the results of this API call by
        time.

        Usage
            jobs_get_dag(dagId)

        Return
            [{GlobalJobId, DAGManJobId, Dataset, Owner, JobStartDate,
              DAGNodeName, DAGParentNodeNames, ExitCode, JobState, JobDuration}]
        """
        fields = ('GlobalJobId',
                  'DAGManJobId',
                  'Dataset',
                  'Owner',
                  'JobStartDate',
                  'DAGNodeName',
                  'DAGParentNodeNames',
                  'ExitCode',
                  'JobState',
                  'JobDuration')
        res = blackboard.getOSFEntry(str(dagId))
        if(not res or len(res) != 4):
            return([])
        return([dict([(f, getattr(e, f, None)) for f in fields]) \
                for e in res[3]])

    def owlapi_jobs_get_info(self, job_id=None):
        """
        Return all info about the given blackboard entry (identified by its
        GlobalJobId `job_id`) as a Python dictionary.

        Usage
            jobs_get_info(job_id)

        Return
            the given blackboard entry as a dictionay.
        """
        if(job_id is None):
            return

        try:
            entry = blackboard.getEntry(job_id)
        except:
            entry = None
        if(not entry):
            return
        return(entry.todict())

    def owlapi_jobs_suspend(self, job_id=None, owner=None,
                            timeout=condor.TIMEOUT):
        """
        Suspend the job corresponding to the given GlobalJobId `job_id` (if not
        None) or all the jobs of the given `owner` (if not None). It is
        important to notice that `jobs_id` and `owner` are mutually exclusive
        and `job_id` has precendence. Return condor_hold exit code.

        Usage
            jobs_suspend(job_id=None, owner=None)

        Return
            0: success
            255: both `job_id` and `owner` are None
            254 if job_id is not a valid Condor (local or global) job ID
            otherwise: error condition (the same returned by condor_hold)
        """
        return(condor.condor_hold(job_id=job_id, owner=owner, timeout=timeout))

    def owlapi_jobs_resume(self, job_id=None, owner=None,
                           timeout=condor.TIMEOUT):
        """
        Resume the job corresponding to the given GlobalJobId `job_id` (if not
        None) or all the jobs of the given `owner` (if not None). It is
        important to notice that `jobs_id` and `owner` are mutually exclusive
        and `job_id` has precendence. Return condor_release exit code.

        Usage
            jobs_resume(job_id=None, owner=None)

        Return
            0: success
            255: both `job_id` and `owner` are None
            254 if job_id is not a valid Condor (local or global) job ID
            otherwise: error condition (the same returned by condor_release)
        """
        return(condor.condor_release(job_id=job_id, owner=owner,
                                     timeout=timeout))

    def owlapi_jobs_kill(self, job_id=None, owner=None, timeout=condor.TIMEOUT):
        """
        Kill the job corresponding to the given GlobalJobId `job_id` (if not
        None) or all the jobs of the given `owner` (if not None). It is
        important to notice that `jobs_id` and `owner` are mutually exclusive
        and `job_id` has precendence. Return condor_rm exit code.

        Usage
            jobs_kill(job_id=None, owner=None)

        Return
            0: success
            255: both `job_id` and `owner` are None
            254 if job_id is not a valid Condor (local or global) job ID
            otherwise: error condition (the same returned by condor_hold)
        """
        return(condor.condor_rm(job_id=job_id, owner=owner, timeout=timeout))

    def owlapi_jobs_set_priority(self, priority, job_id=None, owner=None,
                                 timeout=condor.TIMEOUT):
        """
        Set the `priority` of the job corresponding to the given GlobalJobId
        `job_id` (if not None) or of all the jobs of the given `owner` (if not
        None). It is important to notice that `jobs_id` and `owner` are mutually
        exclusive and `job_id` has precendence. Return condor_prio exit code.

        `priority` can be any positive integer (or 0), with higher numbers
        corresponding to greater priority. For reference, job priority defaults
        to 0.

        Usage
            jobs_set_priority(priority, job_id=None, owner=None)

        Return
            0: success
            255: both `job_id` and `owner` are None
            254 if job_id is not a valid Condor (local or global) job ID
            253: if `priority` is not an integer
            otherwise: error condition (the same returned by condor_release)
        """
        return(condor.condor_setprio(priority, job_id=job_id, owner=owner,
                                     timeout=timeout))

    def owlapi_jobs_get_priority(self, job_id=None, timeout=condor.TIMEOUT):
        """
        Get the `priority` of the job corresponding to the given GlobalJobId
        `job_id`. Return the job priority or None in case of error.

        `priority` can be any positive integer (or 0), with higher numbers
        corresponding to greater priority. For reference, job priority defaults
        to 0.

        Usage
            jobs_get_priority(job_id)

        Return
            int: job priority
            None: error condition
        """
        if(job_id is None):
            return

        return(condor.condor_getprio(job_id, timeout=timeout))

    def owlapi_workflow_resubmit(self, workflow_id, timeout=condor.TIMEOUT):
        """
        Given the `workflow_id` of a previously failed workflow, connect to the
        appropriate local OWLD and ask it to re-submit that wrkflow.

        IMPORTANT
        At the moment, no check is performed onto whether or not that workflow
        had indeed failed. Condor will not allow a currently running/queued
        workflow to be restarted.

        Usage
            workflow_resubmit(workflow_id)

        Return
            0 in case of success
            N>0 in case of error.
        """
        err = 0



        return(err)



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

    message = array.array('H')
    message.append(0)
    message.append(0)
    message.append(0) # padding
    message.append(socket.htons(DC_CHILDALIVE))

    message.append(0)
    message.append(0)
    message.append(0) # padding
    message.append(socket.htons(pid))

    message.append(0)
    message.append(0)
    message.append(0) # padding
    message.append(socket.htons(timeout))
    return(message.tostring())


datetime_handler = lambda obj: obj.isoformat() \
                               if isinstance(obj, datetime.datetime) \
                               else obj



if(__name__ == '__main__'):
    from owl import config
    from owl import utils


    port = int(config.OWLD_PORT)
    hostname = socket.gethostname()
    # this_ip = socket.gethostbyname(hostname)
    this_ip = '0.0.0.0'

    # Do we have limits on the max incoming messgae size?
    max_msg_bytes = None
    if(config.OWLD_MAX_MSG_BYTES):
        max_msg_bytes = int(config.OWLD_MAX_MSG_BYTES)
        if(max_msg_bytes <= 0):
            max_msg_bytes = None

    # Do we have a limit on the maximum number of database rows we can return?
    max_rows = None
    if(config.OWLD_MAX_ROWS):
        max_rows = int(config.OWLD_MAX_ROWS)
        if(max_rows <= 0):
            max_rows = None

    # Where are we supposed to write logs and which logging level should we use?
    log_file_name = os.path.join(config.LOGGING_LOG_DIR, config.OWLD_LOG_NAME)
    verbosity = config.LOGGING_LOG_LEVEL
    logger = utils.get_logger(file_name=log_file_name,
                              verbosity=config.LOGGING_LOG_LEVEL)
    logger.info('OWL Configuration: ' + \
                '; '.join(config.CONFIG_TEXT.split('\n')))

    daemon = Daemon(ipaddr=this_ip,
                    port=port,
                    heartbeat_timeout=HEARTBEAT_TIMEOUT,
                    max_msg_size=max_msg_bytes,
                    max_rows=max_rows,
                    logger=logger)
    try:
        daemon.run()
    except KeyboardInterrupt:
        daemon.stop()
    sys.exit(0)
