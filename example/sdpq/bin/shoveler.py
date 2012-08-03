#!/usr/bin/env python
import datetime
import os
import sys

import signal
import time

import sdpq
from sdpq.utils import SignalHandler, unload_module, dataset_to_instrument



class InvalidPlugin(Exception): pass
class PluginRuntimeError(Exception): pass





def process_element(element, plugins):
    """
    Process a queue element.
    """
    instrument = dataset_to_instrument(element.datasetName)

    plugin = getattr(plugins, instrument.lower(), None)
    if(plugin is None):
        raise(NotImplementedError('Instrument %s is not supported yet.' \
                                  % (instrument)))

    if(not hasattr(plugin, 'process')):
        raise(InvalidPlugin('The %s plugin is not valid.' \
                            % (instrument.lower())))

    _id, _err = plugin.process(element)
    if(_err):
        raise(PluginRuntimeError('The %s plugin returned an error: %d' \
                                 % (instrument.lower(), _err)))
    return(_id, _err)




def main(n, sleep_time):
    unload_module('sdpq.plugins')
    import sdpq.plugins as plugins

    # Register for SIGUSR1s.
    sh = SignalHandler()
    signal.signal(signal.SIGUSR1, sh.handle)

    skip = []
    while(True):
        # Did we get a signal and have to restart?
        if(sh.got_signal):
            return

        # Fetch entries from SDPQ.
        entries = sdpq.pop(limit=n)
        for e in entries:
            if(e.datasetName in skip):
                continue

            try:
                _id, _err = process_element(e, plugins)
            except NotImplementedError as exception:
                print(exception.message)
                skip.append(e.datasetName)
                continue
            except InvalidPlugin as exception:
                print(exception.message)
                continue
            except PluginRuntimeError as exception:
                # FIXME: what to do in this case?
                print(exception.message)
                skip.append(e.datasetName)
                continue

            # We have a case where _id is None but err == 0. In these cases we
            # have not submitted a workflow yet but it is OK (temporarily so).
            if(_id is None):
                # FIXME: Make this an error.
                print('Submitted dataset %s for processing' % (e.datasetName))
            else:
                print('Submitted dataset %s as job %s' % (e.datasetName, _id))
                e.workflowId = unicode(_id)
            e.shoveledDate = datetime.datetime.now()

        # Now that we have submitted all the entries to OWL/Condor, they
        # have been assigned a new workflow ID (by the execute method above)
        # Simply write the changs to the DB and then sleep so that we do not
        # saturate the cluster.
        if(entries):
            sdpq.save()
        time.sleep(sleep_time)
    return(0)





if(__name__ == '__main__'):
    from sdpq.utils import monitor, get_plugin_dir



    # Monitor the plugin directory.
    monitor(get_plugin_dir())

    try:
        while(True):
            main(1000, 1.)
    except KeyboardInterrupt:
        print('\nAll done.')
        sys.exit(0)




