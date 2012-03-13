"""
The code that follows borrows heavily on both the Django and mod_wsgi sources.
"""
import atexit
import os
import Queue
import signal
import sys
import threading
import time



_ROOT_DIR = os.path.dirname(os.path.realpath(__file__))
_FILES = {}
_WATCH_DIR = None
_POLLING_INTERVAL = 1.0
_QUEUE = Queue.Queue()
_LOCK = threading.Lock()
_SLEEPING = True

# dataset_name[0] -> instrument mapping.
# I=WFPC3 L=COS I=WFC3 J=ACS N=NICMOS O=STIS U=WFPC2 W=WFPC X=FOC Y=FOS Z=GHRS 
# F=FGS V=HSP
DATASET_INSTRUMENT = {'i': 'WFPC3',
                      'l': 'COS',
                      'j': 'ACS',
                      'n': 'NICMOS',
                      'o': 'STIS',
                      'u': 'WFPC2',
                      'w': 'WFPC',
                      'x': 'FOC',
                      'y': 'FOS',
                      'z': 'GHRS',
                      'f': 'FGS',
                      'v': 'HSP'}


class SignalHandler(object):
    def __init__(self):
        self.got_signal = False
        pass
    
    def handle(self, signum, frame):
        self.got_signal = True
        return


def dataset_to_instrument(dataset, mapping=DATASET_INSTRUMENT):
    instr = mapping.get(dataset.lower()[0], None)
    if(instr is None):
        raise(NotImplementedException('Unknown instrument for %s' % (dataset)))
    return(instr)


def is_association(instrument, dataset):
    return(dataset.endswith('0'))
    

def get_plugin_dir():
    here = os.path.dirname(os.path.realpath(__file__))
    return(os.path.join(here, 'plugins'))


def unload_module(name):
    delenda = []
    if(name in sys.modules.keys()):
        delenda.append(name)
        
        root = name + '.'
        for n in sys.modules.keys():
            if(n.startswith(root)):
                delenda.append(n)
    for n in delenda:
        del(sys.modules[n])
    return


def _restart(path, reason):
    _QUEUE.put(True)
    sys.stderr.write('File %s was %s: restarting.\n' % (path, reason))
    os.kill(os.getpid(), signal.SIGUSR1)
    return


def _modified(path):
    try:
        if(not os.path.isfile(path)):
            # path disappeared. If we were tracking it, then restart, otherwise
            # ignore the event.
            return(path in _FILES.keys())
        
        # path is still there. Were we tracking it? Restart if we were not or
        # if we were and the file was modified.
        mtime = os.stat(path).st_mtime
        if(path not in _FILES.keys() or _FILES[path] != mtime):
            # Add/update it and restart.
            _FILES[path] = mtime
            return(True)
    except:
        # Something bad happened: likely the file was removed under our feet 
        # (i.e.before we had a chance to stat it). Restart.
        return(True)
    return(False)


def _monitor():
    while(True):
        files = []
        if(_WATCH_DIR):
            files = _get_sources(_WATCH_DIR)
        
        for f in files:
            if(_modified(f)):
                _restart(f, 'created/modified')
        
        # See if any file in _FILES.keys() was deleted.
        diff = list(set(_FILES.keys()) - set(files))
        for f in diff:
            del(_FILES[f])
            _restart(f, 'deleted')
        
        # Sleep
        try:
            _QUEUE.get(timeout=_POLLING_INTERVAL)
        except:
            pass
        time.sleep(_POLLING_INTERVAL)
    return


_thread = threading.Thread(target=_monitor)
_thread.daemon = True


def _exiting():
    try:
        _QUEUE.put(True)
    except:
        pass
    _thread.join(_POLLING_INTERVAL)
    return
atexit.register(_exiting)


def _get_sources(dir):
    return(map(lambda f: os.path.join(dir, f), 
               [f for f in os.listdir(dir) if f.endswith('.py')]))


def monitor(dir, polling_interval=_POLLING_INTERVAL):
    """
    Spawn a thread that monitors Python files in the given directory `dir` every
    `polling_interval` seconds. If any of the code in that directory is either 
    modified, deleted or added, send a SIGALRM signal to the current Python 
    process.
    """
    global _SLEEPING
    global _FILES
    global _POLLING_INTERVAL
    global _WATCH_DIR
    
    
    if(polling_interval != _POLLING_INTERVAL):
        _POLLING_INTERVAL = polling_interval
    
    dir = os.path.realpath(dir)
    _WATCH_DIR = dir
    for path in _get_sources(dir):
        # Even for whatever reason path were in _FILES already, update its 
        # modification time anyway.
        _FILES[path] = os.stat(path).st_mtime
    
    _LOCK.acquire()
    if(_SLEEPING):
        # Time to wake up!
        sys.stderr.write('Looking for code changes in %s\n' % (dir))
        _SLEEPING = False
        _thread.start()
    _LOCK.release()
    return
    
