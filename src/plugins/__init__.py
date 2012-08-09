"""
This is where support for different batch job execution system in implemented.
"""
try:
    import condor
except ImportError:
    pass
try:
    import makefile
except ImportError:
    pass
try:
    import xgrid
except ImportError:
    pass
