"""
This is where support for different batch job execution system in implemented.
"""
try:
    import condor
except:
    pass
try:
    import makefile
except:
    pass
try:
    import xgrid
except:
    pass
