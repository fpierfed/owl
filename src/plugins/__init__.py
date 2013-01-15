"""
This is where support for different batch job execution system in implemented.
"""
try:
    import condor
except:
    print('Warning: condor plugin disabled.')
    pass
try:
    import makefile
except:
    print('Warning: Makefile plugin disabled.')
    pass
try:
    import xgrid
except:
    print('Warning: XGrid plugin disabled.')
    pass
try:
    import spreader
except:
    print('Warning: Spreader plugin disabled.')
    pass