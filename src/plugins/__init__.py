"""
This is where support for different batch job execution system in implemented.
"""
try:
    import condor_plugin
except:
    print('Warning: condor plugin disabled.')
    pass
try:
    import makefile_plugin
except:
    print('Warning: Makefile plugin disabled.')
    pass
try:
    import xgrid_plugin
except:
    print('Warning: XGrid plugin disabled.')
    pass
try:
    import spread_plugin
except:
    print('Warning: Spread plugin disabled.')
    pass
