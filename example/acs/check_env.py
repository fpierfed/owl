#!/usr/bin/env python
import os



# Constants
USAGE = '''Usage: check_env.py [--create_dirs]
    Check that the environment is properly setup for HLA-style ACS processing.
    Optionally create the required directory structure.
'''
REQUIRED_VARS = ('ARCHIVE_DATA', 
                 'CAL', 
                 'CONTROL', 
                 'cracscomp', 
                 'crotacomp', 
                 'DADS', 
                 'FINAL', 
                 'FITSGeneration',
                 'HLA_DATA_PATHS', 
                 'HLAPipelineBin', 
                 'IRAF', 
                 'jref', 
                 'LOG', 
                 'mtab', 
                 'RAW', 
                 'REF', 
                 'SOFTWARE', 
                 'WORK', )




def report(defined, undefined, missing_paths, existing_paths, created_dirs):
    """
    Compose a report of which environment variables were defined, undefined, 
    which paths existed and which ones did not. Also specify is the missing 
    paths were created. Return the report text.
    """
    text = ''
    creatd = str(created_dirs)
    header = 'Variable Name    Defined    Path Exists    All Created    Value'
    row =    '%-15s  %-7s    %-11s    %-11s    %s'
    all = defined + undefined
    
    text += header + '\n'
    for var in all:
        if(var in defined):
            defd = 'True'
            exist = 'False'
            value = os.environ[var]
        else:
            defd = 'False'
            creatd = 'N/A'
            exist = 'N/A'
            value = 'N/A'
        if(var in existing_paths):
            exist = 'True'
        
        text += row % (var, defd, exist, creatd, value) + '\n'
    return(text)



def check_paths(paths, create_dirs=False):
    """
    Check and see if each path in the list `paths` exists and is a directory. If
    `create_dirs` is True, then create any missing path. Return the list of 
    existing paths and the list of originally missing paths.
    """
    existing = []
    missing = []
    for path in paths:
        if(not os.path.exists(path)):
            missing.append(path)
            if(create_dirs):
                os.makedirs(path)
        else:
            existing.append(path)
    return(existing, missing)





def check_env(env_vars, create_dirs=False, verbose=False):
    """
    Check and make sure that tall the environment variables in the given list 
    (`env_vars`) are defined. It is assumed that each of the input environment
    variables refer to the path of a directory or, just as the usual PATH 
    variable a list of column separated paths. If `create_dirs` is True, then 
    also create those directories.
    """
    environment = os.environ.keys()
    
    defined = []
    undefined = []
    missing_paths = []
    existing_paths = []
    for var in env_vars:
        # Check and see if the variable is defined.
        if(var in environment):
            defined.append(var)
        else:
            undefined.append(var)
            continue
        
        # Now see if the path(s) in var exist.
        paths = var.split(':')
        there, not_there = check_paths(paths=paths, create_dirs=create_dirs)
        missing_paths += not_there
        existing_paths += there
        
    # Print out a summary.
    print(report(defined, 
                 undefined, 
                 missing_paths, 
                 existing_paths, 
                 created_dirs=create_dirs))
    return(0)








if(__name__ == '__main__'):
    import optparse
    import sys
    
    
    
    # Setup the command line option parser and do the parsing.
    parser = optparse.OptionParser(USAGE)
    parser.add_option('-c', '--create_dirs',
                      action='store_true',
                      dest='create_dirs',
                      default=False,
                      help='optionally create the required directories.')
    # Verbose flag
    parser.add_option('-v',
                      action='store_true',
                      dest='verbose',
                      default=False)
    
    # Parse the command line args.
    (options, args) = parser.parse_args()
    
    # Run!
    sys.exit(check_env(env_vars=REQUIRED_VARS, 
                       create_dirs=options.create_dirs,
                       verbose=options.verbose))





