#!/usr/bin/env python
import time



def processSif(inSif, outSif):
    """
    Fake processing of a SIF.
    """
    time.sleep(2)
    
    data = open(inSif).read()
    f = open(outSif, 'w')
    f.write('processed ' + data)
    f.close()
    return(0)




if(__name__ == '__main__'):
    import optparse
    import sys
    
    
    
    # Constants.
    USAGE = '''dummyProcess.py -i <input SIF> -o <output SIF>'''
    
    
    # Get user input.
    parser = optparse.OptionParser(USAGE)
    parser.add_option('-i', '--inoput',
                      dest='input',
                      type='str',
                      default=None,
                      help='path to the inpuut SIF.')
    parser.add_option('-o', '--output',
                      dest='output',
                      type='str',
                      default=None,
                      help='path to the output SIF.')
    # Verbose flag
    parser.add_option('-v',
                      action='store_true',
                      dest='verbose',
                      default=False)

    
    # Get the command line options and also whatever is passed on STDIN.
    (options, args) = parser.parse_args()
    
    # Sanity check.
    if(not options.input):
        parser.error('Please specify an input SIF.')
    if(not options.output):
        parser.error('Please specify an output SIF.')
    
    # Run the code.
    sys.exit(processSif(inSif=options.input, outSif=options.output))
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    


