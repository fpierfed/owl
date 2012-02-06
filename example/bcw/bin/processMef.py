#!/usr/bin/env python



def processMef(mef, sifTemplate):
    """
    Fake processing of a MEF and its splitting into its SIF components. The name
    of the output SIFs is given by `sifTemplate` whose only allowed variable is 
    `ccdId`.
    """
    data = open(mef).readlines()
    i = 0
    for line in data:
        if(not line.strip()):
            continue
        
        fileName = sifTemplate % {'ccdId': i}
        print('Writing %s' % (fileName))
        
        f = open(fileName, 'w')
        f.write(line)
        f.close()
        i += 1
    return(0)




if(__name__ == '__main__'):
    import optparse
    import sys
    
    
    
    # Constants.
    USAGE = '''processMef.py -i <input MEF> -o <output file name template>'''
    
    
    # Get user input.
    parser = optparse.OptionParser(USAGE)
    parser.add_option('-i', '--inoput',
                      dest='input',
                      type='str',
                      default=None,
                      help='path to the inpuut MEF.')
    parser.add_option('-o', '--output',
                      dest='output',
                      type='str',
                      default=None,
                      help='template for the output file names.')
    # Verbose flag
    parser.add_option('-v',
                      action='store_true',
                      dest='verbose',
                      default=False)

    
    # Get the command line options and also whatever is passed on STDIN.
    (options, args) = parser.parse_args()
    
    # Sanity check.
    if(not options.input):
        parser.error('Please specify an input MEF.')
    if(not options.output):
        parser.error('Please specify the name of the output SIFs.')
    
    # Run the code.
    print('Running processMef(%s, %s)' % (options.input, options.output))
    sys.exit(processMef(mef=options.input, sifTemplate=options.output))
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    


