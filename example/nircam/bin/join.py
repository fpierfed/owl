#!/usr/bin/env python



def finishMef(sifTemplate, mef, numCcds):
    """
    Fake reassembling of a series of `numCcds` SIFs into an output MEF. The name
    of the output SIFs is given by `sifTemplate` whose only allowed variable is 
    `ccdId`.
    """
    f = open(mef, 'w')
    for i in range(numCcds):
        data = open(sifTemplate % {'ccdId': i}).read()
        f.write(data)
    f.close()
    return(0)




if(__name__ == '__main__'):
    import optparse
    import sys
    
    
    
    # Constants.
    USAGE = '''join.py -i <input file name template> 
                    -o <output MEF> 
                    -n <number of CCDs>'''
    
    
    # Get user input.
    parser = optparse.OptionParser(USAGE)
    parser.add_option('-i', '--input',
                      dest='input',
                      type='str',
                      default=None,
                      help='template for the input file names.')
    parser.add_option('-o', '--output',
                      dest='output',
                      type='str',
                      default=None,
                      help='path to the output MEF.')
    parser.add_option('-n', '--numCcds',
                      dest='n',
                      type='int',
                      default=None,
                      help='number of CCDs to process.')
    # Verbose flag
    parser.add_option('-v',
                      action='store_true',
                      dest='verbose',
                      default=False)

    
    # Get the command line options and also whatever is passed on STDIN.
    (options, args) = parser.parse_args()
    
    # Sanity check.
    if(not options.input):
        parser.error('Please specify an input name template.')
    if(not options.output):
        parser.error('Please specify the name of the output MEF.')
    if(not options.n):
        parser.error('Please specify the number of CCDs to process.')
    
    # Run the code.
    sys.exit(finishMef(mef=options.output, 
                       sifTemplate=options.input,
                       numCcds=options.n))
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    



