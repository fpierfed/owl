#!/usr/bin/env python
import httplib
import os
import urllib2


# Constants
USAGE = '''Usage: get_hst_datasets.py <dataset 1> ... <dataset N>
    Retrieve the given uncalibrated HST dataset(s). Uses the ESO/ECF HST Proxy.
'''
URL = 'http://archive.eso.org/archive/hst/proxy/ecfproxy?file_id=%(dataset_id)s'





# From Dive Into Python
class SmartRedirectHandler(urllib2.HTTPRedirectHandler):     
    def http_error_301(self, req, fp, code, msg, headers):  
        result = urllib2.HTTPRedirectHandler.http_error_301( 
            self, req, fp, code, msg, headers)              
        result.status = code                                 
        return result                                       

    def http_error_302(self, req, fp, code, msg, headers):   
        result = urllib2.HTTPRedirectHandler.http_error_302(
            self, req, fp, code, msg, headers)              
        result.status = code                                
        return result


def smart_fetch(url):
    opener = urllib2.build_opener(SmartRedirectHandler())
    f = opener.open(urllib2.Request(url))
    data = f.read()
    f.close()
    return(data)
    







def get_hst_dataset(datasets, url_tmplt=URL, verbose=False):
    """
    This is a basic wrapper around 
    
    curl -L http://archive.eso.org/archive/hst/proxy/ecfproxy?file_id=`dataset`
    
    for each dataset in datasets. Return the full path of the downloaded 
    datasets.
    """
    for dataset in datasets:
        url = url_tmplt % {'dataset_id': dataset}
        f = open(dataset + '.fits', 'w')
        # FIXME: this is pretty bad: what if the file is huge????
        f.write(smart_fetch(url))
        f.close()
        if(verbose):
            print('Downloaded %s as %s.fits' % (dataset, dataset))
    return(0)






if(__name__ == '__main__'):
    import optparse
    import sys
    
    
    
    # Setup the command line option parser and do the parsing.
    parser = optparse.OptionParser(USAGE)
    # Verbose flag
    parser.add_option('-v',
                      action='store_true',
                      dest='verbose',
                      default=False)
    
    # Parse the command line args.
    (options, args) = parser.parse_args()
    if(not args):
        parser.error('Please specify at least one dataset id.')
        sys.exit(1)
    
    # Run!
    ids = [os.path.splitext(a)[0] for a in args]
    sys.exit(get_hst_dataset(ids, verbose=options.verbose))





