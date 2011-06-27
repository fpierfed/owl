# Copyright (C) 2010 Association of Universities for Research in Astronomy(AURA)
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
#     1. Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
# 
#     2. Redistributions in binary form must reproduce the above
#       copyright notice, this list of conditions and the following
#       disclaimer in the documentation and/or other materials provided
#       with the distribution.
# 
#     3. The name of AURA and its representatives may not be used to
#       endorse or promote products derived from this software without
#       specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY AURA ``AS IS'' AND ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL AURA BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
# OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
# TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
# USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
# DAMAGE.
"""
Workflow class and related utility functions.
"""
import os
import tempfile

# import drmaa
import jinja2

import plugins




def _getNumberOfCCDs(repository, dataset):
    lines = [l 
             for l in open(os.path.join(repository, 
                                        dataset + '.fits')).readlines() 
             if(l.strip())]
    return(len(lines))


def _getNumberOfExposures(repository, dataset):
    """
    Return the number of _raw.fits files in the given ACS dataset/visit.
    """
    exposures = [f for f in os.listdir(repository) if f.startswith(dataset) and
                                                      f.endswith('_raw.fits')]
    return(len(exposures))






class Workflow(object):
    def __init__(self, templateRoot):
        # Init the template engine.
        loader = jinja2.FileSystemLoader(templateRoot)
        env = jinja2.Environment(loader=loader)
        
        # Load all the available templates.
        self.templates = [env.get_template(path) 
                          for path in loader.list_templates()]
        try:
            self.dag = [t for t in self.templates if t.name.endswith('.dag')][0]
        except:
            msg = 'We do not support the case where a workflow .dag file is' + \
                  ' not present.'
            raise(NotImplementedError(msg))
        return
    
    
    def execute(self, codeRoot, repository, dataset, workDir=None, 
                flavour='condor', extraEnvironment={}):
        # Get any extra keyword we might need for our templates.
        extraKw = self.getExtraKeywords(codeRoot, repository, dataset, workDir, 
                                         flavour, extraEnvironment)
        
        # Create the variable dictionary that the templates are going to need.
        kw = {'code_root': codeRoot,
              'repository': repository,
              'dataset': dataset}
        # Extend/override the content of kw with that of extraKw.
        kw.update(extraKw)
        
        # Create a temporary directory unless one was specified.
        if(not workDir):
            workDir = tempfile.mkdtemp()
        
        # If it does not exist, try and create it. Do not try too hard ;-)
        if(not os.path.isdir(workDir)):
            os.makedirs(workDir)
        
        # Render the templates for the given dataset and write them out to 
        # workDir. By convention we rename the templates to make them dataset
        # specific: root_<dataset>.extension
        #   e.g. processMef_J9AM01071.job
        for t in self.templates:
            # Make it dataset specific
            (root, ext) = os.path.splitext(t.name)
            fileName = root + '_' + dataset + ext
            
            f = open(os.path.join(workDir, fileName), 'w')
            f.write(t.render(**kw))
            f.write('\n')
            f.close()
        
        # Update the name of the dag accordingly
        (root, ext) = os.path.splitext(self.dag.name)
        dagName = root + '_' + dataset + ext
        
        # Now submit the whole workflow via DRMAA.
        return(self._submit(dagName, workDir, flavour, extraEnvironment))
    
    
    def getExtraKeywords(self, codeRoot, repository, dataset, workDir, flavour, 
                         extraEnvironment):
        return({})
    
    
    def _submit(self, dagName, workDir, flavour='condor', extraEnvironment={}):
        """
        Simply delegate the work to the appropriate plugin.
        """
        # If we are asked to (by specifying extraEnvironment) agument the user 
        # environment.
        if(extraEnvironment):
            os.environ.update(extraEnvironment)
        
        plugin = getattr(plugins, flavour)
        return(plugin.submit(dagName, workDir))



class BcwWorkflow(Workflow):
    """
    Simple BCW workflow.
    """
    def getExtraKeywords(self, codeRoot, repository, dataset, workDir, flavour, 
                         extraEnvironment):
        return({'num_ccds': _getNumberOfCCDs(repository, dataset)})



class BcwIrodsWorkflow(Workflow):
    """
    Simple BCW+iRODS workflow.
    """
    def getExtraKeywords(self, codeRoot, repository, dataset, workDir, flavour, 
                         extraEnvironment):
        # Root iRODS collection.
        root = '/fooZone/home/foo'
        
        # Remember to remove root and any leading slash from repository.
        if(repository.startswith(root)):
            repository.replace(root, '', 1)
        if(repository.startswith('/')):
            repository = repository[1:]
        
        # FIXME: Handle iRODS user auth better!!!
        # FIXME: Derice the number of CCDs dynamically.
        return({'num_ccds': 4,
                'work_dir': workDir,
                'user': 'foo',
                'zone': 'fooZone',
                'password': 'condor',
                'server': 'jwdmsdevvm2.stsci.edu',
                'port': 1247,
                'root': root,
                'repository': repository})



class AcsWorkflow(Workflow):
    """
    HLA/ACS workflow.
    """
    def getExtraKeywords(self, codeRoot, repository, dataset, workDir, flavour, 
                         extraEnvironment):
        return({'num_exposures': _getNumberOfExposures(repository, dataset)})




























