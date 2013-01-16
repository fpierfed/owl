"""
Workflow class and related utility functions.
"""
import os
import tempfile

# import drmaa
import jinja2

import plugins







class Workflow(object):
    """
    Workflow: a templated DAG.
    """
    def __init__(self, template_root):
        # Init the template engine.
        loader = jinja2.FileSystemLoader(template_root)
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


    def execute(self, code_root, repository, dataset, work_dir=None,
                flavour='condor', extra_env=None, wait=False):
        """
        Execute the workflow by creating a temporary directory, rendering the
        templates and submitting the newly instantiated workflow to the
        appropriate batch job execution system.
        """
        if(extra_env is None):
            extra_env = {}

        # Get any extra keyword we might need for our templates.
        extrakwds = self.get_extra_keywords(code_root, repository, dataset,
                                            work_dir, flavour, extra_env)

        # Create the variable dictionary that the templates are going to need.
        kwds = {'code_root': code_root,
                'repository': repository,
                'dataset': dataset}
        # Extend/override the content of kwds with that of extrakwds.
        kwds.update(extrakwds)

        # Extend/override the content of kwds with that of extra_env.
        kwds.update(extra_env)

        # Create a temporary directory unless one was specified.
        if(not work_dir):
            work_dir = tempfile.mkdtemp()

        # If it does not exist, try and create it. Do not try too hard ;-)
        if(not os.path.isdir(work_dir)):
            os.makedirs(work_dir)

        # Render the templates for the given dataset and write them out to
        # work_dir. By convention we rename the templates to make them dataset
        # specific: root_<dataset>.extension
        #   e.g. processMef_J9AM01071.job
        for tmplt in self.templates:
            # Make it dataset specific
            (root, ext) = os.path.splitext(tmplt.name)
            file_name = root + '_' + dataset + ext

            fid = open(os.path.join(work_dir, file_name), 'w')
            fid.write(tmplt.render(**kwds))
            fid.write('\n')
            fid.close()

        # Update the name of the dag accordingly
        (root, ext) = os.path.splitext(self.dag.name)
        dag_name = root + '_' + dataset + ext

        # Now submit the whole workflow via DRMAA.
        return(self._submit(dag_name, work_dir, flavour, extra_env, wait))


    def get_extra_keywords(self, code_root, repository, dataset, work_dir,
                           flavour, extra_env):
        """
        Customize the workflow from dataset and/or the environment. Return a
        dictionary of extra keywords to use to render the workflow templates.
        """
        return({})


    def _submit(self, dag_name, work_dir, flavour='condor', extra_env=None,
                wait=False):
        """
        Simply delegate the work to the appropriate plugin. If wait==True and
        flavour=='condor', wait for the job to complete and return its exit code
        as well as its id.
        """
        if(extra_env is None):
            extra_env = {}

        if(flavour != 'condor'):
            wait = False

        # If we are asked to (by specifying extra_env) agument the user
        # environment.
        if(extra_env):
            os.environ.update(extra_env)

        plugin = getattr(plugins, flavour + '_plugin')

        if(wait):
            return(plugin.submit(dag_name, work_dir, wait=True))
        return(plugin.submit(dag_name, work_dir))




































