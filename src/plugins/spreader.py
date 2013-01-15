"""
Define all Makefile-specific properties here.

This is a bit of a hack... err... proof of concept, I mean ;-)
"""
import os

from owl import dag
import client





class DAG(dag.DAG):
    """
    This is a spreader DAG. We assume that the comman-line argument do not have
    spaces!
    """
    def execute(self):
        err = 0
        running = _enqueue(self.roots)

        # Now wait. A node is done if and only if al its instances are done.
        while(running):
            instances = running.pop(0)
            done = [True for (p, n) in instances if p.is_ready()]
            if(len(done) == len(instances)):
                # Move to the next nodes.
                running += _enqueue(instances[0][1].children)
            else:
                running.append(instances)
        return(err)




def _enqueue(nodes):
    """
    Submit the given nodes to the active spreader installation.
    """
    running = []
    for node in nodes:
        instances = []
        for _id in range(node.job.Instances):
            promise = client.async_call('system', _mkargv(node, _id))
            instances.append(promise, node)
        running.append(instances)
    return(running)


def _mkargv(node, instance_id):
    """
    We assume that the comman-line argument do not have spaces!
    """
    # We have to do variable expansion by hand.
    # FIXME: Support spaces in job argument string.
    argstr = node.job.Arguments.replace('$(Process)', str(instance_id))
    exe = node.job.Executable.replace('$(Process)', str(instance_id))
    return([exe, ] + argstr.split())


def submit(dagName, workDir):
    """
    Submit the given DAG (described in os.path.join(workDir, dagName)) to a
    spreader installation. Block until the DAG either failed of finished
    executing.
    """
    # All the files we need (the .dag file and the .job files) are in `workDir`
    # and have the names defined in the .dag file (which we are given as
    # `dagName`). So, first thing is to parse `dagName`.
    dag = DAG.new_from_classad(open(os.path.join(workDir, dagName)).read(),
                               workDir)

    # Create the makefile
    f = open(os.path.join(workDir, 'Makefile'), 'w')
    f.write(dag.to_makefile())
    f.close()

    print('Makefile written in work directory %s' % (workDir))
    return(0)


