"""
Define all Makefile-specific properties here.

This is a bit of a hack... err... proof of concept, I mean ;-)
"""
import collections
import os
import shutil
import time

from owl import dag
from spreader import client





class DAG(dag.DAG):
    """
    This is a spreader DAG. We assume that the comman-line argument do not have
    spaces!
    """
    def execute(self, work_dir):
        err = 0
        running = _enqueue(self.roots, work_dir)

        # Now wait. A node is done if and only if al its instances are done.
        total_time = 0.
        while(running):
            instances = running.pop(0)
            not_done = [(p, n) for (p, n) in instances if not p.is_ready()]
            for (promise, node) in not_done:
                promise.wait()
            done = [(p, n) for (p, n) in instances if p.is_ready()]
            for (promise, node) in done:
                res = promise.result()
                if(res['terminated'] or res['exit_code'] != 0):
                    print(res)
                    return(res['exit_code'])

            if(len(done) == len(instances)):
                # Move to the next nodes.
                running += _enqueue(instances[0][1].children, work_dir)
            else:
                running.append(instances)
        return(err)




def _copy_input_files(node, work_dir):
    if(hasattr(node.job, 'transfer_input_files')):
        paths = node.job.transfer_input_files.split(',')
        for path in paths:
            if(not os.path.exists(os.path.basename(path))):
                shutil.copy(path, work_dir)
    return


def _enqueue(nodes, work_dir):
    """
    Submit the given nodes to the active spreader installation.
    """
    running = []
    for node in nodes:
        instances = []
        for _id in range(node.job.Instances):
            node = _expand_vars(node, _id)
            _copy_input_files(node, work_dir)
            promise = client.async_call('system',
                                        _mkargv(node),
                                        cwd=work_dir)
            instances.append((promise, node))
        running.append(instances)
    return(running)


def _expand_vars(node, instance_id):
    mapping = {'$(Process)': str(instance_id)}

    new_node = _copynode(node)
    for key in new_node.job.__dict__.keys():
        for var in mapping.keys():
            value = getattr(new_node.job, key)
            if(isinstance(value, basestring) and var in value):
                setattr(new_node.job, key, value.replace(var, mapping[var]))
            elif(isinstance(value, collections.Iterable)):
                for i in range(len(value)):
                    if(isinstance(value[i], basestring) and var in value):
                        value[i] = value[i].replace(var, mapping[var])
                setattr(new_node.job, key, value)
    return(new_node)


def _copynode(node):
    new_node = dag.Node(name=node.name,
                        script=node.script,
                        children=node.children,
                        parents=node.parents)
    return(new_node)


def _mkargv(node):
    """
    We assume that the comman-line argument do not have spaces!
    """
    # We have to do variable expansion by hand.
    # FIXME: Support spaces in job argument string.
    return([node.job.Executable, ] + node.job.Arguments.split())


def submit(dagName, workDir):
    """
    Submit the given DAG (described in os.path.join(workDir, dagName)) to a
    spreader installation. Block until the DAG either failed of finished
    executing.
    """
    # All the files we need (the .dag file and the .job files) are in `workDir`
    # and have the names defined in the .dag file (which we are given as
    # `dagName`). So, first thing is to parse `dagName`.
    print(workDir)

    here = os.getcwd()
    os.chdir(workDir)
    dag = DAG.new_from_dag(open(os.path.join(workDir, dagName)).read(), workDir)
    err = dag.execute(workDir)
    os.chdir(here)
    return(err)


