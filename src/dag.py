"""
Handle the parsing of OWL DAG files into Python objects.
"""
import os

from classad import Job





def _extract_inouts(arg_string):
    args = arg_string.strip().split()
    input = ''
    output = ''

    nargs = len(args)
    for i in range(nargs):
        if(args[i] == '-i' and i < nargs):
            input = args[i+1]
        elif(args[i] == '-o' and i < nargs):
            output = args[i+1]
    return(input, output)



def _parse(dag, dir):
    # A DAG syntax is pretty simple
    # JOB JOBNAME JOBSCRIPT
    # PARENT JOBNAME CHILD JOBNAME
    # We do not support DATA jobs quite yet.
    dag = os.path.join(dir, dag)
    lines = [l.strip() for l in dag.split('\n') if l.strip()]

    # Nodes.
    nodes = {}                                                  # {name, Node}
    for line in lines:
        if(not line.startswith('JOB')):
            continue

        typ, name, script = line.split()
        nodes[name] = Node(name=name,
                           script=os.path.join(dir, script),
                           children=[],
                           parents=[])

    # Relations.
    for line in lines:
        if(not line.startswith('PARENT')):
            continue

        # PARENT, <node name>, CHILD, <child1> <child2> <child3>...
        #   0          1         2       3:
        tokens = line.split()

        parent = nodes[tokens[1]]
        children = [nodes[n] for n in tokens[3:]]

        # print('%s is parent of %s' % (parent.name, str([c.name for c in children])))
        parent.children = children
        for child in children:
            # print('%s is child of %s' % (child.name, parent.name))
            child.parents.append(parent)
            # print('%s parents are %s' % (child.name, str([p.name for p in child.parents])))
    return(nodes.values())


def _escape(arg_string):
    """
    Shell escape arguments.

    http://stackoverflow.com/questions/35817/how-to-escape-os-system-calls-in-python
    """
    args = arg_string.strip().split()
    return(' '.join(["'" + s.replace("'", "'\\''") + "'" for s in args]))



class Node(object):
    def __init__(self, name, script, children=[], parents=[]):
        ad = open(script).read()

        self.name = name
        self.job = Job.newFromClassAd(ad)
        self.children = children
        self.parents = parents
        return



class DAG(object):
    @classmethod
    def newFromDAG(cls, dag, dir):
        """
        Given a DAG text, parse it and create the corresponding DAG instance.
        """
        return(cls(nodes=_parse(dag, dir)))

    def __init__(self, nodes):
        self.nodes = nodes
        return









