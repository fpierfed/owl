"""
Handle the parsing of OWL DAG files into Python objects.
"""
import os

from classad import Job





def _extract_inouts(arg_string):
    """
    Extract the name of the input and output arguments from a command-line str
    assuming a simple convention:
        -i bla means that bla is an input argument.
        -o bla means that bla is an output.
    Used in the Makefile plugin only... and it is a hack.
    """
    args = arg_string.strip().split()
    in_args = ''
    out_args = ''

    nargs = len(args)
    for i in range(nargs):
        if(args[i] == '-i' and i < nargs):
            in_args = args[i+1]
        elif(args[i] == '-o' and i < nargs):
            out_args = args[i+1]
    return(in_args, out_args)

def _parse(dag, directory):
    """
    A DAG syntax is pretty simple
        JOB <JOBNAME> <JOBSCRIPT>
        PARENT <JOBNAME> CHILD <JOBNAME>
    We do not support DATA jobs quite yet.
    """
    dag = os.path.join(directory, dag)
    lines = [l.strip() for l in dag.split('\n') if l.strip()]

    # Nodes.
    nodes = {}                                                  # {name, Node}
    for line in lines:
        if(not line.startswith('JOB')):
            continue

        _, name, script = line.split()
        nodes[name] = Node(name=name,
                           script=os.path.join(directory, script),
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

        # print('%s is parent of %s' \
        #         % (parent.name, str([c.name for c in children])))
        parent.children = children
        for child in children:
            # print('%s is child of %s' % (child.name, parent.name))
            child.parents.append(parent)
            # print('%s parents are %s' \
            #         % (child.name, str([p.name for p in child.parents])))
    return(nodes.values())

def _escape(arg_string):
    """
    Shell escape arguments.

    http://stackoverflow.com/ \
        questions/35817/how-to-escape-os-system-calls-in-python
    """
    args = arg_string.strip().split()
    return(' '.join(["'" + s.replace("'", "'\\''") + "'" for s in args]))




class Node(object):
    """
    A node in the DAG.
    """
    def __init__(self, name, script, children=None, parents=None):
        if(children is None):
            children = []
        if(parents is None):
            parents = []

        classad = open(script).read()

        self.name = name
        self.script = script
        self.job = Job.new_from_classad(classad)
        self.children = children
        self.parents = parents
        return



class DAG(object):
    """
    A full DAG (i.e. a directed graph with no loops).
    """
    @classmethod
    def new_from_dag(cls, dag, directory):
        """
        Given a DAG text, parse it and create the corresponding DAG instance.
        """
        return(cls(nodes=_parse(dag, directory)))

    def __init__(self, nodes):
        self.nodes = nodes
        self.roots = [n for n in nodes if not n.parents]
        return









