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
Handle the parsing of OWL DAG files into Python objects.
"""
import os

import job





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
        self.job = job.Job.newFromClassAd(ad)
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
    
    







