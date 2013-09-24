#!/usr/bin/env python
import math
import os
import time
import urllib2
import sys
    
from owl import config
from owl import workflow



def workflow_factory(pattern, num_chunks, num_lines):
    class MyWorkflow(workflow.Workflow):
        def get_extra_keywords(self, *args):
            return({'word': pattern, 
                    'n': int(num_chunks), 
                    'lines': int(num_lines)})
    return(MyWorkflow)



# Constants
N = 10000
USAGE = 'distributed_grep.py <pattern> <url>'
TEMPLATE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), 
                                             '..', 
                                             'templates'))
CODE_ROOT = os.path.abspath(os.path.dirname(__file__))
WORK_ROOT = config.DIRECTORIES_WORK_ROOT
REPOSITORY = '/tmp'
FILE_NAME = os.path.join(REPOSITORY, 'text.html')


if(len(sys.argv) != 3):
    sys.stderr.write(USAGE + '\n')
    sys.exit(1)

pattern = sys.argv[1]
url = sys.argv[2]

n = 0
if(os.path.exists(FILE_NAME)):
    for line in open(FILE_NAME):
        n += 1
else:
    out = open(FILE_NAME, 'w')
    for line in urllib2.urlopen(url):
        out.write(line)
        n += 1
    out.close()

# Create a simple work directory path: workRoot/<user>_<timestamp>
now = time.time()
dirName = '%s_%f' % (os.environ.get('USER', 'UNKNOWN'), now)
workDir = os.path.join(WORK_ROOT, dirName)

# We now create a dataset name to identify this run in the blackboard.
# No white spaces!
dataset = 'GrepRun%d' % (int(now))

# Create a instrument/mode Workflow instance (dataset independent)...
W = workflow_factory(pattern, int(math.ceil(float(n) / float(N))), N)
wflow = W(template_root=TEMPLATE_ROOT)
# ... and submit it to the grid (for this particular piece of data).
_id, err = wflow.execute(code_root=CODE_ROOT, 
                         repository=REPOSITORY, 
                         dataset=dataset, 
                         work_dir=workDir,
                         wait=True)
# print('Dataset %s submitted as workflow %s' % (dataset, _id))
# print('Exit status: %d' % (err))
if(err):
    print('Dataset %s submitted as workflow %s' % (dataset, _id))
    print('Exit status: %d' % (err))
    sys.exit(err)

for line in open(os.path.join(workDir, 'text.result')):
    sys.stdout.write(line)
sys.exit(0)





