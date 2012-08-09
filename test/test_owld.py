#!/usr/bin/env python
import os
import sys
import time


sys.path.append('./bin')
from owl_client import OwlClient

c = OwlClient('vesta.local', 9998)


if(False):
    res = c.resources_get_list()
    print(type(res))
    print(res)



if(True):
    print(c.jobs_suspend(owner='fpierfed'))

    time.sleep(1)
    os.system('condor_q')
    time.sleep(1)

    print(c.jobs_resume(owner='fpierfed'))

    time.sleep(1)
    os.system('condor_q')
    time.sleep(1)



if(False):
    res = c.jobs_set_priority(1, job_id='vesta.local#506.0#123')
    print(res)

    res = c.jobs_set_priority(2, job_id='vesta.local#506.1#123')
    print(res)

    res = c.jobs_set_priority(3, job_id='vesta.local#506.2#123')
    print(res)

    res = c.jobs_set_priority(4, job_id='vesta.local#506.3#123')
    print(res)

    time.sleep(1)
    os.system('condor_q')
    time.sleep(1)

    res = c.jobs_get_priority('vesta.local#506.0#123')
    print(res)

    res = c.jobs_get_priority('vesta.local#506.1#123')
    print(res)

    res = c.jobs_get_priority('vesta.local#506.2#123')
    print(res)

    res = c.jobs_get_priority('vesta.local#506.3#123')
    print(res)

    time.sleep(1)
    os.system('condor_q')
    time.sleep(1)

    res = c.jobs_set_priority(0, job_id='vesta.local#506.0#123')
    print(res)

    res = c.jobs_set_priority(0, job_id='vesta.local#506.1#123')
    print(res)

    res = c.jobs_set_priority(0, job_id='vesta.local#506.2#123')
    print(res)

    res = c.jobs_set_priority(0, job_id='vesta.local#506.3#123')
    print(res)

    time.sleep(1)
    os.system('condor_q')
    time.sleep(1)

    res = c.jobs_get_priority('vesta.local#506.0#123')
    print(res)

    res = c.jobs_get_priority('vesta.local#506.1#123')
    print(res)

    res = c.jobs_get_priority('vesta.local#506.2#123')
    print(res)

    res = c.jobs_get_priority('vesta.local#506.3#123')
    print(res)
