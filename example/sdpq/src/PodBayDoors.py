#!/usr/bin/env python

'''
Created on Mar 1, 2012

@author: gaffney
'''

import stpydb
import os, sys
import httplib, urllib

mustDefineEnv = ["OPUS_DB_SERVER", "OPUS_DB", "DADS_DB_SERVER", "DADS_DB", \
                 "DELIVERY_HOST", "DELIVERY_DIR", "DELIVERY_USER", "DELIVERY_PW", \
                 "ARCHIVE_USER", "ARCHIVE_PASS", "SDPQ_DB_SERVER","SDPQ_DB"]

MAX_DATASETS_PER_REQUEST = 100

def checkEnv():
    if (len(sys.argv) < 1 ):
        usage()
        sys.exit(1)
    for env in mustDefineEnv:
        try:
            os.environ[env]
        except:
            usage()
            sys.exit(1);

def usage():
    print "usage: PodBayDoors datasets"
    print
    print "Must have defined DELIVERY_HOST, DELIVERY_DIR, DELIVERY_USER,"
    print "DELIVERY_PW, ARCHIVE_USER, ARCHIVE_PASS, DADS_DB_SERVER, DADS_DB,"
    print "SDPQ_DB_SERVER, SDPQ_DB OPUS_DB_SERVER, and OPUS_DB environment variables"    


queueSql = "select datasetName from SdpQueue where shoveledDate is null order by priority DESC"
def readqueue():
    queue_db = stpydb.stpydb(os.environ['SDPQ_DB_SERVER'], os.environ['SDPQ_DB'])
    result = {}
    dss = []
    queue_db.query(queueSql)
    while queue_db.execute(result):
        ds = result['datasetName']
        dss.append(ds)
    return dss


exposureSql = "select distinct(asm_member_name) from assoc_member where asm_asn_id = \'%(asnId)s\'"
def getExposures(datasets):
    dads_db = stpydb.stpydb(os.environ['DADS_DB_SERVER'], os.environ['DADS_DB'])
    expHash = {}
    for dataset in datasets:
        sql = exposureSql % {"asnId":dataset}
        result = {}
        dads_db.query(sql)
        exps = []
        while dads_db.execute(result):
            exp=result['asm_member_name']
            exps.append(exp)
        if len(exps) == 0:
            exps.append(dataset)
        expHash[dataset] = exps
    return expHash
            
#####
# setup for the SQL to find the pod files in the OPUS database
#####
podSql = "select podname from podnames where ipppssoot = \'%(dataset)s\'"


####
# and then the sql to get the dataset name from 
# the dads database for any special processing datasets
####
edtSql = "select osp_data_set_name from otfr_special_processing where osp_data_source=\'EDT\' " + \
    "and osp_data_set_name = \'%(dataset)s\'"
edtSql2 = "select afi_data_set_name from archive_files where afi_data_set_name LIKE \'%(edtname)s%%\' " + \
    "and afi_archive_class = \'EDT\'"

####
# the method that then goes into the databases and finds the pod file dataset names
# unless there are specialprocessing dataset names and returns
# them in two separate arrays
####
def findPod(datasets):
    podnames = [];
    edtnames = [];
    edtsets = []
    dads_db = stpydb.stpydb(os.environ['DADS_DB_SERVER'], os.environ['DADS_DB'])
    opus_db = stpydb.stpydb(os.environ['OPUS_DB_SERVER'], os.environ['OPUS_DB'])
    for dataset in datasets:
        sql = edtSql % {"dataset": dataset[:8]}
        result = {}
        dads_db.query(sql)
        hasEPC = False
        if dads_db.execute(result):
            edtname = result['osp_data_set_name']
            edtsets.append(edtname)
            hasEPC = True
        if hasEPC:
            continue
        sql = podSql % {"dataset": dataset}
        opus_db.query(sql)
        while opus_db.execute(result):
            podfile = result['podname']
            podnames.append(podfile)
    dads_db.close()
    
    dads_db = stpydb.stpydb(os.environ['DADS_DB_SERVER'], os.environ['DADS_DB'])
    for edtset in edtsets:
        sql = edtSql2 % {"edtname": edtset}
        result = {}
        dads_db.query(sql);
        if dads_db.execute(result):
            edtfile = result['afi_data_set_name']
            edtnames.append(edtfile)
            
    return {"podfiles": podnames, "edtfiles": edtnames}

####
# the skeleton for the DADS request
# broken up so one can insert parts as needed
# to make a whole request
####
dadsReqPre = "<?xml version=\"1.0\"?><!DOCTYPE distributionRequest " + \
    "SYSTEM \"http://ess.stsci.edu/gsd/dst/cm/distribution.dtd\"><distributionRequest><head>"
dadsRequestor = "<requester userId = \"%(userId)s\" email = \"xgaffney@stsci.edu\" " + \
    "archivePassword = \"%(userPw)s\" source = \"starview\"/><delivery>"
dadsMethod = "<ftp hostName=\"%(hostName)s\" loginName=\"%(loginName)s\" " + \
    "loginPassword=\"%(loginPW)s\" directory=\"%(directory)s\" secure=\"true\"/>"
dadsHeadSufix = "</delivery><process compression = \"none\"/></head>"
dadsBody = "<body><include><select><class name=\"EDT\" />" + \
    "<suffix name=\"POD\" /></select>"
dadsDataset ="<rootname>%(dataset)s</rootname>"
dadsReqSuffix="</include></body></distributionRequest>"

#######
# method to make the string of the dads request
######
def createDadsRequest(datasets):
    reqs = []
    for datachunk in chunk(datasets,MAX_DATASETS_PER_REQUEST):
        req = dadsReqPre + dadsRequestor  % {'userId':os.environ['ARCHIVE_USER'], \
                                             'userPw': os.environ['ARCHIVE_PASS']} 
        req = req + dadsMethod  % {'hostName':os.environ['DELIVERY_HOST'], \
                                   'loginName': os.environ['DELIVERY_USER'], \
                                   'loginPW':os.environ['DELIVERY_PW'], \
                                   'directory': os.environ['DELIVERY_DIR']}
        req = req + dadsHeadSufix + dadsBody
        for dataset in datachunk:
            if not dataset:
                break
            req = req + dadsDataset % {'dataset': dataset}
        
        req = req + dadsReqSuffix   
        reqs.append(req) 
    return reqs

######
# config to get to the DADS cgi submit script
# note this sends it to the default HST DADS server
######
archiveServer = 'archive.stsci.edu'
dadsurl = '/cgi-bin/dads.cgi'

#####
# the method to submit an xml method to DADS
# via POST
#####
def submitDadsRequest(requests):
    for request in requests:
        params=urllib.urlencode({'request':request});
        header = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
        connection = httplib.HTTPConnection(archiveServer)
        connection.request("POST", dadsurl, params, header)
        response = connection.getresponse();
        code= response.status
        
        rsp = response.read()
        if (code != 200):
            print response.reason
            sys.exit(1)
        if not '<status>SUCCESS</status>' in rsp:
            print rsp
            sys.exit(1)

######
# a nice way to break a list into chunks
######
def chunk(inputList, size):
        return map(None, *([iter(inputList)] * size))
#####
# the main
#####

if __name__ == '__main__':
    checkEnv()
    
    datasets = set(sys.argv[1:]);
    
    if len(datasets) == 0:
        datasets = readqueue()

    os.environ["LOGNAME"] = "hladads"

    exps = getExposures(datasets)
    
#    results = findPod(["I9ZF04PXQ","N3TQ04C9X"])
    allfiles = []
    for key in exps.iterkeys():
        results = findPod(exps[key])
        allfiles.extend(results["podfiles"])
        allfiles.extend(results["edtfiles"])

    if (len(allfiles) == 0):
        print "Error: No files to retrieve?"
        sys.exit(1)
    
#    if (len(datasets) != len(allfiles)):
#        print "WARNING: not a one to one match in datasets to requestable rootnames"
#        print "  " + str(datasets)
#        print "  " + str(allfiles)
    
    reqs = createDadsRequest(allfiles)

    submitDadsRequest(reqs)