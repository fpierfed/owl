'''
Originally part of the ESO-ECF/CADC HST Cache by J. Haase, D. Durand, F. Stoehr.
Minor modifications by F. Pierfederici
'''
FTP_HOST = 'stdatu.stsci.edu'
EMAIL = 'fpierfed@stsci.edu'
LOGIN = 'anonymous'
PASSWORD = EMAIL
ACCOUNTS = {'default': (LOGIN, None, PASSWORD)}



class Response(object):
    def __init__(self, success, message, retrieved_files=[]):
        self.success = success
        self.message = message
        self.files = retrieved_files
        return
    
    def __repr__(self):
        s = 'Request status: %s\nRequest log: %s\n%s'
        extra = ''
        if(self.success):
            status = 'OK'
            extra = 'These files were transferred: %s\n' % (str(self.files))
        else:
            status = 'FAILED'
        return(s % (status, self.message, extra))
    
    


class Connection(object):
    '''
    contains all the methods needed to make a archive request at the STScI in lack of proxy access
    usage:
    import dads
    h = dads.Connection()
    h.fetch(['u2zk0101p'],['raw','cal','aux','ref'])
    The above will request and download raw, calibrated, auxiliary (OMS,EPC,etc) and used reference files for the dataset u2zk0101p
    all functions can of course also be used standalone
    '''
   
    def __init__(self, default_account=ACCOUNTS['default'], 
                 default_ftphost=FTP_HOST, default_email=EMAIL):
        '''
        username and password for request and download are stored in the ~/.netrc file
        the format of that file is as follows:
        machine stdatu.stsci.edu login <username> password <password>
        '''
        self.stsci_ftphost = default_ftphost
        self.stsci_email = default_email
        try:
            import netrc
            n = netrc.netrc()
            self.stsci_userid, account, self.stsci_password = n.authenticators(self.stsci_ftphost)
        except:
            self.stsci_userid, account, self.stsci_password = accounts[self.stsci_ftphost]
        
        self.waitperiods = (15,15,30,60,60,60,60,120,120,120,120,240,120,240,360,600,1200,2400,999)
        return
      
    def buildxml(self,namelist,steplist):  
        '''
        constuct the xml needed by the dads cgi
        namelist is a list of datasetnames or POD-names without .POD_gendate_POD...
        steplist is a list of steps following the nomenclature used in the cache 
        plus 'pod' for podfiles 
        and 'ref' for the best and used reference files for the datasets 
        '''
        import xml.dom.minidom 
        
        doc = xml.dom.minidom.Document()
        doctype = xml.dom.minidom.DocumentType('distributionRequest')
        doctype.systemId = 'http://archive.stsci.edu/ops/distribution.dtd'
        doc.appendChild(doctype)
        
        distReq = doc.createElement("distributionRequest")
        
        head = doc.createElement("head")
        requester = doc.createElement("requester")
        requester.setAttribute('userId',self.stsci_userid)
        requester.setAttribute('archivePassword',self.stsci_password)
        requester.setAttribute('email',self.stsci_email)
        head.appendChild(requester)
        delivery = doc.createElement('delivery')
        delivery.appendChild(doc.createElement('staging'))
        head.appendChild(delivery)
        process = doc.createElement('process')
        process.setAttribute('compression','none')
        head.appendChild(process)
        distReq.appendChild(head)
        
        body = doc.createElement('body')
        include = doc.createElement('include')
      
        select = doc.createElement('select')
        for step in steplist:
            step = step.lower()
            if step == 'pod':
                suffix = doc.createElement('suffix')
                suffix.setAttribute('name','POD')
                select.appendChild(suffix)
            elif step == 'raw':
                retrievalKeyword = doc.createElement('retrievalKeyword')
                retrievalKeyword.setAttribute('name','Uncalibrated')
                select.appendChild(retrievalKeyword)
            elif step == 'raw_wfpc2':
                for ext in ('D0F','D0M','Q0F','Q0M','Q1F','Q1M','SHF','SHM','X0F','X0M','TRL'):                
                   suffix = doc.createElement('suffix')
                   suffix.setAttribute('name',ext)
                   select.appendChild(suffix)
            elif step == 'cal':
                retrievalKeyword = doc.createElement('retrievalKeyword')
                retrievalKeyword.setAttribute('name','Calibrated')
                select.appendChild(retrievalKeyword)
            elif step == 'cal_wfpc2':
                for ext in ('C0F','C1F','C2F','C3F','ASN','DRZ','X0F','C0M','C1M','C2M','C3M','X0M','TRL'):                
                    suffix = doc.createElement('suffix')
                    suffix.setAttribute('name',ext)
                    select.appendChild(suffix)  
            elif step == 'aux':
                retrievalKeyword = doc.createElement('retrievalKeyword')
                retrievalKeyword.setAttribute('name','JitterFiles')
                select.appendChild(retrievalKeyword)
                retrievalKeyword = doc.createElement('retrievalKeyword')
                retrievalKeyword.setAttribute('name','DataQuality')
                select.appendChild(retrievalKeyword)
            elif step == 'bestref':
                retrievalKeyword = doc.createElement('retrievalKeyword')
                retrievalKeyword.setAttribute('name','UsedReferenceFiles')
                select.appendChild(retrievalKeyword)
                retrievalKeyword = doc.createElement('retrievalKeyword')
                retrievalKeyword.setAttribute('name','BestReferenceFiles')
                select.appendChild(retrievalKeyword)
            elif step == 'oms':
                suffix = doc.createElement('suffix')
                suffix.setAttribute('name','JIT')
                select.appendChild(suffix)
                suffix = doc.createElement('suffix')
                suffix.setAttribute('name','JIF')
                select.appendChild(suffix)
                suffix = doc.createElement('suffix')
                suffix.setAttribute('name','JWT')
                select.appendChild(suffix)
                suffix = doc.createElement('suffix')
                suffix.setAttribute('name','JWF')
                select.appendChild(suffix) 
                #retrievalKeyword = doc.createElement('retrievalKeyword')
                #retrievalKeyword.setAttribute('name','JitterFiles')
                #select.appendChild(retrievalKeyword)
            elif step == 'epc':
                suffix = doc.createElement('suffix')
                suffix.setAttribute('name','EPC')
                select.appendChild(suffix)
            else:
                # lets just pick everything posible
                suffix = doc.createElement('suffix')
                suffix.setAttribute('name','*')
                select.appendChild(suffix) 
        include.appendChild(select)
              
        for name in namelist:
            rootname = doc.createElement('rootname')
            rootname.appendChild(doc.createTextNode(name.upper()))
            include.appendChild(rootname)
            
        body.appendChild(include)
        distReq.appendChild(body)
        doc.appendChild(distReq)
        return doc.toxml()
   
    def dataready(self,reqId):
        '''
        check webinterface if request is done
        TODO: replace with soap request as soon as Gretchen tells us how to do it
        '''
        import urllib2
        url = "http://archive.stsci.edu/cgi-bin/reqstat?reqnum="+reqId 
        handle = urllib2.urlopen(url)
        answer = handle.read() 
        handle.close()
        if "complete-succeeded" in answer or "Complete" in answer or "moved to trouble" in answer:
            return True
        return False
      
    def FTPdownload(self,reqId):
        '''
        grab all files in the institute staging area for the given request
        '''
        from ftplib import FTP
        
        
        transferred = []
        ftp = FTP(self.stsci_ftphost,self.stsci_userid,self.stsci_password)
        ftp.cwd('/stage/'+self.stsci_userid+'/'+reqId)
        try:
            filelist = ftp.nlst()
        except ftplib.error_perm, resp:
            if str(resp) == "550 No files found":
                # print "no files in this directory"
                pass
            else:
                raise Exception
        for filename in filelist:
            # print 'downloading '+filename
            file = open(filename, 'wb')
            try:
                ftp.retrbinary('RETR ' + filename, file.write)
                transferred.append(filename)
            except:
                # lets try to reconnect
                ftp = FTP(self.stsci_ftphost,self.stsci_userid,self.stsci_password)
                ftp.cwd('/stage/'+self.stsci_userid+'/'+reqId)
                ftp.retrbinary('RETR ' + filename, file.write)
        return(transferred)
    
    def fetch(self,namelist,steplist):
        '''
        do everything - perform the actual request
        usage:
        import dads
        h = dads.Connection()
        h.fetch(['u2zk0101p'],['raw','cal','aux','ref'])
        '''
        import urllib,urllib2,xml.dom.minidom, time
        
        
        message = 'OK'
        transferred = []
        for name in namelist:
            if name[0].upper() == 'U':
                steplist = [ x.upper().replace('CAL','CAL_WFPC2').replace('RAW','RAW_WFPC2') for x in steplist]
                break
        reqxml = self.buildxml(namelist,steplist)
        req    = urllib2.Request('http://archive.stsci.edu/cgi-bin/dads.cgi', urllib.urlencode({'request':reqxml}) )
        handle = urllib2.urlopen(req)
        answer = handle.read() 
        handle.close()
        dom = xml.dom.minidom.parseString(answer)
        r = dom.getElementsByTagName('reqId')[0].childNodes[0].data
        s = dom.getElementsByTagName('status')[0].childNodes[0].data 
        if s.strip() in ("SUCCESS","PARTIAL"):     
            message = 'Request status: '+ s + '\n'
            message += 'Reqnum: '+r + '\n'
        else:
            message =  dom.getElementsByTagName('errList')[0].childNodes[0].data
            return(Response(False, message))
        # wait for the data
        for wait in self.waitperiods:
            if self.dataready(r) or wait == 999:
                message += '%s - Starting to download\n' % (r)
                try:
                    transferred += self.FTPdownload(r)
                except:
                    message += '%s - FTP failed, waiting %s seconds and retrying\n' % (r,wait)
                    time.sleep(wait)
                    continue
                break
            else:
                message += '%s - waiting %s seconds\n' % (r,wait)
                time.sleep(wait)
        return(Response(True, message, transferred))
