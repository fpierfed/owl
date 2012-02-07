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
Useful code that does not belong any other place.
"""
import os
import urllib



def which(exe):
    """
    Find and return `exe` in the user unix PATH.
    """
    path = os.environ.get('PATH', '')
    for dir in path.split(':'):
        if(os.path.exists(os.path.join(dir, exe))):
            return(os.path.join(dir, exe))
    return(None)


def dbConnectionStr(dbFlavour, dbUser, dbPassword, dbHost, dbPort, dbName):
    if(dbFlavour == 'sqlite'):
        return('sqlite:///%s' %(os.path.abspath(dbName)))
    
    has_mssql = dbFlavour.startswith('mssql')
    port_info = ''
    connection_tmplt = '%(flavour)s://%(user)s:%(passwd)s@%(host)s'
    
    # We need to handle a few special cases.
    # 0. The password miught contain characters that need to be escaped.
    pwd = urllib.quote_plus(dbPassword)
    
    # 1. Database separator
    db_info = '/' + dbName
    
    # 2. Yes/No port onformation and yes/no MSSQL.
    if(dbPort and dbPort != -1 and not has_mssql):
        port_info += ':' + str(dbPort)
    elif(dbPort and dbPort != -1):
        port_info += '?port=' + str(dbPort)
    
    # 3. MSSSQL wants a different connection string if a port is specified. Bug?
    if(has_mssql):
        connection_tmplt += '%(db_info)s%(port_info)s'
    else:
        connection_tmplt += '%(port_info)s%(db_info)s'
    
    connection_str = connection_tmplt % {'flavour': dbFlavour,
                                         'user': dbUser,
                                         'passwd': pwd,
                                         'host': dbHost,
                                         'port_info': port_info,
                                         'db_info': db_info}
    return(connection_str)
