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
