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
    for directory in path.split(':'):
        if(os.path.exists(os.path.join(directory, exe))):
            return(os.path.join(directory, exe))
    return(None)


def db_connection_str(dbflavour, dbuser, dbpassword, dbhost, dbport, dbname):
    """
    Do the right thing and concoct a proper database connection string for the
    elixir/SQLAlchemy modules. Do so in a way that is mindful of DB vendor
    differences. Return that string.
    """
    if(dbflavour == 'sqlite'):
        return('sqlite:///%s' %(os.path.abspath(dbname)))

    has_mssql = dbflavour.startswith('mssql')
    port_info = ''
    connection_tmplt = '%(flavour)s://%(user)s:%(passwd)s@%(host)s'

    # We need to handle a few special cases.
    # 0. The password might contain characters that need to be escaped.
    pwd = urllib.quote_plus(dbpassword)

    # 1. Database separator
    db_info = '/' + dbname

    # 2. Yes/No port information and yes/no MSSQL.
    if(dbport and dbport != -1 and not has_mssql):
        port_info += ':' + str(dbport)
    elif(dbport and dbport != -1):
        port_info += '?port=' + str(dbport)

    # 3. MSSSQL wants a different connection string if a port is specified. Bug?
    if(has_mssql):
        connection_tmplt += '%(db_info)s%(port_info)s'
    else:
        connection_tmplt += '%(port_info)s%(db_info)s'

    connection_str = connection_tmplt % {'flavour': dbflavour,
                                         'user': dbuser,
                                         'passwd': pwd,
                                         'host': dbhost,
                                         'port_info': port_info,
                                         'db_info': db_info}
    return(connection_str)
