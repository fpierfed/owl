#!/usr/bin/env python
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
Condor iRODS file tranfer plugin. Supports
    - Getting a file from iRODS
    - Putting a file into iRODS
    - Putting a list of files into iRODS
    - Reading/writing files to the UNIX filesystem.

See the Condor manual (section 3.13.3) for more information about file system 
plugins and how to enable them.


iRODS URIs have to have the form
    irods://user.zone:passwrod@server:port/path/to/file/or/collection

file URIs have to have the form
    file:/path/to/file
"""
import os
import subprocess
import tempfile
import urlparse



# Constants
PROTOCOLS = ('irods', 'file')
IRODS_BIN_DIR = '/jwst/bin'




def _set_irods_env(username, zone, password, host, port, bin_dir=IRODS_BIN_DIR):
    """
    Internal use only
    
    Prepare the iRODS environmnment for later execution of iget/iput etc.
    Do not override the environment if already setup.
    
    Return the full environment.
    """
    # Create a temp filename for the auth token.
    (fid, auth_file) = tempfile.mkstemp()
    os.close(fid)
    
    irods_env = {'irodsUserName': username,
                 'irodsHost': host,
                 'irodsPort': str(port),
                 'irodsHome': '/%s/home/%s' % (zone, username),
                 'irodsCwd': '/%s/home/%s' % (zone, username),
                 'irodsDefResource': 'fooResc',
                 'irodsZone': zone,
                 'irodsAuthFileName': auth_file,
                 'rm_irods_auth_file': 'True'}
    irods_env.update(os.environ)
    if(irods_env['irodsAuthFileName'] is not auth_file):
        irods_env['rm_irods_auth_file'] = 'False'
    
    # Init iRODS.
    p = subprocess.Popen((os.path.join(bin_dir, 'iinit'), password), 
                         shell=False,
                         env=irods_env)
    err = p.wait()
    if(err):
        os.remove(auth_file)
        raise(Exception('Error: iinit returned %d' % (err)))
    return(irods_env)


def _parse_irods_uri(uri):
    """
    Internal use only
    
    Parse an irods:// URI and return its components.
    
    Return (username, zone, password, host, port, path)
    
    We are guaranteed that the proper sanity check have already been made by the
    calling code.
    """
    res = urlparse.urlsplit(uri)
    raw_username = res.username
    (username, zone) = raw_username.split('.', 1)
    return(username, zone, res.password, res.hostname, res.port, res.path)


def _parse_file_uri(uri):
    """
    Internal use only
    
    Parse a file: URI and return the path of the file/directory it points to.
    
    We are guaranteed that the proper sanity check have already been made by the
    calling code.
    """
    return(urlparse.urlsplit(uri).path)





def get(src, dst, force=True, bin_dir=IRODS_BIN_DIR):
    """
    Get `src` from iRODS and write it to `dst`. If `force` is True, then 
    overwrite `dst` if it exists. If `force` is False and `dst` exists, abort 
    with an error.
    
    iRODS URIs have to have the form
        irods://user.zone:passwrod@server:port/path/to/file/or/collection
    
    file URIs have to have the form
        file:/path/to/file/or/directory
        
    `src` has to be an iRODS URI pointing to a file and `dst` has to be a file 
    URI pointing to a file or directory.
    """
    # Parse src and dst.
    (username, zone, password, host, port, src_path) = _parse_irods_uri(src)
    dst_path = _parse_file_uri(dst)
    
    # Setup the environment for iRODS and call iinit.
    env = _set_irods_env(username, zone, password, host, port)
    
    # Copy the file from iRODS to `dst`.
    cmd = [os.path.join(bin_dir, 'iget'), src_path, dst_path]
    if(force):
        cmd.insert(1, '-f')
    p = subprocess.Popen(cmd, shell=False, env=env)
    err = p.wait()
    if(err):
        if(os.environ.get('rm_irods_auth_file', 'False').lower() == 'true'):
            os.remove(env['irodsAuthFileName'])
        print('Error: iget returned %d' % (err))
    return(err)


def put(src, dst, force=True, bin_dir=IRODS_BIN_DIR):
    """
    Get `src` from the filesystem and write it to the iRODS collection `dst`.
    If `force` is True, then overwrite `dst`/`stc` if it exists. If `force` is 
    False and `dst`/`src` exists, abort with an error.
    
    iRODS URIs have to have the form
        irods://user.zone:passwrod@server:port/path/to/file/or/collection
    
    file URIs have to have the form
        file:/path/to/file
    
    `src` has to be a file URI pointing to a file and `dst` has to be an iRODS 
    URI pointing to a collection/directory.
    """
    # Parse src and dst.
    src_path = _parse_file_uri(src)
    (username, zone, password, host, port, dst_path) = _parse_irods_uri(dst)
    
    # Setup the environment for iRODS and call iinit.
    env = _set_irods_env(username, zone, password, host, port)
    
    # Copy the file `stc` to the iRODS collection `dst`.
    cmd = [os.path.join(bin_dir, 'iput'), src_path, dst_path]
    if(force):
        cmd.insert(1, '-f')
    p = subprocess.Popen(cmd, shell=False, env=env)
    err = p.wait()
    if(err):
        if(os.environ.get('rm_irods_auth_file', 'False').lower() == 'true'):
            os.remove(env['irodsAuthFileName'])
        print('Error: iput returned %d' % (err))
    return(err)








if(__name__ == '__main__'):
    import sys
    
    
    
    # Simple sanity check: we can have either one or two command line arguments
    # (see below for more information).
    if(len(sys.argv) not in (2, 3)):
        print('Usage: irods.py -classad')
        print('Usage: irods.py <source URI> <destination URI>')
        sys.exit(1)
    
    # The plugin is called two possible ways:
    #   1. By condor_starter to understand each plugin capability. In this mode,
    #      the plugin is executed and given -classad as the only command line 
    #      argument. The plugin has to reply with a three line classad 
    #      specifying the supported URLs.
    if(sys.argv[1] == '-classad'):
        print('PluginVersion = "0.1"')
        print('PluginType = "FileTransfer"')
        print('SupportedMethods = "%s"' % (','.join(PROTOCOLS)))
        sys.exit(0)
    
    #   2. By condor_starter to transfer files. This happens once for each file
    #      to transfer. For input file transfer(s), the plugin is passed the URL
    #      of the input file to retrieve and the full path of the destination 
    #      file on the UNIX filesystem. For output files (new in Condor 7.6) the
    #      plugin is passed the UNIX path of a file to transfer and the URL of a
    #      directory to store the file into. The plugin is invoked once per file
    #      to transfer.
    src_uri = sys.argv[1]
    dst_uri = sys.argv[2]
    
    # We now have two cases that we support:
    #   1. src_uri is an irods file URI and dst_uri is a file URI.
    #   2. dst_uri is an irods directory/collection URI and src_uri is a file 
    #      URI.
    # All other cases are not supported.
    if(src_uri.startswith('irods://') and dst_uri.startswith('file:')):
        sys.exit(get(src_uri, dst_uri))
    elif(src_uri.startswith('file:') and dst_uri.startswith('irods://')):
        sys.exit(put(src_uri, dst_uri))
    else:
        raise(NotImplementedError('Only iRODS<->file transfers are supported.'))
    # We should never get here...
    sys.exit(255)





























