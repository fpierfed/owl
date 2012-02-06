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

# Parse the config file and specify defaults inline.
import ConfigParser
import os


# Find the config file.
config = None
names = []

# We look for the configuration file in a few places, in order:
#   1.$OWL_CONFIG_DIR/owlrc
#   2. $HOME/.owlrc
#   3. /etc/owlrc
#   4. <owl install directory>/etc/owlrc
# In some environments, $HOME is not defined and so $HOME/.owlrc is skipped. 
# Also OWL_CONFIG_DIR might not be defined and, if so,  should be skipped.
owlConfigDir = os.environ.get('OWL_CONFIG_DIR', None)
if(owlConfigDir):
    names.append(os.path.join(owlConfigDir, 'owlrc'))
homeDir = os.environ.get('HOME', None)
if(homeDir):
    names.append(os.path.join(homeDir, '.owlrc'))
names.append(os.path.join('/etc', 'owlrc'))
names.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                          'etc', 'owlrc'))
for name in names:
    if(os.path.exists(name)):
        config = ConfigParser.RawConfigParser(defaults={'port': -1})
        config.read(name)
        break

# Export the config values as constants for the module.
if(not config):
    raise(Exception('No configuration file found in any of %s' % (str(names))))

# Configuration!
DATABASE_HOST = config.get('Database', 'host')
DATABASE_PORT = config.getint('Database', 'port')
DATABASE_USER = config.get('Database', 'user')
DATABASE_PASSWORD = config.get('Database', 'password')
DATABASE_DB = config.get('Database', 'database')
DATABASE_FLAVOUR = config.get('Database', 'flavour')

PIPELINE_ROOT = config.get('Directories', 'pipeline_root')
WORK_ROOT = config.get('Directories', 'work_root')

TEMPLATE_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             'templates')
