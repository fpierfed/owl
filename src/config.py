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
import sys

from utils import dbConnectionStr




# We look for the configuration file in two places, in order:
#   1. /etc/owlrc
#   2. <owl install directory>/etc/owlrc
# 
# However, users can override every configuration entry by defining environment 
# variables called 
#   OWL_<uppercase group name>_<uppercase key name>
# e.g.
#   OWL_DIRECTORIES_PIPELINE_ROOT
# 
# There are two entries that are not present in the configuration file but that 
# can be overridden via environment variables. Those are
#   1. DATABASE_CONNECTION_STR
#   2. DIRECTORIES_TEMPLATE_ROOT
config = None
names = (os.path.join('/etc', 'owlrc'),
         os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                      'etc', 'owlrc'))
for name in names:
    if(os.path.exists(name)):
        # remember the name of the config file we used; for outsiders to examine
        config_name = name  # remek

        # load the config
        config = ConfigParser.RawConfigParser(defaults={'port': -1})
        config.read(name)
        break

if(not config):
    raise(Exception('No configuration file found in any of %s' % (str(names))))

# Fetch the user environment.
env = os.environ

# Export the config values as constants for the module. Override what we read in
# the config file with the corresponding environment variable, if found.
module = sys.modules[__name__]
for section in config.sections():
    for option in config.options(section):
        var = '%s_%s' % (section.upper(), option.upper())
        env_var = 'OWL_' + var
        val = env.get(env_var, config.get(section, option))
        setattr(module, var, val)

# Now the template root directory. We might end up putting this in the config 
# file as well.
here = os.path.dirname(os.path.abspath(__file__))
DIRECTORIES_TEMPLATE_ROOT = env.get('OWL_DIRECTORIES_TEMPLATE_ROOT', 
                                    os.path.join(here, 'templates'))

# Finally the database connection string.
DATABASE_CONNECTION_STR = env.get('OWL_DATABASE_CONNECTION_STR', 
                                  dbConnectionStr(DATABASE_FLAVOUR,
                                                  DATABASE_USER,
                                                  DATABASE_PASSWORD,
                                                  DATABASE_HOST,
                                                  DATABASE_PORT,
                                                  DATABASE_DATABASE))

if __name__ == '__main__' :
    print config_name
