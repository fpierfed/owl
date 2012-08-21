"""
OWL keeps its configuration values in owlrc files. These files are typically
stored in
  <prefix>/etc/owl/
where <prefix> is the Python installation prefix (i.e. sys.prefix). For system
Python, <prefix> is usually "/", meaning that the OWL configuration file is
/etc/owl/owlrc.

Local customizations are stored in a olwrc.local configuration file stored in
the same <prefix>/etc/owl directory.
Each entry in owlrc.local overrides the corresponding entry in owlrc.

It is important to notice that OWL upgrades *will* overwrite the default owlrc
but leave owlrc.local untouched. Which means:

              PUT YOUR CUSTOMIZATIONS IN owlrc.local

You have been warned ;-)

A further level of customization is possible through the use of environment
variables: users can override every configuration entry by defining
environment variables called
  OWL_<uppercase group name>_<uppercase key name>
e.g.
  OWL_DIRECTORIES_PIPELINE_ROOT

There are however two entries that are not present in the configuration file
but that can be overridden via environment variables. Those are
  1. DATABASE_CONNECTION_STR
  2. DIRECTORIES_TEMPLATE_ROOT
and are provided as convenience.

Note 1: previous versions of OWL (before 1.0) stored their configuration file
inside the Python owl module directory. This is not supported anymore.

Note 2: while we support the case when a given configuration section/variable
is present in owlrc.local but not in owlrc, we do NOT support the case where
an environment variable generates a configuration section or variable not
present in either owlrc or owlrc.local.
"""
# We create names at the module level that pylint does not like.
# pylint: disable=C0103
# We manipulate the module dictionary which pylint does not understand.
# pylint: disable=E0602

# Parse the config file and specify defaults inline.
import ConfigParser
import os
import sys

from utils import db_connection_str





# Define the default values for misssing configuration parameters. The format is
# {section_name: {key: value}}
DEFAULTS = {'DATABASE': {'port': -1},
            'OWLD': {'max_msg_bytes': None, 'max_rows': None}}

config = None
prefix = sys.prefix
etc_dir = os.path.join(sys.prefix, 'etc', 'owl')
sys_config = os.path.join(etc_dir, 'owlrc')
local_config = '%s.local' % (sys_config)

# Make sure that we do not use the old config file location.
old_config = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          'etc',
                          'owlrc')
if(os.path.exists(old_config)):
    msg = 'The old configuration file scheme is not supported. ' + \
          'Please move %s to the new location %s.' % (old_config, etc_dir)
    raise(RuntimeError(msg))
del(old_config)

# Read the config files. Here we support four cases:
# 1. both sys_config and local_config exist
# 2. only sys_config exists
# 3. only local_config exists
# 4. neither file exist
# In case 1, we simply read sys_config and then override its entries with those
# in local_config.
# In case 2, we just read sys_config.
# In case 3, we error out.
# In case 4, we error out.
if(not os.path.exists(sys_config)):
    msg = 'OWL requires the configuration file %s to be present.' % (sys_config)
    raise(RuntimeError(msg))

# Load the system config. The ConfigParser.RawConfigParser class already handles
# the 3 cases above for us. Just ask it to read an ordered list of file names
# and it will override the n-th with the content of the n+1-th file.
config = ConfigParser.RawConfigParser()
config.read([sys_config, local_config])

# Fill in for the missing keys using data from the DEFAULTS dict. We do not use
# the defaults parameter in RawConfigParser() because it does not allow to
# specify per-section defaults and hence pollutes our namespace.
for section in config.sections():
    section_defs = DEFAULTS.get(section.upper(), {})
    section_keys = config.options(section)
    for key, value in section_defs.items():
        if(key not in section_keys):
            config.set(section, key, value)

# Fetch the user environment and override whatever appropriate.
env = os.environ

# Export the config values as constants for the module. Override what we read in
# the sys config file with the corresponding value in the local config and
# ultimately in the environment.
module = sys.modules[__name__]
all_vars = []
for section in config.sections():
    for option in config.options(section):
        var = '%s_%s' % (section.upper(), option.upper())
        env_var = 'OWL_' + var
        all_vars.append(var)

        config_val = config.get(section, option)
        setattr(module, var, env.get(env_var, config_val))

# Now the template root directory. We might end up putting this in the config
# file as well.
here = os.path.dirname(os.path.abspath(__file__))
DIRECTORIES_TEMPLATE_ROOT = env.get('OWL_DIRECTORIES_TEMPLATE_ROOT',
                                    os.path.join(here, 'templates'))

# Finally the database connection string.
DATABASE_CONNECTION_STR = env.get('OWL_DATABASE_CONNECTION_STR',
                                  db_connection_str(DATABASE_FLAVOUR,
                                                    DATABASE_USER,
                                                    DATABASE_PASSWORD,
                                                    DATABASE_HOST,
                                                    DATABASE_PORT,
                                                    DATABASE_DATABASE))

# Cleanup the namespace a bit. Should we just manipulate __all__ instead?
try:
    del(env)
    del(here)
    del(section)
    del(option)
    del(config)
    del(lconfig)
    del(section_defs)
    del(var)
    del(key)
    del(config_val)
    del(all_vars)
    del(section_keys)
    del(env_var)
    del(value)
except:
    pass






if __name__ == '__main__' :
    print('System configuration file: %s' % (sys_config))
    print('Local configuration file: %s' % (local_config))

    print('Active configuration:')
    key_vals = module.__dict__.items()
    key_vals.sort()
    for key, val in key_vals:
        if(key.isupper()):
            print('  %s = %s' % (key, str(val)))