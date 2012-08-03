#!/usr/bin/env python
import ConfigParser




config = ConfigParser.RawConfigParser()

config.add_section('Database')
config.set('Database', 'host',
                       'localhost')
config.set('Database', 'port',
                       '')
config.set('Database', 'user',
                       'www')
config.set('Database', 'password',
                       'zxcvbnm')

config.add_section('Directories')
config.set('Directories', 'pipeline_root',
                          '/hstdev/project/condor_bcw/bcw/python')
config.set('Directories', 'data_root',
                          '/hstdev/project/condor_bcw/new_repository')

# Writing our configuration file to 'example.cfg'
configfile = open('example.cfg', 'wb')
config.write(configfile)
configfile.close()





