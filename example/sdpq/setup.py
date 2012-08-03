#!/usr/bin/env python
from distutils.core import setup
import glob




if __name__ == "__main__":
    SCRIPTS = glob.glob('bin/*.py')


    setup(name = 'sdpq',
          description = "HST/JWST Science Data processing Queue",
          author = "Francesco Pierfederici",
          author_email = "fpierfed@stsci.edu",
          license = "BSD",
          version='2.0',

          scripts=SCRIPTS,
          packages=['sdpq', 'sdpq.plugins', ],
          package_dir={'sdpq': 'src'},
)

