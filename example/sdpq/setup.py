#!/usr/bin/env python
from distutils.core import setup
import glob




if __name__ == "__main__":
    SCRIPTS = glob.glob('bin/*.py')


    setup(name = 'sdpq2',
          description = "HST/JWST Science Data processing Queue",
          author = "Francesco Pierfederici",
          author_email = "fpierfed@stsci.edu",
          license = "BSD",
          version='2.0',

          scripts=SCRIPTS,
          packages=['sdpq2', 'sdpq2.plugins', ],
          package_dir={'sdpq2': 'src'},
)

