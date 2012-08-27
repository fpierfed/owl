#!/usr/bin/env python
from distutils.core import setup
import glob
import os
import sys






if __name__ == "__main__":
    SCRIPTS = glob.glob('bin/*.py')



    from src import __version__



    setup(name = 'owl_client',
          description = "Standalone package for interacting with OWL.",
          author = "Francesco Pierfederici",
          author_email = "fpierfed@stsci.edu",
          license = "BSD",
          version=__version__,

          scripts=SCRIPTS,
          packages=['owl_client', ],
          package_dir={'owl_client': 'src'},
    )


