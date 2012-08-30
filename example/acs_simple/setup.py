#!/usr/bin/env python
from distutils.core import setup
import glob
import os
import sys





if __name__ == "__main__":
    SCRIPTS = glob.glob('bin/*.py')
    from owl import __version__


    setup(name = 'acs_simple_pipeline',
          description = "Simpe ACS association pipeline.",
          author = "Francesco Pierfederici",
          author_email = "fpierfed@stsci.edu",
          license = "BSD",
          version=__version__,

          scripts=SCRIPTS,
    )


