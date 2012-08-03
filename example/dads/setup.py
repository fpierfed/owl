#!/usr/bin/env python
from distutils.core import setup
import glob




if __name__ == "__main__":
    SCRIPTS = glob.glob('bin/*.py')


    setup(name = 'dads',
          description = "HST/JWST Science Data processing Queue",
          author = "Francesco Pierfederici",
          author_email = "fpierfed@stsci.edu",
          license = "BSD",
          version='0.1',

          scripts=SCRIPTS,
          packages=['dads', ],
          package_dir={'dads': 'src'},
)

