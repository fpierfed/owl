#!/usr/bin/env python
from distutils.core import setup
import glob
import os
import shutil
import sys




def _make_safe_backup(file_name, maxn=100):
    """
    Given a file name with full path `file_name`, rename it to
        `file_name`.bakN
    where N is 0 or a positive integer such that so existing file in the same
    directory is clobbered. Give up after `maxn` tries.

    Return whether or not a safe backup was created.
    """
    for i in range(maxn):
        new_name = '%s.bak%d' % (file_name, i)
        if(os.path.exists(new_name)):
            continue
        shutil.move(file_name, new_name)
        return(True)
    return(False)



if __name__ == "__main__":
    SCRIPTS = glob.glob('bin/*.py')

    # Copy ./etc to sys.prefix/etc/owl.
    src = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etc')
    dst = os.path.join(sys.prefix, 'etc', 'owl')
    if(os.path.exists(dst)):
        # No need to create it: just copy everything in src to dst.
        for file_name in os.listdir(src):
            dst_file_name = os.path.join(dst, file_name)
            if(os.path.exists(dst_file_name)):
                # Move it out of the way.
                ok = _make_safe_backup(dst_file_name)
                if(not ok):
                    msg = 'Unable to safely create %s.bakN in %s. Giving up.' \
                          % (file_name, dst)
                    raise(RuntimeError(msg))
            shutil.copy(os.path.join(src, file_name), dst)
    else:
        # Copy the whole directory tree.
        shutil.copytree(src, dst)


    from src import __version__



    setup(name = 'owl',
          description = "Governance according to good laws",
          author = "Francesco Pierfederici",
          author_email = "fpierfed@stsci.edu",
          license = "BSD",
          version=__version__,

          scripts=SCRIPTS,
          packages=['owl', 'owl.plugins', ],
          package_dir={'owl': 'src'},
          package_data={'owl': ['templates/*/*/*', ]},
    )


# Understand where the scripts were installed. Python has a special treatment of
# Darwin/Mac OS X (see distutils/install.py:install.finalize_options())
bin_dir = os.path.join(sys.prefix, 'bin'),
if(sys.platform == 'darwin'):
    from sysconfig import get_config_var
    bin_dir = get_config_var('BINDIR')


print("""


OWL %(version)s


The full OWL distribution was installed (by default) under
    %(install_dir)s/

The OWL command-line tools are (by default) available under
    %(bin_dir)s/

Configuration file(s) are (by default) available under
    %(config_dir)s/

Please customize the default configuration by creating an owlrc.local file in
the same directory (if you haven't done that already). This installation did not
modify any pre-existing owlrc.local but archived any pre-existing owlrc file
before writing the new one.
""") % {'config_dir': dst,
        'install_dir': sys.prefix,
        'bin_dir': bin_dir,
        'version': __version__}



