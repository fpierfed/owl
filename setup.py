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

    # Create a owlrc.local from owlrc if the user does not have one already and
    # an owlrc.local is not in our src etc directory (which would mean that the
    # user has created a custon distribution since we do not put one there).
    # We do this so that configurations have a change of surviving an upgrade.
    src_owlrc = os.path.join(src, 'owlrc')
    src_owlrc_local = os.path.join(src, 'owlrc.local')
    dst_owlrc_local = os.path.join(dst, 'owlrc.local')
    if(not os.path.exists(src_owlrc_local) and
       not os.path.exists(dst_owlrc_local)):
        # Not there, not copied. Make one.
        shutil.copy(src_owlrc, dst_owlrc_local)


    setup(name = 'owl',
          description = "Governance according to good laws",
          author = "Francesco Pierfederici",
          author_email = "fpierfed@stsci.edu",
          license = "BSD",
          version='0.1',

          scripts=SCRIPTS,
          packages=['owl', 'owl.plugins', ],
          package_dir={'owl': 'src'},
          package_data={'owl': ['templates/*/*/*', ]},
    )

    print('Installed OWL configuration file(s) in %s/' % (dst))



