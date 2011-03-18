#!/usr/bin/env python
#
#       ------------------------------------------------
#       Pipeline script to run calacs on a single exposure.
#
#       Anton M. Koekemoer, September 2003 - August 2006
#
#       Used for GOODS, UDF, COSMOS, EGS, E-CDFS, etc.
#       Modified for HLA
#       ------------------------------------------------
#
# pipeline_calacs.py:
#       - monitors the directory "RAW" for new raw.fits files
#       - for each new raw.fits file that appears, it does the following:
#               - optionally insert new reference filenames into the header
#                 (assuming we will have our own new reference files - this
#                 step is not currently activated)
#               - extract the reference filenames from the header
#               - check whether the reference files are present in the
#                 "ref/" directory, and if not then copy them from
#                 the standard directory /data/cdbs7/jref/
#               - run calacs to produce flt.fits file
#               - move the resulting flt.fits file to the directory "flt/"
#       - continue the above loop indefinitely
#
# How to copy only the WFC images from the opusdata/orig directory:
#  import glob, os
#  cd opusdata_oslo/orig
#  length_wfc_raw = 34341120L
#  files = glob.glob('*_raw.fits')
#  for rawfile in files:
#      filelength = os.stat(rawfile)[6]
#      if (filelength == length_wfc_raw):
#         print(rawfile, filelength)
#         os.system('/bin/cp -fp '+rawfile+' ../orig_wfc')
#
# mv opusdata_oslo/orig_wfc/* opusdata
#
# cd opusdata
#  files = glob.glob('*_raw.fits')
#  for rawfile in files:
#      os.system('touch ../control/pipeline_calacs_todo/'+rawfile)




import os, sys, glob, time, re, shutil, string

import pyraf, pyfits
from pyraf import iraf
from iraf import stsdas, hst_calib, acs, calacs

SOFTWARE  = os.getenv('SOFTWARE')
sys.path.insert(1,SOFTWARE)

yes = iraf.yes
no = iraf.no 




def check_reffile(rawfile, refparam):

  print('Checking for reference file: '+refparam+' ...')

  REF = os.getenv('REF')
  REF = os.getenv('jref')

  reffile = iraf.hselect(images=rawfile+'[0]',fields=refparam,expr=yes,Stdout=1)[0]

  refdir = reffile[0:4]         # extract the "jref$"

  reffile = reffile[5:]         # remove the "jref$" which is always prepended

  # Note: in order for the "scp" to work successfully, need to do the following:
  #
  # udf2:> ssh-keygen -t dsa
  #     Generating public/private dsa key pair.
  #     Enter file in which to save the key (/home/koekemoe/.ssh/id_dsa): 
  #     Enter passphrase (empty for no passphrase): 
  #     Enter same passphrase again: 
  #     Your identification has been saved in /home/koekemoe/.ssh/id_dsa.
  #     Your public key has been saved in /home/koekemoe/.ssh/id_dsa.pub.
  #     The key fingerprint is:
  #     d9:e3:06:21:96:87:ff:93:8f:43:46:d8:09:44:b1:97 koekemoe@udf2.stsci.edu
  #
  # then copy the new line from:
  #     ~koekemoe/.ssh/id_dsa.pub
  #
  # into my file on the science cluster:
  #
  #     ~koekemoe/.ssh/authorized_keys2 
  #

  if (glob.glob(REF+reffile) != []):
    print('      Reference file exists: '+REF+reffile)
  else:
    print('      Reference file does not exist: '+REF+reffile)
    print('      Copying file "'+reffile+'" to: "'+REF+'"')
    if (refdir == 'jref'):
      #print('      scp -pq tib.stsci.edu:/data/cdbs7/jref/'+reffile+' '+REF)
      #os.system('scp -pq tib.stsci.edu:/data/cdbs7/jref/'+reffile+' '+REF)
      print('      cp -fp /grp/hst/cdbs/jref/'+reffile+' '+REF)
      os.system('/bin/cp -fp /grp/hst/cdbs/jref/'+reffile+' '+REF)
      #
      # If this did not work, then try copying it from the reference directory
      # on HLA:
      #
      if (glob.glob(REF+reffile) != []):
        print('      cp -fp /hladata1/DADS/acs-ref/'+reffile+' '+REF)
        os.system('/bin/cp -fp /hladata1/DADS/acs-ref/'+reffile+' '+REF)
      #
    if (refdir == 'mtab'):
      #print('      scp -pq tib.stsci.edu:/data/cdbs2/mtab/'+reffile+' '+REF)
      #os.system('scp -pq tib.stsci.edu:/data/cdbs2/mtab/'+reffile+' '+REF)
      print('      cp -fp /grp/hst/cdbs/mtab/'+reffile+' '+REF)
      os.system('/bin/cp -fp /grp/hst/cdbs/mtab/'+reffile+' '+REF)
    if (glob.glob(REF+reffile) != []):
      print('      Reference file copied successfully: '+reffile)
    else:
      print('      Reference file NOT copied successfully: '+reffile)





def run(visit, process_id):

      CAL       = os.getenv('CAL')
      RAW       = os.getenv('RAW')
      REF       = os.getenv('REF')
      WORK      = os.getenv('WORK')

      jref      = os.getenv('jref')
      mtab      = os.getenv('mtab')
      crotacomp = os.getenv('crotacomp')
      cracscomp = os.getenv('cracscomp')

      iraf.set(jref=jref)
      iraf.set(mtab=mtab)
      iraf.set(crotacomp=crotacomp)
      iraf.set(cracscomp=cracscomp)

      hostmachine = os.getenv('hostmachine')


      print('/bin/cd '+RAW)
      os.chdir(RAW)
      rawfiles = glob.glob(visit+'*_raw.fits*')
      print(rawfiles)
      rawfiles.sort()                                       # <-- important!!!!!

      # Set up a working directory for this visit:
      #
      os.system('/bin/rm -rf '+ os.path.join(WORK, visit+'_calacs'))
      os.system('mkdir ' + os.path.join(WORK, visit+'_calacs'))
      iraf.chdir(os.path.join(WORK, visit+'_calacs'))
      os.chdir(os.path.join(WORK, visit+'_calacs'))
      
      len_rawfiles = len(rawfiles)
      if(process_id not in range(len_rawfiles)):
        print('Not enough files to process: process_id=%d but %d raw files.' \
              % (proces_id, len_rawfiles))
        sys.exit(0)
      
      
      # Now just process one single file out of rawfiles. Which one to process 
      # depends on $PROCESS_ID which is set (in our job description file) to the
      # Condor cluster process ID (i.e. 0...Queue-1).
      filename = rawfiles[process_id]

      if (filename[-3:] == '.gz'):
        rawfile = filename[:-3]
      else:
        rawfile = filename

      fileroot = rawfile[0:9]

      # Copy the rawfile (which could be either compressed or uncompressed)
      #
      cpstat = -1
      ntry   = 0
      while ((cpstat != 0) and (ntry <= 5)):
        print('/bin/cp -fp '+RAW+filename+'* '+WORK+visit+'_calacs')
        cpstat = os.system('/bin/cp -fp '+RAW+filename+'* '+WORK+visit+'_calacs')
        fileglob = glob.glob(filename+'*')
        if (fileglob == []):  cpstat = -1
        if (cpstat == 0):
          print('')
          print(fileglob)
          print('Copied successfully:  '+filename+'\n')
        else:
          print('')
          print(fileglob)
          print('Problem copying:  '+filename+' ; trying again')
          ntry = ntry + 1
          if (ntry >=5): print('Problems 5 times in a row; giving up!\n')

      # Uncompress if necessary (user "-f" just in case the output exists):
      #
      if (filename[-3:] == '.gz'):
        print('\n/bin/gunzip -f '+filename)
        os.system('/bin/gunzip -f '+filename)
        if (glob.glob(rawfile) == []):
          print('Problem with gunzipping the raw file...')


      # Change the "DRIZCORR" keyword to "OMIT"
      #
      iraf.hedit(images=rawfile+'[0]', fields='DRIZCORR', value='OMIT',
        add=no, addonly=no, delete=no, verify=no, show=yes, update=yes)

      # Change the "PHOTCORR" keyword to "OMIT"
      # -- for some reason it currently crashes, saying:
      #               IRAF fatal error: segmentation violation (501)
      #               Aborting...
      #
      # This is  fixed by setting the "crotacomp" and "cracscomp" environment
      # variables correctly.
      #
      # Also need to check that crrefer is set correctly in the login.cl file;
      # if it is not set explicitly, then it will likely point to some system
      # directory which may well be out of date and does not have the most
      # up to date files.
      #
      # Sometimes even this doesn't work - then the best approach is to simply
      # copy the current versions of these directories:
      #
      #               tib:/data/cdbs2/comp/
      #                                       acs/
      #                                       ota/
      #
      #iraf.hedit(images=rawfile+'[0]', fields='PHOTCORR', value='OMIT',
      #       add=no, addonly=no, delete=no, verify=no, show=yes, update=yes)


      # Change the "ATODCORR" keyword to "OMIT" (usually it should already be)
      #
      # (Not sure why this needs to be done; better comment it out, since
      # this keyword will *always* be set to "OMIT" for COSMOS data)
      #
      #iraf.hedit(images=rawfile+'[0]', fields='ATODCORR', value='OMIT',
      #       add=no, addonly=no, delete=no, verify=no, show=yes, update=yes)


      # Update the "IDCTAB" keyword to point to the current one.
      #
      # As of Jan 2005: this is no longer necessary, particularly if I have
      # re-retrieved the files from the archive, in which case they will now
      # be populated with the correct IDCTAB keyword.
      #
      # So, comment this out.
      #
      #iraf.hedit(images=rawfile+'[0]', fields='IDCTAB', value=idctab,
      #       add=no, addonly=no, delete=no, verify=no, show=yes, update=yes)


      # Update the "BIASFILE" keyword - make use of Doug van Orsow's new biases
      # (as of 1 Apr 2003). NOTE: they need to be present in the REF directory
      # in order for this to work.
      #
      # This whole section is too manually hardcoded, but it is necessary since
      # we do not have these optimal biases in the pipeline.
      #
      # As of Jan 2005: this whole section is also no longer needed as we now
      # have the best biases in the pipeline.
      #
      #
      #dateobs = iraf.hselect(rawfile+'[0]',fields='DATE-OBS',expr=yes,Stdout=1)[0]
      #datestr = int(dateobs[0:4]+dateobs[5:7]+dateobs[8:10])
      #biasfile = ''
      #if (datestr >= 20031012 and datestr <= 20031018):  biasfile = 'jref$wfc_bias_20031012-18.fits'
      #if (datestr >= 20031019 and datestr <= 20031024):  biasfile = 'jref$wfc_bias_20031019-24.fits'
      #if (datestr >= 20031121 and datestr <= 20031125):  biasfile = 'jref$wfc_bias_20031121-25.fits'
      #if biasfile != '':
      #  iraf.hedit(images=rawfile+'[0]', fields='BIASFILE', value=biasfile,
      #         add=no, addonly=no, delete=no, verify=no, show=yes, update=yes)


      # Check whether all the required reference files are present in the
      # local directory. Don't do this on the SunFire system, since I am
      # unable to access the /data/cdbs7 directory.
      #
      # Also don't do it on the cosmos cluster, since again I can't see the
      # cdbs directory. Instead will just assume that all the files are there,
      # and if they are not then I manually copy them over with scp
      #
      # However, do it on the UDF cluster, sinc eI have copied over the entire
      # ~100Gb of reffiles from tib onto the udf cluster.
      #
      print('\nStart checking reference files\n')
      check_reffile(rawfile, 'BPIXTAB')
      check_reffile(rawfile, 'CCDTAB')
      check_reffile(rawfile, 'ATODTAB')
      check_reffile(rawfile, 'OSCNTAB')
      check_reffile(rawfile, 'BIASFILE')
      check_reffile(rawfile, 'CRREJTAB')
      check_reffile(rawfile, 'SHADFILE')
      check_reffile(rawfile, 'DARKFILE')
      check_reffile(rawfile, 'PFLTFILE')
      check_reffile(rawfile, 'GRAPHTAB')
      check_reffile(rawfile, 'COMPTAB')
      check_reffile(rawfile, 'IDCTAB')
      print('\nFinished checking reference files\n')


      # Correct the WCS (CD matrix and CRVALs) to include the VAFACTOR
      # since I'll be using wdrizzle most of the way (not pydrizzle, which
      # calls drizzle).
      #
      # Safety check: only run upinwcs if it has not already been run
      #
      # As of Jan 2005: again, this is no longer necessary since multidrizzle
      # includes a call to upinwcs before it does anything else.
      #
      # So, comment this out.
      #
      #wcs_done = fileutil.getKeyword(rawfile+'[sci,1]','OCD1_1')
      #if wcs_done == None:
      #  print('\nupinwcs has not yet been run on this image: running it..')
      #  print('\nupinwcs.run(image="'+rawfile+'")\n')
      #  upinwcs.run(image=rawfile)
      #else:
      #  print('\nupinwcs has already been run on this image: skipping it..')


      # Now run calacs:
      #
      #iraf.calacs(input=rawfile)
      print calacs.run(input=rawfile)

      fltfile = fileroot+'_flt.fits'
      trafile = fileroot+'.tra'

      # Move the calibrated and trailer file:
      # (first remove old ones, which could possibly be gzipped).
      #
      # Also gzip the fltfile (to save space) (use "-f" in case output exists)
      #
      os.system('/bin/gzip -f --best '+fltfile)
      #
      os.system('/bin/rm -f '+CAL+fltfile+'*')
      os.system('/bin/rm -f '+CAL+trafile+'*')
      #
      err = os.system('/bin/mv -f '+WORK+visit+'_calacs/'+fltfile+'* '+CAL)
      if err != 0:
          print "flt file " + fltfile + " not produced...exiting"
          sys.exit(102)
      os.system('/bin/mv -f '+WORK+visit+'_calacs/'+trafile+'* '+CAL)
      #
      os.system('chmod go+r '+CAL+visit+'*')


      # Finally, gzip the raw file in "RAW" if it is not already compressed:
      #
###      if filename[-2:] != 'gz': os.system('/bin/gzip --best '+RAW+filename)








if __name__ == '__main__':
  #
  # This script is invoked as follows:
  #
  #     drz_astrom.py visit
  #
  # where 'visit' is, for example, 'j91k10'

  #print(sys.argv)

  #visit = sys.argv[1]
  visit  = os.environ ['OSF_DATASET']
  process_id = int(os.environ['PROCESS_ID'])

  print('\n------------------------')
  print(time.ctime())
  print('------------------------\n')
  print('\n**********\n')
  print('calacs: calibrating all files for visit number: '+visit)
  print('\n**********\n')

  run(visit, process_id)

  print('\n**********\n')
  print('calacs: completed')
  print('\n**********\n')
  print('\n------------------------')
  print(time.ctime())
  print('------------------------\n')

