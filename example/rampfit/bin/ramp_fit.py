#! /usr/bin/env python
#
#  ramp_fit.py - calculate weighted mean of slope, based on Massimo
#                Robberto's "On the Optimal Strategy to fit MULTIACCUM
#                ramps in the presence of cosmic rays."
#                (JWST-STScI-0001490,SM-12; 07/25/08).   The derivation
#                is a generalization for >1 cosmic rays, calculating
#                the slope and variance of the slope for each section
#                of the ramp (in between cosmic rays).
#
from __future__ import division
import sys
import os
import time
import numpy as np
import pyfits
   
SIG_FACTOR = 4.0  # sigma factor used in threshold
BUFSIZE = 1024*3000   # 3Mb cache size for data section
MIN_ERR = 1.E-10 # minimum allowed value for error cube 

def calc_slope( data_section, inttime, adc_gain): 
    """
    Extended Summary
    ---------
    Calculate the count rate for each pixel in the data cube section
    for the current integration, equal to the weighted slope for all
    sections (intervals between cosmic rays) of the pixel's ramp.

    Parameters
    ---------    
    data_section: float
       section of input data cube array 

    inttime: float
       integration time 

    Returns
    ---------

    any_cr: int, 2d array
       0 if there are no CRs in pixel, 1 otherwise 
      
    cr_total: int, 2d array
       total number of CRs in each pixel in 2d array

    cr_cube: int, 3d array 
       total number of CRs in each pixel in 3d cube

    m_by_var: float, 1d array
       values of slope/variance for good pixels

    one_by_var: float, 1d array
       values of 1/variance for good pixels
    
    """ 
    nreads, asize2, asize1 = data_section.shape   
    npix = asize2*asize1  # number of pixels in section of 2d array 
    imshape = data_section.shape[-2:]      
    cubeshape = (nreads,)+imshape  # cube section shape    

    all_pix = np.arange( npix )   

    arange_nreads_col = np.arange( nreads )[:,np.newaxis ]

    cr_flag_2d = np.zeros(( nreads, npix), dtype = np.int32)    

    cos_rays = np.zeros( imshape, dtype = np.uint32) 
    any_cr = np.zeros(imshape, dtype = np.uint32) 

    start = np.zeros( npix, dtype = np.int32)  # lowest channel in fit.

    # Highest channel in fit initialized to last read
    end = np.zeros( npix, dtype = np.int32) + (nreads-1)    

    pixel_done = (end < 0) # False until processing is done

    # Weighted average for slopes, equal to sum(m/v)/sum(1/v)
    #    for the pixels in which the variance v is nonzero
    one_by_var = np.zeros( npix, dtype = np.float64) 
    m_by_var = np.zeros( npix, dtype = np.float64) 

    bad_var = np.zeros( npix, dtype = np.int32) 

    # End stack array - endpoints for each pixel
    # initialize with nreads for each pixel; set 0th channel to 0
    end_st = np.zeros((nreads+1, npix), dtype = np.int32)
    end_st[0,:] = nreads-1  

    end_heads = np.ones( npix, dtype = np.int32) # vector for end stack

    # Create nominal 2d ERR array, which is 0th slice of
    #    avged_data_cube * readtime (of 10.6 seconds)
    err_2d_array = data_section[0,:,:] * inttime 
    err_2d_array[ err_2d_array <= 0.0 ] = MIN_ERR # for pixels having no signal

    read_noise = 10./adc_gain  # in electrons

    thresh = ( SIG_FACTOR*np.sqrt((read_noise*read_noise +
                err_2d_array * adc_gain)/adc_gain)).ravel(0) 

    # frames >= start and <= end will be included in fit
    mask_2d = (( arange_nreads_col >= start[np.newaxis,:]) & 
            (arange_nreads_col <=end[np.newaxis,:] ))    
    end = 0 

    # LS fit until 'nreads' iterations or all pixels in
    #    section have been processed 
    for iter_num in range( nreads ):  
        if  pixel_done.all():
            break

        # for all pixels, update arrays, summing slope and variance
        var_den_0 = fit_next_segment( start, end_st, end_heads, thresh,
             pixel_done, cr_flag_2d, data_section, mask_2d, one_by_var, m_by_var)

        if ( var_den_0 != None ):  
           bad_var[ var_den_0[0] ] += 4  # 'bad detector pixel'
           
        # frames >= start and <= end_st will be included in fit
        mask_2d = ((arange_nreads_col >= start) & 
                   (arange_nreads_col <
                   (end_st[ end_heads[all_pix]-1, all_pix]+1 )))

    arange_nreads_col = 0    
    all_pix = 0 

    cr_cube = cr_flag_2d.reshape( cubeshape)

    # set locations where cr was found to have 4096 added to them in dq_array
    dq_array = np.zeros( cubeshape, dtype = np.int16)
    dq_array[ cr_cube >0 ] += 4096 

    bad_var_2d = bad_var.reshape( imshape )
    err_array = np.zeros( cubeshape, dtype = np.float32)

    # for now, making all slices within an integration identical
    for ii in range(cubeshape[0]): 
        err_array[ ii,:,: ]= err_2d_array
        dq_array[ ii,:,: ] += bad_var_2d 
    
    cr_total = cr_cube.sum( axis=0 ) 
    any_cr = np.where( cr_total > 0, 1, 0 )

    return any_cr, cr_total, err_array, dq_array, cr_cube, m_by_var, \
           one_by_var, bad_var 


def fit_next_segment( start, end_st, end_heads, thresh, pixel_done,
                      cr_flag_2d, data_section, mask_2d, one_by_var,
                      m_by_var ):     
    """
    Extended Summary
    ---------
    Call routine to LS fit masked data for all pixels in data section. Then
    categorize each pixel's fitting interval based on interval length,
    threshold, and whether the interval is at the end of the array.
    Udate the start array, the end stack array, the end_heads array
    which contains the number of endpoints. For pixels in which the fit
    revealed no new cosmic rays, the resulting slope and variance are added
    to the appropriate stack arrays.


    Parameters
    ---------
    start: int, 1d array
       lowest channel in fit

    end_st: int, 2d array
       stack array of endpoints 

    end_heads: int, 1d array 
       number of endpoints for each pixel
   
    thresh: float, 1d array
       threshold for each pixel

    pixel_done: boolean, 1d array 
       whether each pixel's calculations are completed

    cr_flag_2d: int, 2d arry
       channels of cosmic rays for each pixel

    data_section: float, array
       data cube section
   
    mask_2d: int, 2d array
       delineates which channels to fit for each pixel

    Returns
    ---------
    var_den_0: indices, 1d array
       delineates which channels have an invalid (NaN) value for the variance 

    """

    nreads, asize2, asize1 = data_section.shape    
    all_pix = np.arange( asize2*asize1 )    

    slope, variance, resid, var_den_0 = fit_lines( data_section, mask_2d,
        end_heads, end_st, start)  
         
    #  calculate the differences of the residuals for each pixel
    resid_diff = resid[1:,:] - resid[:-1,:]   

    # get the index and value for the max resid_diff for all pixels
    ind_max_per_pixel = resid_diff.argmax( axis = 0 )
    max_rd_all_pix = resid_diff.max( axis = 0 ) 

    end_locs = end_st[ end_heads[ all_pix ]-1, all_pix ] 
    l_interval = end_locs - start # fitting interval length
    
    wh_done = ( start == -1)  
    l_interval[ wh_done ] = 0  # set interval lengths for done pixels to 0 

    # CASE 1 - interval too short to fit well, not at array end (any thresh )
    #    set start to 1 beyond end of current interval
    #    remove current end from end stack
    #    decrement number of ends     
    wh_check = np.where(( l_interval <= 2) & (end_locs != nreads-1))
    
    if( len( wh_check[0]) > 0 ):
        these_pix = wh_check[0] 
        start[ these_pix ] = end_locs[ these_pix ] + 1     
        end_st[ end_heads[ these_pix ]-1, these_pix ] = 0 
        end_heads[ these_pix ] -= 1  

        wh_neg = ( end_heads < 0. )    
        end_heads[ wh_neg ] = 0. 

    # CASE 2 - interval too short to fit well, at end of array
    #    set start to -1 to designate all fitting done
    #    remove current end from end stack
    #    set number of ends to 0     
    #    set pixel_done to True to designate all fitting done
    wh_check = np.where(( l_interval <= 2) & (end_locs == nreads-1))
    
    if( len( wh_check[0]) > 0 ):
        these_pix = wh_check[0] 
        start[ these_pix ] = -1
        end_st[ end_heads[ these_pix ]-1, these_pix ] = 0 
        end_heads[ these_pix ] = 0
        pixel_done[ these_pix ] = True

    # CASE 3 - interval long enough, not at end of array, below thresh 
    #    remove current end from end stack
    #    decrement number of ends     
    #    add slopes and variances to running sums
    wh_check = np.where(( l_interval > 2) & (end_locs != nreads-1) & 
                        (max_rd_all_pix <= thresh ))   

    if( len(wh_check[0]) > 0 ):        
        these_pix = wh_check[0]          
        start[ these_pix ] = end_locs[ these_pix ] + 1        
        end_st[end_heads[ these_pix ]-1, these_pix ] = 0 
        end_heads[ these_pix ] -= 1
        wh_neg = ( end_heads < 0. )
        end_heads[ wh_neg ] = 0. 
        
        good_pix = these_pix[ variance[these_pix] > 0. ] 
        if ( len(good_pix ) > 0 ):
            one_by_var[ good_pix ] += 1.0/variance[ good_pix ]
            m_by_var[ good_pix ] += slope[ good_pix ]/variance[ good_pix ]

    # CASE 4 - interval long enough, at end of array, below thresh
    #    set start to -1 to designate all fitting done
    #    remove current end from end stack
    #    set number of ends to 0    
    #    add slopes and variances to running sums
    wh_check = np.where(( l_interval > 2) & (end_locs == nreads-1) & 
                        (max_rd_all_pix <= thresh )) 

    if( len( wh_check[0]) > 0 ):
        these_pix = wh_check[0]   
        start[ these_pix ] = -1   # all processing for this pixel is completed
        end_st[ end_heads[ these_pix ]-1, these_pix ] = 0 
        end_heads[ these_pix ] = 0
        pixel_done[ these_pix ] = True # all processing for pixel is completed
        
        good_pix = these_pix[ variance[these_pix] > 0. ] 
        if ( len(good_pix ) > 0 ):
            one_by_var[ good_pix ] += 1.0/variance[ good_pix ]
            m_by_var[ good_pix ] += slope[ good_pix ]/variance[ good_pix ]
        
    # CASE 5 - interval long enough, above thresh, at or not at end
    #    set new end at location of largest difference of residuals (new CR) 
    #    increment number of ends     
    #    add location of this newly found CR to cr_flag_2d
    wh_check = np.where(( l_interval > 2) & (max_rd_all_pix > thresh )) 
    if( len( wh_check[0]) > 0 ):
        these_pix = wh_check[0]         
        end_st[ end_heads[ these_pix ], these_pix ] = \
                ind_max_per_pixel[ these_pix ] 
        end_heads[ these_pix ] += 1  
        cr_amp = end_st[ end_heads[these_pix]-1,these_pix] + 1  
        cr_flag_2d[[cr_amp], [these_pix] ] = 1

    return var_den_0 


def open_input_cube( infile ):
    """
    Short Summary
    ---------
    Try to open input file.

    Parameters
    ---------    
    infile: str
       name of input data cube

    Returns
    ---------
    fh: file handle
       handle for input dataset

       
    """

    try: 
      fh = pyfits.open(infile, do_not_scale_image_data = True) 
    except Exception, errmess:
      print 'FATAL ERROR : unable to open data cube '  

    return fh


def get_dataset_info( fh ):
    """
    Extended Summary
    ---------
    Calculate values for the number of pixels, number of bins, and shapes. 

    Parameters
    ---------
    fh: file handle
       handle for input dataset


    Returns
    ---------
    nreads: int
       number of reads in input dataset

    npix: int
       number of pixels in 2d array 

    imshape: (int, int) tuple
       shape of 2d image

    cubeshape: (int, int, int) tuple
       shape of input dataset

    n_int: int
       number of integrations

    instrume: strin
       instrument

    """

    inhdr = fh[0].header 

    try:
       instrume = inhdr['INSTRUME']
    except Exception, errmess: 
       instrume = None

    try: # get integration time
       inttime = inhdr['INTTIME']
    except Exception, errmess: 
       inttime = 1.0 # when only a single integration        
    if (not is_number(inttime)): # ensure a float
        inttime = 1.0

    try: # get integration time/group (if needed later)
       intgrtim = inhdr['INTGRTIM']
    except Exception, errmess: 
       intgrtim = None

    scihdr = fh['SCI'].header

    nreads = scihdr['NAXIS3']  # number of groups per integration
    asize2 = scihdr['NAXIS2']
    asize1 = scihdr['NAXIS1']

    try:  
      n_int = scihdr['NAXIS4']  # number of integrations
    except Exception, errmess:
      n_int = 1 
  
    npix = asize2*asize1  # number of pixels in 2d array  
    imshape = (asize2, asize1)
    cubeshape = (nreads,)+imshape     

    return  nreads, npix, imshape, cubeshape, n_int, instrume, inttime, intgrtim


def is_number( val ):
    """
    Short Summary
    ---------
    Check that input is a number and not a string

    Parameters
    ---------
    val: input
       numerical value or (empty) string

    Returns
    ---------
    True/False: boolean
       True if val is numerical, otherwise False
    
    """
    try:
        float( val )
        return True
    except ValueError, TypeError:
        return False

def fit_lines ( data, mask_2d, end_heads, end_st, start):
    
    """
    Extended Summary
    ---------
    Do linear least squares fit to data cube in this integration.  This
    function will later be generalized to accept input data of arbitrary
    dimensionality. In addition to applying the mask due to identified
    cosmic rays, the data is also masked to exclude intervals that are
    too short to fit well.

    Parameters
    ---------
    data: float        
       array of values for current data section

    mask_2d: boolean, 2d array 
       delineates which channels to fit for each pixel

    end_heads: int, 1d array
       number of endpoints for each pixel

    end_st: int, 2d array
       stack array of endpoints 

    start: int, 1d array
       lowest channel in fit


    Returns
    ---------
    full_slope: float, 1d array 
       weighted slope for current iteration's pixels for data section

    full_variance: float, 1d array
       variance of residuals for fit for data section

    full_r_mask: boolean, 2d array
       delineates which channels were fit for each pixel, incorporating
       interval length  

    denom_bad: indices, 1d array
       delineates which channels have an invalid (NaN) value for the
       variance

    """

    # verify that incoming data is either 2 or 3-dimensional
    try:
        assert ( data.ndim == 2 or data.ndim == 3 )
    except AssertionError:
        print 'FATAL ERROR: Data input to fit_lines must be 2 or 3 dimensions' 

    nreads = mask_2d.astype(np.int16).sum(axis=0) # number per pixel
    npix = mask_2d.shape[1] 

    full_slope = np.zeros( npix, dtype = np.float64)
    full_variance = np.zeros( npix, dtype = np.float64)
    full_r_mask = np.zeros( (data.shape[0],npix), dtype = np.float64)
    
    all_pix = np.arange( npix )

    # set final channel for each pixel for interval
    end_locs = end_st[ end_heads[ all_pix ]-1, all_pix ]  

    l_interval = end_locs - start # fitting interval length
    all_pix = 0
    end_locs = 0

    # ignore pixels whose fitting length < 3, and mask all slices are False
    mask_2d[:,l_interval <3] = False    
    wh_pix_to_use = np.where(mask_2d.sum(axis = 0) != 0)  

    good_pix = wh_pix_to_use[0] 

    l_interval = 0

    # reshape data_masked and eliminate pixels with too-short intervals
    data_masked = data * np.reshape(mask_2d, data.shape) 
    data_masked = np.reshape(data_masked,(data_masked.shape[0], npix))
    data_masked = data_masked[:, good_pix ]    

    xvalues = np.arange(data_masked.shape[0])[:,np.newaxis] * mask_2d  # all
    xvalues = xvalues[:, good_pix]  # set to those pixels to be used 

    sumx = xvalues.sum( axis=0 ) 
    sumxx = (xvalues**2).sum( axis=0 ) 
    sumy = (np.reshape(data_masked.sum( axis=0 ), sumx.shape))   
    sumxy =  (xvalues * np.reshape(data_masked,xvalues.shape)).sum(axis=0)   

    nreads = nreads[ good_pix ]  
    denominator = nreads * sumxx - sumx**2
    
    slope = (nreads * sumxy - sumx * sumy)/denominator    
    intercept = (sumxx * sumy - sumx * sumxy)/denominator
    denominator = 0
    line_fit = (slope * xvalues) + intercept
    xvalues = 0
    intercept = 0 

    # eliminate all mask pixels that are false for all slices
    mask_2d = mask_2d.compress( mask_2d.sum(axis = 0) >0., axis = 1)

    # create residual mask 
    r_mask = (data_masked.reshape(line_fit.shape) - line_fit) * mask_2d  

    line_fit = 0 
    r_mask_2 = r_mask**2.

    # check variance residual denominator to prevent NaN propagation
    v_numer = r_mask_2.sum(axis=0)**2.-(r_mask_2**2.).sum(axis=0) 
    v_denom = ((r_mask_2 != 0.).astype(np.float64).sum(axis=0))

    # where denom=0, remove these locations from arrays
    denom_bad = None 
    if (( v_denom == 0.0).any()):
           denom_bad = np.where( v_denom == 0. )
           denom_ok= np.where( v_denom != 0.)

           v_denom = v_denom[ denom_ok ]
           v_numer = v_numer[ denom_ok ]
           slope = slope[ denom_ok ]
           r_mask = r_mask[:, denom_ok[0]] 
           good_pix = good_pix[ denom_ok[0] ]  

    variance = v_numer/v_denom # variance of residuals for good pixels only

    full_slope[ good_pix ] = slope
    full_variance[ good_pix ] = variance
    full_r_mask[:, good_pix ] = r_mask

    return full_slope, full_variance, full_r_mask, denom_bad



def calc_nrows( fh, buffsize, cubeshape, nreads):
    """ 
    Short Summary
    ---------
    Calculate the number of rows per data section to process


    Parameters
    ---------
    fh: file handle
       handle for input dataset

    buffsize: int
       size of data section (buffer) in bytes

    cubeshape: (int, int, int) tuple
       shape of input dataset

    nreads: int
       number of reads in input dataset

    Returns
    ---------
    nrows: int
       number of rows in buffer
       
    """

    bitpix = fh[0].header.get('BITPIX')
    bytepix = int( abs(bitpix)/8 )
    nrows = int( buffsize/(bytepix * cubeshape[2] * nreads) )
    if nrows < 1: nrows = 1
    if nrows > cubeshape[1]: nrows = cubeshape[1]

    return nrows

def write_all( infile, any_cr, slopes, cr_total, cr_cube, err_data, dq_data ):
    """
    Extended Summary
    ---------
    Write arrays to files:
      'anycr_<basename>.fits': 0 if there are no cosmic rays 
                                   along stack, 1 otherwise 
      'slopes_<basename>.fits': calculated count rates for each pixel 
      'num_cr_<basename>.fits': number of CRs along stack for each pixel
      'found_cr_<basename>.fits': cube of detected cosmic rays ( 0 for no CR,
                                 1 for at least 1 CR) 

       ... where basename is the section of input file name past the
            rightmost '/'

    Parameters
    ---------    
    infile: str
       input file name

    any_cr: int, 2d array
       0 if there are no CRs in pixel, 1 otherwise 

    slopes: float, 2d array
       weighted count rate
      
    cr_total: int, 2d array
       total number of CRs in each pixel in 2d array

    cr_cube: int, 3d array 
       total number of CRs in each pixel in 3d cube

    err_data: float, 4d array
       estimated error from slope calculation

    dq_data: int, 4d array
       data quality array - 4096 added where CRs found

    Returns
    ---------
    
    """
    basename = infile.split("/")[-1]
    write_file( any_cr, err_data, dq_data, "any_cr_" + basename )
    write_file( slopes.astype(np.float32), err_data, dq_data, "slopes_" + basename)
    write_file( cr_total, err_data, dq_data, "num_cr_" + basename )
    write_file( cr_cube, err_data, dq_data, "found_cr_" + basename )


def write_file( sci_data, err_data, dq_data, output_fname):

    """
    Short Summary
    ---------
    Write data to specified file.


    Parameters
    ---------    
    data: float        
       array of values

    output_fname: str
       name of output file

    Returns
    ---------

    """    
    try:
       os.remove (output_fname)
    except:
       pass
   
    fitsobj = pyfits.HDUList()
    hdu = pyfits.PrimaryHDU()
    prihdr = hdu.header
    fitsobj.append( hdu )

    sci_hdu = pyfits.ImageHDU(name='SCI')
    sci_hdu.data = sci_data
    fitsobj.append( sci_hdu )

    err_hdu = pyfits.ImageHDU(name='ERR')
    err_hdu.data = err_data
    fitsobj.append( err_hdu )

    dq_hdu = pyfits.ImageHDU(name='DQ')
    dq_hdu.data = dq_data
    fitsobj.append( dq_hdu )
    
    fitsobj.writeto( output_fname )
    fitsobj.close()


def ramp_fit( fname, buffsize, verb ):
        """  
        Extended Summary
        ---------
        Calculate the count rate for each pixel in all data cube sections
        and all integrations ,equal to the weighted slope for all sections
        (intervals between cosmic rays) of the pixel's ramp. 

        Parameters
        ---------    
        fname: string
           name of input file 

        buffsize: int
           size of data section (buffer) in bytes

        verb: int
           level of verbosity
           

        Returns
        ---------

        """
        fh = open_input_cube( fname )

        # get needed sizes and shapes
        nreads, npix, imshape, cubeshape, n_int, instrume, inttime, intgrtim = \
                get_dataset_info( fh )  

        any_cr = np.zeros( imshape, dtype = np.uint32 )
        slopes = np.zeros( imshape, dtype = np.float32 )        
        cr_total = np.zeros( imshape, dtype = np.uint32 )
        cr_cube = np.zeros( (n_int,) + cubeshape, dtype = np.uint8 )  

        try:
           dq_cube = fh['DQ'].data #  copy dq info from input
        except Exception, errmess:
           print ' Fatal ERROR: no dq array in input. '

        try:
           err_cube = fh['ERR'].data #  copy err info from input
        except Exception, errmess:
           print ' Fatal ERROR: no err array in input. '

        # calculate number of (contiguous) rows per data section
        nrows = calc_nrows( fh, buffsize, cubeshape, nreads )
        
        slope_wtd = np.zeros( imshape, dtype = np.float64)    

        m_sum_2d = np.zeros( imshape, dtype = np.float64 )   
        var_sum_2d = np.zeros( imshape, dtype = np.float64 ) 

        if ( instrume != 'MIRI'): # refine later
            skip_f = 0
            adc_gain = 2.2 # electrons/DN
        elif ( instrume != 'NIRCAM'):
            skip_f = 0
            adc_gain = 1.3
        else:   # NIRSPEC
            skip_f = 0
            adc_gain = 1.3
            
        # loop over data integrations
        for num_int in range ( 0, n_int):
 
              # loop over data sections
           for rlo in range ( 0, cubeshape[1], nrows): 
              rhi = rlo + nrows
              if ( rhi > cubeshape[1] ):
                 rhi = cubeshape[1] 

              data_section = fh['SCI'].section[num_int, skip_f:, rlo:rhi ,:]
              dq_section = fh['dq'].section[num_int, skip_f:, rlo:rhi ,:] 

              t_any_cr, t_cr_total, t_err_cube, t_dq_cube, t_cr_cube, \
                 m_by_var, one_by_var, bad_var = calc_slope( data_section, inttime, adc_gain)

              any_cr[ rlo:rhi,:] += t_any_cr
              cr_total[ rlo:rhi,:] += t_cr_total
                
              err_cube[num_int, skip_f:, rlo:rhi,:] += t_err_cube  
              dq_cube[num_int, skip_f:, rlo:rhi,:] += t_dq_cube + dq_section
              cr_cube[num_int, skip_f:, rlo:rhi,:] += t_cr_cube 

              sect_shape = data_section.shape[-2:] 
              m_sum_2d[ rlo:rhi, : ] += m_by_var.reshape( sect_shape )  
              var_sum_2d[ rlo:rhi, : ] += one_by_var.reshape( sect_shape ) 
   
        wh_non_zero = (var_sum_2d != 0.0)          
        slope_wtd[ wh_non_zero ] = (m_sum_2d[ wh_non_zero ]/ 
                               var_sum_2d[ wh_non_zero ])

        slopes = slope_wtd.reshape( imshape ) 

        # values in err_cube have been 1./variance, so take reciprocal
        #    of non-zero values to write
        err_cube = 1./err_cube # are no zero values 

        write_all( fname, any_cr, slopes, cr_total, cr_cube, err_cube, dq_cube )

        if (verb > 0):
            tstop = time.time()
            print 'Starting the program ramp_fit_arr at: ', tstart_clock
            print '  Input dataset: ', fname      
            print '  Instrument = ' , instrume 
            print '  Number of reads in input dataset: ', nreads       
            print '  Number of pixels in 2d array: ', npix
            print '  Shape of 2d image: ', imshape 
            print '  Shape of data cube: ', cubeshape 
            print '  Buffer size (bytes): ', buffsize
            print '  Number of rows per buffer: ' , nrows        
            print '  Number of integrations: ' , n_int
            print '  Integration time: ' , inttime,' seconds.'
            print '  Integration time per group: ' , intgrtim,' seconds.'
            print '  Number of initial frames skipped: ' , skip_f
            print '    '
            print_stats( slopes, cr_total ) 
            print 'Completing the program ramp_fit_arr at:', time.asctime()
            print 'The execution time: ' , tstop - tstart,' seconds.'



def print_stats( slopes,  cr_total):  
    """
    Short Summary
    ---------
    Optionally print statistics of detected cosmic rays

    Parameters
    ---------    
    slopes: float, 2d array
       weighted count rate
    
    cr_total: int, 2d array
       total number of CRs in each pixel in 2d array

    Returns
    ---------
    """

    wh_slope_0 = np.where( slopes == 0.) # insuff data or no signal
    for ii in range( cr_total.max()+1 ): 
       cr_ii = np.where( cr_total == ii )
       print 'The number of pixels in 2d array having ' , ii,\
             ' crs : ' , len(cr_ii[0])

    print 'The number of pixels having insufficient data'
    print '  due to excessive CRs :', len(wh_slope_0[0])  
    print 'Count rates - min, mean, max, std:'\
         ,slopes.min(), slopes.mean(), slopes.max()\
         ,slopes.std() 
    print 'Cosmic rays - min, mean, max, std :',\
         cr_total.min(), cr_total.mean(), cr_total.max(), cr_total.std() 


if __name__=="__main__":
    """Get input file and other arguments, and call ramp_fit.
    """
    usage = "usage: ./ramp_fit_arr.py datacube [buffsize] [[verb]]" 

    tstart_clock = time.asctime()
    tstart = time.time()
    
    if ( sys.argv[1] ): fname = sys.argv[1]

    if ( len(sys.argv) > 2 ):
        buffsize = int(sys.argv[2])
    else: 
        buffsize = BUFSIZE

    if ( len(sys.argv) > 3 ):
        verb = int(sys.argv[3])
    else: 
        verb = 0
 
    try:
        ramp_fit( fname, buffsize, verb )
    except Exception, errmess:
        print 'Fatal ERROR: ', errmess


                                                          
