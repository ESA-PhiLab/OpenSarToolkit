'''
Based on a reduced set of processing parameters, this script allows to
produce Sentinel-1 backscatter ARD data from GRD products.
The script allows to process consecutive frames from one acquisition and
outputs a single file.


----------------
Functions:
----------------
    _slice_assembly:
        creates an urllib opener object for authentication on scihub server
    _grd_frame_import:
        gets the next page from a multi-page result from a scihub search
    _grd_remove_border:
        creates a string in the Open Search format that is added to the
        base scihub url
    _grd_backscatter:
        applies the search and writes the reults in a Geopandas GeoDataFrame
    _grd_speckle_filter:
        applies the Lee-Sigma filter with SNAP standard parameters
    _grd_ls_mask:
        writes the search result into an ESRI Shapefile
    _grd_terrain_correction:
        writes the search result into a PostGreSQL/PostGIS Database

------------------
Main function
------------------
  grd_to_ard:
    handles the whole workflow

------------------
Contributors
------------------

Andreas Vollrath, ESA phi-lab
-----------------------------------
August 2018: Original implementation
September 2018: New workflow adapted to SNAP changes
                (i.e. Thermal Noise Removal before Slice Assembly)

------------------
Usage
------------------

python3 grd_to_ard.py -p /path/to/scene -r 20 -p RTC -l True -s False
                   -t /path/to/tmp -o /path/to/output

    -i    defines the path to one or a list of consecutive slices
    -r    resolution in meters (should be 10 or more, default=20)
    -p    defines the product type (GTCsigma, GTCgamma, RTC, default=GTCgamma)
    -l    defines the layover/shadow mask creation (True/False, default=True)
    -s    defines the speckle filter (True/False, default=False)
    -t    defines the folder for temporary products (default=/tmp)
    -o    defines the /path/to/the/output

'''


# import stdlib modules
import os
import sys
import glob
import shutil
import time
import rasterio
import numpy as np
import gdal
import logging

from os.path import join as opj
from ost.helpers import helpers as h
from ost.settings import OST_ROOT

logger = logging.getLogger(__name__)


def _grd_frame_import(infile, outfile, logfile, polarisation='VV,VH,HH,HV'):
    '''A wrapper of SNAP import of a single Sentinel-1 GRD product

    This function takes an original Sentinel-1 scene (either zip or
    SAFE format), updates the orbit information (does not fail if not
    available), removes the thermal noise and stores it as a SNAP
    compatible BEAM-Dimap format.

    Args:
        infile: string or os.path object for
                an original Sentinel-1 GRD product in zip or SAFE format
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        polarisation (str): a string consisiting of the polarisation (comma separated)
                     e.g. 'VV,VH',
                     default value: 'VV,VH,HH,HV'
    '''

    logger.debug('INFO: Importing {} by applying precise orbit file and'
                 'removing thermal noise'.format(os.path.basename(infile))
                 )

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    graph = opj(OST_ROOT, 'graphs', 'S1_GRD2ARD', '1_AO_TNR.xml')

    # construct command
    command = '{} {} -x -q {} -Pinput=\'{}\' -Ppolarisation={} \
               -Poutput=\'{}\''.format(
        gpt_file, graph, os.cpu_count(), infile, polarisation, outfile)

    # run command
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.debug('INFO: Succesfully imported product')
    else:
        logger.debug('ERROR: Frame import exited with an error. \
                See {} for Snap Error output'.format(logfile))
        sys.exit(102)

    return return_code


def _grd_frame_import_subset(infile, outfile, georegion,
                             logfile, polarisation='VV,VH,HH,HV'):
    '''A wrapper of SNAP import of a subset of single Sentinel-1 GRD product

    This function takes an original Sentinel-1 scene (either zip or
    SAFE format), updates the orbit information (does not fail if not
    available), removes the thermal noise, subsets it to the given georegion
    and stores it as a SNAP
    compatible EAM-Dimap format.


    Args:
        infile: string or os.path object for
                an original Sentinel-1 GRD product in zip or SAFE format
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        polarisation (str): a string consisiting of the polarisation (comma separated)
                     e.g. 'VV,VH',
                     default value: 'VV,VH,HH,HV'
        georegion (str): a WKT style formatted POLYGON that bounds the
                         subset region
    '''

    logger.debug(
        'INFO: Importing {} by applying precise orbit file and'
        'removing thermal noise, as well as subsetting.'.format(os.path.basename(infile))
    )

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    # get path to ost package
    graph = opj(OST_ROOT, 'graphs', 'S1_GRD2ARD', '1_AO_TNR_SUB.xml')

    # construct command
    command = '{} {} -x -q {} -Pinput=\'{}\' -Pregion=\'{}\' -Ppolarisation={} \
                      -Poutput=\'{}\''.format(
        gpt_file, graph, 2 * os.cpu_count(),
        infile, georegion, polarisation, outfile)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.debug('INFO: Succesfully imported product')
    else:
        logger.debug('ERROR: Frame import exited with an error. \
                See {} for Snap Error output'.format(logfile))
        sys.exit(102)

    return return_code


def _slice_assembly(filelist, outfile, logfile, polarisation='VV,VH,HH,HV'):
    '''A wrapper of SNAP's slice assembly routine

    This function assembles consecutive frames acquired at the same date.
    Can be either GRD or SLC products

    Args:
        filelist (str): a string of a space separated list of OST imported
                        Sentinel-1 product slices to be assembled
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
    '''

    logger.debug('INFO: Assembling consecutive frames:')

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    # construct command
    command = '{} SliceAssembly -x -q {} -PselectedPolarisations={} \
               -t \'{}\'{}'.format(
        gpt_file, 2 * os.cpu_count(), polarisation, outfile, filelist)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.debug('INFO: Succesfully assembled products')
    else:
        logger.debug('ERROR: Slice Assembly exited with an error. \
                See {} for Snap Error output'.format(logfile))
        sys.exit(101)

    return return_code


def _grd_subset(infile, outfile, logfile, region):
    '''A wrapper around SNAP's subset routine

    This function takes an OST imported frame and subsets it according to
    the coordinates given in the region

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        region (str): a list of image coordinates that bound the subset region
    '''

    # get Snap's gpt file
    gpt_file = h.gpt_path()

    # format region string
    region = ','.join([str(int(x)) for x in region])

    # construct command
    command = '{} Subset -x -q {} -Pregion={} -t \'{}\'\'{}\''.format(
        gpt_file, os.cpu_count(), region, outfile, infile)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.debug('INFO: Succesfully subsetted product')
    else:
        logger.debug('ERROR: Subsetting exited with an error. \
                See {} for Snap Error output'.format(logfile))
        sys.exit(107)

    return return_code


def _grd_subset_georegion(infile, outfile, logfile, georegion):
    '''A wrapper around SNAP's subset routine

    This function takes an OST imported frame and subsets it according to
    the coordinates given in the region

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        georegion (str): a WKT style formatted POLYGON that bounds the
                   subset region
    '''
    
    logger.debug('INFO: Subsetting imported imagery.')
    # get Snap's gpt file
    gpt_file = h.gpt_path()

    # extract window from scene
    command = '{} Subset -x -q {} -Ssource=\'{}\'-t \'{}\'\
                 -PcopyMetadata=true -PgeoRegion=\'{}\''.format(
        gpt_file, 2 * os.cpu_count(), infile, outfile, georegion)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.debug('INFO: Succesfully subsetted product.')
    else:
        logger.debug('ERROR: Subsetting exited with an error. \
                See {} for Snap Error output'.format(logfile))
        sys.exit(107)

    return return_code


def _grd_remove_border(infile):
    '''An OST function to remove GRD border noise from Sentinel-1 data

    This is a custom routine to remove GRD border noise
    from Sentinel-1 GRD products. It works on the original intensity
    images.

    NOTE: For the common dimap format, the infile needs to be the
    ENVI style file inside the *data folder.

    The routine checks the outer 3000 columns for its mean value.
    If the mean value is below 100, all values will be set to 0,
    otherwise the routine will continue fpr another 150 columns setting
    the value to 0. All further columns towards the inner image are
    considered valid.

    Args:
        infile: string or os.path object for a
                gdal compatible intensity file of Sentinel-1

    Notes:
        The file will be manipulated inplace, meaning,
        no new outfile is created.
    '''

    # logger.debug('INFO: Removing the GRD Border Noise.')
    currtime = time.time()

    # read raster file and get number of columns adn rows
    raster = gdal.Open(infile, gdal.GA_Update)
    cols = raster.RasterXSize
    rows = raster.RasterYSize

    # create 3000xrows array for the left part of the image
    array_left = np.array(raster.GetRasterBand(1).ReadAsArray(0,
                                                              0, 3000, rows))

    for x in range(3000):
        # condition if more than 50 pixels within the line have values
        # less than 500, delete the line
        # if np.sum(np.where((array_left[:,x] < 200)
        # & (array_left[:,x] > 0) , 1, 0)) <= 50:
        if np.mean(array_left[:, x]) <= 100:
            array_left[:, x].fill(0)
        else:
            z = x + 150
            if z > 3000:
                z = 3000
            for y in range(x, z, 1):
                array_left[:, y].fill(0)

            cols_left = y
            break

    try:
        cols_left
    except NameError:
        cols_left = 3000

    # write array_left to disk
    # logger.debug('INFO: Total amount of columns: {}'.format(cols_left))
    # logger.debug('INFO: Number of colums set to 0 on the left side: '
    #     '{}'.format(cols_left))
    # raster.GetRasterBand(1).WriteArray(array_left[:, :+cols_left], 0, 0, 1)
    raster.GetRasterBand(1).WriteArray(array_left[:, :+cols_left], 0, 0)

    array_left = None

    # create 2d array for the right part of the image (3000 columns and rows)
    cols_last = cols - 3000
    array_right = np.array(raster.GetRasterBand(1).ReadAsArray(cols_last,
                                                               0, 3000, rows))

    # loop through the array_right columns in opposite direction
    for x in range(2999, 0, -1):

        if np.mean(array_right[:, x]) <= 100:
            array_right[:, x].fill(0)
        else:
            z = x - 150
            if z < 0:
                z = 0
            for y in range(x, z, -1):
                array_right[:, y].fill(0)

            cols_right = y
            break

    try:
        cols_right
    except NameError:
        cols_right = 0

    #
    col_right_start = cols - 3000 + cols_right
    # logger.debug('INFO: Number of columns set to 0 on the'
    #     'right side: {}'.format(3000 - cols_right))
    # logger.debug('INFO: Amount of columns kept: {}'.format(col_right_start))
    raster.GetRasterBand(1).WriteArray(array_right[:, cols_right:],
                                       col_right_start, 0)
    array_right = None
    h.timer(currtime)


def _grd_backscatter(
        infile,
        outfile,
        logfile,
        product_type='GTCgamma',
        dem='SRTM 1Sec HGT',
        dem_file='',
        resampling='BILINEAR_INTERPOLATION'
):
    '''A wrapper around SNAP's radiometric calibration

    This function takes OST imported Sentinel-1 product and generates
    it to calibrated backscatter.

    3 different calibration modes are supported.
        - Radiometrically terrain corrected Gamma nought (RTC)
        - ellipsoid based Gamma nought (GTCgamma)
        - Sigma nought (GTCsigma).

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        resolution (int): the resolution of the output product in meters
        product_type (str): the product type of the output product
                            i.e. RTC, GTCgamma or GTCsigma
        dem (str): A Snap compliant string for the dem to use.
                   Possible choices are:
                       'SRTM 1sec HGT'(default)
                       'SRTM 3sec'
                       'ASTER 1sec GDEM'
                       'ACE30'

    '''

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    # select xml according to product type
    if product_type == 'RTC':
        logger.debug('INFO: Calibrating the product to a RTC product.')
        graph = opj(OST_ROOT, 'graphs', 'S1_GRD2ARD', '2_CalBeta_TF.xml')
        if dem_file != '':
            with rasterio.open(dem_file, 'r') as dem_f:
                dem_nodata = dem_f.nodata
        else:
            dem_nodata = 0.0
    elif product_type == 'GTCgamma':
        logger.debug('INFO: Calibrating the product to a GTC product (Gamma0).')
        graph = opj(OST_ROOT, 'graphs', 'S1_GRD2ARD', '2_CalGamma.xml')
    elif product_type == 'GTCsigma':
        logger.debug('INFO: Calibrating the product to a GTC product (Sigma0).')
        graph = opj(OST_ROOT, 'graphs', 'S1_GRD2ARD', '2_CalSigma.xml')
    else:
        logger.debug('ERROR: Wrong product type selected.')
        sys.exit(103)

    # construct command sring
    if product_type == 'RTC':
        command = '{} {} -x -q {} -Pinput=\'{}\' -Pdem=\'{}\' \
                   -Pdem_file=\'{}\' -Pdem_nodata={} -Presampling={} \
                   -Poutput=\'{}\''.format(gpt_file, graph, 2 * os.cpu_count(),
                                           infile, dem, dem_file, dem_nodata, resampling,
                                           outfile
                                           )
    else:
        command = '{} {} -x -q {} -Pinput=\'{}\' -Poutput=\'{}\''.format(
            gpt_file, graph, 2 * os.cpu_count(), infile, outfile)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.debug('INFO: Succesfully calibrated product')
    else:
        logger.debug('ERROR: Backscatter calibration exited with an error. \
                See {} for Snap Error output'.format(logfile))
        sys.exit(103)

    return return_code


def _grd_speckle_filter(infile, outfile, logfile):
    '''A wrapper around SNAP's Refined Lee Speckle Filter

    This function takes OST imported Sentinel-1 product and applies
    a standardised version of the Lee-Sigma Speckle Filter with
    SNAP's defaut values.

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
    '''

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    logger.debug('INFO: Applying the Refined-Lee Speckle Filter')
    # contrcut command string
    command = '{} Speckle-Filter -x -q {} -PestimateENL=true -Pfilter=\'Refined Lee\' \
              -t \'{}\' \'{}\''.format(gpt_file, 2 * os.cpu_count(),
                                       outfile, infile)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # hadle errors and logs
    if return_code == 0:
        logger.debug('INFO: Succesfully imported product')
    else:
        logger.debug('ERROR: Speckle Filtering exited with an error. \
                See {} for Snap Error output'.format(logfile))
        sys.exit(111)

    return return_code


def _grd_to_db(infile, outfile, logfile):
    '''A wrapper around SNAP's linear to db routine

    This function takes an OST calibrated Sentinel-1 product
    and converts it to dB.

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
    '''

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    logger.debug('INFO: Converting the image to dB-scale.')
    # construct command string
    command = '{} LinearToFromdB -x -q {} -t \'{}\' {}'.format(
        gpt_file, 2 * os.cpu_count(), outfile, infile)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.debug('INFO: Succesfully converted product to dB-scale.')
    else:
        logger.debug('ERROR: Linear to dB conversion exited with an error. \
                See {} for Snap Error output'.format(logfile))
        sys.exit(113)

    return return_code


def _grd_terrain_correction(
        infile,
        outfile,
        logfile,
        resolution,
        dem='SRTM 1Sec HGT',
        dem_file='',
        resampling='BILINEAR_INTERPOLATION'
):
    '''A wrapper around SNAP's Terrain Correction routine

    This function takes an OST calibrated Sentinel-1 product and
    does the geocodification.

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        resolution (int): the resolution of the output product in meters
        dem (str): A Snap compliant string for the dem to use.
                   Possible choices are:
                       'SRTM 1sec HGT'(default)
                       'SRTM 3sec'
                       'ASTER 1sec GDEM'
                       'ACE30'

    '''

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    # get path to ost package
    logger.debug('INFO: Geocoding the calibrated product')

    # calculate the multi-look factor
    multilook_factor = int(int(resolution) / 10)

    graph = opj(OST_ROOT, 'graphs', 'S1_GRD2ARD', '3_ML_TC.xml')

    if dem_file != '':
        with rasterio.open(dem_file, 'r') as dem_f:
            dem_nodata = dem_f.nodata
    else:
        dem_nodata = 0.0

    # construct command string
    command = '{} {} -x -q {} -Pinput=\'{}\' -Presol={} -Pml={} -Pdem=\'{}\' \
                 -Pdem_file=\'{}\' -Pdem_nodata={} -Presampling={} \
                 -Poutput=\'{}\''.format(gpt_file, graph, 2 * os.cpu_count(),
                                         infile, resolution, multilook_factor,
                                         dem, dem_file, dem_nodata, resampling, outfile)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.debug('INFO: Succesfully imported product')
    else:
        logger.debug('ERROR: Terain Correction exited with an error. \
                See {} for Snap Error output'.format(logfile))
        sys.exit(112)

    return return_code


def _grd_terrain_correction_deg(
        infile,
        outfile,
        logfile,
        resolution,
        dem='SRTM 1Sec HGT',
        dem_file='',
        resampling='BILINEAR_INTERPOLATION'
):
    '''A wrapper around SNAP's Terrain Correction routine

    This function takes an OST calibrated Sentinel-1 product and
    does the geocodification.

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        resolution (int): the resolution of the output product in meters
        dem (str): A Snap compliant string for the dem to use.
                   Possible choices are:
                       'SRTM 1sec HGT'(default)
                       'SRTM 3sec'
                       'ASTER 1sec GDEM'
                       'ACE30'

    '''

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    logger.debug('INFO: Geocoding the calibrated product')

    # calculate the multi-look factor
    # multilook_factor = int(int(resolution) / 10)
    multilook_factor = 1

    graph = opj(OST_ROOT, 'graphs', 'S1_GRD2ARD', '3_ML_TC_deg.xml')

    if dem_file != '':
        with rasterio.open(dem_file, 'r') as dem_f:
            dem_nodata = dem_f.nodata
    else:
        dem_nodata = 0.0

    # construct command string
    command = '{} {} -x -q {} -Pinput=\'{}\' -Presol={} -Pml={} -Pdem=\'{}\' \
                 -Pdem_file=\'{}\' -Pdem_nodata={} -Presampling={} \
                 -Poutput=\'{}\''.format(gpt_file, graph, 2 * os.cpu_count(),
                                         infile, resolution, multilook_factor,
                                         dem, dem_file, dem_nodata, resampling, outfile
                                         )

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.debug('INFO: Succesfully imported product')
    else:
        logger.debug('ERROR: Terain Correction exited with an error. \
                See {} for Snap Error output'.format(logfile))
        sys.exit(112)

    return return_code


def _grd_ls_mask(
        infile,
        outfile,
        logfile,
        resolution,
        dem='SRTM 1Sec HGT',
        dem_file='',
        resampling='BILINEAR_INTERPOLATION'
):
    '''A wrapper around SNAP's Layover/Shadow mask routine

    This function takes OST imported Sentinel-1 product and calculates
    the Layover/Shadow mask.

    Args:
        infile: string or os.path object for
                an OST imported frame in BEAM-Dimap format (i.e. *.dim)
        outfile: string or os.path object for the output
                 file written in BEAM-Dimap format
        logfile: string or os.path object for the file
                 where SNAP'S STDOUT/STDERR is written to
        resolution (int): the resolution of the output product in meters
        dem (str): A Snap compliant string for the dem to use.
                   Possible choices are:
                       'SRTM 1sec HGT'(default)
                       'SRTM 3sec'
                       'ASTER 1sec GDEM'
                       'ACE30'

    '''

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    logger.debug('INFO: Creating the Layover/Shadow mask')
    # get path to workflow xml
    graph = opj(OST_ROOT, 'graphs', 'S1_GRD2ARD', '3_LSmap.xml')

    if dem_file != '':
        with rasterio.open(dem_file, 'r') as dem_f:
            dem_nodata = dem_f.nodata
    else:
        dem_nodata = 0.0

    # construct command string
    command = '{} {} -x -q {} -Pinput=\'{}\' -Presol={} -Pdem=\'{}\' \
                 -Pdem_file=\'{}\' -Pdem_nodata={} -Presampling={} \
                 -Poutput=\'{}\''.format(
        gpt_file, graph, 2 * os.cpu_count(), infile, resolution, dem, dem_file,
        dem_nodata, resampling, outfile
    )

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        logger.debug('INFO: Succesfully create a Layover/Shadow mask')
    else:
        logger.debug('ERROR: Layover/Shadow mask creation exited with an error. \
                See {} for Snap Error output'.format(logfile))
        raise RuntimeError
        sys.exit(112)

    return return_code


def grd_to_ard(filelist,
               output_dir,
               out_prefix,
               temp_dir,
               resolution,
               resampling,
               product_type,
               ls_mask_create,
               speckle_filter,
               dem,
               to_db,
               border_noise,
               subset=None,
               polarisation='VV,VH,HH,HV'
               ):
    '''The main function for the grd to ard generation

    This function represents the full workflow for the generation of an
    Analysis-Ready-Data product. The standard parameters reflect the CEOS
    ARD defintion for Sentinel-1 backcsatter products.

    By changing the parameters, taking care of all parameters
    that can be given. The function can handle multiple inputs of the same
    acquisition, given that there are consecutive data takes.

    Args:
        filelist (list): must be a list with one or more absolute
                  paths to GRD scene(s)
        output_dir: os.path object or string for the folder
                    where the output file should be written#
        out_prefix (str): prefix of the final output file
        temp_dir:
        resolution: the resolution of the output product in meters
        ls_mask: layover/shadow mask generation (Boolean)
        speckle_filter: speckle filtering (Boolean)

    Returns:
        nothing

    Notes:
        no explicit return value, since output file is our actual return
    '''

    # get processing parameters from dict
    #    resolution = processing_dict['resolution']
    #    product_type = processing_dict['product_type']
    #    ls_mask = processing_dict['ls_mask']
    #    speckle_filter = processing_dict['speckle_filter']
    #    border_noise = processing_dict['border_noise']
    #    dem = processing_dict['dem']
    #    to_db = processing_dict['to_db']

    # Check if dem is file, else use default dem
    if dem.endswith('.tif') or dem.endswith('.hgt') or dem.endswith('.hdf'):
        dem_file = dem
        dem = 'External DEM'
    else:
        dem_file = ''
    # Check out_prefix for empty spaces
    out_prefix = out_prefix.replace(' ', '_')

    # slice assembly if more than one scene
    if len(filelist) > 1:

        for file in filelist:

            grd_import = opj(temp_dir, '{}_imported'.format(
                os.path.basename(file)[:-5]))
            logfile = opj(output_dir, '{}_Import.errLog'.format(
                os.path.basename(file)[:-5]))

            return_code = _grd_frame_import(file, grd_import, logfile)
            if return_code != 0:
                h.remove_folder_content(temp_dir)
                return return_code

        # create list of scenes for full acquisition in
        # preparation of slice assembly
        scenelist = ' '.join(glob.glob(opj(temp_dir, '*imported.dim')))

        # create file strings
        grd_import = opj(temp_dir, '{}_imported'.format(out_prefix))
        logfile = opj(output_dir, '{}_slice_assembly.errLog'.format(out_prefix))
        return_code = _slice_assembly(scenelist, grd_import, logfile,
                                      polarisation)
        if return_code != 0:
            h.remove_folder_content(temp_dir)
            return return_code

        for file in filelist:
            h.delete_dimap(opj(temp_dir, '{}_imported'.format(
                os.path.basename(str(file))[:-5])))

        if subset:
            grd_subset = opj(temp_dir, '{}_imported_subset'.format(out_prefix))
            return_code = _grd_subset_georegion('{}.dim'.format(grd_import),
                                                grd_subset, logfile, subset)
            if return_code != 0:
                h.remove_folder_content(temp_dir)
                return return_code

            # delete slice assembly
            h.delete_dimap(grd_import)
    
    # single scene case
    else:
        grd_import = opj(temp_dir, '{}_imported'.format(out_prefix))
        logfile = opj(output_dir, '{}_Import.errLog'.format(out_prefix))

        if subset is None:
            return_code = _grd_frame_import(filelist[0], grd_import, logfile,
                                            polarisation)
        else:
            # georegion = vec.shp_to_wkt(subset, buffer=0.1, envelope=True)
            return_code = _grd_frame_import_subset(filelist[0], grd_import, 
                                                   subset, logfile, 
                                                   polarisation)
        if return_code != 0:
            h.remove_folder_content(temp_dir)
            return return_code
    # ---------------------------------------------------------------------
    # Remove the grd border noise from existent channels (OST routine)

    if border_noise and not subset:
        for polarisation in ['VV', 'VH', 'HH', 'HV']:

            infile = glob.glob(opj(
                temp_dir, '{}_imported*data'.format(out_prefix),
                'Intensity_{}.img'.format(polarisation)))

            if len(infile) == 1:
                # run grd Border Remove
                logger.debug('INFO: Remove border noise for {} band.'.format(
                    polarisation))
                _grd_remove_border(infile[0])

    # ----------------------
    # do the calibration
    infile = glob.glob(opj(temp_dir, '{}_imported*dim'.format(out_prefix)))[0]
    outfile = opj(temp_dir, '{}_{}'.format(out_prefix, product_type))
    logfile = opj(output_dir, '{}_Backscatter.errLog'.format(out_prefix))
    return_code = _grd_backscatter(infile,
                                   outfile,
                                   logfile,
                                   product_type,
                                   dem,
                                   dem_file,
                                   resampling
                                   )
    if return_code != 0:
        h.remove_folder_content(temp_dir)
        return return_code

    data_dir = glob.glob(opj(temp_dir, '{}*imported.data'.format(out_prefix)))
    h.delete_dimap(str(data_dir[0])[:-5])

    infile = opj(temp_dir, '{}_{}.dim'.format(out_prefix, product_type))
    # -------------------------------------------
    # in case we want to apply Speckle filtering
    if speckle_filter:
        logfile = opj(temp_dir, '{}_Speckle.errLog'.format(out_prefix))
        outfile = opj(temp_dir, '{}_imported_spk'.format(out_prefix))

        # run processing
        return_code = _grd_speckle_filter(infile, outfile, logfile)

        if return_code != 0:
            h.remove_folder_content(temp_dir)
            return return_code

        # define infile for next processing step
        infile = opj(temp_dir, '{}_imported_spk.dim'.format(out_prefix))
        data_dir = opj(temp_dir, '{}_{}'.format(out_prefix, product_type))
        h.delete_dimap(str(data_dir))

    # ----------------------------------------------
    # let's create a Layover shadow mask if needed
    if ls_mask_create is True:
        outfile = opj(temp_dir, '{}_ls_mask'.format(out_prefix))
        logfile = opj(output_dir, '{}_ls_mask.errLog'.format(out_prefix))
        return_code = _grd_ls_mask(infile,
                                   outfile,
                                   logfile,
                                   resolution,
                                   dem,
                                   dem_file,
                                   resampling
                                   )
        if return_code != 0:
            h.remove_folder_content(temp_dir)
            return return_code

        # last check on ls data
        return_code = h.check_out_dimap(outfile, test_stats=False)
        if return_code != 0:
            h.remove_folder_content(temp_dir)
            return return_code

        # move to final destination
        out_ls_mask = opj(output_dir, '{}_LS'.format(out_prefix))

        # delete original file sin case they exist
        if os.path.exists(str(out_ls_mask) + '.dim'):
            h.delete_dimap(out_ls_mask)

        # move out of temp
        shutil.move('{}.dim'.format(outfile), '{}.dim'.format(out_ls_mask))
        shutil.move('{}.data'.format(outfile), '{}.data'.format(out_ls_mask))

    # to db
    if to_db:
        logfile = opj(output_dir, '{}.linToDb.errLog'.format(out_prefix))
        outfile = opj(temp_dir, '{}_{}_db'.format(out_prefix, product_type))
        return_code = _grd_to_db(infile, outfile, logfile)
        if return_code != 0:
            h.remove_folder_content(temp_dir)
            return return_code

        # delete
        h.delete_dimap(infile[:-4])
        # re-define infile
        infile = opj(temp_dir, '{}_{}_db.dim'.format(out_prefix, product_type))

    # -----------------------
    # let's geocode the data
    # infile = opj(temp_dir, '{}.{}.dim'.format(out_prefix, product_type))
    outfile = opj(temp_dir, '{}_{}_TC'.format(out_prefix, product_type))
    logfile = opj(output_dir, '{}_TC.errLog'.format(out_prefix))
    return_code = _grd_terrain_correction(infile,
                                          outfile,
                                          logfile,
                                          resolution,
                                          dem,
                                          dem_file,
                                          resampling
                                          )
    if return_code != 0:
        h.remove_folder_content(temp_dir)
        return return_code

    # remove calibrated files
    h.delete_dimap(infile[:-4])

    # move to final destination
    out_final = opj(output_dir, '{}_{}_TC'.format(out_prefix, product_type))

    # remove file if exists
    if os.path.exists(out_final + '.dim'):
        h.delete_dimap(out_final)

    return_code = h.check_out_dimap(outfile)
    if return_code != 0:
        h.remove_folder_content(temp_dir)
        return return_code

    shutil.move('{}.dim'.format(outfile), '{}.dim'.format(out_final))
    shutil.move('{}.data'.format(outfile), '{}.data'.format(out_final))

    # write file, so we know this burst has been succesfully processed
    if return_code == 0:
        check_file = opj(output_dir, '.processed')
        with open(str(check_file), 'w') as file:
            file.write('passed all tests \n')
        return return_code
    else:
        h.remove_folder_content(temp_dir)
        h.remove_folder_content(output_dir)
