#! /usr/bin/env python3
# -*- coding: utf-8 -*-

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
import importlib
import json
import glob
import shutil
import time
import rasterio
import numpy as np
import gdal

from os.path import join as opj
from ost.snap_common import common
from ost.helpers import helpers as h, raster as ras

# script infos
__author__ = 'Andreas Vollrath'
__copyright__ = 'phi-lab, European Space Agency'
__license__ = 'GPL'
__version__ = '1.0'
__maintainer__ = 'Andreas Vollrath'
__email__ = ''
__status__ = 'Production'


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

    print(' INFO: Importing {} by applying precise orbit file and'
          ' removing thermal noise'.format(os.path.basename(infile)))

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    # get path to ost package
    rootpath = importlib.util.find_spec('ost').submodule_search_locations[0]
    graph = opj(rootpath, 'graphs', 'S1_GRD2ARD', '1_AO_TNR.xml')

    # construct command
    command = '{} {} -x -q {} -Pinput=\'{}\' -Ppolarisation={} \
               -Poutput=\'{}\''.format(
                   gpt_file, graph, os.cpu_count(), infile, polarisation, outfile)

    # run command
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        print(' INFO: Succesfully imported product')
    else:
        print(' ERROR: Frame import exited with an error. \
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

    print(' INFO: Importing {} by applying precise orbit file and'
          ' removing thermal noise, as well as subsetting.'.format(
              os.path.basename(infile)))

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    # get path to ost package
    rootpath = importlib.util.find_spec('ost').submodule_search_locations[0]
    graph = opj(rootpath, 'graphs', 'S1_GRD2ARD', '1_AO_TNR_SUB.xml')

    # construct command
    command = '{} {} -x -q {} -Pinput=\'{}\' -Pregion=\'{}\' -Ppolarisation={} \
                      -Poutput=\'{}\''.format(
                          gpt_file, graph, 2 * os.cpu_count(),
                          infile, georegion, polarisation, outfile)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        print(' INFO: Succesfully imported product')
    else:
        print(' ERROR: Frame import exited with an error. \
                See {} for Snap Error output'.format(logfile))

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

    print(' INFO: Assembling consecutive frames:')

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    # construct command
    command = '{} SliceAssembly -x -q {} -PselectedPolarisations={} \
               -t \'{}\' {}'.format(
                   gpt_file, 2 * os.cpu_count(), polarisation, outfile, filelist)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        print(' INFO: Succesfully assembled products')
    else:
        print(' ERROR: Slice Assembly exited with an error. \
                See {} for Snap Error output'.format(logfile))

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
    command = '{} Subset -x -q {} -Pregion={} -t \'{}\' \'{}\''.format(
        gpt_file, os.cpu_count(), region, outfile, infile)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        print(' INFO: Succesfully subsetted product')
    else:
        print(' ERROR: Subsetting exited with an error. \
                See {} for Snap Error output'.format(logfile))

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
    
    print(' INFO: Subsetting imported imagery.')
    # get Snap's gpt file
    gpt_file = h.gpt_path()

    # extract window from scene
    command = '{} Subset -x -q {} -Ssource=\'{}\' -t \'{}\' \
                 -PcopyMetadata=true -PgeoRegion=\'{}\''.format(
                     gpt_file, 2 * os.cpu_count(), infile, outfile, georegion)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        print(' INFO: Succesfully subsetted product.')
    else:
        print(' ERROR: Subsetting exited with an error. \
                See {} for Snap Error output'.format(logfile))

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

    # print(' INFO: Removing the GRD Border Noise.')
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
    # print(' INFO: Total amount of columns: {}'.format(cols_left))
    # print(' INFO: Number of colums set to 0 on the left side: '
    #     ' {}'.format(cols_left))
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
    # print(' INFO: Number of columns set to 0 on the'
    #     ' right side: {}'.format(3000 - cols_right))
    # print(' INFO: Amount of columns kept: {}'.format(col_right_start))
    raster.GetRasterBand(1).WriteArray(array_right[:, cols_right:],
                                       col_right_start, 0)
    array_right = None
    h.timer(currtime)


def _grd_backscatter(infile, outfile, logfile, dem_dict, product_type='GTCgamma'):
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
                       'SRTM 1sec HGT' (default)
                       'SRTM 3sec'
                       'ASTER 1sec GDEM'
                       'ACE30'

    '''

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    # get path to ost package
    rootpath = importlib.util.find_spec('ost').submodule_search_locations[0]

    # select xml according to product type
    if product_type == 'RTC':
        print(' INFO: Calibrating the product to a RTC product.')
        graph = opj(rootpath, 'graphs', 'S1_GRD2ARD', '2_CalBeta_TF.xml')
    elif product_type == 'GTCgamma':
        print(' INFO: Calibrating the product to a GTC product (Gamma0).')
        graph = opj(rootpath, 'graphs', 'S1_GRD2ARD', '2_CalGamma.xml')
    elif product_type == 'GTCsigma':
        print(' INFO: Calibrating the product to a GTC product (Sigma0).')
        graph = opj(rootpath, 'graphs', 'S1_GRD2ARD', '2_CalSigma.xml')
    else:
        print(' ERROR: Wrong product type selected.')
        sys.exit(103)

    # construct command sring
    if product_type == 'RTC':
        command = ('{} {} -x -q {} -Pinput=\'{}\'' 
                                 ' -Pdem=\'{}\'' 
                                 ' -Pdem_file=\'{}\' '
                                 ' -Pdem_nodata=\'{}\'' 
                                 ' -Pdem_resampling=\'{}\''
                                 ' -Poutput=\'{}\''.format(
            gpt_file, graph, 2 * os.cpu_count(), infile, 
            dem_dict['dem name'], dem_dict['dem file'], 
            dem_dict['dem nodata'], dem_dict['dem resampling'], 
            outfile))
    else:
        command = '{} {} -x -q {} -Pinput=\'{}\' -Poutput=\'{}\''.format(
            gpt_file, graph, 2 * os.cpu_count(), infile, outfile)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        print(' INFO: Succesfully calibrated product')
    else:
        print(' ERROR: Backscatter calibration exited with an error. \
                See {} for Snap Error output'.format(logfile))

    return return_code


def _grd_speckle_filter(infile, outfile, logfile, speckle_dict):
    '''A wrapper around SNAP's Lee-Sigma Speckle Filter

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

    print(' INFO: Applying speckle filtering.')
    # contrcut command string
    command = ('{} Speckle-Filter -x -q {}'
                  ' -PestimateENL={}'
                  ' -PanSize={}'
                  ' -PdampingFactor={}'
                  ' -Penl={}'
                  ' -Pfilter={}'
                  ' -PfilterSizeX={}'
                  ' -PfilterSizeY={}'
                  ' -PnumLooksStr={}'
                  ' -PsigmaStr={}'
                  ' -PtargetWindowSizeStr={}'
                  ' -PwindowSize={}'
                  '-t \'{}\' \'{}\''.format(
                      gpt_file, 2 * os.cpu_count(),
                      speckle_dict['estimate ENL'],
                      speckle_dict['pan size'],
                      speckle_dict['damping'],
                      speckle_dict['ENL'],
                      speckle_dict['filter'],
                      speckle_dict['filter x size'],
                      speckle_dict['filter y size'],
                      speckle_dict['num of looks'],
                      speckle_dict['sigma'],
                      speckle_dict['target window size'],
                      speckle_dict['window size'],
                      outfile, infile)
              )

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # hadle errors and logs
    if return_code == 0:
        print(' INFO: Succesfully applied speckle filtering.')
    else:
        print(' ERROR: Speckle Filtering exited with an error. \
                See {} for Snap Error output'.format(logfile))

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

    print(' INFO: Converting the image to dB-scale.')
    # construct command string
    command = '{} LinearToFromdB -x -q {} -t \'{}\' {}'.format(
        gpt_file, 2 * os.cpu_count(), outfile, infile)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        print(' INFO: Succesfully converted product to dB-scale.')
    else:
        print(' ERROR: Linear to dB conversion exited with an error. \
                See {} for Snap Error output'.format(logfile))

    return return_code


def _grd_terrain_correction(infile, outfile, logfile, resolution, dem_dict):
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
                       'SRTM 1sec HGT' (default)
                       'SRTM 3sec'
                       'ASTER 1sec GDEM'
                       'ACE30'

    '''

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    # get path to ost package
    rootpath = importlib.util.find_spec('ost').submodule_search_locations[0]
    print(' INFO: Geocoding the calibrated product')

    # calculate the multi-look factor
    multilook_factor = int(int(resolution) / 10)

    graph = opj(rootpath, 'graphs', 'S1_GRD2ARD', '3_ML_TC.xml')

    # construct command string
#    command = '{} {} -x -q {} -Pinput=\'{}\' -Presol={} -Pml={} -Pdem=\'{}\' \
#                 -Poutput=\'{}\''.format(gpt_file, graph, 2 * os.cpu_count(),
#                                         infile, resolution, multilook_factor,
#                                         dem, outfile)
    command = ('{} {} -x -q {} -Pinput=\'{}\' -Presol={} -Pml={}' 
                                 ' -Pdem=\'{}\'' 
                                 ' -Pdem_file=\'{}\''
                                 ' -Pdem_nodata=\'{}\'' 
                                 ' -Pdem_resampling=\'{}\''
                                 ' -Pimage_resampling=\'{}\''
                                 ' -Poutput=\'{}\''.format(
            gpt_file, graph, 2 * os.cpu_count(), 
            infile, resolution, multilook_factor, 
            dem_dict['dem name'], dem_dict['dem file'], dem_dict['dem nodata'], 
            dem_dict['dem resampling'], dem_dict['image resampling'],
            outfile))
    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        print(' INFO: Succesfully terrain corrected product')
    else:
        print(' ERROR: Terain Correction exited with an error. \
                See {} for Snap Error output'.format(logfile))

    return return_code


def _grd_terrain_correction_deg(infile, outfile, logfile, resolution,
                                dem='SRTM 1Sec HGT'):
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
                       'SRTM 1sec HGT' (default)
                       'SRTM 3sec'
                       'ASTER 1sec GDEM'
                       'ACE30'

    '''

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    # get path to ost package
    rootpath = importlib.util.find_spec('ost').submodule_search_locations[0]
    print(' INFO: Geocoding the calibrated product')

    # calculate the multi-look factor
    # multilook_factor = int(int(resolution) / 10)
    multilook_factor = 1

    graph = opj(rootpath, 'graphs', 'S1_GRD2ARD', '3_ML_TC_deg.xml')

    # construct command string
    command = '{} {} -x -q {} -Pinput=\'{}\' -Presol={} -Pml={} -Pdem=\'{}\' \
                 -Poutput=\'{}\''.format(gpt_file, graph, 2 * os.cpu_count(),
                                         infile, resolution, multilook_factor,
                                         dem, outfile)

    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        print(' INFO: Succesfully imported product')
    else:
        print(' ERROR: Terain Correction exited with an error. \
                See {} for Snap Error output'.format(logfile))

    return return_code


def _grd_ls_mask(infile, outfile, logfile, resolution, dem_dict):
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
                       'SRTM 1sec HGT' (default)
                       'SRTM 3sec'
                       'ASTER 1sec GDEM'
                       'ACE30'

    '''

    # get path to SNAP's command line executable gpt
    gpt_file = h.gpt_path()

    # get path to ost package
    rootpath = importlib.util.find_spec('ost').submodule_search_locations[0]

    print(' INFO: Creating the Layover/Shadow mask')
    # get path to workflow xml
    graph = opj(rootpath, 'graphs', 'S1_GRD2ARD', '3_LSmap.xml')

    # construct command string
#    command = '{} {} -x -q {} -Pinput=\'{}\' -Presol={} -Pdem=\'{}\' \
#             -Poutput=\'{}\''.format(gpt_file, graph, 2 * os.cpu_count(),
#                                     infile, resolution, dem, outfile)
    command = ('{} {} -x -q {} -Pinput=\'{}\' -Presol={} ' 
                                 ' -Pdem=\'{}\'' 
                                 ' -Pdem_file=\'{}\''
                                 ' -Pdem_nodata=\'{}\'' 
                                 ' -Pdem_resampling=\'{}\''
                                 ' -Pimage_resampling=\'{}\''
                                 ' -Poutput=\'{}\''.format(
            gpt_file, graph, 2 * os.cpu_count(), infile, resolution, 
            dem_dict['dem name'], dem_dict['dem file'], dem_dict['dem nodata'], 
            dem_dict['dem resampling'], dem_dict['image resampling'],
            outfile))
    # run command and get return code
    return_code = h.run_command(command, logfile)

    # handle errors and logs
    if return_code == 0:
        print(' INFO: Succesfully created a Layover/Shadow mask')
    else:
        print(' ERROR: Layover/Shadow mask creation exited with an error. \
                See {} for Snap Error output'.format(logfile))
        
    return return_code


def grd_to_ard(filelist, 
               output_dir, 
               file_id, 
               temp_dir, 
               proc_file,
               subset=None):
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
        file_id (str): prefix of the final output file
        temp_dir:
        resolution: the resolution of the output product in meters
        ls_mask: layover/shadow mask generation (Boolean)
        speckle_filter: speckle filtering (Boolean)

    Returns:
        nothing

    Notes:
        no explicit return value, since output file is our actual return
    '''

    # load ard parameters
    with open(proc_file, 'r') as ard_file:
        ard_params = json.load(ard_file)['processing parameters']
        ard = ard_params['single ARD']
        polars = ard['polarisation'].replace(' ', '')
        
    # ---------------------------------------------------------------------
    # 1 Import
    
    # slice assembly if more than one scene
    if len(filelist) > 1:

        for file in filelist:

            grd_import = opj(temp_dir, '{}_imported'.format(
                os.path.basename(file)[:-5]))
            logfile = opj(output_dir, '{}.Import.errLog'.format(
                os.path.basename(file)[:-5]))
            
            return_code = _grd_frame_import(file, grd_import, logfile, polars)
            if return_code != 0:
                h.delete_dimap(grd_import)
                return return_code

        # create list of scenes for full acquisition in
        # preparation of slice assembly
        scenelist = ' '.join(glob.glob(opj(temp_dir, '*imported.dim')))

        # create file strings
        grd_import = opj(temp_dir, '{}_imported'.format(file_id))
        logfile = opj(output_dir, '{}._slice_assembly.errLog'.format(file_id))
        return_code = _slice_assembly(scenelist, grd_import, logfile)
        
        # delete inputs
        for file in filelist:
            h.delete_dimap(opj(temp_dir, '{}_imported'.format(
                os.path.basename(str(file))[:-5])))
        
        # delete output if command failed for some reason and return
        if return_code != 0:
            h.delete_dimap(grd_import)
            return return_code

        # subset mode after slice assembly
        if subset:
            grd_subset = opj(temp_dir, '{}_imported_subset'.format(file_id))
            return_code = _grd_subset_georegion('{}.dim'.format(grd_import), 
                                                grd_subset, logfile, subset)
            
            # delete slice assembly input to subset
            h.delete_dimap(grd_import)
            
            # delete output if command failed for some reason and return
            if return_code != 0:
                h.delete_dimap(grd_subset)
                return return_code
            
    # single scene case
    else:
        grd_import = opj(temp_dir, '{}_imported'.format(file_id))
        logfile = opj(output_dir, '{}.Import.errLog'.format(file_id))

        if subset is None:
            return_code = _grd_frame_import(filelist[0], grd_import, logfile, 
                                            polars)
        else:
            return_code = _grd_frame_import_subset(filelist[0], grd_import, 
                                                   subset, logfile, 
                                                   polars)
        
        # delete output if command failed for some reason and return
        if return_code != 0:
            h.delete_dimap(grd_import)
            return return_code
    
    # ---------------------------------------------------------------------
    # 2 GRD Border Noise
    if ard['remove border noise'] and not subset:
        for polarisation in ['VV', 'VH', 'HH', 'HV']:

            infile = glob.glob(opj(
                    temp_dir, '{}_imported*data'.format(file_id),
                    'Intensity_{}.img'.format(polarisation)))

            if len(infile) == 1:
                # run grd Border Remove
                print(' INFO: Remove border noise for {} band.'.format(
                    polarisation))
                _grd_remove_border(infile[0])

    # set input for next step
    infile = glob.glob(opj(temp_dir, '{}_imported*dim'.format(file_id)))[0]
    
    # ---------------------------------------------------------------------
    # 3 Calibration
    if ard['product type'] == 'GTC-sigma0':
        calibrate_to = 'sigma0'
    elif ard['product type'] == 'GTC-gamma0':
        calibrate_to = 'gamma0'
    elif ard['product type'] == 'RTC-gamma0':
        calibrate_to = 'beta0'
       
    calibrated = opj(temp_dir, '{}_cal'.format(file_id))
    logfile = opj(output_dir, '{}.Calibration.errLog'.format(file_id))
    return_code = common._calibration(infile, calibrated, logfile, calibrate_to)
    
    # delete input
    h.delete_dimap(infile[:-4])
    
    # delete output if command failed for some reason and return
    if return_code != 0:
        h.delete_dimap(calibrated)
        return return_code
    
    # input for next step
    infile = '{}.dim'.format(calibrated)
    
    # ---------------------------------------------------------------------
    # 4 Multi-looking
    if int(ard['resolution']) >= 20:
        # calculate the multi-look factor
        ml_factor = int(int(ard['resolution']) / 10)
        
        multi_looked = opj(temp_dir, '{}_ml'.format(file_id))
        logfile = opj(output_dir, '{}.multilook.errLog'.format(file_id))
        return_code = common._multi_look(infile, multi_looked, logfile,
                                         ml_factor, ml_factor)
        
        # delete input
        h.delete_dimap(infile[:-4])
        
        # delete output if command failed for some reason and return
        if return_code != 0:
            h.delete_dimap(multi_looked)
            return return_code
            
        # define input for next step
        infile = '{}.dim'.format(multi_looked)
    
    # ---------------------------------------------------------------------
    # 5 Layover shadow mask
    if  ard['create ls mask'] is True:
        ls_mask = opj(temp_dir, '{}.ls_mask'.format(file_id))
        logfile = opj(output_dir, '{}.ls_mask.errLog'.format(file_id))
        return_code = common._ls_mask(infile, ls_mask, logfile, ard['resolution'],
                                      ard['dem'])
        
        # delete output if command failed for some reason and return
        if return_code != 0:
            h.delete_dimap(ls_mask)
            return return_code

        # last check on ls data
        return_code = h.check_out_dimap(ls_mask, test_stats=False)
        if return_code != 0:
            h.delete_dimap(ls_mask)
            return return_code
        
        # move to final destination
        out_ls_mask = opj(output_dir, '{}.LS'.format(file_id))

        # delete original file sin case they exist
        if os.path.exists(str(out_ls_mask) + '.dim'):
            h.delete_dimap(out_ls_mask)

        # move out of temp
        shutil.move('{}.dim'.format(ls_mask), '{}.dim'.format(out_ls_mask))
        shutil.move('{}.data'.format(ls_mask), '{}.data'.format(out_ls_mask))
        
    # ---------------------------------------------------------------------
    # 6 Speckle filtering
    if ard['remove speckle']:
        
        logfile = opj(output_dir, '{}.Speckle.errLog'.format(file_id))
        filtered = opj(temp_dir, '{}_spk'.format(file_id))

        # run processing
        return_code = common._speckle_filter(infile, filtered, logfile,
                                             ard['speckle filter'])
        
        # delete input
        h.delete_dimap(infile[:-4])
        
        # delete output if command failed for some reason and return
        if return_code != 0:
            h.delete_dimap(filtered)
            return return_code
       
        # define input for next step
        infile = '{}.dim'.format(filtered)
        
    # ---------------------------------------------------------------------
    # 7 Terrain flattening
    if ard['product type'] == 'RTC-gamma0':
        flattened = opj(temp_dir, '{}_flat'.format(file_id))
        logfile = opj(output_dir, '{}.tf.errLog'.format(file_id))
        return_code = common._terrain_flattening(infile, flattened, logfile,
                                                 ard['dem']
                                                 )
        
        # delete input file
        h.delete_dimap(infile[:-4])
        
        # delete output if command failed for some reason and return
        if return_code != 0:
            h.delete_dimap(flattened)
            return return_code
        
        # define input for next step
        infile = '{}.dim'.format(flattened)

    # ---------------------------------------------------------------------
    # 8 Linear to db
    if ard['to db']:
        db_scaled = opj(temp_dir, '{}_db'.format(file_id))
        logfile = opj(output_dir, '{}.db.errLog'.format(file_id))
        return_code = common._linear_to_db(infile, db_scaled, logfile)
        
        # delete input file
        h.delete_dimap(infile[:-4])
        
        # delete output if command failed for some reason and return
        if return_code != 0:
            h.delete_dimap(db_scaled)
            return return_code
        
        # set input for next step
        infile = '{}.dim'.format(db_scaled)

    # ---------------------------------------------------------------------
    # 9 Geocoding
    geocoded = opj(temp_dir, '{}_bs'.format(file_id))
    logfile = opj(output_dir, '{}_bs.errLog'.format(file_id))
    return_code = common._terrain_correction(
        infile, geocoded, logfile, ard['resolution'], ard['dem']
    )
    
    # delete input file
    h.delete_dimap(infile[:-4])
    
    # delete output if command failed for some reason and return
    if return_code != 0:
        h.delete_dimap(geocoded)
        return return_code

    # define final destination
    out_final = opj(output_dir, '{}.bs'.format(file_id))

    # ---------------------------------------------------------------------
    # 10 Checks and move to output directory
    # remove output file if exists
    if os.path.exists(out_final + '.dim'):
        h.delete_dimap(out_final)   
    
    # check final output
    return_code = h.check_out_dimap(geocoded)
    if return_code != 0:
        h.delete_dimap(geocoded)
        return return_code
    
    # move to final destination
    shutil.move('{}.dim'.format(geocoded), '{}.dim'.format(out_final))
    shutil.move('{}.data'.format(geocoded), '{}.data'.format(out_final))

    # write processed file to keep track of files already processed
    with open(opj(output_dir, '.processed'), 'w') as file:
        file.write('passed all tests \n')
            

def ard_to_rgb(infile, outfile, driver='GTiff', to_db=True):

    prefix = glob.glob(os.path.abspath(infile[:-4]) + '*data')[0]

    if len(glob.glob(opj(prefix, '*VV*.img'))) == 1:
        co_pol = glob.glob(opj(prefix, '*VV*.img'))[0]

    if len(glob.glob(opj(prefix, '*VH*.img'))) == 1:
        cross_pol = glob.glob(opj(prefix, '*VH*.img'))[0]

    if len(glob.glob(opj(prefix, '*HH*.img'))) == 1:
        co_pol = glob.glob(opj(prefix, '*HH*.img'))[0]

    if len(glob.glob(opj(prefix, '*HV*.img'))) == 1:
        cross_pol = glob.glob(opj(prefix, '*HV*.img'))[0]

    # !!!!assure and both pols exist!!!
    with rasterio.open(co_pol) as co:

        # get meta data
        meta = co.meta

        # update meta
        meta.update(driver=driver, count=3, nodata=0)

        with rasterio.open(cross_pol) as cr:

            # !assure that dimensions match ####
            with rasterio.open(outfile, 'w', **meta) as dst:

                if co.shape != cr.shape:
                    print(' dimensions do not match')
                # loop through blocks
                for i, window in co.block_windows(1):

                    # read arrays and turn to dB (in case it isn't)
                    co_array = co.read(window=window)
                    cr_array = cr.read(window=window)

                    if to_db:
                        # turn to db
                        co_array = ras.convert_to_db(co_array)
                        cr_array = ras.convert_to_db(cr_array)

                        # adjust for dbconversion
                        co_array[co_array == -130] = 0
                        cr_array[cr_array == -130] = 0

                    # turn 0s to nan
                    co_array[co_array == 0] = np.nan
                    cr_array[cr_array == 0] = np.nan

                    # create log ratio by subtracting the dbs
                    ratio_array = np.subtract(co_array, cr_array)

                    # write file
                    for k, arr in [(1, co_array), (2, cr_array),
                                   (3, ratio_array)]:
                        dst.write(arr[0, ], indexes=k, window=window)


def ard_to_thumbnail(infile, outfile, driver='JPEG', shrink_factor=25,
                     to_db=True):

    prefix = glob.glob(os.path.abspath(infile[:-4]) + '*data')[0]

    if len(glob.glob(opj(prefix, '*VV*.img'))) == 1:
        co_pol = glob.glob(opj(prefix, '*VV*.img'))[0]

    if len(glob.glob(opj(prefix, '*VH*.img'))) == 1:
        cross_pol = glob.glob(opj(prefix, '*VH*.img'))[0]

    if len(glob.glob(opj(prefix, '*HH*.img'))) == 1:
        co_pol = glob.glob(opj(prefix, '*HH*.img'))[0]

    if len(glob.glob(opj(prefix, '*HV*.img'))) == 1:
        cross_pol = glob.glob(opj(prefix, '*HV*.img'))[0]

    # !!!assure and both pols exist
    with rasterio.open(co_pol) as co:

        # get meta data
        meta = co.meta

        # update meta
        meta.update(driver=driver, count=3, dtype='uint8')

        with rasterio.open(cross_pol) as cr:

            # !!!assure that dimensions match ####
            new_height = int(co.height/shrink_factor)
            new_width = int(co.width/shrink_factor)
            out_shape = (co.count, new_height, new_width)

            meta.update(height=new_height, width=new_width)

            if co.shape != cr.shape:
                print(' dimensions do not match')

            # read arrays and turn to dB

            co_array = co.read(out_shape=out_shape, resampling=5)
            cr_array = cr.read(out_shape=out_shape, resampling=5)

            if to_db:
                co_array = ras.convert_to_db(co_array)
                cr_array = ras.convert_to_db(cr_array)

            co_array[co_array == 0] = np.nan
            cr_array[cr_array == 0] = np.nan

            # create log ratio
            ratio_array = np.subtract(co_array, cr_array)

            r = ras.scale_to_int(co_array, -20, 0, 'uint8')
            g = ras.scale_to_int(cr_array, -25, -5, 'uint8')
            b = ras.scale_to_int(ratio_array, 1, 15, 'uint8')

            with rasterio.open(outfile, 'w', **meta) as dst:

                for k, arr in [(1, r), (2, g), (3, b)]:
                    dst.write(arr[0, ], indexes=k)


if __name__ == "__main__":

    import argparse

    # write a description
    descript = """
               This is a command line client for the creation of
               Sentinel-1 ARD data from Level 1 GRD products

               Output is a terrain corrected product that is
               calibrated to
                    - Gamma nought (corrected for slopes)
                    - Gamma nought (corrected for ellipsoid)
                    - Sigma nought (corrected for flat terrain)
               """

    epilog = """
             Example:
             grd_to_ard.py -i /path/to/scene -r 20 -p RTC -l True -s False
                        -t /path/to/tmp -o /path/to/search.shp
             """
    # create a parser
    parser = argparse.ArgumentParser(description=descript, epilog=epilog)

    # search paramenters
    parser.add_argument("-i", "--input",
                        help=' path to one or more consecutive slices'
                             ' (given comma separated list)',
                        required=True, default=None)
    parser.add_argument("-r", "--resolution",
                        help=" The output resolution in meters",
                        default=20)
    parser.add_argument("-p", "--producttype",
                        help=" The Product Type (RTC, GTCgamma, GTCsigma) ",
                        default='GTCgamma')
    parser.add_argument("-l", "--layover",
                        help=" generation of layover/shadow mask (True/False)",
                        default=True)
    parser.add_argument("-s", "--speckle",
                        help=" speckle filtering (True/False) ",
                        default=False)
    parser.add_argument("-t", "--tempdir",
                        help=" temporary directory (/path/to/temp) ",
                        default='/tmp')
    # output parameters
    parser.add_argument("-o", "--output",
                        help=' Output file in BEAM-dimap format. This should'
                             ' only be the prefix, since the workflow will'
                             ' add the file suffixes on its own.',
                        required=True)

    args = parser.parse_args()

    # create args for grd_to_ard
    infiles = args.input.split(',')
    output_dir = os.path.dirname(args.output)
    file_id = os.path.basename(args.output)

    # execute processing
    grd_to_ard(infiles, output_dir, file_id, args.tempdir,
               int(args.resolution), args.producttype, args.ls_mask,
               args.speckle_filter)
