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
    sliceAssembly:
        creates an urllib opener object for authentication on scihub server
    grdFrameImport:
        gets the next page from a multi-page result from a scihub search
    grdRemoveBorder:
        creates a string in the Open Search format that is added to the
        base scihub url
    grdBackscatter:
        applies the search and writes the reults in a Geopandas GeoDataFrame
    grdSpkFlt:
        applies the Lee-Sigma filter with SNAP standard parameters
    grdLSmap:
        writes the search result into an ESRI Shapefile
    grdTC:
        writes the search result into a PostGreSQL/PostGIS Database

------------------
Main function
------------------
  grd2ard:
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

python3 grd2ard.py -p /path/to/scene -r 20 -p RTC -l True -s False
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
import imp
import glob
import shutil
import time
#import pkg_resources
import numpy as np
import gdal

from pathlib import Path
from ost.helpers import helpers as h, raster as ras

# script infos
__author__ = 'Andreas Vollrath'
__copyright__ = 'phi-lab, European Space Agency'
__license__ = 'GPL'
__version__ = '1.0'
__maintainer__ = 'Andreas Vollrath'
__email__ = ''
__status__ = 'Production'

# get the SNAP CL executable
#global gpt_file
#gpt_file = h.getGPT()

# define the resource package for getting the xml workflow files
#global package
#package = 'ost'


def sliceAssembly(fileList, outFile, logFile, polar='VV,VH,HH,HV'):
    '''
    This function assembles consecutive frames acquired at the same date.
    Can be either GRD or SLC products

    :param fileList: a list of Sentinel-1 product slices to be assembled
    :param outFile: the assembled file
    :return:
    '''

    gpt_file = h.getGPT()
    print(" INFO: Assembling consecutive frames:")
    #print([file for file in os.path.basename(fileList)])
    sliceAssemblyCmd = '{} SliceAssembly -x -q {} -PselectedPolarisations={} \
                       -t \'{}\' {}'.format(gpt_file, os.cpu_count(), polar,
                                        outFile, fileList)

    rc = h.runCmd(sliceAssemblyCmd, logFile)

    if rc == 0:
        print(' INFO: Succesfully assembled products')
    else:
        print(' ERROR: Slice Assembly exited with an error. \
                See {} for Snap Error output'.format(logFile))
        sys.exit(101)


def grdSubsetRegion(inFile, outFile, logFile, region):
    
    # get Snap's gpt file
    gpt_file = h.getGPT()
    region = ','.join([str(int(x)) for x in region])
    # extract window from scene
    subsetCmd = '{} Subset -x -q {} -Pregion={} -t \'{}\'\
                 \'{}\''.format(gpt_file, os.cpu_count(), region, outFile, inFile)

    rc = h.runCmd(subsetCmd, logFile)
    rc=0
    if rc == 0:
        print(' INFO: Succesfully subsetted product')
    else:
        print(' ERROR: Subsetting exited with an error. \
                See {} for Snap Error output'.format(logFile))
        sys.exit(107)


def grdFrameImport(inFile, outFile, logFile, polar='VV,VH,HH,HV'):
    '''
    Import a single S1 GRD acquisition

    :param inFile: original file (zip or manifest) or swath assembled product
    :param outFile:
    :return:
    '''

    
    print(' INFO: Importing {} by applying precise orbit file and'
          ' removing thermal noise'.format(os.path.basename(inFile)))

    # get gpt file
    gpt_file = h.getGPT()
    
    # get path to graph
    rootPath = str(Path(imp.find_module('ost')[1]))
    graph = str(Path('{}/graphs/S1_GRD2ARD/1_AO_TNR.xml'.format(rootPath)))
    
    # construct command
    frameImportCmd = '{} {} -x -q {} -Pinput=\'{}\' -Ppolar={} \
                      -Poutput=\'{}\''.format(gpt_file, graph, os.cpu_count(),
                                          inFile, polar, outFile)
    
    # run command
    rc = h.runCmd(frameImportCmd, logFile)

    # handle errors and logs
    if rc == 0:
        print(' INFO: Succesfully imported product')
    else:
        print(' ERROR: Frame import exited with an error. \
                See {} for Snap Error output'.format(logFile))
        sys.exit(102)


def grdRemoveBorder(inFile):
    '''
    This is a custom routine to remove GRD border noise
    from Sentinel-1 GRD products. It works on the original intensity
    images.

    NOTE: For the common dimap format, the inFile needs to be the
    ENVI style file inside the *data folder.

    The routine checks the outer 3000 columns for its mean value.
    If the mean value is below 100, all values will be set to 0,
    otherwise the routine will stop, assuming all columns towards
    the inner image are valid.


    :param inFile: gdal compatible intensity file of Sentinel-1
    :return:
    '''

    print(' INFO: Removing the GRD Border Noise.')
    currtime = time.time()

    # read raster file and get number of columns adn rows
    raster = gdal.Open(inFile, gdal.GA_Update)
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
    #print(' INFO: Total amount of columns: {}'.format(cols_left))
    #print(' INFO: Number of colums set to 0 on the left side: '
    #     ' {}'.format(cols_left))

    #raster.GetRasterBand(1).WriteArray(array_left[:, :+cols_left], 0, 0, 1)
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
    #print(' INFO: Number of columns set to 0 on the'
    #     ' right side: {}'.format(3000 - cols_right))
    #print(' INFO: Amount of columns kept: {}'.format(col_right_start))
    raster.GetRasterBand(1).WriteArray(array_right[:, cols_right:],
                                       col_right_start, 0)
    array_right = None
    h.timer(currtime)


def grdBackscatter(inFile, outFile, logFile, prType='GTCgamma'):
    '''
    This function is a wrapper for the calibration of Sentinel-1
    GRD backscatter data. 3 different calibration modes are supported.
        - Radiometrically terrain corrected Gamma nought (RTC)
        - ellipsoid based Gamma nought (GTCgamma)
        - Sigma nought (GTCsigma).

    :param inFile: an imported Sentinel-1 file
    :param outFile: the calibrated file
    :param prType: the product type (RTC, GTCgamma or GTCsigma)
    :return:
    '''

    # get gpt file
    gpt_file = h.getGPT()
    
    # get path to graph
    rootPath = str(Path(imp.find_module('ost')[1]))
    
    if prType == 'RTC':
        print(' INFO: Calibrating the product to a RTC product.')
        #graph = ('/'.join(('graphs', 'S1_GRD2ARD', '2_CalBeta_TF.xml')))
        graph = str(Path('{}/graphs/S1_GRD2ARD/2_CalBeta_TF.xml'.format(rootPath)))
    elif prType == 'GTCgamma':
        print(' INFO: Calibrating the product to a GTC product (Gamma0).')
        #graph = ('/'.join(('graphs', 'S1_GRD2ARD', '2_CalGamma.xml')))
        graph = str(Path('{}/graphs/S1_GRD2ARD/2_CalGamma.xml'.format(rootPath)))
    elif prType == 'GTCsigma':
        print(' INFO: Calibrating the product to a GTC product (Sigma0).')
        #graph = ('/'.join(('graphs', 'S1_GRD2ARD', '2_CalSigma.xml')))
        graph = str(Path('{}/graphs/S1_GRD2ARD/2_CalSigma.xml'.format(rootPath)))
    else:
        print(' ERROR: Wrong product type selected.')
        exit
    # create system command
    #graph = pkg_resources.resource_filename(package, graph)
    # construct command sring
    calibrationCmd = '{} {} -x -q {} -Pinput=\'{}\' \
                      -Poutput=\'{}\''.format(gpt_file, graph, 2 * os.cpu_count(),
                                          inFile, outFile)
    # run command
    rc = h.runCmd(calibrationCmd, logFile)

    # handle errors and logs
    if rc == 0:
        print(' INFO: Succesfully calibrated product')
    else:
        print(' ERROR: Backscatter calibration exited with an error. \
                See {} for Snap Error output'.format(logFile))
        sys.exit(103)


def grdSpkFlt(inFile, outFile, logFile):

    
    # get gpt file
    gpt_file = h.getGPT()
    
    print(" INFO: Applying the Lee-Sigma Speckle Filter")
    # contrcut command string
    spkCmd = '{} Speckle-Filter -x -q {} -PestimateENL=true \
              -t \'{}\' \'{}\''.format(gpt_file, 2 * os.cpu_count(), 
                                       outFile, inFile)
    
    # run command
    rc = h.runCmd(spkCmd, logFile)

    # hadle errors and logs
    if rc == 0:
        print(' INFO: Succesfully imported product')
    else:
        print(' ERROR: Speckle Filtering exited with an error. \
                See {} for Snap Error output'.format(logFile))
        sys.exit(111)


def grdLSMap(inFile, lsFile, logFile, resol):
    '''

    '''
    
    # get gpt file
    gpt_file = h.getGPT()
    
    # get path to graph
    rootPath = str(Path(imp.find_module('ost')[1]))
    
    print(" INFO: Creating the Layover/Shadow mask")
    #graph = ('/'.join(('graphs', 'S1_GRD2ARD', '3_LSmap.xml')))
    #graph = pkg_resources.resource_filename(package, graph)
    
    # get path to workflow xml
    graph = str(Path('{}/graphs/S1_GRD2ARD/3_LSmap.xml'.format(rootPath)))
    
    # construct command string
    lsCmd = '{} {} -x -q {} -Pinput=\'{}\' -Presol={} \
             -Poutput=\'{}\''.format(gpt_file, graph, 2 * os.cpu_count(), inFile,
                                 resol, lsFile)
             
    # run command
    rc = h.runCmd(lsCmd, logFile)

    # handle errors and logs
    if rc == 0:
        print(' INFO: Succesfully create a layover shadow mask')
    else:
        print(' ERROR: Lazover&Shadow Mask creation exited with an error. \
                See {} for Snap Error output'.format(logFile))
        sys.exit(112)


def grdTC(inFile, outFile, logFile, resol):
    '''

    '''

    # get gpt file
    gpt_file = h.getGPT()
    
    # get path to graph
    rootPath = str(Path(imp.find_module('ost')[1]))
    print(" INFO: Geocoding the calibrated product")
    # calculate the multi-look factor
    mlFactor = int(int(resol) / 10)

    #graph = ('/'.join(('graphs', 'S1_GRD2ARD', '3_ML_TC.xml')))
    #graph = pkg_resources.resource_filename(package, graph)
    graph = str(Path('{}/graphs/S1_GRD2ARD/3_ML_TC.xml'.format(rootPath)))
    
    # construct command string
    geocodeCmd = '{} {} -x -q {} -Pinput=\'{}\' -Presol={} -Pml={} \
                 -Poutput=\'{}\''.format(gpt_file, graph, 2 * os.cpu_count(),
                                     inFile, resol, mlFactor, outFile)
    
    # run command
    rc = h.runCmd(geocodeCmd, logFile)

    # handle errors and logs
    if rc == 0:
        print(' INFO: Succesfully imported product')
    else:
        print(' ERROR: Frame import exited with an error. \
                See {} for Snap Error output'.format(logFile))
        sys.exit(112)


def grd2Ard(fileList, outDir, fileID, tmpDir, outResolution,
            prdType='GTCgamma', lsMap=True, spkFlt=True, polar='VV,VH,HH,HV'):
    '''
    :param fileList: must be a list object with one or more absolute
           paths to GRD scene(s)
    :param outDir: the folder where the output file should be written#
    :param fileID: prefix of the final output file
    :param tmpDir: we need a folder for temp files
    :param outResolution: the resolution of the output product in meters
    :param lsMap: layover/shadow mask generation (Boolean)
    :param spkFlt: speckle filtering (Boolean)
    :return: no explicit return value, since output file is our actual return
    '''
    
    # convert into Path objects
    tmpDir = Path(tmpDir)
    outDir = Path(outDir)
    
    # slice assembly if more than one scene
    if len(fileList) > 1:

        for file in fileList:
        
            file = str(file)
            grdImport = str(tmpDir / '{}_imported'.format(os.path.basename(file)[:-5]))
            logFile = str(outDir / '{}.Import.errLog'.format(os.path.basename(file)[:-5]))
            grdFrameImport(str(file), grdImport, logFile)

        # create list of scenes for full acquisition in preparation of slice assembly
        #sceneList = ' '.join(glob.glob('{}/*imported.dim'.format(tmpDir)))
        sceneList = ' '.join([str(x) for x in tmpDir.glob('*imported.dim')])
        
        # create file strings
        grdImport = str(tmpDir / '{}_imported'.format(fileID))
        logFile = str(outDir / '{}.SliceAssembly.errLog'.format(fileID))
        sliceAssembly(sceneList, grdImport, logFile)
    
        for file in fileList:
            h.delDimap(str(tmpDir / '{}_imported'.format(os.path.basename(str(file))[:-5])))
    
    else:
        grdImport = str(tmpDir / '{}_imported'.format(fileID))
        logFile = str(outDir / '{}.Import.errLog'.format(fileID))
        grdFrameImport(fileList[0], grdImport, logFile) 
        
    # ---------------------------------------------------------------------
    # Remove the grd border noise from existent channels (OST routine)
    for p in ['VV', 'VH', 'HH', 'HV']:

        inFile = tmpDir / '{}_imported.data/Intensity_{}.img'.format(fileID, p)
        if inFile.is_file():
            # run grd Border Remove
            grdRemoveBorder(str(inFile))
    
    # -------------------------------------------
    # in case we want to apply Speckle filtering
    if spkFlt is True:
        inFile = str(tmpDir / '{}_imported.dim'.format(fileID))
        logFile = str(outDir / '{}.Speckle.errLog'.format(fileID))
        outFile =  str(tmpDir / '{}_imported_spk'.format(fileID))

        # run processing
        grdSpkFlt(inFile, outFile, logFile)

        # define inFile for next processing step
        inFile = str(tmpDir / '{}_imported_spk.dim'.format(fileID))
        h.delDimap(str(tmpDir / '{}_imported'.format(fileID)))

    else:
        # let's calibrate the data
        inFile = str(tmpDir / '{}_imported.dim'.format(fileID))

    # ----------------------
    # do the calibration
    outFile = str(tmpDir / '{}.{}'.format(fileID, prdType))
    logFile = str(outDir / '{}.Backscatter.errLog'.format(fileID))
    grdBackscatter(inFile, outFile, logFile, prdType)

    dataDir = glob.glob('{}/{}*imported*.data'.format(str(tmpDir), fileID))
    h.delDimap(str(dataDir[0])[:-5])
    
    # -----------------------
    # let's geocode the data
    inFile = str(tmpDir / '{}.{}.dim'.format(fileID, prdType))
    outFile = str(tmpDir / '{}.{}.TC'.format(fileID, prdType))
    logFile = str(outDir / '{}.TC.errLog'.format(fileID))
    grdTC(inFile, outFile, logFile, outResolution)

    # move to final destination
    outFinal = str(outDir / '{}.{}.TC'.format(fileID, prdType))

    # remove file if exists
    if os.path.exists(str(outFinal) + '.dim'):
        h.delDimap(outFinal)
    
    shutil.move('{}.dim'.format(outFile), '{}.dim'.format(outFinal))
    shutil.move('{}.data'.format(outFile), '{}.data'.format(outFinal))

     # ----------------------------------------------
    # let's create a Layover shadow mask if needed
    if lsMap is True:
        outFile = str(tmpDir / '{}.lsmap'.format(fileID))
        logFile = str(outDir / '{}.lsmap.errLog'.format(fileID))
        grdLSMap(inFile, outFile, logFile, outResolution)

        # move to final destination
        outFinalLs = str(outDir / '{}.LS'.format(fileID))
        
        # delete original file sin case they exist
        if os.path.exists(str(outFinalLs) + '.dim'):
            h.delDimap(outFinalLs)
        
        # move out of temp
        shutil.move('{}.dim'.format(outFile), '{}.dim'.format(outFinalLs))
        shutil.move('{}.data'.format(outFile), '{}.data'.format(outFinalLs))
    
    # remove calibrated files
    h.delDimap(str(tmpDir / '{}.{}'.format(fileID, prdType)))


def grd2ardOld(fileList, outDir, fileID, tmpDir, outResolution,
            prdType='GTCgamma', lsMap=True, spkFlt=True, polar='VV,VH,HH,HV'):
    '''
    :param fileList: must be a list object with one or more absolute
           paths to GRD scene(s)
    :param outDir: the folder where the output file should be written
    :param tmpDir: we need a folder for temp files
    :param outResolution: the resolution of the output product in meters
    :param lsMap: layover/shadow mask generation (Boolean)
    :param spkFlt: speckle filtering (Boolean)
    :return: none
    '''

    # create import file name and translate list into string
    grdImport = '{}/{}.imported'.format(tmpDir, fileID)
    sceneList = '{}'.format(' '.join(fileList))

    # --------------------------------------------------------
    # do the slice assembly if there is more than one product
    if len(sceneList.split(' ')) > 1:

        grdFile = '{}/{}_assembled'.format(tmpDir, fileID)
        logFile = '{}/{}.SliceAssembly.errLog'.format(outDir, fileID)
        sliceAssembly(sceneList, grdFile, logFile)

        logFile = '{}/{}.Import.errLog'.format(outDir, fileID)
        grdFrameImport('{}.dim'.format(grdFile), grdImport, logFile)
        # remove assembled files
        os.remove('{}.dim'.format(grdFile))
        shutil.rmtree('{}.data'.format(grdFile))
    else:
        grdFile = sceneList
        logFile = '{}/{}.Import.errLog'.format(outDir, fileID)
        grdFrameImport(grdFile, grdImport, logFile)

    # ---------------------------------------------------------------------
    # Remove the grd border noise from existent channels (phiSAR routine)
    for p in ['VV', 'VH', 'HH', 'HV']:

        if os.path.exists('{}/{}.imported.data/Intensity_{}.img'.format(tmpDir,
                          fileID, p)):

            inFile = '{}/{}.imported.data/Intensity_{}.img'.format(tmpDir,
                                                                   fileID, p)
            grdRemoveBorder(inFile)

    # -------------------------------------------
    # in case we want to apply Speckle filtering
    if spkFlt is True:
        inFile = '{}/{}_imported.dim'.format(tmpDir, fileID)
        logFile = '{}/{}.Speckle.errLog'.format(outDir, fileID)
        outFile = '{}/{}_imported_spk'.format(tmpDir, fileID)

        # run processing
        grdSpkFlt(inFile, outFile, logFile)

        # define inFile for next processing step
        inFile = '{}/{}_imported_spk.dim'.format(tmpDir, fileID)

        os.remove('{}/{}_imported.dim'.format(tmpDir, fileID))
        shutil.rmtree('{}/{}_imported.data'.format(tmpDir, fileID))
    else:
        # let's calibrate the data
        inFile = '{}/{}_imported.dim'.format(tmpDir, fileID)

    # ----------------------
    # do the calibration
    outFile = '{}/{}.{}'.format(tmpDir, fileID, prdType)
    logFile = '{}/{}.Backscatter.errLog'.format(outDir, fileID)
    grdBackscatter(inFile, outFile, logFile, prdType)

    dimFile = glob.glob('{}/{}*imported*.dim'.format(tmpDir, fileID))
    dataDir = glob.glob('{}/{}*imported*.data'.format(tmpDir, fileID))
    os.remove(dimFile[0])
    shutil.rmtree(dataDir[0])

    # -----------------------
    # let's geocode the data
    inFile = '{}/{}.{}.dim'.format(tmpDir, fileID, prdType)
    outFile = '{}/{}.{}.TC'.format(tmpDir, fileID, prdType)
    logFile = '{}/{}.TC.errLog'.format(outDir, fileID)
    grdTC(inFile, outFile, logFile, outResolution)

    # move to final destination
    outFinal = '{}/{}.{}.TC'.format(outDir, fileID, prdType)
    shutil.move('{}.dim'.format(outFile), '{}.dim'.format(outFinal))
    shutil.move('{}.data'.format(outFile), '{}.data'.format(outFinal))

    # ----------------------------------------------
    # let's create a Layover shadow mask if needed
    if lsMap is True:
        outFile = '{}/{}.lsmap'.format(tmpDir, fileID)
        logFile = '{}/{}.lsmap.errLog'.format(outDir, fileID)
        grdLSMap(inFile, outFile, logFile, outResolution)

    # move to final destination
    outFinalLs = '{}/{}.LS'.format(outDir, fileID)
    shutil.move('{}.dim'.format(outFile), '{}.dim'.format(outFinalLs))
    shutil.move('{}.data'.format(outFile), '{}.data'.format(outFinalLs))

    # remove calibrated files
    os.remove('{}/{}.{}.dim'.format(tmpDir, fileID, prdType))
    shutil.rmtree('{}/{}.{}.data'.format(tmpDir, fileID, prdType))


def grd2RGB(inDir, fileName, outDir):

    VV = glob.glob('{}/{}*.data/*VV.img'.format(inDir, fileName))
    VH = glob.glob('{}/{}*.data/*VH.img'.format(inDir, fileName))

    newRasterVV = '{}/{}.VV.tif'.format(outDir, fileName)
    newRasterVH = '{}/{}.VH.tif'.format(outDir, fileName)
    newRasterVVVH = '{}/{}.VVVH.tif'.format(outDir, fileName)

    geo_list = ras.readFile(VV[0])

    ras.createFile(newRasterVV, geo_list, 1)
    ras.createFile(newRasterVH, geo_list, 1)
    ras.createFile(newRasterVVVH, geo_list, 1)

    rasterVV = gdal.Open(VV[0])
    rasterVH = gdal.Open(VH[0])

    x_block_size = 128
    y_block_size = 128

    # Get image sizes
    cols = rasterVV.RasterXSize
    rows = rasterVV.RasterYSize

    for y in range(0, rows, y_block_size):
        if y + y_block_size < rows:
            ysize = y_block_size
        else:
            ysize = rows - y

        # loop throug x direction
        for x in range(0, cols, x_block_size):
            if x + x_block_size < cols:
                xsize = x_block_size
            else:
                xsize = cols - x

            dBArrayVV = np.empty((rasterVV.RasterCount, ysize, xsize),
                                 dtype=np.float32)
            rasterArrayVV = np.array(rasterVV.GetRasterBand(1).ReadAsArray(x,
                                     y, xsize, ysize))
            dBArrayVV = ras.convert2DB(rasterArrayVV)
            dBArrayVV[dBArrayVV == -130] = 0

            dBArrayVH = np.empty((rasterVV.RasterCount, ysize, xsize),
                                 dtype=np.float32)
            rasterArrayVH = np.array(rasterVH.GetRasterBand(1).ReadAsArray(x,
                                     y, xsize, ysize))
            dBArrayVH = ras.convert2DB(rasterArrayVH)
            dBArrayVH[dBArrayVH == -130] = 0

            arrayVVVH = np.empty((rasterVV.RasterCount, ysize, xsize),
                                 dtype=np.float32)
            arrayVVVH = np.divide(rasterArrayVV.clip(min=0.0000001),
                                  rasterArrayVH.clip(min=0.0000001))
            arrayVVVH[arrayVVVH == 1] = 0

            ras.chunk2Raster(newRasterVV, dBArrayVV, 0, x, y, 1)
            ras.chunk2Raster(newRasterVH, dBArrayVH, 0, x, y, 1)
            ras.chunk2Raster(newRasterVVVH, arrayVVVH, 0, x, y, 1)

    cmd = 'gdalbuildvrt -separate -srcnodata 0 {}/{}.vrt \
           {} {} {}'.format(outDir, fileName, newRasterVV,
                            newRasterVH, newRasterVVVH)
    os.system(cmd)


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
             grd2ard.py -i /path/to/scene -r 20 -p RTC -l True -s False
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

    # create args for grd2ard
    inFiles = args.input.split(',')
    outDir = os.path.dirname(args.output)
    fileID = os.path.basename(args.output)

    # execute processing
    grd2Ard(inFiles, outDir, fileID, args.tempdir, int(args.resolution),
            args.producttype, args.lsmap, args.spkFlt)
