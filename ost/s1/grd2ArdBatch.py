#! /usr/bin/env python3
# -*- coding: utf-8 -*-

'''
This script allows to produce Sentinel-1 backscatter ARD data 
from a set of different GRD products. 
The script allows to process consecutive frames from one acquisition and 
outputs a single file.


----------------
Functions:
----------------
    grd2ArdBatch:
        processes all acquisitions
    ard2Ts:
        processes all time-series

------------------
Main function
------------------
  grd2Ts:
    handles the whole workflow

------------------
Contributors
------------------

Andreas Vollrath, ESA phi-lab
-----------------------------------
November 2018: Original implementation
                
------------------
Usage
------------------

python3 grd2ArdBatch.py -i /path/to/inventory -r 20 -p RTC -l True -s False
                   -t /path/to/tmp -o /path/to/output

    -i    defines the path to one or a list of consecutive slices
    -r    resolution in meters (should be 10 or more, default=20)
    -p    defines the product type (GTCsigma, GTCgamma, RTC, default=GTCgamma)
    -l    defines the layover/shadow mask creation (True/False, default=True)
    -s    defines the speckle filter (True/False, default=False)
    -t    defines the folder for temporary products (default=/tmp)
    -o    defines the /path/to/the/output
'''

import os
import shutil
import glob
import datetime
from osgeo import gdal

from ost.helpers import raster as ras
from ost.s1.metadata import s1Metadata
from ost.s1 import refine, grd2Ard, ts


def grd2ArdBatch(inputDf, dwnDir, prcDir, tmpDir, outResolution, 
                 prdType, lsMap, spkFlt):
    
    # we create a processing dictionary, 
    # where all frames are grouped into acquisitions
    procDict = refine.createProcDict(inputDf)

    for track, allScenes in procDict.items():
        for sceneList in procDict[track]:

                # get acquisition date
                acqDate = s1Metadata(sceneList[0]).start_date
                # create a subdirectory baed on acq. date
                outDir = '{}/{}/{}'.format(prcDir, track, acqDate)
                os.makedirs(outDir, exist_ok=True)
                
                # get the paths to the file
                scenePaths = ([s1Metadata(i).s1DwnPath(dwnDir) 
                               for i in sceneList])

                # apply the grd2ard function
                grd2Ard.grd2Ard(scenePaths, outDir, acqDate, tmpDir, 
                                outResolution, prdType, lsMap, spkFlt)
                
                
def ard2Ts(inputDf, prjDir, tmpDir, toDB, dType, lsMap=True, mtSpkFlt=True):
    
    # 1) we convert input to a geopandas GeoDataFrame object
    procDict = refine.createProcDict(inputDf)
    vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
        
    for track, allScenes in procDict.items():

        trackDir = '{}/{}'.format(prjDir, track)
        
        # 1) get minimum valid extent (i.e. assure value fo each pixel throughout the whole time-series)
        print(' INFO: Calculating the minimum extent.')
        listOfScenes = glob.glob('{}/*/*data/*img'.format(trackDir))
        listOfScenes = [x for x in listOfScenes if not 'layover' in x] # exclude the layovers and create a string
        gdal.BuildVRT('{}/extent.vrt'.format(tmpDir), listOfScenes, options=vrt_options)
        ras.outline('{}/extent.vrt'.format(tmpDir), '{}/extent.shp'.format(tmpDir), 0, True)

        # create a list of dimap files and format to comma-separated list
        dimListTC = sorted(glob.glob('{}/20*/*TC*dim'.format(trackDir)))
        dimListTC = '\'{}\''.format(','.join(dimListTC))

        for p in ['VV', 'VH', 'HH', 'HV']:

            # check if polarisation is existent
            polListTC = sorted(glob.glob('{}/20*/*TC*data/Gamma0*{}*.img'.format(trackDir, p)))

            if len(polListTC) >= 2:

                # create output stack name for RTC
                tmpStack = '{}/stack_{}_{}'.format(tmpDir, track, p)
                outStack = '{}/mt_stack_{}_{}'.format(tmpDir, track, p)

                os.makedirs('{}/Timeseries/'.format(trackDir), exist_ok=True)
                logFile = '{}/Timeseries/{}.stack.errLog'.format(trackDir, p)

                # create the stack of same polarised data if polarisation is existent
                ts.createStackPol(dimListTC, p, tmpStack, logFile)

                if mtSpkFlt is True:
                    # do the multi-temporal filtering
                    logFile = '{}/Timeseries/{}.mtSpkFlt.errLog'.format(trackDir, p)
                    ts.mtSpeckle('{}.dim'.format(tmpStack), outStack, logFile)
                    os.remove('{}.dim'.format(tmpStack))
                    shutil.rmtree('{}.data'.format(tmpStack))
                else:
                    outStack = tmpStack

                # get the dates of the files
                dates = [datetime.datetime.strptime(x.split('_')[-1][:-4], '%d%b%Y') for x in glob.glob('{}.data/*img'.format(outStack))]
                # sort them
                dates.sort()
                # write them back to string for following loop
                sortedDates = [datetime.datetime.strftime(ts, "%d%b%Y") for ts in dates]

                i = 1
                for date in sortedDates:

                    # restructure date to YYMMDD
                    inDate = datetime.datetime.strptime(date, '%d%b%Y')
                    outDate = datetime.datetime.strftime(inDate, '%y%m%d')

                    inFile = glob.glob('{}.data/*{}*{}*img'.format(outStack, p, date))[0]
                    # create outFile
                    outFile = '{}/Timeseries/{}.{}.TC.{}.tif'.format(trackDir, i, outDate, p)
                    ras.maskByShape(inFile, outFile, '{}/extent.shp'.format(tmpDir), toDB=toDB, 
                                    dType=dType, minVal=-30, maxVal=5, ndv=0)

                    i += 1

                #if os.path.isdir('{}.data'.format(outStack)):
                os.remove('{}.dim'.format(outStack))
                shutil.rmtree('{}.data'.format(outStack))
        
        
        # join LS maps
        if lsMap is True:
            print(' INFO: Calculating the LS map.')
            listOfLS = glob.glob('{}/*/*LS.data/*img'.format(trackDir))
            gdal.BuildVRT('{}/LSstack.vrt'.format(tmpDir), listOfLS, options=vrt_options)  
            
            #ts.mtMetrics('{}/LSstack.vrt'.format(tmpDir))

        for file in glob.glob('{}/extent*'.format(tmpDir)):
            os.remove(file)
