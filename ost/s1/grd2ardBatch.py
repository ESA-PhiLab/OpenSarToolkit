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
        creates an urllib opener object for authentication on scihub server
    ard2Ts:
        gets the next page from a multi-page result from a scihub search

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
from ost.helpers import raster as ras
from ost.s1.metadata import s1Metadata
from ost.s1 import refine, grd2ard, ts

def grd2ArdBatch(inputData, dwnDir, prcDir, tmpDir, outResolution, prdType, lsMap, spkFlt):
    
    
    # 1) we convert input to a geopandas GeoDataFrame object
    fpDataFrame = refine.readS1Inventory(inputData) # function to convert the input to GeoDataFrame
    procDict = refine.createProcDict(fpDataFrame)

    for track, allScenes in procDict.items():
        for sceneList in procDict[track]:

                acqDate = s1Metadata(sceneList[0]).start_date
                outDir = '{}/{}/{}'.format(prcDir, track, acqDate)
                os.makedirs(outDir, exist_ok=True)
                
                scenePaths = [s1Metadata(i).s1DwnPath(dwnDir) for i in sceneList]
                
                #createAWSJob
                #if cloudProvider is 'IPTPBS':
                #    outDir = '/host{}'.format(outDir)
                #    dwnDir = '/host{}'
                #else:
                #print('grd2ard.grd2ard({},{},{},{},{},{},{},{})'.format(scenePaths, outDir, acqDate, tmpDir, outResolution, prdType, lsMap, spkFlt))
                grd2ard.grd2ard(scenePaths, outDir, acqDate, tmpDir, outResolution, prdType, lsMap, spkFlt)
                
                
def ard2Ts(inputData, prjDir, tmpDir, mtSpkFlt=True, toDB=True, lsMap=True, dType='float32'):
    
    # 1) we convert input to a geopandas GeoDataFrame object
    fpDataFrame = refine.readS1Inventory(inputData) # function to convert the input to GeoDataFrame
    procDict = refine.createProcDict(fpDataFrame)
    #print(procDict)
    
    for track, allScenes in procDict.items():

        trackDir = '{}/{}'.format(prjDir, track)
        
        # 1) get minimum valid extent (i.e. assure value fo each pixel throughout the whole time-series)
        print(' INFO: Calculating the minimum extent.')
        listOfScenes = glob.glob('{}/*/*data/*img'.format(trackDir))
        listOfScenes = ' '.join([x for x in listOfScenes if not 'layover' in x]) # exclude the layovers and create a string
        cmd = 'gdalbuildvrt -separate -srcnodata 0 {}/extent.vrt {}'.format(tmpDir, listOfScenes)
        os.system(cmd)

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

                    i = 1
                    for inFile in sorted(glob.glob('{}.data/*img'.format(outStack))):

                        preDate = datetime.datetime.strptime(inFile.split('_')[-1][:-4], '%d%b%Y')
                        outDate = preDate.strftime('%y%m%d')

                        outFile = '{}/Timeseries/{}.{}.TC.{}.tif'.format(trackDir, i, outDate, p)
                        ras.maskByShape(inFile, outFile, '{}/extent.shp'.format(tmpDir), toDB=toDB, 
                                        dType=dType, minVal=-30, maxVal=5, ndv=0)

                        i += 1

        #if os.path.isdir('{}.data'.format(outStack)):
        os.remove('{}.dim'.format(outStack))
        shutil.rmtree('{}.data'.format(outStack))
        for file in glob.glob('{}/extent*'.format(tmpDir)):
            os.remove(file)
        
        # join LS maps
        if lsMap is True:
            print(' INFO: Calculating the minimum extent.')
            listOfLS = ' '.join(glob.glob('{}/*/*LS.data/*img'.format(trackDir)))
            cmd = 'gdalbuildvrt -separate -srcnodata 0 {}/LSstack.vrt {}'.format(tmpDir, listOfLS)
            os.system(cmd)
                        
                    #ts.mtMetrics('{}/LSstack.vrt'.format(tmpDir))   