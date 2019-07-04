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

# import standard python libs
import os
import glob
import datetime
import gdal

# for os independent paths use opj shortcut
from os.path import join as opj

# import ost libs
from .metadata import s1Metadata
from . import refine, grd2Ard, ts
from ..helpers import raster as ras
from ..helpers import helpers as h


def createProcParamDict(
                    aoi, outResolution, lsMask, spkFlt, outPrdType,
                    mtSpkFlt, metrics, toDB, dType,
                    subset=None, polarisation='VV,VH,HH,HV',
                    toPower=True, rescale=True, outRem=True,
                        ):

    procParams = {
                  'aoi': aoi,
                  'resolution': outResolution,
                  'prdType': outPrdType,
                  'polarisation': polarisation,
                  'lsMask': lsMask,
                  'spkFlt': spkFlt,
                  'subset': subset,
                  'mtSpkFlt': mtSpkFlt,
                  'metrics': metrics,
                  'toDB': toDB,
                  'dType': dType,
                  'toPower': toPower,
                  'rescale': rescale,
                  'outlierRemoval': outRem,
                }

    return procParams


def grd2ArdBatch(inputDf, dwnDir, prcDir, tmpDir, procParam):

    # get params
    outResolution = procParam['resolution']
    prdType = procParam['prdType']
    lsMask = procParam['lsMask']
    spkFlt = procParam['spkFlt']
    subset = procParam['subset']
    polarisation = procParam['polarisation']

    # we create a processing dictionary,
    # where all frames are grouped into acquisitions
    procDict = refine.createProcDict(inputDf)

    for track, allScenes in procDict.items():
        for sceneList in procDict[track]:

                # get acquisition date
                acqDate = s1Metadata(sceneList[0]).start_date
                # create a subdirectory baed on acq. date
                outDir = opj(prcDir, track, acqDate)
                os.makedirs(outDir, exist_ok=True)

                # get the paths to the file
                scenePaths = ([s1Metadata(i).s1DwnPath(dwnDir)
                               for i in sceneList])

                # apply the grd2ard function
                grd2Ard.grd2Ard(scenePaths, outDir, acqDate, tmpDir,
                                outResolution, prdType, lsMask, spkFlt,
                                subset, polarisation)


def ard2Ts(inputDf, prjDir, tmpDir, procParam):

    # get params
    toDB = procParam['toDB']
    dType = procParam['dType']
    lsMask = procParam['lsMask']
    mtSpkFlt = procParam['mtSpkFlt']

    # 1) we convert input to a geopandas GeoDataFrame object
    procDict = refine.createProcDict(inputDf)
    vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)

    for track, allScenes in procDict.items():

        trackDir = opj(prjDir, track)

        # 1) get minimum valid extent (i.e. assure value fo each pixel throughout the whole time-series)
        print(' INFO: Calculating the minimum extent.')
        listOfScenes = glob.glob(opj(trackDir, '*', '*data', '*img'))
        listOfScenes = [x for x in listOfScenes if not 'layover' in x] # exclude the layovers and create a string
        gdal.BuildVRT(opj(tmpDir, 'extent.vrt'), listOfScenes, options=vrt_options)
        ras.outline(opj(tmpDir, 'extent.vrt'), opj(tmpDir, 'extent.shp'), 0, True)

        # create a list of dimap files and format to comma-separated list
        dimListTC = sorted(glob.glob(opj(trackDir, '20*', '*TC*dim')))
        dimListTC = '\'{}\''.format(','.join(dimListTC))

        # join LS maps
        if lsMask is True:
            print(' INFO: Calculating the LS map.')
            listOfLS = glob.glob(opj(trackDir, '*', '*LS.data', '*img'))
            gdal.BuildVRT(opj(tmpDir, 'LSstack.vrt'), listOfLS, options=vrt_options)

            tmpLs = '{}/lsMask.tif'.format(tmpDir)
            outLs = '{}/Timeseries/lsMask'.format(trackDir)
            ts.mtMetricsMain(opj(tmpDir, 'LSstack.vrt'), tmpLs, ['max'],
                                                        False, False, False)

            # get 0 and 1s for the mask
            cmd = ('gdal_calc.py -A {} --outfile {} --calc=A/A \
                        --NodataValue=0 --overwrite --type=Byte').format(tmpLs, outLs)
            os.system(cmd)


        for p in ['VV', 'VH', 'HH', 'HV']:

            # check if polarisation is existent
            polListTC = sorted(glob.glob(opj(trackDir, '20*', '*TC*data', 'Gamma0*{}*.img'.format(p))))

            if len(polListTC) >= 2:

                # create output stack name for RTC
                tmpStack = opj(tmpDir, 'stack_{}_{}'.format(track, p))
                outStack = opj(tmpDir, 'mt_stack_{}_{}'.format(track, p))

                os.makedirs(opj(trackDir, 'Timeseries'), exist_ok=True)
                logFile = opj(trackDir, 'Timeseries', '{}.stack.errLog'.format(p))

                # create the stack of same polarised data if polarisation is existent
                ts.createStackPol(dimListTC, p, tmpStack, logFile)

                if mtSpkFlt is True:
                    # do the multi-temporal filtering
                    logFile = opj(trackDir, 'Timeseries', '{}.mtSpkFlt.errLog'.format(p))
                    ts.mtSpeckle('{}.dim'.format(tmpStack), outStack, logFile)
                    h.delDimap(tmpStack)
                    #os.remove('{}.dim'.format(tmpStack))
                    #shutil.rmtree('{}.data'.format(tmpStack))
                else:
                    outStack = tmpStack

                # get the dates of the files
                dates = [datetime.datetime.strptime(x.split('_')[-1][:-4], '%d%b%Y') for x in glob.glob(opj('{}.data'.format(outStack), '*img'))]
                # sort them
                dates.sort()
                # write them back to string for following loop
                sortedDates = [datetime.datetime.strftime(ts, "%d%b%Y") for ts in dates]

                i, outFiles = 1, []
                for date in sortedDates:

                    # restructure date to YYMMDD
                    inDate = datetime.datetime.strptime(date, '%d%b%Y')
                    outDate = datetime.datetime.strftime(inDate, '%y%m%d')

                    inFile = glob.glob(opj('{}.data'.format(outStack), '*{}*{}*img'.format(p, date)))[0]
                    # create outFile
                    outFile = opj(trackDir, 'Timeseries', '{}.{}.TC.{}.tif'.format(i, outDate, p))
                    # mask by extent
                    ras.maskByShape(inFile, outFile, opj(tmpDir, 'extent.shp'),
                            toDB=toDB, dType=dType, minVal=-30, maxVal=5, ndv=0)
                    # add ot a list for subsequent vrt creation
                    outFiles.append(outFile)

                    i += 1

                # build vrt of timeseries
                gdal.BuildVRT(opj(trackDir, 'Timeseries', 'Timeseries.{}.vrt'.format(p)), outFiles, options=vrt_options)
                #if os.path.isdir('{}.data'.format(outStack)):
                h.delDimap(outStack)
                #os.remove('{}.dim'.format(outStack))
                #shutil.rmtree('{}.data'.format(outStack))

        for file in glob.glob(opj(tmpDir, 'extent*')):
            os.remove(file)


def ts2Timescan(fpDataFrame, prcDir, procParam):

    metric = procParam['metrics']
    rescale = procParam['rescale']
    outlierRemoval = procParam['outlierRemoval']
    toPower = procParam['toPower']


    if metric is 'all':
        metrics = ['avg', 'max', 'min', 'std', 'cov' ]
    elif metric is 'perc':
        metrics = ['p90', 'p10', 'pDiff']
    else:
        metrics = metric
    # read fpDataFrame to processing dictionary
    procDict = refine.createProcDict(fpDataFrame)

    # loop through tracks
    for track, allScenes in procDict.items():

        # get track directory
        trackDir = '{}/{}'.format(prcDir, track)

        # define and create Timescan directory
        tScanDir = '{}/Timescan'.format(trackDir)
        os.makedirs(tScanDir, exist_ok=True)

        # loop thorugh each polarization
        for p in ['VV', 'VH', 'HH', 'HV']:

            #get timeseries vrt
            tsVrt = '{}/Timeseries/Timeseries.{}.vrt'.format(trackDir, p)

            # define timescan prefix
            timeScan = '{}/{}.Timescan'.format(tScanDir, p)

            # check if timeseries vrt exists
            if os.path.exists(tsVrt):

                # calculate the multi-temporal metrics
                ts.mtMetricsMain(tsVrt, timeScan, metrics, toPower, rescale, outlierRemoval)


def ts2Mosaic(fpDataFrame, prcDir, tmpDir, Timeseries=True, Timescan=True):

    for p in ['VV', 'VH', 'HH', 'HV']:


        procDict = refine.createProcDict(fpDataFrame)
        keys = [x for x in procDict.keys()]

        if Timeseries is True:
            os.makedirs('{}/Mosaic/Timeseries'.format(prcDir), exist_ok=True)
            print('INFO: Mosaicking Time-series layers')
            nrOfTs = len(glob.glob('{}/{}/Timeseries/*.{}.tif'.format(prcDir, keys[0],p)))
            if nrOfTs >= 2:

                for i in range(nrOfTs):

                    j = i + 1
                    listOfFiles = ' '.join(glob.glob('{}/*/Timeseries/{}.*.{}.tif'.format(prcDir, j, p)))
                    cmd = ('otbcli_Mosaic -ram 4096 -progress 1 \
                            -comp.feather large -harmo.method band -harmo.cost rmse -tmpdir {} -il {} \
                            -out {}/Mosaic/Timeseries/{}.Gamma0.{}.tif'.format(tmpDir, listOfFiles, prcDir, j, p))

                    os.system(cmd)


        if Timescan is True:
            os.makedirs('{}/Mosaic/Timescan'.format(prcDir), exist_ok=True)
            print('INFO: Mosaicking Timescan layers')
            metrics = ["avg", "max", "min", "std", "cov" ]

            for metric in metrics:

                listOfFiles = ' '.join(glob.glob('{}/*/Timescan/*{}.Timescan.{}.tif'.format(prcDir, p, metric)))
                cmd = ('otbcli_Mosaic -ram 4096 -progress 1 \
                            -comp.feather large -harmo.method band -harmo.cost rmse -tmpdir {} -il {} \
                            -out {}/Mosaic/Timescan/{}.Gamma0.{}.tif'.format(tmpDir, listOfFiles, prcDir, p, metric))

                os.system(cmd)
