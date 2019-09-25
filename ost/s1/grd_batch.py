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
    grd_to_ardBatch:
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

python3 grd_to_ardBatch.py -i /path/to/inventory -r 20 -p RTC -l True -s False
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
from ost import Sentinel1_Scene
from ost.s1 import refine, grd_to_ard, ts
from ost.helpers import raster as ras
from ost.helpers import helpers as h


def _create_processing_dict(inventory_df):
    ''' This function might be obsolete?

    '''

    # initialize empty dictionary
    dict_scenes = {}

    # get relative orbits and loop through each
    tracklist = inventory_df['relativeorbit'].unique()
    for track in tracklist:

        # initialize an empty list that will be filled by
        # list of scenes per acq. date
        all_ids = []

        # get acquisition dates and loop through each
        acquisition_dates = inventory_df['acquisitiondate'][
            inventory_df['relativeorbit'] == track].unique()

        # loop through dates
        for acquisition_date in acquisition_dates:

            # get the scene ids per acquisition_date and write into a list
            single_id = []
            single_id.append(inventory_df['identifier'][
                (inventory_df['relativeorbit'] == track) &
                (inventory_df['acquisitiondate'] == acquisition_date)].tolist())

            # append the list of scenes to the list of scenes per track
            all_ids.append(single_id[0])

        # add this list to the dctionary and associate the track number
        # as dict key
        dict_scenes[track] = all_ids

    return dict_scenes


def grd_to_ard_batch(inventory_df, download_dir, processing_dir,
                     temp_dir, ard_parameters, subset=None, 
                     data_mount='/eodata'):
    
    
    # get params
    resolution = ard_parameters['resolution']
    product_type = ard_parameters['product_type']
    ls_mask_create = ard_parameters['ls_mask_create']
    speckle_filter = ard_parameters['speckle_filter']
    polarisation = ard_parameters['polarisation']
    dem = ard_parameters['dem']
    to_db = ard_parameters['to_db']
    border_noise = ard_parameters['border_noise']

    # we create a processing dictionary,
    # where all frames are grouped into acquisitions
    processing_dict = _create_processing_dict(inventory_df)

    for track, allScenes in processing_dict.items():
        for list_of_scenes in processing_dict[track]:

                # get acquisition date
                acquisition_date = Sentinel1_Scene(list_of_scenes[0]).start_date
                # create a subdirectory baed on acq. date
                out_dir = opj(processing_dir, track, acquisition_date)
                os.makedirs(out_dir, exist_ok=True)

                # get the paths to the file
                scene_paths = ([Sentinel1_Scene(i).get_path(download_dir)
                               for i in list_of_scenes])

                # apply the grd_to_ard function
                grd_to_ard.grd_to_ard(scene_paths, 
                                      out_dir, 
                                      acquisition_date, 
                                      temp_dir,
                                      resolution=resolution, 
                                      product_type=product_type,
                                      ls_mask_create=ls_mask_create,
                                      speckle_filter=speckle_filter,
                                      dem=dem, 
                                      to_db=to_db, 
                                      border_noise=border_noise,
                                      subset=subset, 
                                      polarisation=polarisation)


def ard2Ts(inputDf, prjDir, temp_dir, procParam):

    # get params
    toDB = procParam['toDB']
    dType = procParam['dType']
    ls_mask_create = procParam['ls_mask_create']
    mtspeckle_filter = procParam['mtspeckle_filter']

    # 1) we convert input to a geopandas GeoDataFrame object
    processing_dict = refine.createprocessing_dict(inputDf)
    vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)

    for track, allScenes in processing_dict.items():

        trackDir = opj(prjDir, track)

        # 1) get minimum valid extent (i.e. assure value fo each pixel throughout the whole time-series)
        print(' INFO: Calculating the minimum extent.')
        listOfScenes = glob.glob(opj(trackDir, '*', '*data', '*img'))
        listOfScenes = [x for x in listOfScenes if not 'layover' in x] # exclude the layovers and create a string
        gdal.BuildVRT(opj(temp_dir, 'extent.vrt'), listOfScenes, options=vrt_options)
        ras.outline(opj(temp_dir, 'extent.vrt'), opj(temp_dir, 'extent.shp'), 0, True)

        # create a list of dimap files and format to comma-separated list
        dimListTC = sorted(glob.glob(opj(trackDir, '20*', '*TC*dim')))
        dimListTC = '\'{}\''.format(','.join(dimListTC))

        # join LS maps
        if ls_mask_create is True:
            print(' INFO: Calculating the LS map.')
            listOfLS = glob.glob(opj(trackDir, '*', '*LS.data', '*img'))
            gdal.BuildVRT(opj(temp_dir, 'LSstack.vrt'), listOfLS, options=vrt_options)

            tmpLs = '{}/ls_mask_create.tif'.format(temp_dir)
            outLs = '{}/Timeseries/ls_mask_create'.format(trackDir)
            ts.mtMetricsMain(opj(temp_dir, 'LSstack.vrt'), tmpLs, ['max'],
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
                tmpStack = opj(temp_dir, 'stack_{}_{}'.format(track, p))
                outStack = opj(temp_dir, 'mt_stack_{}_{}'.format(track, p))

                os.makedirs(opj(trackDir, 'Timeseries'), exist_ok=True)
                logFile = opj(trackDir, 'Timeseries', '{}.stack.errLog'.format(p))

                # create the stack of same polarised data if polarisation is existent
                ts.createStackPol(dimListTC, p, tmpStack, logFile)

                if mtspeckle_filter is True:
                    # do the multi-temporal filtering
                    logFile = opj(trackDir, 'Timeseries', 
                                  '{}.mt_speckle_filter.errLog'.format(p))
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
                    ras.maskByShape(inFile, outFile, opj(temp_dir, 'extent.shp'),
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

        for file in glob.glob(opj(temp_dir, 'extent*')):
            os.remove(file)


def ts2Timescan(fpDataFrame, processing_dir, procParam):

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
    processing_dict = refine.createprocessing_dict(fpDataFrame)

    # loop through tracks
    for track, allScenes in processing_dict.items():

        # get track directory
        trackDir = '{}/{}'.format(processing_dir, track)

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


def ts2Mosaic(fpDataFrame, processing_dir, temp_dir, Timeseries=True, Timescan=True):

    for p in ['VV', 'VH', 'HH', 'HV']:


        processing_dict = refine.createprocessing_dict(fpDataFrame)
        keys = [x for x in processing_dict.keys()]

        if Timeseries is True:
            os.makedirs('{}/Mosaic/Timeseries'.format(processing_dir), exist_ok=True)
            print('INFO: Mosaicking Time-series layers')
            nrOfTs = len(glob.glob('{}/{}/Timeseries/*.{}.tif'.format(processing_dir, keys[0],p)))
            if nrOfTs >= 2:

                for i in range(nrOfTs):

                    j = i + 1
                    listOfFiles = ' '.join(glob.glob('{}/*/Timeseries/{}.*.{}.tif'.format(processing_dir, j, p)))
                    cmd = ('otbcli_Mosaic -ram 4096 -progress 1 \
                            -comp.feather large -harmo.method band -harmo.cost rmse -temp_dir {} -il {} \
                            -out {}/Mosaic/Timeseries/{}.Gamma0.{}.tif'.format(temp_dir, listOfFiles, processing_dir, j, p))

                    os.system(cmd)


        if Timescan is True:
            os.makedirs('{}/Mosaic/Timescan'.format(processing_dir), exist_ok=True)
            print('INFO: Mosaicking Timescan layers')
            metrics = ["avg", "max", "min", "std", "cov" ]

            for metric in metrics:

                listOfFiles = ' '.join(glob.glob('{}/*/Timescan/*{}.Timescan.{}.tif'.format(processing_dir, p, metric)))
                cmd = ('otbcli_Mosaic -ram 4096 -progress 1 \
                            -comp.feather large -harmo.method band -harmo.cost rmse -temp_dir {} -il {} \
                            -out {}/Mosaic/Timescan/{}.Gamma0.{}.tif'.format(temp_dir, listOfFiles, processing_dir, p, metric))

                os.system(cmd)
