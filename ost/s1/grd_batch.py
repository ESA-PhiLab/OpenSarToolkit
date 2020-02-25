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
from os.path import join as opj
import json
import glob
import itertools

import gdal

# import ost libs
from ost import Sentinel1_Scene
from ost.s1 import grd_to_ard
from ost.helpers import raster as ras
from ost.multitemporal import common_extent
from ost.multitemporal import common_ls_mask
from ost.multitemporal import ard_to_ts
from ost.multitemporal import timescan
from ost.mosaic import mosaic


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
                     temp_dir, proc_file, subset=None,
                     data_mount='/eodata', exec_file=None):

    # where all frames are grouped into acquisitions
    processing_dict = _create_processing_dict(inventory_df)

    for track, allScenes in processing_dict.items():
        for list_of_scenes in processing_dict[track]:

                # get acquisition date
                acquisition_date = Sentinel1_Scene(list_of_scenes[0]).start_date
                # create a subdirectory baed on acq. date
                out_dir = opj(processing_dir, track, acquisition_date)
                os.makedirs(out_dir, exist_ok=True)

                # check if already processed
                if os.path.isfile(opj(out_dir, '.processed')):
                    print(' INFO: Acquisition from {} of track {}'
                          ' already processed'.format(acquisition_date, track))
                else:
                    # get the paths to the file
                    scene_paths = ([Sentinel1_Scene(i).get_path(download_dir)
                                   for i in list_of_scenes])

                    file_id = '{}_{}'.format(acquisition_date, track)

                    # apply the grd_to_ard function
                    grd_to_ard.grd_to_ard(scene_paths,
                                          out_dir,
                                          file_id,
                                          temp_dir,
                                          proc_file,
                                          subset=subset)


def ards_to_timeseries(inventory_df, processing_dir, temp_dir,
                       proc_file, exec_file):

    # load ard parameters
    with open(proc_file, 'r') as ard_file:
        ard_params = json.load(ard_file)['processing parameters']
        ard = ard_params['single ARD']

    for track in inventory_df.relativeorbit.unique():

        # get the burst directory
        track_dir = opj(processing_dir, track)

        # get common burst extent
        list_of_scenes = glob.glob(opj(track_dir, '20*', '*data*', '*img'))
        list_of_scenes = [x for x in list_of_scenes if 'layover' not in x]
        extent = opj(track_dir, '{}.extent.shp'.format(track))

        # placeholder for parallelisation
        if exec_file:
            if os.path.isfile(exec_file):
                os.remove(exec_file)

            print('create command')
            continue

        print(' INFO: Creating common extent mask for track {}'.format(track))
        common_extent.mt_extent(list_of_scenes, extent, temp_dir, -0.0018)

    if ard['create ls mask'] or ard['apply ls mask']:

        for track in inventory_df.relativeorbit.unique():

            # get the burst directory
            track_dir = opj(processing_dir, track)

            # get common burst extent
            list_of_scenes = glob.glob(opj(track_dir, '20*', '*data*', '*img'))
            list_of_layover = [x for x in list_of_scenes if 'layover' in x]

            # layover/shadow mask
            out_ls = opj(track_dir, '{}.ls_mask.tif'.format(track))

            print(' INFO: Creating common Layover/Shadow mask for track {}'.format(track))
            common_ls_mask.mt_layover(list_of_layover, out_ls, temp_dir,
                                      extent, ard['apply ls mask'])


    for track in inventory_df.relativeorbit.unique():

        # get the burst directory
        track_dir = opj(processing_dir, track)

        for pol in ['VV', 'VH', 'HH', 'HV']:

            # see if there is actually any imagery in thi polarisation
            list_of_files = sorted(glob.glob(
                opj(track_dir, '20*', '*data*', '*ma0*{}*img'.format(pol))))

            if not len(list_of_files) > 1:
                continue

            # create list of dims if polarisation is present
            list_of_dims = sorted(glob.glob(
                opj(track_dir, '20*', '*bs*dim')))

            ard_to_ts.ard_to_ts(
                            list_of_dims,
                            processing_dir,
                            temp_dir,
                            track,
                            proc_file,
                            product='bs',
                            pol=pol
            )


def timeseries_to_timescan(inventory_df, processing_dir, proc_file,
                           exec_file=None):


    # load ard parameters
    with open(proc_file, 'r') as ard_file:
        ard_params = json.load(ard_file)['processing parameters']
        ard = ard_params['single ARD']
        ard_mt = ard_params['time-series ARD']
        ard_tscan = ard_params['time-scan ARD']


    # get the db scaling right
    to_db = ard['to db']
    if ard['to db'] or ard_mt['to db']:
        to_db = True

    dtype_conversion = True if ard_mt['dtype output'] != 'float32' else False

    for track in inventory_df.relativeorbit.unique():

        print(' INFO: Entering track {}.'.format(track))
        # get track directory
        track_dir = opj(processing_dir, track)
        # define and create Timescan directory
        timescan_dir = opj(track_dir, 'Timescan')
        os.makedirs(timescan_dir, exist_ok=True)

        # loop thorugh each polarization
        for polar in ['VV', 'VH', 'HH', 'HV']:

            if os.path.isfile(opj(timescan_dir, '.{}.processed'.format(polar))):
                print(' INFO: Timescans for track {} already'
                      ' processed.'.format(track))
                continue

            #get timeseries vrt
            timeseries = opj(track_dir,
                             'Timeseries',
                             'Timeseries.bs.{}.vrt'.format(polar)
            )

            if not os.path.isfile(timeseries):
                continue

            print(' INFO: Processing Timescans of {} for track {}.'.format(polar, track))
            # create a datelist for harmonics
            scenelist = glob.glob(
                opj(track_dir, '*bs.{}.tif'.format(polar))
            )

            # create a datelist for harmonics calculation
            datelist = []
            for file in sorted(scenelist):
                datelist.append(os.path.basename(file).split('.')[1])

            # define timescan prefix
            timescan_prefix = opj(timescan_dir, 'bs.{}'.format(polar))

            # placeholder for parallel execution
            if exec_file:
                print(' Write command to a text file')
                continue

            # run timescan
            timescan.mt_metrics(
                timeseries,
                timescan_prefix,
                ard_tscan['metrics'],
                rescale_to_datatype=dtype_conversion,
                to_power=to_db,
                outlier_removal=ard_tscan['remove outliers'],
                datelist=datelist
            )

        if not exec_file:
            # create vrt file (and rename )
            ras.create_tscan_vrt(timescan_dir, proc_file)


def mosaic_timeseries(inventory_df, processing_dir, temp_dir, cut_to_aoi=False,
                      exec_file=None):

    print(' -----------------------------------')
    print(' INFO: Mosaicking Time-series layers')
    print(' -----------------------------------')

    # create output folder
    ts_dir = opj(processing_dir, 'Mosaic', 'Timeseries')
    os.makedirs(ts_dir, exist_ok=True)

    # loop through polarisations
    for p in ['VV', 'VH', 'HH', 'HV']:

        tracks = inventory_df.relativeorbit.unique()
        nr_of_ts = len(glob.glob(opj(
            processing_dir, tracks[0], 'Timeseries', '*.{}.tif'.format(p))))

        if not nr_of_ts >= 1:
            continue

        outfiles = []
        for i in range(1, nr_of_ts + 1):

            filelist = glob.glob(opj(
                processing_dir, '*', 'Timeseries',
                '{}.*.{}.tif'.format(i, p)))
            filelist = [file for file in filelist if 'Mosaic' not in file]

            # create
            datelist = []
            for file in filelist:
                datelist.append(os.path.basename(file).split('.')[1])

            filelist = ' '.join(filelist)
            start, end = sorted(datelist)[0], sorted(datelist)[-1]

            if start == end:
                outfile = opj(ts_dir, '{}.{}.bs.{}.tif'.format(i, start, p))
            else:
                outfile = opj(ts_dir, '{}.{}-{}.bs.{}.tif'.format(i, start, end, p))

            check_file = opj(
                os.path.dirname(outfile),
                '.{}.processed'.format(os.path.basename(outfile)[:-4])
            )
               # logfile = opj(ts_dir, '{}.{}-{}.bs.{}.errLog'.format(i, start, end, p))

            outfiles.append(outfile)

            if os.path.isfile(check_file):
                print(' INFO: Mosaic layer {} already'
                      ' processed.'.format(os.path.basename(outfile)))
                continue

            print(' INFO: Mosaicking layer {}.'.format(os.path.basename(outfile)))
            mosaic.mosaic(filelist, outfile, temp_dir, cut_to_aoi)

        if exec_file:
            print(' gdalbuildvrt ....command, outfiles')
            continue

        # create vrt
        vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
        gdal.BuildVRT(opj(ts_dir, 'Timeseries.{}.vrt'.format(p)),
                      outfiles,
                      options=vrt_options
        )


def mosaic_timescan(inventory_df, processing_dir, temp_dir, proc_file,
                    cut_to_aoi=False, exec_file=None):

    # load ard parameters
    with open(proc_file, 'r') as ard_file:
        ard_params = json.load(ard_file)['processing parameters']
        metrics = ard_params['time-scan ARD']['metrics']

    if 'harmonics' in metrics:
        metrics.remove('harmonics')
        metrics.extend(['amplitude', 'phase', 'residuals'])

    if 'percentiles' in metrics:
            metrics.remove('percentiles')
            metrics.extend(['p95', 'p5'])

    # create out directory of not existent
    tscan_dir = opj(processing_dir, 'Mosaic', 'Timescan')
    os.makedirs(tscan_dir, exist_ok=True)
    outfiles = []

    # loop through all pontial proucts
    for polar, metric in itertools.product(['VV', 'HH', 'VH', 'HV'], metrics):

        # create a list of files based on polarisation and metric
        filelist = glob.glob(opj(processing_dir, '*', 'Timescan',
                                 '*bs.{}.{}.tif'.format(polar, metric)
                            )
                   )

        # break loop if there are no files
        if not len(filelist) >= 2:
            continue

        # get number
        filelist = ' '.join(filelist)
        outfile = opj(tscan_dir, 'bs.{}.{}.tif'.format(polar, metric))
        check_file = opj(
                os.path.dirname(outfile),
                '.{}.processed'.format(os.path.basename(outfile)[:-4])
        )

        if os.path.isfile(check_file):
            print(' INFO: Mosaic layer {} already '
                  ' processed.'.format(os.path.basename(outfile)))
            continue

        print(' INFO: Mosaicking layer {}.'.format(os.path.basename(outfile)))
        mosaic.mosaic(filelist, outfile, temp_dir, cut_to_aoi)
        outfiles.append(outfile)

    if exec_file:
        print(' gdalbuildvrt ....command, outfiles')
    else:
        ras.create_tscan_vrt(tscan_dir, proc_file)
