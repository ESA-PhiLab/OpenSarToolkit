#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""Batch processing for GRD products

"""

import os
from os.path import join as opj
import json
import glob
import itertools
import logging
import gdal
import pandas as pd
from pathlib import Path

from godale._concurrent import Executor

from ost import Sentinel1Scene
from ost.s1 import grd_to_ard
from ost.helpers import raster as ras
from ost.generic import ts_extent
from ost.generic import ts_ls_mask
from ost.generic import ard_to_ts
from ost.generic import timescan
from ost.generic import mosaic

logger = logging.getLogger(__name__)


def _create_processing_dict(inventory_df):
    """Function that creates a dictionary to handle GRD batch processing

    This helper function takes the inventory dataframe and creates
    a dictionary with the track as key, and all the files to process as
    a list, whereas the list is

    :param inventory_df:
    :return:
    """

    # initialize empty dictionary
    dict_scenes = {}

    # get relative orbits and loop through each
    track_list = inventory_df['relativeorbit'].unique()

    for track in track_list:

        # get acquisition dates and loop through each
        acquisition_dates = inventory_df['acquisitiondate'][
            inventory_df['relativeorbit'] == track].unique()

        # loop through dates
        for i, acquisition_date in enumerate(acquisition_dates):

            # get the scene ids per acquisition_date and write into a list
            single_id = inventory_df['identifier'][
                (inventory_df['relativeorbit'] == track) &
                (inventory_df['acquisitiondate'] == acquisition_date)
            ].tolist()

            # add this list to the dictionary and associate the track number
            # as dict key
            dict_scenes[f'{track}_{i+1}'] = single_id

    return dict_scenes


def create_processed_df(inventory_df, list_of_scenes, outfile, out_ls, error):

    df = pd.DataFrame(columns=['identifier', 'outfile', 'out_ls', 'error'])

    for scene in list_of_scenes:

        temp_df = pd.DataFrame()
        # get scene_id
        temp_df['identifier'] = inventory_df.identifier[
            inventory_df.identifier == scene
        ].values
        # fill outfiles/error
        temp_df['outfile'] = outfile
        temp_df['out_ls'] = out_ls
        temp_df['error'] = error

        # append to final df and delete temp_df for next loop
        df = df.append(temp_df)
        del temp_df

    return df


def grd_to_ard_batch(inventory_df, config_file):

    # load relevant config parameters
    with open(config_file, 'r') as file:
        config_dict = json.load(file)
        download_dir = Path(config_dict['download_dir'])
        data_mount = Path(config_dict['data_mount'])

    # where all frames are grouped into acquisitions
    processing_dict = _create_processing_dict(inventory_df)
    processing_df = pd.DataFrame(
        columns=['identifier', 'outfile', 'out_ls', 'error']
    )

    iter_list = []
    for _, list_of_scenes in processing_dict.items():

        # get the paths to the file
        scene_paths = (
            [Sentinel1Scene(scene).get_path(download_dir, data_mount)
             for scene in list_of_scenes]
        )

        iter_list.append(scene_paths)

    # now we run with godale, which works also with 1 worker
    executor = Executor(
        executor=config_dict['executer_type'],
        max_workers=config_dict['max_workers']
    )

    for task in executor.as_completed(
        func=grd_to_ard.grd_to_ard,
        iterable=iter_list,
        fargs=([str(config_file), ])
    ):

        list_of_scenes, outfile, out_ls, error = task.result()

        # return the info of processing as dataframe
        temp_df = create_processed_df(
            inventory_df, list_of_scenes, outfile, out_ls, error
        )

        processing_df = processing_df.append(temp_df)

    return processing_df


def ards_to_timeseries(inventory_df, config_file):

    with open(config_file) as file:
        config_dict = json.load(file)
        ard = config_dict['processing']['single_ARD']
        ard_mt = config_dict['processing']['time-series_ARD']

    # create all extents
    _create_extents(inventory_df, config_file)

    # update extents in case of ls_mask
    if ard['create_ls_mask'] or ard_mt['apply_ls_mask']:
        _create_mt_ls_mask(inventory_df, config_file)

    # finally create time-series
    _create_timeseries(inventory_df, config_file)


def _create_extents(inventory_df, config_file):

    with open(config_file, 'r') as file:
        config_dict = json.load(file)
        processing_dir = Path(config_dict['processing_dir'])

    iter_list = []
    for track in inventory_df.relativeorbit.unique():

        # get the burst directory
        track_dir = processing_dir.joinpath(track)

        # get common burst extent
        list_of_scenes = list(track_dir.glob('**/*img'))

        list_of_scenes = [
            str(x) for x in list_of_scenes if 'layover' not in str(x)
        ]

        # if extent does not already exist, add to iterable
        if not track_dir.joinpath(f'{track}.extent.gpkg').exists():
            iter_list.append(list_of_scenes)

    # now we run with godale, which works also with 1 worker
    executor = Executor(
        executor=config_dict['executer_type'],
        max_workers=config_dict['max_workers']
    )

    for task in executor.as_completed(
            func=ts_extent.mt_extent,
            iterable=iter_list,
            fargs=([str(config_file), ])
    ):
        task.result()


def _create_mt_ls_mask(inventory_df, config_file):

    with open(config_file, 'r') as file:
        config_dict = json.load(file)
        processing_dir = Path(config_dict['processing_dir'])

    iter_list = []
    for track in inventory_df.relativeorbit.unique():

        # get the burst directory
        track_dir = processing_dir.joinpath(track)

        # get common burst extent
        list_of_scenes = list(track_dir.glob('**/*img'))

        list_of_layover = [
            str(x) for x in list_of_scenes if 'layover' in str(x)
        ]

        iter_list.append(list_of_layover)
        #ts_ls_mask.mt_layover(list_of_layover, config_file)

    # now we run with godale, which works also with 1 worker
    executor = Executor(
        executor=config_dict['executer_type'],
        max_workers=config_dict['max_workers']
    )
    for task in executor.as_completed(
            func=ts_ls_mask.mt_layover,
            iterable=iter_list,
            fargs=([str(config_file), ])
    ):
        task.result()


def _create_timeseries(inventory_df, config_file):

    with open(config_file, 'r') as file:
        config_dict = json.load(file)
        processing_dir = Path(config_dict['processing_dir'])

    iter_list = []
    for track in inventory_df.relativeorbit.unique():

        # get the burst directory
        track_dir = processing_dir.joinpath(track)

        for pol in ['VV', 'VH', 'HH', 'HV']:

            # see if there is actually any imagery in thi polarisation
            list_of_files = sorted(
                str(file) for file in list(
                    track_dir.glob(f'20*/*data*/*ma0*{pol}*img')
                )
            )

            if not len(list_of_files) > 1:
                continue

            # create list of dims if polarisation is present
            list_of_dims = sorted(
                str(dim) for dim in list(track_dir.glob('20*/*bs*dim'))
            )

            iter_list.append([list_of_dims, track, 'bs', pol])

    executor = Executor(
        executor=config_dict['executer_type'],
        max_workers=config_dict['max_workers']
    )

    for task in executor.as_completed(
            func=ard_to_ts.gd_ard_to_ts,
            iterable=iter_list,
            fargs=([str(config_file), ])
    ):
        task.result()


def timeseries_to_timescan(inventory_df, config_file):

    # load ard parameters
    with open(config_file, 'r') as file:
        config_dict = json.load(file)
        processing_dir = Path(config_dict['processing_dir'])
        ard = config_dict['processing']['single_ARD']
        ard_mt = config_dict['processing']['time-series_ARD']
        ard_tscan = config_dict['processing']['time-scan_ARD']

    # get the db scaling right
    to_db = ard['to_db']
    if ard['to_db'] or ard_mt['to_db']:
        to_db = True

    dtype_conversion = True if ard_mt['dtype_output'] != 'float32' else False

    iter_list = []
    for track in inventory_df.relativeorbit.unique():

        logger.info('Entering track {}.'.format(track))
        # get track directory
        track_dir = processing_dir.joinpath(track)
        # define and create Timescan directory
        timescan_dir = track_dir.joinpath('Timescan')
        timescan_dir.mkdir(parents=True, exist_ok=True)

        # loop thorugh each polarization
        for polar in ['VV', 'VH', 'HH', 'HV']:

            if timescan_dir.joinpath(f'.{polar}.processed').exists():
                logger.info(f'Timescans for track {track} already processed.')
                continue

            # get timeseries vrt
            time_series = track_dir.joinpath(
                f'Timeseries/Timeseries.bs.{polar}.vrt'
            )

            if not time_series.exists():
                continue

            # create a datelist for harmonics
            scene_list = [
                str(file) for file in list(track_dir.glob(f'*bs.{polar}.tif'))
            ]

            # create a datelist for harmonics calculation
            datelist = []
            for file in sorted(scene_list):
                datelist.append(os.path.basename(file).split('.')[1])

            # define timescan prefix
            timescan_prefix = timescan_dir.joinpath(f'bs.{polar}')

            iter_list.append([
                time_series, timescan_prefix, ard_tscan['metrics'],
                dtype_conversion, to_db, ard_tscan['remove_outliers'],
                datelist
            ])

    executor = Executor(
        executor=config_dict['executer_type'],
        max_workers=config_dict['max_workers']
    )

    for task in executor.as_completed(
            func=timescan.gd_mt_metrics,
            iterable=iter_list,
    ):
        task.result()

    for track in inventory_df.relativeorbit.unique():
        track_dir = processing_dir.joinpath(track)
        timescan_dir = track_dir.joinpath('Timescan')
        ras.create_tscan_vrt([timescan_dir, config_file])


def mosaic_timeseries(inventory_df, processing_dir, temp_dir, cut_to_aoi=False,
                      exec_file=None):

    print(' -----------------------------------')
    logger.info('Mosaicking Time-series layers')
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
                logger.info('Mosaic layer {} already'
                      ' processed.'.format(os.path.basename(outfile)))
                continue

            logger.info('Mosaicking layer {}.'.format(os.path.basename(outfile)))
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
            logger.info('Mosaic layer {} already '
                  ' processed.'.format(os.path.basename(outfile)))
            continue

        logger.info('Mosaicking layer {}.'.format(os.path.basename(outfile)))
        mosaic.mosaic(filelist, outfile, temp_dir, cut_to_aoi)
        outfiles.append(outfile)

    if exec_file:
        print(' gdalbuildvrt ....command, outfiles')
    else:
        ras.create_tscan_vrt(tscan_dir, proc_file)
