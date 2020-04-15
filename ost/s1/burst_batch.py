#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Batch processing routines for Sentinel-1 bursts

This module handles all the batch processing routines involved
in the full workflow from raw Sentinel-1 SLC imagery to
large-scale time-series and timescan mosaics.
"""

import os
import json
import itertools
import logging
import multiprocessing as mp
from pathlib import Path

from godale._concurrent import Executor

from ost.helpers import raster as ras
from ost.s1.burst_inventory import prepare_burst_inventory
from ost.s1.burst_to_ard import burst_to_ard
from ost.generic import ard_to_ts, ts_extent, ts_ls_mask, timescan, mosaic

# set up logger
logger = logging.getLogger(__name__)

# ---------------------------------------------------
# Global variable
PRODUCT_LIST = [
    'bs.HH', 'bs.VV', 'bs.HV', 'bs.VH',
    'coh.VV', 'coh.VH', 'coh.HH', 'coh.HV',
    'pol.Entropy', 'pol.Anisotropy', 'pol.Alpha'
]


def bursts_to_ards(
        burst_gdf,
        config_file,
        executor_type='multiprocessing',
        max_workers=1
):
    """Batch processing from single bursts to ARD format

    This function handles the burst processing based on a OST burst inventory
    file and an OST config file that contains all necessary information
    about the project (e.g. project directory) and processing steps applied
    for the ARD generation based on the JSON ARD-type templates.

    :param burst_gdf: an OST burst inventory
    :type burst_gdf: GeoDataFrame
    :param config_file: (str/Path) path to the project config file
    :param executor_type: executer type for parallel processing with godale,
                          defaults to multiprocessing
    :param max_workers: number of parallel burst processing jobs to start
    :return:
    """

    print('--------------------------------------------------------------')
    logger.info('Processing all single bursts to ARD')
    print('--------------------------------------------------------------')

    logger.info('Preparing the processing pipeline. This may take a moment.')
    proc_inventory = prepare_burst_inventory(burst_gdf, config_file)

    with open(config_file, 'r') as file:
        config_dict = json.load(file)
    # we update max_workers in case we have less cpus_per_process
    # then cpus available
    if max_workers == 1 and config_dict['cpus_per_process'] < os.cpu_count():
        max_workers = int(os.cpu_count() / config_dict['cpus_per_process'])

    # now we run with godale, which works also with 1 worker
    executor = Executor(executor=executor_type, max_workers=max_workers)
    for task in executor.as_completed(
            func=burst_to_ard,
            iterable=proc_inventory.iterrows(),
            fargs=(str(config_file), )
    ):
        task.result()


def _create_extents(burst_gdf, config_file):
    """Batch processing for multi-temporal Layover7Shadow mask

    This function handles the organization of the

    :param burst_gdf:
    :param config_file:
    :return:
    """

    with open(config_file, 'r') as file:
        config_dict = json.load(file)['project']
        processing_dir = Path(config_dict['processing_dir'])
        temp_dir = Path(config_dict['temp_dir'])

    # create extent iterable
    iter_list = []
    for burst in burst_gdf.bid.unique():  # ***

        # get the burst directory
        burst_dir = processing_dir.joinpath(burst)

        # get common burst extent
        list_of_bursts = list(burst_dir.glob('**/*img'))
        list_of_bursts = [
            str(x) for x in list_of_bursts if 'layover' not in str(x)
        ]
        extent = burst_dir.joinpath(f'{burst}.extent.gpkg')

        # if the file does not already exist, add to iterable
        if not extent.exists():
            iter_list.append([list_of_bursts, extent, temp_dir, -0.0018])

    # parallelizing on all cpus
    concurrent = mp.cpu_count()
    pool = mp.Pool(processes=concurrent)
    pool.map(ts_extent.mt_extent, iter_list)


def _create_mt_ls_mask(burst_gdf, config_file):
    """Batch processing for multi-temporal Layover7Shadow mask

    This function handles the organization of the

    :param burst_gdf:
    :param config_file:
    :return:
    """

    # read config file
    with open(config_file, 'r') as file:
        project_params = json.load(file)
        processing_dir = project_params['project']['processing_dir']
        temp_dir = project_params['project']['temp_dir']
        ard = project_params['processing_parameters']['time-series_ARD']

    # create layover
    iter_list = []
    for burst in burst_gdf.bid.unique():  # ***

        # get the burst directory
        burst_dir = Path(processing_dir).joinpath(burst)

        # get layover scenes
        list_of_scenes = list(burst_dir.glob('20*/*data*/*img'))
        list_of_layover = [
            str(x) for x in list_of_scenes if 'layover' in str(x)
            ]

        # we need to redefine the namespace of the already created extents
        extent = burst_dir.joinpath(f'{burst}.extent.gpkg')
        if not extent.exists():
            raise FileNotFoundError(
                f'Extent file for burst {burst} not found.'
            )

        # layover/shadow mask
        out_ls = burst_dir.joinpath(f'{burst}.ls_mask.tif')

        # if the file does not already exists, then put into list to process
        if not out_ls.exists():
            iter_list.append(
                [list_of_layover, out_ls, temp_dir, str(extent),
                 ard['apply_ls_mask']]
            )

    # parallelizing on all cpus
    concurrent = int(
        mp.cpu_count() / project_params['project']['cpus_per_process']
    )
    pool = mp.Pool(processes=concurrent)
    pool.map(ts_ls_mask.mt_layover, iter_list)


def _create_timeseries(burst_gdf, project_file):

    # we need a
    dict_of_product_types = {'bs': 'Gamma0', 'coh': 'coh', 'pol': 'pol'}
    pols = ['VV', 'VH', 'HH', 'HV', 'Alpha', 'Entropy', 'Anisotropy']

    # read config file
    with open(project_file, 'r') as file:
        project_params = json.load(file)
        processing_dir = project_params['project']['processing_dir']

    # create iterable
    iter_list = []
    for burst in burst_gdf.bid.unique():

        burst_dir = Path(processing_dir).joinpath(burst)

        for pr, pol in itertools.product(dict_of_product_types.items(), pols):

            # unpack items
            product, product_name = list(pr)

            # take care of H-A-Alpha naming for file search
            if pol in ['Alpha', 'Entropy', 'Anisotropy'] and product is 'pol':
                list_of_files = sorted(
                    list(burst_dir.glob(f'20*/*data*/*{pol}*img')))
            else:
                # see if there is actually any imagery for this
                # combination of product and polarisation
                list_of_files = sorted(
                    list(burst_dir.glob(
                        f'20*/*data*/*{product_name}*{pol}*img')
                    )
                )

            if len(list_of_files) <= 1:
                continue

            # create list of dims if polarisation is present
            list_of_dims = sorted(list(burst_dir.glob(f'20*/*{product}*dim')))
            iter_list.append([list_of_dims, burst, product, pol, project_file])

    # parallelizing on all cpus
    concurrent = int(
        mp.cpu_count() / project_params['project']['cpus_per_process']
    )
    pool = mp.Pool(processes=concurrent)
    pool.map(ard_to_ts.ard_to_ts, iter_list)


def ards_to_timeseries(burst_gdf, project_file):

    print('--------------------------------------------------------------')
    logger.info('Processing all burst ARDs time-series')
    print('--------------------------------------------------------------')

    # load ard parameters
    with open(project_file, 'r') as ard_file:
        ard_params = json.load(ard_file)['processing_parameters']
        ard = ard_params['single_ARD']
        ard_mt = ard_params['time-series_ARD']


    # create all extents
    _create_extents(burst_gdf, project_file)

    # update extents in case of ls_mask
    if ard['create_ls_mask'] or ard_mt['apply_ls_mask']:
        _create_mt_ls_mask(burst_gdf, project_file)

    # finally create time-series
    _create_timeseries(burst_gdf, project_file)


# --------------------
# timescan part
# --------------------
def timeseries_to_timescan(burst_gdf, project_file):
    """Function to create a timescan out of a OST timeseries.

    """

    print('--------------------------------------------------------------')
    logger.info('Processing all burst ARDs time-series to ARD timescans')
    print('--------------------------------------------------------------')

    # -------------------------------------
    # 1 load project config
    with open(project_file, 'r') as ard_file:
        project_params = json.load(ard_file)
        processing_dir = project_params['project']['processing_dir']
        ard = project_params['processing_parameters']['single_ARD']
        ard_mt = project_params['processing_parameters']['time-series_ARD']
        ard_tscan = project_params['processing_parameters']['time-scan_ARD']

    # get the db scaling right
    if ard['to_db'] or ard_mt['to_db']:
        to_db = True

    # get datatype right
    dtype_conversion = True if ard_mt['dtype_output'] != 'float32' else False

    # -------------------------------------
    # 2 create iterable for parallel processing
    iter_list, vrt_iter_list = [], []
    for burst in burst_gdf.bid.unique():

        # get relevant directories
        burst_dir = Path(processing_dir).joinpath(burst)
        timescan_dir = burst_dir.joinpath('Timescan')
        timescan_dir.mkdir(parents=True, exist_ok=True)

        for product in PRODUCT_LIST:

            # check if already processed
            if timescan_dir.joinpath(f'.{product}.processed').exists():
                #logger.info(f'Timescans for burst {burst} already processed.')
                continue

            # get respective timeseries
            timeseries = burst_dir.joinpath(
                f'Timeseries/Timeseries.{product}.vrt'
            )

            # che if this timsereis exists ( since we go through all products
            if not timeseries.exists():
                continue

            # datelist for harmonics
            scenelist = list(burst_dir.glob(f'Timeseries/*{product}*tif'))
            datelist = [
                file.name.split('.')[1][:6] for file in sorted(scenelist)
            ]

            # define timescan prefix
            timescan_prefix = timescan_dir.joinpath(product)

            # get rescaling and db right (backscatter vs. coh/pol)
            if 'bs.' in str(timescan_prefix):
                to_power, rescale = to_db, dtype_conversion
            else:
                to_power, rescale = False, False

            iter_list.append(
                [timeseries, timescan_prefix, ard_tscan['metrics'],
                 rescale, to_power, ard_tscan['remove_outliers'], datelist]
            )

        vrt_iter_list.append([timescan_dir, project_file])

    concurrent = mp.cpu_count()
    pool = mp.Pool(processes=concurrent)
    pool.map(timescan.mt_metrics, iter_list)
    pool.map(ras.create_tscan_vrt, vrt_iter_list)


def mosaic_timeseries(burst_inventory, project_file):

    print(' -----------------------------------------------------------------')
    logger.info('Mosaicking time-series layers.')
    print(' -----------------------------------------------------------------')

    # -------------------------------------
    # 1 load project config
    with open(project_file, 'r') as ard_file:
        project_params = json.load(ard_file)
        processing_dir = project_params['project']['processing_dir']

    # create output folder
    ts_dir = Path(processing_dir).joinpath('Mosaic/Timeseries')
    ts_dir.mkdir(parents=True, exist_ok=True)

    # -------------------------------------
    # 2 create iterable
    # loop through each product
    iter_list, vrt_iter_list = [], []
    for product in PRODUCT_LIST:

        #
        bursts = burst_inventory.bid.unique()
        nr_of_ts = len(list(
            Path(processing_dir).glob(
                f'{bursts[0]}/Timeseries/*.{product}.tif'
            )
        ))

        # in case we only have one layer
        if not nr_of_ts > 1:
            continue

        outfiles = []
        for i in range(1, nr_of_ts + 1):

            # create the file list for files to mosaic
            filelist = list(Path(processing_dir).glob(
                f'*/Timeseries/{i:02d}.*{product}.tif'
            ))

            # assure that we do not inlcude potential Mosaics
            # from anterior runs
            filelist = [file for file in filelist if 'Mosaic' not in str(file)]

            logger.info(f'Creating timeseries mosaic {i} for {product}.')

            # create dates for timseries naming
            datelist = []
            for file in filelist:
                if '.coh.' in str(file):
                    datelist.append(
                        f"{file.name.split('.')[2]}_{file.name.split('.')[1]}"
                    )
                else:
                    datelist.append(file.name.split('.')[1])

            # get start and endate of mosaic
            start, end = sorted(datelist)[0], sorted(datelist)[-1]
            filelist = ' '.join([str(file) for file in filelist])

            # create namespace for output file
            if start == end:
                outfile = ts_dir.joinpath(
                              f'{i:02d}.{start}.{product}.tif'
                )

            else:
                outfile = ts_dir.joinpath(
                              f'{i:02d}.{start}-{end}.{product}.tif'
                )

            # create nmespace for check_file
            check_file = outfile.parent.joinpath(
                f'.{outfile.name[:-4]}.processed'
            )

            if os.path.isfile(check_file):
                print('INFO: Mosaic layer {} already'
                      ' processed.'.format(outfile))
                continue

            # append to list of outfile for vrt creation
            outfiles.append(outfile)
            iter_list.append([filelist, outfile, project_file])

        vrt_iter_list.append([ts_dir, product, outfiles])

    concurrent = mp.cpu_count()
    pool = mp.Pool(processes=concurrent)
    pool.map(mosaic.mosaic, iter_list)
    pool.map(mosaic.create_timeseries_mosaic_vrt, vrt_iter_list)


def mosaic_timescan(burst_inventory, project_file):

    print(' -----------------------------------------------------------------')
    logger.info('Mosaicking time-scan layers.')
    print(' -----------------------------------------------------------------')

    with open(project_file, 'r') as ard_file:
        project_params = json.load(ard_file)
        processing_dir = project_params['project']['processing_dir']
        metrics = project_params['processing']['time-scan_ARD']['metrics']

    if 'harmonics' in metrics:
        metrics.remove('harmonics')
        metrics.extend(['amplitude', 'phase', 'residuals'])

    if 'percentiles' in metrics:
        metrics.remove('percentiles')
        metrics.extend(['p95', 'p5'])

    tscan_dir = Path(processing_dir).joinpath('Mosaic/Timescan')
    tscan_dir.mkdir(parents=True, exist_ok=True)

    iter_list, outfiles = [], []
    for product, metric in itertools.product(PRODUCT_LIST, metrics):

        filelist = list(Path(processing_dir).glob(
            f'*/Timescan/*{product}.{metric}.tif'
        ))

        if not len(filelist) >= 1:
            continue

        filelist = ' '.join([str(file) for file in filelist])

        outfile = tscan_dir.joinpath(f'{product}.{metric}.tif')
        check_file = outfile.parent.joinpath(
            f'.{outfile.name[:-4]}.processed'
        )

        if check_file.exists():
            logger.info(f'Mosaic layer {outfile.name} already processed.')
            continue

        logger.info(f'Mosaicking layer {outfile.name}.')
        outfiles.append(outfile)
        iter_list.append([filelist, outfile, project_file])

    concurrent = mp.cpu_count()
    pool = mp.Pool(processes=concurrent)
    pool.map(mosaic.mosaic, iter_list)
    ras.create_tscan_vrt([tscan_dir, project_file])
