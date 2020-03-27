# -*- coding: utf-8 -*-
import os
from os.path import join as opj

from pathlib import Path
import glob
import json
import datetime
import logging
import gdal

from ost.generic.common_wrappers import create_stack, mt_speckle_filter
from ost.helpers import raster as ras, helpers as h

logger = logging.getLogger(__name__)


def ard_to_ts(list_of_files, processing_dir, temp_dir,
              burst, proc_file, product, pol, ncores=os.cpu_count()):
    """

    :param list_of_files:
    :param processing_dir:
    :param temp_dir:
    :param burst:
    :param proc_file:
    :param product:
    :param pol:
    :param ncores:
    :return:
    """

    if type(list_of_files) == str:
        list_of_files = list_of_files.replace("'", '').strip('][').split(', ')

    # get the burst directory
    burst_dir = processing_dir.joinpath(burst)

    # get timeseries directory and create if non existent
    ts_dir = burst_dir.joinpath('Timeseries')
    Path.mkdir(ts_dir, parents=True, exist_ok=True)

    # in case some processing has been done before, check if already processed
    check_file = ts_dir.joinpath('.{}.{}.processed'.format(product, pol))
    if Path.exists(check_file):
        logger.info('Timeseries of {} for {} in {} polarisation already'
                    ' processed'.format(burst, product, pol))
        return

    # load ard parameters
    with open(proc_file, 'r') as ard_file:
        ard_params = json.load(ard_file)['processing_parameters']
        ard = ard_params['single_ARD']
        ard_mt = ard_params['time-series_ARD']
        if ard_mt['remove_mt_speckle'] is True:
            ard_mt_speck = ard_params['time-series_ARD']['mt_speckle_filter']

    # get the db scaling right
    to_db = ard['to_db']
    if to_db or product != 'bs':
        to_db = False
        logger.info('Not converting to dB for {}'.format(product))
    else:
        to_db = ard_mt['to_db']
        logger.info('Converting to dB for {}'.format(product))

    if ard['apply_ls_mask']:
        extent = burst_dir.joinpath('{}.extent.masked.shp'.format(burst))
    else:
        extent = burst_dir.joinpath('{}.extent.shp'.format(burst))

    # min max dict for stretching in case of 16 or 8 bit datatype
    mm_dict = {'bs': {'min': -30, 'max': 5},
               'coh': {'min': 0.000001, 'max': 1},
               'Alpha': {'min': 0.000001, 'max': 90},
               'Anisotropy': {'min': 0.000001, 'max': 1},
               'Entropy': {'min': 0.000001, 'max': 1}
               }

    stretch = pol if pol in ['Alpha', 'Anisotropy', 'Entropy'] else product

    # define out_dir for stacking routine
    out_dir = ts_dir

    # create namespaces
    temp_stack = temp_dir.joinpath(f'{burst}_{product}_{pol}_mt')
    out_stack = temp_dir.joinpath(f'{burst}_{product}_{pol}_mt')
    stack_log = out_dir.joinpath(f'{burst}_{product}_{pol}_stack.err_log')

    # run stacking routines
    # convert list of files readable for snap
    list_of_files = '\'{}\''.format(','.join(list_of_files))

    if pol in ['Alpha', 'Anisotropy', 'Entropy']:
        logger.info(
            'Creating multi-temporal stack of images of burst/track {} for'
            ' the {} band of the polarimetric H-A-Alpha'
            ' decomposition.'.format(burst, pol))
        create_stack(list_of_files, temp_stack, stack_log, pattern=pol)
    else:
        logger.info(
            'Creating multi-temporal stack of images of burst/track {} for'
            ' {} product in {} polarization.'.format(burst, product, pol))
        create_stack(list_of_files, temp_stack, stack_log, polarisation=pol)

    # run mt speckle filter
    if ard_mt['remove_mt_speckle'] is True:
        speckle_log = opj(out_dir, '{}_{}_{}_mt_speckle.err_log'.format(
            burst, product, pol))

        logger.info('Applying multi-temporal speckle filter')
        mt_speckle_filter('{}.dim'.format(temp_stack),
                          out_stack, speckle_log, speckle_dict=ard_mt_speck,
                          ncores=ncores)
        # remove tmp files
        h.delete_dimap(temp_stack)
    else:
        out_stack = temp_stack

    if product == 'coh':

        # get slave and master Date
        mstDates = [datetime.datetime.strptime(
            os.path.basename(x).split('_')[3].split('.')[0],
            '%d%b%Y') for x in glob.glob(
            opj('{}.data'.format(out_stack), '*img'))]

        slvDates = [datetime.datetime.strptime(
            os.path.basename(x).split('_')[4].split('.')[0],
            '%d%b%Y') for x in glob.glob(
            opj('{}.data'.format(out_stack), '*img'))]
        # sort them
        mstDates.sort()
        slvDates.sort()
        # write them back to string for following loop
        sortedMstDates = [datetime.datetime.strftime(
            ts, "%d%b%Y") for ts in mstDates]
        sortedSlvDates = [datetime.datetime.strftime(
            ts, "%d%b%Y") for ts in slvDates]

        i, outfiles = 1, []
        for mst, slv in zip(sortedMstDates, sortedSlvDates):
            inMst = datetime.datetime.strptime(mst, '%d%b%Y')
            inSlv = datetime.datetime.strptime(slv, '%d%b%Y')

            outMst = datetime.datetime.strftime(inMst, '%y%m%d')
            outSlv = datetime.datetime.strftime(inSlv, '%y%m%d')
            infile = glob.glob(opj('{}.data'.format(out_stack),
                                   '*{}*{}_{}*img'.format(pol, mst, slv)))[0]

            outfile = opj(out_dir, '{:02d}.{}.{}.{}.{}.tif'.format(
                i, outMst, outSlv, product, pol))

            ras.mask_by_shape(infile, outfile, extent,
                              to_db=to_db,
                              datatype=ard_mt['dtype_output'],
                              min_value=mm_dict[stretch]['min'],
                              max_value=mm_dict[stretch]['max'],
                              ndv=0.0,
                              description=True)
            # add ot a list for subsequent vrt creation
            outfiles.append(outfile)
            i += 1


    else:
        # get the dates of the files
        dates = [datetime.datetime.strptime(x.split('_')[-1][:-4], '%d%b%Y')
                 for x in glob.glob(opj('{}.data'.format(out_stack), '*img'))]
        # sort them
        dates.sort()
        # write them back to string for following loop
        sortedDates = [datetime.datetime.strftime(ts, "%d%b%Y")
                       for ts in dates]

        i, outfiles = 1, []
        for date in sortedDates:
            # restructure date to YYMMDD
            inDate = datetime.datetime.strptime(date, '%d%b%Y')
            outDate = datetime.datetime.strftime(inDate, '%y%m%d')

            infile = glob.glob(opj('{}.data'.format(out_stack),
                                   '*{}*{}*img'.format(pol, date)))[0]

            # create outfile
            outfile = opj(out_dir, '{:02d}.{}.{}.{}.tif'.format(
                i, outDate, product, pol))

            ras.mask_by_shape(infile, outfile, extent,
                              to_db=to_db,
                              datatype=ard_mt['dtype_output'],
                              min_value=mm_dict[stretch]['min'],
                              max_value=mm_dict[stretch]['max'],
                              ndv=0.0)

            # add ot a list for subsequent vrt creation
            outfiles.append(outfile)
            i += 1

    for file in outfiles:
        return_code = h.check_out_tiff(file)
        if return_code != 0:
            h.remove_folder_content(temp_dir)
            os.remove(file)
            return return_code

    # write file, so we know this ts has been succesfully processed
    if return_code == 0:
        with open(str(check_file), 'w') as file:
            file.write('passed all tests \n')

    # build vrt of timeseries
    vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
    gdal.BuildVRT(opj(out_dir, 'Timeseries.{}.{}.vrt'.format(product, pol)),
                  outfiles,
                  options=vrt_options)

    # remove tmp files
    h.delete_dimap(out_stack)
