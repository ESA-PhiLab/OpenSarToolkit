#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import glob
import logging
import rasterio
import numpy as np
from pathlib import Path
from tempfile import TemporaryDirectory

from os.path import join as opj

from ost.generic import common_wrappers as common
from ost.helpers import helpers as h, raster as ras
from ost.helpers.errors import GPTRuntimeError, NotValidFileError
from ost.s1 import grd_wrappers as grd
from ost.s1.s1scene import Sentinel1Scene


logger = logging.getLogger(__name__)


def grd_to_ard(filelist, config_file):
    """Main function for the grd to ard generation

    This function represents the full workflow for the generation of an
    Analysis-Ready-Data product. The standard parameters reflect the CEOS
    ARD defintion for Sentinel-1 backcsatter products.

    By changing the parameters, taking care of all parameters
    that can be given. The function can handle multiple inputs of the same
    acquisition, given that there are consecutive data takes.

    :param filelist: must be a list with one or more
                     absolute paths to GRD scene(s)
    :param config_file:
    :return:
    """

    # load relevant config parameters
    with open(config_file, 'r') as file:
        config_dict = json.load(file)
        ard = config_dict['processing']['single_ARD']
        processing_dir = Path(config_dict['processing_dir'])
        subset = config_dict['subset']

    # construct output directory and file name
    first = Sentinel1Scene(Path(filelist[0]).stem)
    acquisition_date = first.start_date
    track = first.rel_orbit
    out_dir = processing_dir.joinpath(f'{track}/{acquisition_date}')
    file_id = f'{acquisition_date}_{track}'

    with TemporaryDirectory(prefix=f"{config_dict['temp_dir']}/") as temp:

        # convert temp directory to Path object
        temp = Path(temp)

        # ---------------------------------------------------------------------
        # 1 Import
        # slice assembly if more than one scene
        if len(filelist) > 1:

            # if more than one frame import all files
            for file in filelist:

                # create namespace for temporary imported product
                grd_import = temp.joinpath(f'{file.stem}_imported')

                # create namespace for import log
                logfile = out_dir.joinpath(f'{file.stem}.Import.errLog')

                # import frame
                try:
                    grd.grd_frame_import(
                        file, grd_import, logfile, config_dict
                    )
                except GPTRuntimeError as error:
                    logger.info(error)
                    return None, None, error

            # create list of scenes for full acquisition in
            # preparation of slice assembly
            scenelist = ' '.join(
                [str(file) for file in list(temp.glob('*imported.dim'))]
            )

            # create namespace for temporary slice assembled import product
            grd_import = temp.joinpath(f'{file_id}_imported')

            # create namespace for slice assembled log
            logfile = out_dir.joinpath(f'{file_id}._slice_assembly.errLog')

            # run slice assembly
            try:
                grd.slice_assembly(scenelist, grd_import, logfile)
            except (GPTRuntimeError, NotValidFileError) as error:
                logger.info(error)
                return None, None, error

            # delete imported frames
            for file in filelist:
                h.delete_dimap(temp.joinpath(f'{file.stem}_imported'))
        
            # subset mode after slice assembly
            if subset:

                # create namespace for temporary subset product
                grd_subset = temp.joinpath(f'{file_id}_imported_subset')

                # create namespace for subset log
                logfile = out_dir.joinpath(f'{file_id}._slice_assembly.errLog')

                # run subset routine
                try:
                    grd.grd_subset_georegion(
                        grd_import.with_suffix('.dim'), grd_subset, logfile,
                        subset
                    )
                except (GPTRuntimeError, NotValidFileError) as error:
                    logger.info(error)
                    return None, None, error

                # delete slice assembly input to subset
                h.delete_dimap(grd_import)

                # set subset to import for subsequent functions
                grd_import = grd_subset

        # single scene case
        else:
            # create namespace for temporary imported product
            grd_import = temp.joinpath(f'{file_id}_imported')

            # create namespace for import log
            logfile = out_dir.joinpath(f'{file_id}.Import.errLog')

            # run frame import
            try:
                grd.grd_frame_import(
                    filelist[0], grd_import, logfile, config_dict
                )
            except (GPTRuntimeError, NotValidFileError) as error:
                logger.info(error)
                return None, None, error

        # set input for next step
        infile = grd_import.with_suffix('.dim')

        # ---------------------------------------------------------------------
        # 2 GRD Border Noise
        if ard['remove_border_noise'] and not subset:

            # loop through possible polarisations
            for polarisation in ['VV', 'VH', 'HH', 'HV']:

                # get input file
                file = list(temp.glob(
                    f'{file_id}_imported*data/Intensity_{polarisation}.img'
                ))[0]

                # remove border noise
                if file.exists():
                    # run grd Border Remove
                    grd.grd_remove_border(file)

        # ---------------------------------------------------------------------
        # 3 Calibration

        # create namespace for temporary calibrated product
        calibrated = temp.joinpath(f'{file_id}_cal')

        # create namespace for calibration log
        logfile = out_dir.joinpath(f'{file_id}.calibration.errLog')

        # run calibration
        try:
            grd.calibration(infile, calibrated, logfile, config_dict)
        except (GPTRuntimeError, NotValidFileError) as error:
            logger.info(error)
            return None, None, error

        # delete input
        h.delete_dimap(infile[:-4])

        # input for next step
        infile = calibrated.with_suffix('.dim')

        # ---------------------------------------------------------------------
        # 4 Multi-looking
        if int(ard['resolution']) >= 20:

            # create namespace for temporary multi-looked product
            multi_looked = temp.joinpath(f'{file_id}_ml')

            # create namespace for multi-loook log
            logfile = out_dir.joinpath(f'{file_id}.multilook.errLog')

            # run multi-looking
            try:
                grd.multi_look(infile, multi_looked, logfile, config_dict)
            except (GPTRuntimeError, NotValidFileError) as error:
                logger.info(error)
                return None, None, error

            # delete input
            h.delete_dimap(infile[:-4])

            # define input for next step
            infile = multi_looked.with_suffix('.dim')

        # ---------------------------------------------------------------------
        # 5 Layover shadow mask
        out_ls = None   # set to none for final return statement
        if ard['create_ls_mask'] is True:

            # create namespace for temporary ls mask product
            ls_mask = temp.joinpath(f'{file_id}_ls_mask')

            # create namespace for ls mask log
            logfile = out_dir.joinpath(f'{file_id}.ls_mask.errLog')

            # run ls mask routine
            try:
                out_ls = common.ls_mask(infile, ls_mask, logfile, config_dict)
            except (GPTRuntimeError, NotValidFileError) as error:
                logger.info(error)
                return None, None, error

            # move to final destination
            out_ls_mask = out_dir.joinpath(f'{file_id}.LS')
            h.move_dimap(ls_mask, out_ls_mask)

        # ---------------------------------------------------------------------
        # 6 Speckle filtering
        if ard['remove_speckle']:

            # create namespace for temporary speckle filtered product
            filtered = temp.joinpath('{file_id}_spk')

            # create namespace for speckle filter log
            logfile = out_dir.joinpath(f'{file_id}.Speckle.errLog')

            # run speckle filter
            try:
                common.speckle_filter(infile, filtered, logfile, config_dict)
            except (GPTRuntimeError, NotValidFileError) as error:
                logger.info(error)
                return None, None, error

            # delete input
            h.delete_dimap(infile[:-4])

            # define input for next step
            infile = filtered.with_suffix('.dim')

        # ---------------------------------------------------------------------
        # 7 Terrain flattening
        if ard['product_type'] == 'RTC-gamma0':

            # create namespace for temporary terrain flattened product
            flattened = temp.joinpath(f'{file_id}_flat')

            # create namespace for terrain flattening log
            logfile = out_dir.joinpath(f'{file_id}.tf.errLog')

            # run terrain flattening
            try:
                common.terrain_flattening(
                    infile, flattened, logfile, config_dict
                )
            except (GPTRuntimeError, NotValidFileError) as error:
                logger.info(error)
                return None, None, error

            # delete input file
            h.delete_dimap(infile[:-4])

            # define input for next step
            infile = flattened.with_suffix('.dim')

        # ---------------------------------------------------------------------
        # 8 Linear to db
        if ard['to_db']:

            # create namespace for temporary db scaled product
            db_scaled = temp.joinpath(f'{file_id}_db')

            # create namespace for db scaled log
            logfile = out_dir.joinpath(f'{file_id}.db.errLog')

            # run db scaling routine
            try:
                common.linear_to_db(infile, db_scaled, logfile, config_dict)
            except (GPTRuntimeError, NotValidFileError) as error:
                logger.info(error)
                return None, None, error

            # delete input file
            h.delete_dimap(infile[:-4])

            # set input for next step
            infile = db_scaled.with_suffix('.dim')

        # ---------------------------------------------------------------------
        # 9 Geocoding

        # create namespace for temporary geocoded product
        geocoded = temp.joinpath(f'{file_id}_bs')

        # create namespace for geocoding log
        logfile = out_dir.joinpath(f'{file_id}_bs.errLog')

        # run geocoding
        try:
            common.terrain_correction(infile, geocoded, logfile, config_dict)
        except (GPTRuntimeError, NotValidFileError) as error:
            logger.info(error)
            return None, None, error

        # delete input file
        h.delete_dimap(infile[:-4])

        # define final destination
        out_final = out_dir.joinpath(f'{file_id}.bs')

        # ---------------------------------------------------------------------
        # 10 Move to output directory
        h.move_dimap(geocoded, out_final)

        # write processed file to keep track of files already processed
        with open(out_dir.joinpath('.processed'), 'w') as file:
            file.write('passed all tests \n')

        return out_final.with_suffix('.bs.dim'), out_ls, None


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
