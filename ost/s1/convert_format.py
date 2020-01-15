import os
import glob
import logging
import numpy as np
from os.path import join as opj
from tempfile import TemporaryDirectory

import rasterio
from rasterio.merge import merge
from rasterio.enums import Resampling

from godale._concurrent import Executor

from ost.helpers import raster as ras
from ost.settings import GTIFF_OST_PROFILE


logger = logging.getLogger(__name__)


def execute_burst_to_tif(dim_file, out_path, driver='GTiff', to_db=False):
    i, dim_file = dim_file
    out_tif = opj(out_path, str(i)+'.tif')
    prefix = glob.glob(os.path.abspath(dim_file[:-4]) + '*data')[0]
    if len(glob.glob(opj(prefix, '*VV*.img'))) == 1:
        co_pol = glob.glob(opj(prefix, '*VV*.img'))[0]
    if len(glob.glob(opj(prefix, '*VH*.img'))) == 1:
        cross_pol = glob.glob(opj(prefix, '*VH*.img'))[0]
    if len(glob.glob(opj(prefix, '*HH*.img'))) == 1:
        co_pol = glob.glob(opj(prefix, '*HH*.img'))[0]

    if len(glob.glob(opj(prefix, '*HV*.img'))) == 1:
        cross_pol = glob.glob(opj(prefix, '*HV*.img'))[0]
    with rasterio.open(co_pol) as co, rasterio.open(cross_pol) as cr:
        out_profile = co.meta
        out_profile.update(driver=driver,
                           count=3,
                           nodata=0.0,
                           compress='Deflate',
                           dtype='float32'
                           )
        with rasterio.open(out_tif, 'w', **out_profile) as dst:
            if co.shape != cr.shape:
                logger.debug('dimensions do not match')
            # read arrays and turn to dB (in case it isn't)
            co_array = co.read(
                resampling=Resampling.cubic_spline
            ).astype(np.float32)
            cr_array = cr.read(
                resampling=Resampling.cubic_spline
            ).astype(np.float32)
            if to_db:
                # turn to db
                co_array = ras.convert_to_db(co_array)
                cr_array = ras.convert_to_db(cr_array)
                # adjust for dbconversion
                co_array[co_array == -130] = 0
                cr_array[cr_array == -130] = 0
            # turn 0s to nan
            co_array[co_array == 0] = 0.
            cr_array[cr_array == 0] = 0.

            border_mask = ras.np_binary_erosion(
                co_array,
            ).astype(np.bool)

            co_array = np.where(border_mask, co_array, 0)
            cr_array = np.where(border_mask, cr_array, 0)

            # create log ratio by subtracting the dbs
            ratio_array = np.subtract(co_array, cr_array)
            # write file
            for k, arr in [(1, co_array), (2, cr_array),
                           (3, ratio_array)]:
                dst.write(arr[0, ], indexes=k)
    return out_tif


def ard_slc_to_rgb(
        file_list,
        outfile,
        process_bounds,
        driver='GTiff',
        to_db=False
):
    out_tifs = []
    max_workers = os.cpu_count()
    # Index files
    for i, f in zip(range(len(file_list)), file_list):
        file_list[i] = (i, f)
    with TemporaryDirectory() as temp:
        executor_type = 'concurrent_processes'
        executor = Executor(executor=executor_type, max_workers=max_workers)
        for task in executor.as_completed(
                func=execute_burst_to_tif,
                iterable=file_list,
                fargs=(temp,
                       driver,
                       to_db
                       )
        ):
            tif_file = task.result()
            out_tifs.append(rasterio.open(tif_file))
    arr, out_trans = merge(
        out_tifs,
        nodata=out_tifs[0].nodata,
        bounds=process_bounds
    )
    width = arr.shape[2]
    height = arr.shape[1]
    blockxsize = GTIFF_OST_PROFILE["blockxsize"]
    blockysize = GTIFF_OST_PROFILE["blockysize"]
    if width < 256 or height < 256:
        blockxsize = 64
        blockysize = 64
    profile = out_tifs[0].profile
    profile.update(
        GTIFF_OST_PROFILE,
        width=width,
        height=height,
        transform=out_trans,
        blockxsize=blockxsize,
        blockysize=blockysize,
        count=3
    )
    arr = np.where(arr == out_tifs[0].nodata, 0, arr)
    with rasterio.open(outfile, "w", **profile) as dst:
        dst.write(arr)
    return outfile


def ard_to_rgb(
        infile,
        outfile,
        driver='GTiff',
        to_db=True
):
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
    with rasterio.open(co_pol) as co,  rasterio.open(cross_pol) as cr:
        # get meta data
        meta = co.meta
        # update meta
        meta.update(driver=driver, count=3, nodata=0, compress='Deflate')
        # !assure that dimensions match ####
        with rasterio.open(outfile, 'w', **meta) as dst:
            if co.shape != cr.shape:
                logger.debug('dimensions do not match')
            # loop through blocks
            for i, window in co.block_windows(1):
                from rasterio.enums import Resampling
                # read arrays and turn to dB (in case it isn't)
                co_array = co.read(window=window, resampling=Resampling.cubic_spline)
                cr_array = cr.read(window=window, resampling=Resampling.cubic_spline)
                if to_db:
                    # turn to db
                    co_array = ras.convert_to_db(co_array)
                    cr_array = ras.convert_to_db(cr_array)

                    # adjust for dbconversion
                    co_array[co_array == -130] = 0
                    cr_array[cr_array == -130] = 0

                # turn 0s to nan
                co_array[co_array == 0] = 0
                cr_array[cr_array == 0] = 0
                # create log ratio by subtracting the dbs
                ratio_array = np.subtract(co_array, cr_array)
                # write file
                for k, arr in [(1, co_array), (2, cr_array),
                               (3, ratio_array)]:
                    dst.write(arr[0, ], indexes=k, window=window)


def ard_slc_to_thumbnail(
        infile,
        outfile,
        driver='JPEG',
        shrink_factor=25
):
    with rasterio.open(infile) as in_tif:
        out_profile = in_tif.meta
        out_profile.update(driver=driver, count=3, dtype='uint8', nodata=0)
        new_height = int(in_tif.height/shrink_factor)
        new_width = int(in_tif.width/shrink_factor)
        out_shape = (in_tif.count, new_height, new_width)
        out_profile.update(height=new_height, width=new_width)
        with rasterio.open(outfile, 'w', **out_profile) as out_tif:
            out_arr = in_tif.read(out_shape=out_shape, resampling=5)
            out_arr[out_arr == 0] = 0
            r = ras.scale_to_int(out_arr[0], -20, 0, 'uint8')
            g = ras.scale_to_int(out_arr[1], -25, -5, 'uint8')
            b = ras.scale_to_int(out_arr[2], 1, 15, 'uint8')
            out_tif.write(np.stack([r, g, b]))


def ard_to_thumbnail(
        infile,
        outfile,
        driver='JPEG',
        shrink_factor=25,
        to_db=True
):
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
    with rasterio.open(co_pol) as co, rasterio.open(cross_pol) as cr:
        # get meta data
        meta = co.meta
        # update meta
        meta.update(driver=driver, count=3, dtype='uint8')
        # !!!assure that dimensions match ####
        new_height = int(co.height/shrink_factor)
        new_width = int(co.width/shrink_factor)
        out_shape = (co.count, new_height, new_width)

        meta.update(height=new_height, width=new_width)

        if co.shape != cr.shape:
            logger.debug('dimensions do not match')

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