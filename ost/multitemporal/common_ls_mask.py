# -*- coding: utf-8 -*-
# import stdlib modules
import os
from os.path import join as opj
import time

import gdal
import rasterio
import numpy as np

from ost.helpers import helpers as h, raster as ras, vector as vec


def mt_layover(filelist, outfile, temp_dir, extent, update_extent=False):
    '''
    This function is usally used in the time-series workflow of OST. A list
    of the filepaths layover/shadow masks

    :param filelist - list of files
    :param out_dir - directory where the output file will be stored
    :return path to the multi-temporal layover/shadow mask file generated
    '''
    if type(filelist) == str:
        filelist = filelist.replace("'", '').strip('][').split(', ')
    if type(update_extent) == str:
        if update_extent == 'False':
            update_extent = False
    # get some info
    burst_dir = os.path.dirname(outfile)
    burst = os.path.basename(burst_dir)
    extent = opj(burst_dir, '{}.extent.shp'.format(burst))
    
    # get the start time for Info on processing time
    start = time.time()
    # create path to out file
    ls_layer = opj(temp_dir, os.path.basename(outfile))

    # create a vrt-stack out of
    print(' INFO: Creating common Layover/Shadow Mask')
    vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
    gdal.BuildVRT(opj(temp_dir, 'ls.vrt'), filelist, options=vrt_options)

    with rasterio.open(opj(temp_dir, 'ls.vrt')) as src:

        # get metadata
        meta = src.meta
        # update driver and reduced band count
        meta.update(driver='GTiff', count=1, dtype='uint8')

        # create outfiles
        with rasterio.open(ls_layer, 'w', **meta) as out_min:

            # loop through blocks
            for _, window in src.block_windows(1):

                # read array with all bands
                stack = src.read(range(1, src.count + 1), window=window)

                # get stats
                arr_max = np.nanmax(stack, axis=0)
                arr = arr_max / arr_max

                out_min.write(np.uint8(arr), window=window, indexes=1)

    ras.mask_by_shape(ls_layer, outfile, extent, to_db=False,
                      datatype='uint8', rescale=False, ndv=0)
    os.remove(ls_layer)
    h.timer(start)

    if update_extent:
        print(' INFO: Calculating symetrical difference of extent and ls_mask')
        # polygonize the multi-temporal ls mask
        ras.polygonize_raster(outfile, '{}.shp'.format(outfile[:-4]))
        
        # create file for masked extent
        extent_ls_masked = opj(burst_dir, '{}.extent.masked.shp'.format(burst))
        
        # calculate difference between burst exntetn and ls mask, fr masked extent
        vec.difference(extent, '{}.shp'.format(outfile[:-4]), extent_ls_masked)
                