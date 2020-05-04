#! /usr/bin/env python
# -*- coding: utf-8 -*-

import json
import shutil
import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import gdal
import rasterio
import numpy as np
from retrying import retry

from ost.helpers import raster as ras, vector as vec

logger = logging.getLogger(__name__)

@retry(stop_max_attempt_number=3, wait_fixed=1)
def mt_layover(list_of_files, config_file):
    """

    :param list_of_files:
    :param config_file:
    :return:
    """

    # this is a godale thing
    with open(config_file) as file:
        config_dict = json.load(file)
        temp_dir = Path(config_dict['temp_dir'])
        update_extent = (
            config_dict['processing']['time-series_ARD']['apply_ls_mask']
        )

    target_dir = Path(list_of_files[0]).parent.parent.parent
    outfile = target_dir.joinpath(f'{target_dir.name}.ls_mask.tif')
    extent = target_dir.joinpath(f'{target_dir.name}.extent.gpkg')
    burst_dir = Path(outfile).parent
    burst = burst_dir.name

    logger.info(
        f'Creating common Layover/Shadow mask for track {target_dir.name}.'
    )

    with TemporaryDirectory(prefix=f'{temp_dir}/') as temp:

        # temp to Path object
        temp = Path(temp)

        # create path to temp file
        ls_layer = temp.joinpath(Path(outfile).name)

        # create a vrt-stack out of
        vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
        gdal.BuildVRT(
            str(temp.joinpath('ls.vrt')),
            list_of_files,
            options=vrt_options
        )

        with rasterio.open(temp.joinpath('ls.vrt')) as src:

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
                    arr = np.divide(arr_max, arr_max)

                    out_min.write(np.uint8(arr), window=window, indexes=1)

        ras.mask_by_shape(
            ls_layer, outfile, extent, to_db=False,
            datatype='uint8', rescale=False, ndv=0
        )

        ls_layer.unlink()

        extent_ls_masked = None
        if update_extent:

            logger.info(
                'Calculating symmetrical difference of extent and ls_mask'
            )

            # polygonize the multi-temporal ls mask
            ras.polygonize_raster(outfile, f'{str(outfile)[:-4]}.gpkg')

            # create file for masked extent
            extent_ls_masked = burst_dir.joinpath(
                f'{burst}.extent.masked.gpkg'
            )

            # calculate difference between burst extent
            # and ls mask, for masked extent
            try:
                vec.difference(
                    extent, f'{outfile.stem}.gpkg', extent_ls_masked
                )
            except:
                shutil.copy(extent, extent_ls_masked)

    return burst_dir, list_of_files, outfile, extent_ls_masked
