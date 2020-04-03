# -*- coding: utf-8 -*-
# import stdlib modules
import time
import shutil
import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import gdal
import rasterio
import numpy as np

from ost.helpers import helpers as h, raster as ras, vector as vec

logger = logging.getLogger(__name__)

def mt_layover(list_of_args):
    """

    :param list_of_args:
    :return:
    """

    # extract args from list
    filelist, outfile, temp_dir, extent, update_extent = list_of_args

    # get the start time for Info on processing time
    start = time.time()

    with TemporaryDirectory(prefix=f'{temp_dir}/') as temp:

        # temp to Path object
        temp = Path(temp)

        # create path to temp file
        ls_layer = temp.joinpath(Path(outfile).name)

        # create a vrt-stack out of
        logger.info('Creating common Layover/Shadow Mask')
        vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
        gdal.BuildVRT(
            str(temp.joinpath('ls.vrt')), filelist, options=vrt_options
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
                    arr = arr_max / arr_max

                    out_min.write(np.uint8(arr), window=window, indexes=1)

        ras.mask_by_shape(ls_layer, outfile, extent, to_db=False,
                          datatype='uint8', rescale=False, ndv=0)

        ls_layer.unlink()
        h.timer(start)

        if update_extent:

            # get some info
            burst_dir = Path(outfile).parent
            burst = burst_dir.name
            extent = burst_dir.joinpath(f'{burst}.extent.gpkg')

            logger.info(
                'Calculating symetrical difference of extent and ls_mask'
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
                    extent, f'{str(outfile)[:-4]}.gpkg', extent_ls_masked
                )
            except:
                shutil.copy(extent, extent_ls_masked)
