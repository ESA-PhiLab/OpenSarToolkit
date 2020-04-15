#! /usr/bin/env python
# -*- coding: utf-8 -*-

import time
import gdal
from pathlib import Path
from tempfile import TemporaryDirectory

from ost.helpers import helpers as h, raster as ras, vector as vec


def mt_extent(list_of_args):

    # extract list
    list_of_scenes, out_file, temp_dir, buffer = list_of_args

    # get out directory
    out_dir = out_file.parent

    # build vrt stack from all scenes
    vrt_options = gdal.BuildVRTOptions(srcNodata=0, separate=True)
    gdal.BuildVRT(
        str(out_dir.joinpath('extent.vrt')), list_of_scenes,
        options=vrt_options
    )

    # start timer
    start = time.time()

    with TemporaryDirectory(prefix=f'{temp_dir}/') as temp:

        # create namespace for temp file
        outline_file = Path(temp).joinpath(out_file.name)

        # create outline
        ras.outline(out_dir.joinpath('extent.vrt'), outline_file, 0, False)

        # create exterior ring and write out
        vec.exterior(outline_file, out_file, buffer)

    out_dir.joinpath('extent.vrt').unlink()
    h.timer(start)
