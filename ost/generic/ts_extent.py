#! /usr/bin/env python
# -*- coding: utf-8 -*-

import json
import gdal
import logging
from pathlib import Path
from tempfile import TemporaryDirectory

from ost.helpers import raster as ras, vector as vec

logger = logging.getLogger(__name__)


def mt_extent(list_of_scenes, config_file):

    with open(config_file) as file:
        config_dict = json.load(file)
        temp_dir = Path(config_dict['temp_dir'])
        aoi = config_dict['aoi']

    # get track/burst dir from first scene
    target_dir = Path(list_of_scenes[0]).parent.parent.parent
    out_file = target_dir.joinpath(f'{target_dir.name}.extent.gpkg')

    logger.info(f'Creating common extent mask for track {target_dir.name}.')
    # get out directory
    out_dir = out_file.parent

    temp_extent = out_dir.joinpath('extent.vrt')
    # build vrt stack from all scenes
    gdal.BuildVRT(
        str(temp_extent),
        list_of_scenes,
        options=gdal.BuildVRTOptions(srcNodata=0, separate=True)
    )

    with TemporaryDirectory(prefix=f'{temp_dir}/') as temp:

        # create namespace for temp file
        temp = Path(temp)
        image_bounds = temp.joinpath(out_file.name)
        exterior = temp.joinpath(out_file.name + '_ext')

        # create outline
        ras.outline(temp_extent, image_bounds, 0, False)

        # create exterior ring and write out
        vec.exterior(image_bounds, exterior, -0.0018)

        # intersect with aoi
        if config_dict['processing']['mosaic']['cut_to_aoi']:
            vec.aoi_intersection(aoi, exterior, out_file)
        else:
            exterior.rename(out_file)

    return target_dir.name, list_of_scenes, out_file
