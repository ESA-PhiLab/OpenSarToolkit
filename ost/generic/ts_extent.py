#! /usr/bin/env python
# -*- coding: utf-8 -*-

import json
import shutil
import logging
from pathlib import Path
from tempfile import TemporaryDirectory

try:
    import gdal
except ModuleNotFoundError as e:
    from osgeo import gdal
except ModuleNotFoundError as e:
    raise e

import geopandas as gpd
from retrying import retry

from ost.helpers import raster as ras, vector as vec

logger = logging.getLogger(__name__)


@retry(stop_max_attempt_number=3, wait_fixed=1)
def mt_extent_old(list_of_scenes, config_file):

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
            try:
                vec.aoi_intersection(aoi, exterior, out_file)
            except ValueError:
                shutil.move(exterior, out_file)
        else:
            shutil.move(exterior, out_file)

    return target_dir.name, list_of_scenes, out_file


@retry(stop_max_attempt_number=3, wait_fixed=1)
def mt_extent(list_of_extents, config_file):

    with open(config_file) as file:
        config_dict = json.load(file)

    import warnings
    warnings.filterwarnings('ignore', 'GeoSeries.isna', UserWarning)

    # get track/burst dir from first scene
    target_dir = list_of_extents[0].parent.parent.parent
    out_file = target_dir.joinpath(f'{target_dir.name}.min_bounds.json')

    logger.info(f'Creating common image bounds for track {target_dir.name}.')

    for i, file in enumerate(list_of_extents):

        if i == 0:
            df1 = gpd.read_file(file)
            df1 = df1[~(df1.geometry.is_empty | df1.geometry.isna())]
        elif i > 0:
            # read filter out invalid geometries
            df2 = gpd.read_file(file)
            df2 = df2[~(df2.geometry.is_empty | df2.geometry.isna())]

            # do intersect
            df1 = gpd.overlay(
                df1, df2, how='intersection'
            )[['raster_val_1', 'geometry']]

            # rename columns
            df1.columns = ['raster_val', 'geometry']
            # remove empty or non geometries
            df1 = df1[~(df1.geometry.is_empty | df1.geometry.isna())]
        else:
            raise RuntimeError('No extents found.')

    if config_dict['processing']['mosaic']['cut_to_aoi']:

        try:
            aoi_df = vec.wkt_to_gdf(config_dict['aoi'])
            df = gpd.overlay(aoi_df, df1, how='intersection')
            df.to_file(out_file, driver='GPKG')
        except ValueError as e:
            df1.to_file(out_file, driver='GeoJSON')
    else:
        df1.to_file(out_file, driver='GeoJSON')

    return target_dir.name, list_of_extents, out_file
