#! /usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging

import geopandas as gpd
from retrying import retry

from ost.helpers import vector as vec

logger = logging.getLogger(__name__)


@retry(stop_max_attempt_number=3, wait_fixed=1)
def mt_extent(list_of_extents, config_file):

    with open(config_file) as file:
        config_dict = json.load(file)

    import warnings

    warnings.filterwarnings("ignore", "GeoSeries.isna", UserWarning)

    # get track/burst dir from first scene
    target_dir = list_of_extents[0].parent.parent.parent
    out_file = target_dir / f"{target_dir.name}.min_bounds.json"

    logger.info(f"Creating common image bounds for track {target_dir.name}.")

    for i, file in enumerate(list_of_extents):

        if i == 0:
            df1 = gpd.read_file(file)
            df1 = df1[~(df1.geometry.is_empty | df1.geometry.isna())]
        elif i > 0:
            # read filter out invalid geometries
            df2 = gpd.read_file(file)
            df2 = df2[~(df2.geometry.is_empty | df2.geometry.isna())]

            # do intersect
            df1 = gpd.overlay(df1, df2, how="intersection")[
                ["raster_val_1", "geometry"]
            ]

            # rename columns
            df1.columns = ["raster_val", "geometry"]
            # remove empty or non geometries
            df1 = df1[~(df1.geometry.is_empty | df1.geometry.isna())]
        else:
            raise RuntimeError("No extents found.")

    if config_dict["processing"]["mosaic"]["cut_to_aoi"]:

        try:
            aoi_df = vec.wkt_to_gdf(config_dict["aoi"])
            df = gpd.overlay(aoi_df, df1, how="intersection")
            df.to_file(out_file, driver="GPKG")
        except ValueError:
            df1.to_file(out_file, driver="GeoJSON")
    else:
        df1.to_file(out_file, driver="GeoJSON")

    return target_dir.name, list_of_extents, out_file
