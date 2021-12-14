#! /usr/bin/env python
# -*- coding: utf-8 -*-

import json
import shutil
import logging
from pathlib import Path

import geopandas as gpd
from shapely.ops import unary_union
from retrying import retry

from ost.helpers import vector as vec

logger = logging.getLogger(__name__)


@retry(stop_max_attempt_number=3, wait_fixed=1)
def mt_layover(list_of_ls):

    import warnings

    warnings.filterwarnings("ignore", "GeoSeries.isna", UserWarning)

    target_dir = Path(list_of_ls[0]).parent.parent.parent
    bounds = target_dir / f"{target_dir.name}.min_bounds.json"
    outfile = target_dir / f"{target_dir.name}.ls_mask.json"
    valid_file = target_dir / f"{target_dir.name}.valid.json"

    logger.info(f"Creating common Layover/Shadow mask for track {target_dir.name}.")

    y = 0
    for i, file in enumerate(list_of_ls):

        if y == 0:
            df1 = gpd.read_file(file)
            df1 = df1[~(df1.geometry.is_empty | df1.geometry.isna())]
            if len(df1) > 0:
                geom = df1.geometry.buffer(0).unary_union
                y = 1

        if y > 0:

            df2 = gpd.read_file(file)
            df2 = df2[~(df2.geometry.is_empty | df2.geometry.isna())]
            if not df2.empty:
                geom2 = df2.geometry.buffer(0).unary_union
                geom = unary_union([geom, geom2])

    if y > 0:
        # make geometry valid in case it isn't
        geom = geom.buffer(0)

        # remove slivers
        buffer = 0.00001
        geom = (
            geom.buffer(-buffer, 1, join_style=2)
            .buffer(buffer, 1, cap_style=1, join_style=2)
            .__geo_interface__
        )

        # write to output
        with open(outfile, "w") as file:
            json.dump(geom, file)

        # create difference file for valid data shape
        vec.difference(bounds, outfile, valid_file)
    else:
        shutil.copy(bounds, valid_file)
