#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""Functions for searching and downloading from Copernicus scihub.
"""

import logging
from pathlib import Path
import warnings
import requests
import tqdm
import geopandas as gpd
from godale._concurrent import Executor

from ost.helpers import vector as vec
from ost.helpers.settings import OST_ROOT

logger = logging.getLogger(__name__)


def download_copdem_tile(tile_id):

    # path to snaps aux folder
    # snap_aux = Path.home() / "bucket" / "snap_aux" / "auxdata" / "dem" / "Copernicus 30m Global DEM"
    snap_aux = Path.home() / ".snap" / "auxdata" / "dem" / "Copernicus 30m Global DEM"

    if not snap_aux.exists():
        try:
            snap_aux.mkdir(parents=True, exist_ok=True)
        except Exception:
            raise RuntimeError(" Snap aux folder not found")

    # construct url
    url = f"https://copernicus-dem-30m.s3.amazonaws.com/{tile_id}/{tile_id}.tif"

    # construct outfile
    filename = snap_aux / f"{tile_id}.tif"

    # get first response for file Size
    response = requests.get(url, stream=True)

    # get download size
    total_length = int(response.headers.get("content-length", 0))

    # define chunk_size
    chunk_size = 1024

    # check if file is partially downloaded
    first_byte = filename.stat().st_size if filename.exists() else 0

    if first_byte >= total_length:
        return

    while first_byte < total_length:

        # get byte offset for already downloaded file
        header = {"Range": f"bytes={first_byte}-{total_length}"}

        # logger.info(f'Downloading scene to: {filename.name}')
        response = requests.get(url, headers=header, stream=True)

        # actual download
        with open(filename, "ab") as file:

            pbar = tqdm.tqdm(
                total=total_length,
                initial=first_byte,
                unit="B",
                unit_scale=True,
                desc=" INFO: Downloading: ",
            )
            for chunk in response.iter_content(chunk_size):
                if chunk:
                    file.write(chunk)
                    pbar.update(chunk_size)
        pbar.close()

        # update first_byte
        first_byte = filename.stat().st_size


def download_copdem(aoi):
    warnings.filterwarnings("ignore", "Geometry is in a geographic CRS", UserWarning)

    copdem = gpd.read_file(OST_ROOT / "auxdata" / "copdem30tiles.gpkg")
    copdem["tile_id"] = copdem["id"]
    aoi_gdf = vec.wkt_to_gdf(aoi)
    aoi_gdf["geometry"] = aoi_gdf.geometry.buffer(1)
    overlap_df = gpd.overlay(copdem, aoi_gdf, how="intersection")

    iter_list = []
    for file in overlap_df["tile_id"].values:
        iter_list.append(file)
        # download_copdem_tile(file)### fro debugging

    # now we run with godale, which works also with 1 worker
    executor = Executor(executor="concurrent_processes", max_workers=10)

    for task in executor.as_completed(func=download_copdem_tile, iterable=iter_list):
        task.result()
