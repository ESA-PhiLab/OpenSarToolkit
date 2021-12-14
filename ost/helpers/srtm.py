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

from ost.helpers import helpers as h
from ost.helpers import vector as vec
from ost.helpers.settings import OST_ROOT


logger = logging.getLogger(__name__)


def download_srtm_tile(url):

    snap_aux = Path.home() / ".snap" / "auxdata" / "dem" / "SRTM 1Sec HGT"

    if not snap_aux.exists():
        try:
            snap_aux.mkdir(parents=True, exist_ok=True)
        except Exception:
            raise RuntimeError(" Snap aux folder not found")

    filename = snap_aux / url.split("/")[-1]

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

    zip_test = 1
    while zip_test is not None:

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

        # zipFile check
        logger.info(f"Checking zip archive {filename.name} for inconsistency")
        zip_test = h.check_zipfile(filename)
        if zip_test is not None:
            logger.info(
                f"{filename.name} did not pass the zip test. Re-downloading "
                f"the full scene."
            )
            filename.unlink()
            first_byte = 0
        # otherwise we change the status to True
        else:
            logger.info(f"{filename.name} passed the zip test.")


def download_srtm(aoi):

    warnings.filterwarnings("ignore", "Geometry is in a geographic CRS", UserWarning)

    srtm = gpd.read_file(OST_ROOT / "auxdata" / "srtm1sectiles.gpkg")

    aoi_gdf = vec.wkt_to_gdf(aoi)
    aoi_gdf["geometry"] = aoi_gdf.geometry.buffer(1)
    overlap_df = gpd.overlay(srtm, aoi_gdf, how="intersection")

    iter_list = []
    for file in overlap_df.url.values:
        iter_list.append(file)

    # now we run with godale, which works also with 1 worker
    executor = Executor(executor="concurrent_processes", max_workers=10)

    for task in executor.as_completed(func=download_srtm_tile, iterable=iter_list):
        task.result()
