#! /usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import logging
import warnings
from pathlib import Path

import numpy as np
import geopandas as gpd
from shapely.wkt import loads

from ost.helpers import scihub, vector as vec
from ost.s1.s1scene import Sentinel1Scene as S1Scene


logger = logging.getLogger(__name__)


def burst_extract(scene_id, track, acq_date, et_root):
    """Extract all bursts from the Sentinel-1 annotation files

    :param scene_id:
    :param track:
    :param acq_date:
    :param et_root:
    :return:
    """

    # define columns for burst gdf and create empty gdf
    column_names = [
        "SceneID",
        "Track",
        "Date",
        "SwathID",
        "AnxTime",
        "BurstNr",
        "geometry",
    ]
    gdf = gpd.GeoDataFrame(columns=column_names)

    # pol = root.find('adsHeader').find('polarisation').text
    swath = et_root.find("adsHeader").find("swath").text
    burst_lines = np.int(et_root.find("swathTiming").find("linesPerBurst").text)

    burst_samples = np.int(et_root.find("swathTiming").find("samplesPerBurst").text)

    list_of_bursts = et_root.find("swathTiming").find("burstList")
    geolocation_grid = et_root.find("geolocationGrid")[0]

    first, last = {}, {}

    # Get burst corner geolocation info
    for geo_point in geolocation_grid:

        if geo_point.find("pixel").text == "0":
            first[geo_point.find("line").text] = np.float32(
                [geo_point.find("latitude").text, geo_point.find("longitude").text]
            )

        elif geo_point.find("pixel").text == str(burst_samples - 1):
            last[geo_point.find("line").text] = np.float32(
                [geo_point.find("latitude").text, geo_point.find("longitude").text]
            )

    for i, b in enumerate(list_of_bursts):

        firstline = str(i * burst_lines)
        lastline = str((i + 1) * burst_lines)

        azi_anx_time = np.float32(b.find("azimuthAnxTime").text)
        orbit_time = 12 * 24 * 60 * 60 / 175

        if azi_anx_time > orbit_time:
            azi_anx_time = np.mod(azi_anx_time, orbit_time)

        azi_anx_time = np.int32(np.round(azi_anx_time * 10))

        # first and lastline sometimes shifts by 1 for some reason?
        try:
            firstthis = first[firstline]
        except KeyError:
            firstline = str(int(firstline) - 1)
            try:
                firstthis = first[firstline]
            except KeyError:
                print("First line not found in annotation file")
                firstthis = []
        try:
            lastthis = last[lastline]
        except KeyError:
            lastline = str(int(lastline) - 1)
            try:
                lastthis = last[lastline]
            except KeyError:
                print("Last line not found in annotation file")
                lastthis = []

        corners = np.zeros([4, 2], dtype=np.float32)

        # Had missing info for 1 burst in a file, hence the check
        if len(firstthis) > 0 and len(lastthis) > 0:
            corners[0] = first[firstline]
            corners[1] = last[firstline]
            corners[3] = first[lastline]
            corners[2] = last[lastline]

        wkt = "POLYGON (({} {},{} {},{} {},{} {},{} {}))".format(
            np.around(float(corners[0, 1]), 3),
            np.around(float(corners[0, 0]), 3),
            np.around(float(corners[3, 1]), 3),
            np.around(float(corners[3, 0]), 3),
            np.around(float(corners[2, 1]), 3),
            np.around(float(corners[2, 0]), 3),
            np.around(float(corners[1, 1]), 3),
            np.around(float(corners[1, 0]), 3),
            np.around(float(corners[0, 1]), 3),
            np.around(float(corners[0, 0]), 3),
        )

        geo_dict = {
            "SceneID": scene_id,
            "Track": track,
            "Date": acq_date,
            "SwathID": swath,
            "AnxTime": azi_anx_time,
            "BurstNr": i + 1,
            "geometry": loads(wkt),
        }

        gdf = gdf.append(geo_dict, ignore_index=True)

    return gdf


def burst_inventory(
    inventory_df,
    outfile,
    download_dir=os.getenv("HOME"),
    data_mount=None,
    uname=None,
    pword=None,
):
    """Creates a Burst GeoDataFrame from an OST inventory file
    Args:
    Returns:
    """
    # create column names for empty data frame
    column_names = [
        "SceneID",
        "Track",
        "Direction",
        "Date",
        "SwathID",
        "AnxTime",
        "BurstNr",
        "geometry",
    ]

    # crs for empty dataframe
    crs = "epsg:4326"
    # create empty dataframe
    gdf_full = gpd.GeoDataFrame(columns=column_names, crs=crs)

    for scene_id in inventory_df.identifier:

        # read into S1scene class
        scene = S1Scene(scene_id)

        logger.info("Getting burst info from {}.".format(scene.scene_id))

        # get orbit direction
        orbit_direction = inventory_df[
            inventory_df.identifier == scene_id
        ].orbitdirection.values[0]

        file_path = scene.get_path(download_dir, data_mount)
        if not file_path:

            logger.info(
                "Retrieving burst info from scihub" " (need to download xml files)"
            )
            if not uname and not pword:
                uname, pword = scihub.ask_credentials()

            opener = scihub.connect(uname=uname, pword=pword)
            if scene.scihub_online_status(opener) is False:
                raise RuntimeError(
                    "Product needs to be downloaded or online "
                    "to create a burst database."
                    "Download the product first and "
                    "do the burst inventory with the local data."
                )
            else:
                single_gdf = scene.scihub_annotation_get(uname, pword)

        elif file_path.suffix == ".zip":
            single_gdf = scene.zip_annotation_get(download_dir, data_mount)

        elif file_path.suffix == ".SAFE":
            single_gdf = scene.safe_annotation_get(download_dir, data_mount)

        else:
            raise RuntimeError(
                "Burst inventory failed because of unavailability of data. "
                "Make sure to download all scenes first."
            )

        # add orbit direction
        single_gdf["Direction"] = orbit_direction

        # append
        gdf_full = gdf_full.append(single_gdf, sort=True)

    gdf_full = gdf_full.reset_index(drop=True)

    for i in gdf_full["AnxTime"].unique():

        # get similar burst times
        idx = (
            gdf_full.index[
                (gdf_full.AnxTime >= i - 6)
                & (gdf_full.AnxTime <= i + 6)
                & (gdf_full.AnxTime != i)
            ]
            .unique()
            .values
        )

        # reset all to first value
        for j in idx:
            gdf_full.loc[j, "AnxTime"] = i

    # create the actual burst id
    gdf_full["bid"] = (
        gdf_full.Direction.str[0]
        + gdf_full.Track.astype(str)
        + "_"
        + gdf_full.SwathID.astype(str)
        + "_"
        + gdf_full.AnxTime.astype(str)
    )

    # save file to out
    gdf_full.to_file(outfile, driver="GPKG")

    return gdf_full


def refine_burst_inventory(aoi, burst_gdf, outfile, coverages=None):
    """Creates a Burst GeoDataFrame from an OST inventory file
    Args:
    Returns:
    """

    warnings.filterwarnings("ignore", "Geometry is in a geographic CRS", UserWarning)

    # turn aoi into a geodataframe
    aoi_gdf = gpd.GeoDataFrame(vec.wkt_to_gdf(aoi).buffer(0.05))
    aoi_gdf.columns = ["geometry"]
    aoi_gdf.crs = "epsg:4326"

    # get columns of input dataframe for later return function
    cols = burst_gdf.columns

    # 1) get only intersecting footprints (double, since we do this before)
    burst_gdf = gpd.sjoin(burst_gdf, aoi_gdf, how="inner", op="intersects")

    # if aoi  gdf has an id field we need to rename the changed id_left field
    if "id_left" in burst_gdf.columns:
        # rename id_left to id
        burst_gdf.columns = ["id" if x == "id_left" else x for x in burst_gdf.columns]

    # remove duplicates
    burst_gdf.drop_duplicates(["SceneID", "Date", "bid"], inplace=True)

    # check if number of bursts align with number of coverages
    if coverages:
        for burst in burst_gdf.bid.unique():
            if len(burst_gdf[burst_gdf.bid == burst]) != coverages:
                logging.info(f"Removing burst {burst} because of unsuffcient coverage.")

                burst_gdf.drop(burst_gdf[burst_gdf.bid == burst].index, inplace=True)

    # save file to out
    burst_gdf.to_file(outfile, driver="GPKG")
    return burst_gdf[cols]


def prepare_burst_inventory(burst_gdf, config_file):

    cols = [
        "AnxTime",
        "BurstNr",
        "Date",
        "Direction",
        "SceneID",
        "SwathID",
        "Track",
        "geometry",
        "bid",
        "master_prefix",
        "out_directory",
        "slave_date",
        "slave_scene_id",
        "slave_file",
        "slave_burst_nr",
        "slave_prefix",
    ]

    # create empty geodataframe
    proc_burst_gdf = gpd.GeoDataFrame(
        columns=cols, geometry="geometry", crs="epsg:4326"
    )

    # load relevant config parameters
    with open(config_file, "r") as file:
        config_dict = json.load(file)
        processing_dir = Path(config_dict["processing_dir"])
        download_dir = Path(config_dict["download_dir"])
        data_mount = Path(config_dict["data_mount"])

    # loop through burst_gdf and add slave infos
    for burst in burst_gdf.bid.unique():

        # create a list of dates over which we loop
        dates = burst_gdf.Date[burst_gdf.bid == burst].sort_values().tolist()

        # loop through dates
        for idx, date in enumerate(dates):

            # get master date
            burst_row = burst_gdf[
                (burst_gdf.Date == date) & (burst_gdf.bid == burst)
            ].copy()

            # get parameters for master
            master_scene = S1Scene(burst_row.SceneID.values[0])
            burst_row["file_location"] = master_scene.get_path(download_dir, data_mount)
            burst_row["master_prefix"] = f"{date}_{burst_row.bid.values[0]}"
            burst_row["out_directory"] = processing_dir / burst / date

            # try to get slave date
            try:
                # get slave date and add column to burst row
                slave_date = dates[idx + 1]
                burst_row["slave_date"] = slave_date

                # read slave burst line
                slave_burst = burst_gdf[
                    (burst_gdf.Date == slave_date) & (burst_gdf.bid == burst)
                ]

                # get scene id and add into master row
                slave_scene_id = S1Scene(slave_burst.SceneID.values[0])
                burst_row["slave_scene_id"] = slave_scene_id.scene_id

                # get path to slave file
                burst_row["slave_file"] = slave_scene_id.get_path(
                    download_dir, data_mount
                )

                # burst number in slave file (subswath is same)
                burst_row["slave_burst_nr"] = slave_burst.BurstNr.values[0]

                # outfile name
                burst_row["slave_prefix"] = f"{slave_date}_{slave_burst.bid.values[0]}"

            except IndexError:
                burst_row["slave_date"], burst_row["slave_scene_id"] = None, None
                burst_row["slave_file"], burst_row["slave_burst_nr"] = None, None
                burst_row["slave_prefix"] = None

            proc_burst_gdf = proc_burst_gdf.append(burst_row, sort=False)

    return proc_burst_gdf
