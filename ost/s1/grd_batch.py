#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""Batch processing for GRD products

"""

import os
import json
import itertools
import logging
import pandas as pd
from pathlib import Path

from godale._concurrent import Executor

from ost import Sentinel1Scene
from ost.s1 import grd_to_ard
from ost.helpers import raster as ras
from ost.generic import ts_extent
from ost.generic import ts_ls_mask
from ost.generic import ard_to_ts
from ost.generic import timescan
from ost.generic import mosaic

logger = logging.getLogger(__name__)


def _create_processing_dict(inventory_df):
    """Function that creates a dictionary to handle GRD batch processing

    This helper function takes the inventory dataframe and creates
    a dictionary with the track as key, and all the files to process as
    a list, whereas the list is

    :param inventory_df:
    :return:
    """

    # initialize empty dictionary
    dict_scenes = {}

    # get relative orbits and loop through each
    track_list = inventory_df["relativeorbit"].unique()

    for track in track_list:

        # get acquisition dates and loop through each
        acquisition_dates = inventory_df["acquisitiondate"][
            inventory_df["relativeorbit"] == track
        ].unique()

        # loop through dates
        for i, acquisition_date in enumerate(acquisition_dates):

            # get the scene ids per acquisition_date and write into a list
            single_id = inventory_df["identifier"][
                (inventory_df["relativeorbit"] == track)
                & (inventory_df["acquisitiondate"] == acquisition_date)
            ].tolist()

            # add this list to the dictionary and associate the track number
            # as dict key
            dict_scenes[f"{track}_{i+1}"] = single_id

    return dict_scenes


def create_processed_df(inventory_df, list_of_scenes, outfile, out_ls, error):

    df = pd.DataFrame(columns=["identifier", "outfile", "out_ls", "error"])

    for scene in list_of_scenes:

        temp_df = pd.DataFrame()
        # get scene_id
        temp_df["identifier"] = inventory_df.identifier[
            inventory_df.identifier == scene
        ].values
        # fill outfiles/error
        temp_df["outfile"] = outfile
        temp_df["out_ls"] = out_ls
        temp_df["error"] = error

        # append to final df and delete temp_df for next loop
        df = df.append(temp_df)
        del temp_df

    return df


def grd_to_ard_batch(inventory_df, config_file):

    # load relevant config parameters
    with open(config_file, "r") as file:
        config_dict = json.load(file)
        download_dir = Path(config_dict["download_dir"])
        data_mount = Path(config_dict["data_mount"])

    # where all frames are grouped into acquisitions
    processing_dict = _create_processing_dict(inventory_df)
    processing_df = pd.DataFrame(columns=["identifier", "outfile", "out_ls", "error"])

    iter_list = []
    for _, list_of_scenes in processing_dict.items():

        # get the paths to the file
        scene_paths = [
            Sentinel1Scene(scene).get_path(download_dir, data_mount)
            for scene in list_of_scenes
        ]

        iter_list.append(scene_paths)

    # now we run with godale, which works also with 1 worker
    executor = Executor(
        executor=config_dict["executor_type"], max_workers=config_dict["max_workers"]
    )

    for task in executor.as_completed(
        func=grd_to_ard.grd_to_ard,
        iterable=iter_list,
        fargs=(
            [
                str(config_file),
            ]
        ),
    ):

        list_of_scenes, outfile, out_ls, error = task.result()

        # return the info of processing as dataframe
        temp_df = create_processed_df(
            inventory_df, list_of_scenes, outfile, out_ls, error
        )

        processing_df = processing_df.append(temp_df)

    return processing_df


def ards_to_timeseries(inventory_df, config_file):

    with open(config_file) as file:
        config_dict = json.load(file)
        ard = config_dict["processing"]["single_ARD"]
        ard_mt = config_dict["processing"]["time-series_ARD"]

    # create all extents
    _create_extents(inventory_df, config_file)

    # update extents in case of ls_mask
    if ard["create_ls_mask"] or ard_mt["apply_ls_mask"]:
        _create_mt_ls_mask(inventory_df, config_file)

    # finally create time-series
    _create_timeseries(inventory_df, config_file)


def _create_extents(inventory_df, config_file):

    with open(config_file, "r") as file:
        config_dict = json.load(file)
        processing_dir = Path(config_dict["processing_dir"])

    iter_list = []
    for track in inventory_df.relativeorbit.unique():

        # get the burst directory
        track_dir = processing_dir / track

        list_of_extents = list(track_dir.glob("*/*/*bounds.json"))

        # if extent does not already exist, add to iterable
        if not (track_dir / f"{track}.min_bounds.json").exists():
            iter_list.append(list_of_extents)

    # now we run with godale, which works also with 1 worker
    executor = Executor(
        executor=config_dict["executor_type"], max_workers=os.cpu_count()
    )

    out_dict = {"track": [], "list_of_scenes": [], "extent": []}
    for task in executor.as_completed(
        func=ts_extent.mt_extent,
        iterable=iter_list,
        fargs=(
            [
                str(config_file),
            ]
        ),
    ):
        track, list_of_scenes, extent = task.result()
        out_dict["track"].append(track)
        out_dict["list_of_scenes"].append(list_of_scenes)
        out_dict["extent"].append(extent)

    return pd.DataFrame.from_dict(out_dict)


def _create_extents_old(inventory_df, config_file):

    with open(config_file, "r") as file:
        config_dict = json.load(file)
        processing_dir = Path(config_dict["processing_dir"])

    iter_list = []
    for track in inventory_df.relativeorbit.unique():

        # get the burst directory
        track_dir = processing_dir / track

        # get common burst extent
        list_of_scenes = list(track_dir.glob("**/*img"))

        list_of_scenes = [str(x) for x in list_of_scenes if "layover" not in str(x)]

        # if extent does not already exist, add to iterable
        if not (track_dir / f"{track}.extent.gpkg").exists():
            iter_list.append(list_of_scenes)

    # now we run with godale, which works also with 1 worker
    executor = Executor(
        executor=config_dict["executor_type"], max_workers=config_dict["max_workers"]
    )

    out_dict = {"track": [], "list_of_scenes": [], "extent": []}
    for task in executor.as_completed(
        func=ts_extent.mt_extent,
        iterable=iter_list,
        fargs=(
            [
                str(config_file),
            ]
        ),
    ):
        track, list_of_scenes, extent = task.result()
        out_dict["track"].append(track)
        out_dict["list_of_scenes"].append(list_of_scenes)
        out_dict["extent"].append(extent)

    return pd.DataFrame.from_dict(out_dict)


def _create_mt_ls_mask(inventory_df, config_file):
    """Helper function to union the Layover/Shadow masks of a Time-series

    This function creates a

    :param inventory_df:
    :param config_file:
    :return:
    """
    with open(config_file, "r") as file:
        config_dict = json.load(file)
        processing_dir = Path(config_dict["processing_dir"])

    iter_list = []
    for track in inventory_df.relativeorbit.unique():

        # get the burst directory
        track_dir = processing_dir / track

        # get common burst extent
        list_of_masks = list(track_dir.glob("*/*/*_ls_mask.json"))

        # if extent does not already exist, add to iterable
        if not (track_dir / f"{track}.ls_mask.json").exists():
            iter_list.append(list_of_masks)

    # now we run with godale, which works also with 1 worker
    executor = Executor(
        executor=config_dict["executor_type"], max_workers=os.cpu_count()
    )

    for task in executor.as_completed(func=ts_ls_mask.mt_layover, iterable=iter_list):
        task.result()


def _create_mt_ls_mask_old(inventory_df, config_file):

    with open(config_file, "r") as file:
        config_dict = json.load(file)
        processing_dir = Path(config_dict["processing_dir"])

    iter_list = []
    for track in inventory_df.relativeorbit.unique():

        # get the burst directory
        track_dir = processing_dir / track

        # get common burst extent
        list_of_scenes = list(track_dir.glob("**/*img"))

        list_of_layover = [str(x) for x in list_of_scenes if "layover" in str(x)]

        iter_list.append(list_of_layover)

    # now we run with godale, which works also with 1 worker
    executor = Executor(
        executor=config_dict["executor_type"], max_workers=config_dict["max_workers"]
    )

    out_dict = {"track": [], "list_of_layover": [], "ls_mask": [], "ls_extent": []}
    for task in executor.as_completed(
        func=ts_ls_mask.mt_layover,
        iterable=iter_list,
        fargs=(
            [
                str(config_file),
            ]
        ),
    ):
        track, list_of_layover, ls_mask, ls_extent = task.result()
        out_dict["track"].append(track)
        out_dict["list_of_layover"].append(list_of_layover)
        out_dict["ls_mask"].append(list_of_layover)
        out_dict["ls_extent"].append(ls_extent)

    return pd.DataFrame.from_dict(out_dict)


def _create_timeseries(inventory_df, config_file):
    """Helper function to create Timeseries out of OST ARD products

    Based on the inventory GeoDataFrame and the configuration file,
    this function triggers the time-series processing for all bursts/tracks
    within the respective project. Each product/polarisation is treated
    singularly.

    Based on the ARD type/configuration settings, the function uses
    SNAP's Create-Stack function to unify the grid of each scene and
    applies a multi-temporal speckle filter if selected.

    The output are single GeoTiff files, whereas there is the possibility to
    reduce the data by converting the data format into uint8 or uint16.
    This is done by linearly stretching the data between -30 and +5
    for backscatter, 0 and 1 for coherence, polarimetric anisotropy #
    and entropy, as well 0 and 90 for polarimetric alpha channel. All
    the data is cropped to the same extent based on the minimum bounds layer.

    This function executes the underlying functions using the godale framework
    for parallel execution. Executor type and number of parallel processes is
    defined within the configuration file.


    :param inventory_df:
    :type GeoDataFrame
    :param config_file:
    :type str/Path
    :return:
    """
    with open(config_file, "r") as file:
        config_dict = json.load(file)
        processing_dir = Path(config_dict["processing_dir"])

    iter_list = []
    for track in inventory_df.relativeorbit.unique():

        # get the burst directory
        track_dir = processing_dir / track

        for pol in ["VV", "VH", "HH", "HV"]:

            # see if there is actually any imagery in thi polarisation
            list_of_files = sorted(
                str(file) for file in list(track_dir.glob(f"20*/*data*/*ma0*{pol}*img"))
            )

            if len(list_of_files) <= 1:
                continue

            # create list of dims if polarisation is present
            list_of_dims = sorted(
                str(dim) for dim in list(track_dir.glob("20*/*bs*dim"))
            )

            iter_list.append([list_of_dims, track, "bs", pol])

    executor = Executor(
        executor=config_dict["executor_type"], max_workers=config_dict["max_workers"]
    )

    out_dict = {
        "track": [],
        "list_of_dims": [],
        "out_files": [],
        "out_vrt": [],
        "product": [],
        "error": [],
    }
    for task in executor.as_completed(
        func=ard_to_ts.gd_ard_to_ts,
        iterable=iter_list,
        fargs=(
            [
                str(config_file),
            ]
        ),
    ):
        track, list_of_dims, out_files, out_vrt, product, error = task.result()
        out_dict["track"].append(track)
        out_dict["list_of_dims"].append(list_of_dims)
        out_dict["out_files"].append(out_files)
        out_dict["out_vrt"].append(out_vrt)
        out_dict["product"].append(product)
        out_dict["error"].append(error)

    return pd.DataFrame.from_dict(out_dict)


def timeseries_to_timescan(inventory_df, config_file):

    # load ard parameters
    with open(config_file, "r") as file:
        config_dict = json.load(file)
        processing_dir = Path(config_dict["processing_dir"])
        ard = config_dict["processing"]["single_ARD"]
        ard_mt = config_dict["processing"]["time-series_ARD"]
        ard_tscan = config_dict["processing"]["time-scan_ARD"]

    # get the db scaling right
    to_db = ard["to_db"]
    if ard["to_db"] or ard_mt["to_db"]:
        to_db = True

    dtype_conversion = True if ard_mt["dtype_output"] != "float32" else False

    iter_list, vrt_iter_list = [], []
    for track in inventory_df.relativeorbit.unique():

        # get track directory
        track_dir = processing_dir / track
        # define and create Timescan directory
        timescan_dir = track_dir / "Timescan"
        timescan_dir.mkdir(parents=True, exist_ok=True)

        # loop thorugh each polarization
        for polar in ["VV", "VH", "HH", "HV"]:

            if (timescan_dir / f".bs.{polar}.processed").exists():
                logger.info(f"Timescans for track {track} already processed.")
                continue

            # get timeseries vrt
            time_series = track_dir / "Timeseries" / f"Timeseries.bs.{polar}.vrt"

            if not time_series.exists():
                continue

            # create a datelist for harmonics
            scene_list = list(track_dir.glob(f"Timeseries/*bs.{polar}.tif"))

            # create a datelist for harmonics calculation
            datelist = []
            for file in sorted(scene_list):
                datelist.append(file.name.split(".")[1])

            # define timescan prefix
            timescan_prefix = timescan_dir / f"bs.{polar}"

            iter_list.append(
                [
                    time_series,
                    timescan_prefix,
                    ard_tscan["metrics"],
                    dtype_conversion,
                    to_db,
                    ard_tscan["remove_outliers"],
                    datelist,
                ]
            )

        vrt_iter_list.append(timescan_dir)

    # now we run with godale, which works also with 1 worker
    executor = Executor(
        executor=config_dict["executor_type"], max_workers=config_dict["max_workers"]
    )

    # run timescan creation
    out_dict = {"track": [], "prefix": [], "metrics": [], "error": []}
    for task in executor.as_completed(func=timescan.gd_mt_metrics, iterable=iter_list):
        burst, prefix, metrics, error = task.result()
        out_dict["track"].append(burst)
        out_dict["prefix"].append(prefix)
        out_dict["metrics"].append(metrics)
        out_dict["error"].append(error)

    timescan_df = pd.DataFrame.from_dict(out_dict)

    # run vrt creation
    for task in executor.as_completed(
        func=ras.create_tscan_vrt,
        iterable=vrt_iter_list,
        fargs=(
            [
                str(config_file),
            ]
        ),
    ):
        task.result()

    return timescan_df


def mosaic_timeseries(inventory_df, config_file):

    print(" -----------------------------------")
    logger.info("Mosaicking Time-series layers")
    print(" -----------------------------------")

    # -------------------------------------
    # 1 load project config
    with open(config_file, "r") as ard_file:
        config_dict = json.load(ard_file)
        processing_dir = Path(config_dict["processing_dir"])

    # create output folder
    ts_dir = processing_dir / "Mosaic" / "Timeseries"
    ts_dir.mkdir(parents=True, exist_ok=True)

    # loop through polarisations
    iter_list, vrt_iter_list = [], []
    for p in ["VV", "VH", "HH", "HV"]:

        tracks = inventory_df.relativeorbit.unique()
        nr_of_ts = len(
            list((processing_dir / f"{tracks[0]}" / "Timeseries").glob(f"*.{p}.tif"))
        )

        if not nr_of_ts >= 1:
            continue

        outfiles = []
        for i in range(1, nr_of_ts + 1):

            filelist = list(processing_dir.glob(f"*/Timeseries/{i:02d}.*.{p}.tif"))
            filelist = [str(file) for file in filelist if "Mosaic" not in str(file)]

            # create
            datelist = []
            for file in filelist:
                datelist.append(Path(file).name.split(".")[1])

            filelist = " ".join(filelist)
            start, end = sorted(datelist)[0], sorted(datelist)[-1]

            if start == end:
                outfile = ts_dir / f"{i:02d}.{start}.bs.{p}.tif"
            else:
                outfile = ts_dir / f"{i:02d}.{start}-{end}.bs.{p}.tif"

            check_file = outfile.parent / f".{outfile.stem}.processed"

            outfiles.append(outfile)

            if check_file.exists():
                logger.info(f"Mosaic layer {outfile.name} already processed.")
                continue

            logger.info(f"Mosaicking layer {outfile.name}.")
            iter_list.append([filelist, outfile, config_file])

        vrt_iter_list.append([ts_dir, p, outfiles])

    # now we run with godale, which works also with 1 worker
    executor = Executor(
        executor=config_dict["executor_type"], max_workers=config_dict["max_workers"]
    )

    # run mosaicking
    for task in executor.as_completed(func=mosaic.gd_mosaic, iterable=iter_list):
        task.result()

    # run mosaicking vrts
    for task in executor.as_completed(
        func=mosaic.create_timeseries_mosaic_vrt, iterable=vrt_iter_list
    ):
        task.result()


def mosaic_timescan(config_file):

    # load ard parameters
    with open(config_file, "r") as ard_file:
        config_dict = json.load(ard_file)
        processing_dir = Path(config_dict["processing_dir"])
        metrics = config_dict["processing"]["time-scan_ARD"]["metrics"]

    if "harmonics" in metrics:
        metrics.remove("harmonics")
        metrics.extend(["amplitude", "phase", "residuals"])

    if "percentiles" in metrics:
        metrics.remove("percentiles")
        metrics.extend(["p95", "p5"])

    # create out directory of not existent
    tscan_dir = processing_dir / "Mosaic" / "Timescan"
    tscan_dir.mkdir(parents=True, exist_ok=True)

    # loop through all pontial proucts
    iter_list = []
    for polar, metric in itertools.product(["VV", "HH", "VH", "HV"], metrics):

        # create a list of files based on polarisation and metric
        filelist = list(processing_dir.glob(f"*/Timescan/*bs.{polar}.{metric}.tif"))

        # break loop if there are no files
        if not len(filelist) >= 2:
            continue

        # get number
        filelist = " ".join([str(file) for file in filelist])
        outfile = tscan_dir / f"bs.{polar}.{metric}.tif"
        check_file = outfile.parent / f".{outfile.stem}.processed"

        if check_file.exists():
            logger.info(f"Mosaic layer {outfile.name} already processed.")
            continue

        iter_list.append([filelist, outfile, config_file])

    # now we run with godale, which works also with 1 worker
    executor = Executor(
        executor=config_dict["executor_type"], max_workers=config_dict["max_workers"]
    )

    # run mosaicking
    for task in executor.as_completed(func=mosaic.gd_mosaic, iterable=iter_list):
        task.result()

    ras.create_tscan_vrt(tscan_dir, config_file)
